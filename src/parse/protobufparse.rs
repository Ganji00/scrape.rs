// Copyright 2026 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Delimited protobuf parser, a port of
//! [`protobufparse.go`](https://github.com/prometheus/prometheus/blob/v0.315.1/model/textparse/protobufparse.go).

use std::borrow::Cow;
use std::sync::LazyLock;

use prost::Message;

use super::float::{FloatBuf, format_openmetrics_float};
use super::{
    BUCKET_LABEL, CounterResetHint, Entry, Histogram, HistogramRef, LabelsBuffer,
    METRIC_NAME_LABEL, MetricType, ParseError, Parser, QUANTILE_LABEL, Span,
};
use crate::dto;

/// Go's `binary.MaxVarintLen32`, the longest length prefix Go accepts.
const MAX_VARINT_LEN_32: usize = 5;

static EMPTY_METRIC: LazyLock<dto::Metric> = LazyLock::new(dto::Metric::default);
static EMPTY_DTO_HISTOGRAM: LazyLock<dto::Histogram> = LazyLock::new(dto::Histogram::default);

/// Parser for varint length-delimited `MetricFamily` messages (`FmtProtoDelim`).
///
/// Mirrors Go's `textparse.NewProtobufParser` with NHCB conversion and type and unit labels
/// turned off.
pub struct ProtobufParser<'a> {
    input: &'a [u8],
    /// Offset of the next length prefix in `input`.
    pos: usize,
    family_offset: usize,
    family: dto::MetricFamily,
    metric_index: usize,
    next_metric_index: usize,
    ignore_native_histograms: bool,
    parse_classic_histograms: bool,
    /// `None` is Go's `EntryInvalid`: the next call decodes the next metric family.
    state: Option<Entry>,
    /// Go's `fieldPos`: -2 is `_count`, -1 is `_sum`, otherwise the quantile or bucket index.
    field_pos: isize,
    fields_done: bool,
    redo_classic: bool,
    exemplar_pos: usize,
    exemplar_returned: bool,
    /// Metric name plus the `_count`, `_sum`, or `_bucket` suffix of the current series.
    series_name: String,
    /// The `quantile` or `le` label of the current series.
    magic_label: Option<(&'static str, f64)>,
    entry_bytes: Vec<u8>,
    int_histogram: Histogram<u64, i64>,
    float_histogram: Histogram<f64, f64>,
    is_float_histogram: bool,
}

impl<'a> ProtobufParser<'a> {
    /// Creates a parser over `input`. With `ignore_native_histograms`, histograms are only
    /// returned as classic series. With `parse_classic_histograms`, a histogram that has both
    /// native and classic buckets is returned in both forms.
    pub fn new(
        input: &'a [u8],
        ignore_native_histograms: bool,
        parse_classic_histograms: bool,
    ) -> Self {
        Self {
            input,
            pos: 0,
            family_offset: 0,
            family: dto::MetricFamily::default(),
            metric_index: 0,
            next_metric_index: 0,
            ignore_native_histograms,
            parse_classic_histograms,
            state: None,
            field_pos: -2,
            fields_done: false,
            redo_classic: false,
            exemplar_pos: 0,
            exemplar_returned: false,
            series_name: String::new(),
            magic_label: None,
            entry_bytes: Vec::new(),
            int_histogram: Histogram::default(),
            float_histogram: Histogram::default(),
            is_float_histogram: false,
        }
    }

    fn next_metric_family(&mut self) -> Result<bool, ParseError> {
        let rest = &self.input[self.pos..];
        if rest.is_empty() {
            return Ok(false);
        }
        let mut message = rest;
        let length = prost::encoding::decode_varint(&mut message)
            .map_err(|_| ParseError::InvalidProtobuf("invalid varint".to_owned()))?;
        let varint_len = rest.len() - message.len();
        if varint_len > MAX_VARINT_LEN_32 {
            return Err(ParseError::InvalidProtobuf("invalid varint".to_owned()));
        }
        let message = usize::try_from(length)
            .ok()
            .and_then(|length| message.get(..length))
            .ok_or_else(|| {
                ParseError::InvalidProtobuf(format!(
                    "insufficient length of buffer, expected at least {} bytes, got {} bytes",
                    length.saturating_add(varint_len as u64),
                    rest.len()
                ))
            })?;
        self.family_offset = self.pos;
        self.pos += varint_len + message.len();
        // TODO: decodes the whole family up front, so a malformed metric fails the family
        // before its earlier metrics are returned. A lazy decoder like Go's would fix that.
        self.family.clear();
        self.family
            .merge(message)
            .map_err(|err| ParseError::InvalidProtobuf(err.to_string()))?;
        self.next_metric_index = 0;
        Ok(true)
    }

    fn next_metric(&mut self) -> bool {
        if self.next_metric_index >= self.family.metric.len() {
            return false;
        }
        self.metric_index = self.next_metric_index;
        self.next_metric_index += 1;
        true
    }

    fn metric(&self) -> &dto::Metric {
        self.family
            .metric
            .get(self.metric_index)
            .unwrap_or(&EMPTY_METRIC)
    }

    fn dto_histogram(&self) -> &dto::Histogram {
        self.metric()
            .histogram
            .as_ref()
            .unwrap_or(&EMPTY_DTO_HISTOGRAM)
    }

    fn kind(&self) -> dto::MetricType {
        self.family.r#type()
    }

    fn is_histogram_kind(&self) -> bool {
        matches!(
            self.kind(),
            dto::MetricType::Histogram | dto::MetricType::GaugeHistogram
        )
    }

    fn is_classic_histogram(&self) -> bool {
        self.ignore_native_histograms || !is_native_histogram(self.dto_histogram())
    }

    fn timestamp(&self) -> Option<i64> {
        // Go treats 0 as unset, because gogo-protobuf has no optional fields.
        Some(self.metric().timestamp_ms).filter(|&millis| millis != 0)
    }

    /// Go's `getMagicName`.
    fn series_suffix(&self) -> &'static str {
        let kind = self.kind();
        if self.state == Some(Entry::Histogram)
            || !(self.is_histogram_kind() || kind == dto::MetricType::Summary)
        {
            return "";
        }
        match self.field_pos {
            -2 => "_count",
            -1 => "_sum",
            _ if self.is_histogram_kind() => "_bucket",
            _ => "",
        }
    }

    /// Go's `getMagicLabel`, which also sets `fields_done`.
    fn update_magic_label(&mut self) {
        self.magic_label = None;
        if self.state == Some(Entry::Histogram) || self.field_pos < 0 {
            return;
        }
        let pos = self.field_pos as usize;
        if self.kind() == dto::MetricType::Summary {
            let quantiles = self
                .metric()
                .summary
                .as_ref()
                .map_or(&[][..], |summary| &summary.quantile);
            let (len, quantile) = (quantiles.len(), quantiles.get(pos).map(|q| q.quantile));
            match quantile {
                Some(quantile) => {
                    self.fields_done = pos == len - 1;
                    self.magic_label = Some((QUANTILE_LABEL, quantile));
                }
                None => self.fields_done = true,
            }
        } else if self.is_histogram_kind() {
            let upper_bound = match self.dto_histogram().bucket.get(pos) {
                Some(bucket) => bucket.upper_bound,
                None => f64::INFINITY,
            };
            self.fields_done = upper_bound == f64::INFINITY;
            self.magic_label = Some((BUCKET_LABEL, upper_bound));
        }
    }

    /// Go's `onSeriesOrHistogramUpdate`, plus the parts of Go's `Series` and `Histogram` that
    /// change parser state, so the accessors can take `&self`.
    fn update_series(&mut self) -> Result<(), ParseError> {
        if self
            .metric()
            .label
            .iter()
            .any(|label| label.name.is_empty())
        {
            return Err(ParseError::InvalidProtobuf(
                "invalid label name: empty".to_owned(),
            ));
        }
        let suffix = self.series_suffix();
        self.series_name.clear();
        self.series_name.push_str(&self.family.name);
        self.series_name.push_str(suffix);
        if self.kind() == dto::MetricType::Summary
            && self.field_pos == -1
            && self
                .metric()
                .summary
                .as_ref()
                .is_none_or(|summary| summary.quantile.is_empty())
        {
            self.fields_done = true;
        }
        self.update_magic_label();

        let mut magic_value = FloatBuf::default();
        if let Some((_, value)) = self.magic_label {
            // A FloatBuf fits every f64.
            let _ = format_openmetrics_float(value, &mut magic_value);
        }
        // TODO: collects the labels per series to sort them, measure with the Phase 4 benchmarks.
        let mut entry_bytes = std::mem::take(&mut self.entry_bytes);
        entry_bytes.clear();
        let mut pairs: Vec<(&str, &str)> = self.label_pairs(&magic_value).collect();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        for (name, value) in pairs {
            if name == METRIC_NAME_LABEL {
                entry_bytes.extend_from_slice(value.as_bytes());
                continue;
            }
            entry_bytes.push(0xff);
            entry_bytes.extend_from_slice(name.as_bytes());
            entry_bytes.push(0xff);
            entry_bytes.extend_from_slice(value.as_bytes());
        }
        self.entry_bytes = entry_bytes;

        if self.state == Some(Entry::Histogram) {
            self.load_histogram()?;
        }
        Ok(())
    }

    /// Labels of the current series in Go's insertion order, before sorting.
    fn label_pairs<'s>(
        &'s self,
        magic_value: &'s FloatBuf,
    ) -> impl Iterator<Item = (&'s str, &'s str)> {
        std::iter::once((METRIC_NAME_LABEL, self.series_name.as_str()))
            .chain(
                self.metric()
                    .label
                    .iter()
                    .map(|label| (label.name.as_str(), label.value.as_str())),
            )
            .chain(
                self.magic_label
                    .map(|(name, _)| (name, magic_value.as_str())),
            )
    }

    /// Go's `Histogram`, run when the parser reaches a native histogram.
    fn load_histogram(&mut self) -> Result<(), ParseError> {
        let counter_reset_hint = if self.kind() == dto::MetricType::GaugeHistogram {
            CounterResetHint::Gauge
        } else {
            CounterResetHint::Unknown
        };
        // Field access instead of `dto_histogram()`, so the targets below can be borrowed.
        let source = self
            .family
            .metric
            .get(self.metric_index)
            .and_then(|metric| metric.histogram.as_ref())
            .unwrap_or(&EMPTY_DTO_HISTOGRAM);
        let name = self.family.name.as_str();
        self.redo_classic = self.parse_classic_histograms && !source.bucket.is_empty();
        self.is_float_histogram = source.sample_count_float > 0.0 || source.zero_count_float > 0.0;
        if self.is_float_histogram {
            check_span_buckets(
                name,
                "positive",
                &source.positive_span,
                source.positive_count.len(),
            )?;
            check_span_buckets(
                name,
                "negative",
                &source.negative_span,
                source.negative_count.len(),
            )?;
            let target = &mut self.float_histogram;
            target.count = source.sample_count_float;
            target.zero_count = source.zero_count_float;
            source
                .positive_count
                .clone_into(&mut target.positive_buckets);
            source
                .negative_count
                .clone_into(&mut target.negative_buckets);
            fill_histogram(target, source, counter_reset_hint);
        } else {
            check_span_buckets(
                name,
                "positive",
                &source.positive_span,
                source.positive_delta.len(),
            )?;
            check_span_buckets(
                name,
                "negative",
                &source.negative_span,
                source.negative_delta.len(),
            )?;
            let target = &mut self.int_histogram;
            target.count = source.sample_count;
            target.zero_count = source.zero_count;
            source
                .positive_delta
                .clone_into(&mut target.positive_buckets);
            source
                .negative_delta
                .clone_into(&mut target.negative_buckets);
            fill_histogram(target, source, counter_reset_hint);
        }
        Ok(())
    }

    fn switch_to_classic(&mut self) {
        self.field_pos = -3;
        self.fields_done = false;
        self.state = Some(Entry::Series);
    }

    fn start_family(&mut self) -> Result<bool, ParseError> {
        self.exemplar_pos = 0;
        self.field_pos = -2;
        if !self.next_metric_family()? {
            return Ok(false);
        }
        if !self.next_metric() {
            // Go skips empty metric families.
            return Ok(true);
        }
        if self.family.name.is_empty() {
            return Err(ParseError::InvalidMetricName {
                offset: self.family_offset,
            });
        }
        if dto::MetricType::try_from(self.family.r#type).is_err() {
            return Err(ParseError::InvalidMetricType(
                self.family.r#type.to_string(),
            ));
        }
        self.entry_bytes.clear();
        self.entry_bytes
            .extend_from_slice(self.family.name.as_bytes());
        self.state = Some(Entry::Help);
        Ok(true)
    }
}

impl<'a> Parser<'a> for ProtobufParser<'a> {
    fn pos(&self) -> usize {
        self.pos
    }

    /// Go's `Next`, with its recursive calls turned into loop iterations.
    fn next_entry(&mut self) -> Result<Option<Entry>, ParseError> {
        self.exemplar_returned = false;
        loop {
            match self.state {
                None => {
                    if !self.start_family()? {
                        return Ok(None);
                    }
                    if self.state.is_none() {
                        continue;
                    }
                }
                Some(Entry::Help) => {
                    self.state = Some(if self.family.unit.is_empty() {
                        Entry::Type
                    } else {
                        Entry::Unit
                    });
                }
                Some(Entry::Unit) => self.state = Some(Entry::Type),
                Some(Entry::Type) => {
                    if self.is_histogram_kind() && self.is_classic_histogram() {
                        self.switch_to_classic();
                        continue;
                    }
                    self.state = Some(if self.is_histogram_kind() {
                        Entry::Histogram
                    } else {
                        Entry::Series
                    });
                    self.update_series()?;
                }
                Some(Entry::Series) => {
                    let complex =
                        self.is_histogram_kind() || self.kind() == dto::MetricType::Summary;
                    if complex {
                        if !self.fields_done {
                            self.field_pos += 1;
                            self.update_series()?;
                            return Ok(self.state);
                        }
                        self.field_pos = -2;
                        self.fields_done = false;
                        self.exemplar_pos = 0;
                    }
                    if !self.next_metric() {
                        self.state = None;
                        continue;
                    }
                    if self.is_histogram_kind() {
                        // The next series may be native or classic only.
                        if self.is_classic_histogram() {
                            self.switch_to_classic();
                            continue;
                        }
                        self.state = Some(Entry::Histogram);
                    }
                    self.update_series()?;
                }
                Some(Entry::Histogram) => {
                    if self.redo_classic {
                        self.redo_classic = false;
                        self.switch_to_classic();
                        continue;
                    }
                    if !self.next_metric() {
                        self.state = None;
                        continue;
                    }
                    if self.is_classic_histogram() {
                        self.switch_to_classic();
                        continue;
                    }
                    self.update_series()?;
                }
                Some(Entry::Comment) => unreachable!("the protobuf parser never sets Comment"),
            }
            return Ok(self.state);
        }
    }

    fn help(&self) -> (&[u8], Cow<'_, [u8]>) {
        (
            &self.entry_bytes,
            Cow::Borrowed(self.family.help.as_bytes()),
        )
    }

    fn metric_type(&self) -> (&[u8], MetricType) {
        let metric_type = match self.kind() {
            dto::MetricType::Counter => MetricType::Counter,
            dto::MetricType::Gauge => MetricType::Gauge,
            dto::MetricType::Histogram => MetricType::Histogram,
            dto::MetricType::GaugeHistogram => MetricType::GaugeHistogram,
            dto::MetricType::Summary => MetricType::Summary,
            dto::MetricType::Untyped => MetricType::Unknown,
        };
        (&self.entry_bytes, metric_type)
    }

    fn unit(&self) -> (&[u8], &[u8]) {
        (&self.entry_bytes, self.family.unit.as_bytes())
    }

    fn comment(&self) -> &'a [u8] {
        b""
    }

    fn series(&self) -> (&[u8], Option<i64>, f64) {
        let metric = self.metric();
        let value = match self.kind() {
            dto::MetricType::Counter => {
                metric.counter.as_ref().map_or(0.0, |counter| counter.value)
            }
            dto::MetricType::Gauge => metric.gauge.as_ref().map_or(0.0, |gauge| gauge.value),
            dto::MetricType::Untyped => {
                metric.untyped.as_ref().map_or(0.0, |untyped| untyped.value)
            }
            dto::MetricType::Summary => {
                let summary = metric.summary.as_ref();
                match self.field_pos {
                    -2 => summary.map_or(0.0, |summary| summary.sample_count as f64),
                    -1 => summary.map_or(0.0, |summary| summary.sample_sum),
                    pos => summary
                        .and_then(|summary| summary.quantile.get(pos as usize))
                        .map_or(0.0, |quantile| quantile.value),
                }
            }
            dto::MetricType::Histogram | dto::MetricType::GaugeHistogram => {
                let histogram = self.dto_histogram();
                let count = or_float(histogram.sample_count_float, histogram.sample_count);
                match self.field_pos {
                    -2 => count,
                    -1 => histogram.sample_sum,
                    pos => histogram.bucket.get(pos as usize).map_or(count, |bucket| {
                        or_float(bucket.cumulative_count_float, bucket.cumulative_count)
                    }),
                }
            }
        };
        (&self.entry_bytes, self.timestamp(), value)
    }

    fn histogram(&self) -> (&[u8], Option<i64>, HistogramRef<'_>) {
        let histogram = if self.is_float_histogram {
            HistogramRef::Float(&self.float_histogram)
        } else {
            HistogramRef::Int(&self.int_histogram)
        };
        (&self.entry_bytes, self.timestamp(), histogram)
    }

    // TODO: copies label strings per call because they live in the decoded family, not
    // in the input. A borrowing decoder like Go's would make this zero-copy.
    fn labels(&self, buf: &mut LabelsBuffer<'a>) {
        buf.clear();
        let mut magic_value = FloatBuf::default();
        if let Some((_, value)) = self.magic_label {
            let _ = format_openmetrics_float(value, &mut magic_value);
        }
        for (name, value) in self.label_pairs(&magic_value) {
            buf.push(Cow::Owned(name.to_owned()), Cow::Owned(value.to_owned()));
        }
        buf.sort();
    }

    /// Go's `Exemplar`. A native histogram returns its exemplars with timestamps one per call,
    /// from the `exemplars` field or else from the classic buckets.
    fn exemplar(&mut self, buf: &mut LabelsBuffer<'a>) -> Option<(f64, Option<i64>)> {
        buf.clear();
        if self.exemplar_returned && self.state == Some(Entry::Series) {
            return None;
        }
        let exemplar = match self.kind() {
            dto::MetricType::Counter => self.metric().counter.as_ref()?.exemplar.as_ref(),
            dto::MetricType::Histogram | dto::MetricType::GaugeHistogram => {
                self.histogram_exemplar()
            }
            _ => None,
        }?;
        let value = exemplar.value;
        let timestamp = exemplar.timestamp.as_ref().map(timestamp_millis);
        for label in &exemplar.label {
            buf.push(
                Cow::Owned(label.name.clone()),
                Cow::Owned(label.value.clone()),
            );
        }
        buf.sort();
        self.exemplar_returned = true;
        Some((value, timestamp))
    }

    fn start_timestamp(&mut self) -> Option<i64> {
        let metric = self.metric();
        let start = match self.kind() {
            dto::MetricType::Counter => metric.counter.as_ref()?.start_timestamp.as_ref(),
            dto::MetricType::Summary => metric.summary.as_ref()?.start_timestamp.as_ref(),
            dto::MetricType::Histogram | dto::MetricType::GaugeHistogram => {
                self.dto_histogram().start_timestamp.as_ref()
            }
            _ => None,
        };
        start.map(timestamp_millis)
    }
}

impl ProtobufParser<'_> {
    /// The histogram branch of Go's `Exemplar`, which advances `exemplar_pos` or `field_pos`.
    fn histogram_exemplar(&mut self) -> Option<&dto::Exemplar> {
        let is_classic = self.state == Some(Entry::Series);
        let histogram = self
            .family
            .metric
            .get(self.metric_index)
            .and_then(|metric| metric.histogram.as_ref())?;
        if !is_classic && !histogram.exemplars.is_empty() {
            // Native histograms drop exemplars without a timestamp.
            while let Some(exemplar) = histogram.exemplars.get(self.exemplar_pos) {
                self.exemplar_pos += 1;
                if exemplar.timestamp.is_some() {
                    return Some(exemplar);
                }
            }
            return None;
        }
        if self.field_pos < 0 {
            if is_classic {
                return None;
            }
            self.field_pos = 0;
        }
        let mut found = None;
        while let Some(bucket) = histogram.bucket.get(self.field_pos as usize) {
            found = bucket.exemplar.as_ref();
            if is_classic {
                break;
            }
            self.field_pos += 1;
            if found.is_some_and(|exemplar| exemplar.timestamp.is_some()) {
                break;
            }
        }
        if !is_classic && found.is_none_or(|exemplar| exemplar.timestamp.is_none()) {
            return None;
        }
        found
    }
}

/// Go's `isNativeHistogram`.
fn is_native_histogram(histogram: &dto::Histogram) -> bool {
    !histogram.positive_span.is_empty()
        || !histogram.negative_span.is_empty()
        || histogram.zero_threshold > 0.0
        || histogram.zero_count > 0
}

/// Go reads the float field and falls back to the integer field when it is 0.
fn or_float(float: f64, int: u64) -> f64 {
    if float == 0.0 { int as f64 } else { float }
}

/// Same as Go: seconds and nanoseconds to ms, without validation.
fn timestamp_millis(timestamp: &prost_types::Timestamp) -> i64 {
    timestamp
        .seconds
        .wrapping_mul(1000)
        .wrapping_add(i64::from(timestamp.nanos / 1_000_000))
}

/// Go's `checkProtoSpanBucketConsistency`, which keeps malformed spans out of compaction.
fn check_span_buckets(
    name: &str,
    side: &str,
    spans: &[dto::BucketSpan],
    buckets: usize,
) -> Result<(), ParseError> {
    let total: u64 = spans.iter().map(|span| u64::from(span.length)).sum();
    if total != buckets as u64 {
        return Err(ParseError::InvalidProtobuf(format!(
            "histogram {name:?}: {side} side: spans require {total} buckets, have {buckets}"
        )));
    }
    Ok(())
}

/// Copies the shared fields and spans, then compacts like Go's `Compact(0)`.
fn fill_histogram<C, B: Bucket>(
    target: &mut Histogram<C, B>,
    source: &dto::Histogram,
    counter_reset_hint: CounterResetHint,
) {
    target.counter_reset_hint = counter_reset_hint;
    target.schema = source.schema;
    target.zero_threshold = source.zero_threshold;
    target.sum = source.sample_sum;
    copy_spans(&mut target.positive_spans, &source.positive_span);
    copy_spans(&mut target.negative_spans, &source.negative_span);
    compact_buckets(&mut target.positive_buckets, &mut target.positive_spans);
    compact_buckets(&mut target.negative_buckets, &mut target.negative_spans);
}

fn copy_spans(target: &mut Vec<Span>, source: &[dto::BucketSpan]) {
    target.clear();
    target.extend(source.iter().map(|span| Span {
        offset: span.offset,
        length: span.length,
    }));
}

/// Bucket count of a native histogram: delta-encoded `i64` or absolute `f64`.
trait Bucket: Copy + PartialEq {
    const ZERO: Self;
    const DELTA: bool;
    fn add(self, other: Self) -> Self;
    fn neg(self) -> Self;
}

impl Bucket for i64 {
    const ZERO: Self = 0;
    const DELTA: bool = true;
    fn add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
    fn neg(self) -> Self {
        self.wrapping_neg()
    }
}

impl Bucket for f64 {
    const ZERO: Self = 0.0;
    const DELTA: bool = false;
    fn add(self, other: Self) -> Self {
        self + other
    }
    fn neg(self) -> Self {
        -self
    }
}

/// Go's `compactBuckets` with `maxEmptyBuckets` 0: removes empty buckets and merges spans.
/// The caller has checked that the spans cover exactly `buckets.len()` buckets.
fn compact_buckets<B: Bucket>(buckets: &mut Vec<B>, spans: &mut Vec<Span>) {
    let absolute = |current: B, bucket: B| {
        if B::DELTA {
            current.add(bucket)
        } else {
            bucket
        }
    };
    let mut current = B::ZERO;
    let has_empty_bucket = buckets.iter().any(|&bucket| {
        current = absolute(current, bucket);
        current == B::ZERO
    });
    if !has_empty_bucket && spans.iter().all(|span| span.offset > 0 && span.length != 0) {
        return;
    }

    if spans.len() > 1 {
        let mut kept = 0;
        for index in 1..spans.len() {
            let span = spans[index];
            if span.offset == 0 {
                spans[kept].length = spans[kept].length.wrapping_add(span.length);
                continue;
            }
            kept += 1;
            spans[kept] = span;
        }
        spans.truncate(kept + 1);
    }

    let mut kept = 0;
    for index in 0..spans.len() {
        let span = spans[index];
        if span.length == 0 {
            if let Some(next) = spans.get_mut(index + 1) {
                next.offset = next.offset.wrapping_add(span.offset);
            }
            continue;
        }
        spans[kept] = span;
        kept += 1;
    }
    spans.truncate(kept);
    if spans.is_empty() {
        buckets.clear();
        return;
    }

    let (mut bucket_index, mut span_index, mut pos_in_span) = (0, 0, 0u32);
    current = B::ZERO;
    while bucket_index < buckets.len() && span_index < spans.len() {
        current = absolute(current, buckets[bucket_index]);
        let empty = empty_buckets_here(
            buckets,
            spans[span_index].length,
            bucket_index,
            pos_in_span,
            current,
        );
        if empty == 0 {
            bucket_index += 1;
            pos_in_span += 1;
            if pos_in_span >= spans[span_index].length {
                pos_in_span = 0;
                span_index += 1;
            }
            continue;
        }
        if B::DELTA && bucket_index + empty < buckets.len() {
            current = buckets[bucket_index].neg();
            buckets[bucket_index + empty] =
                buckets[bucket_index + empty].add(buckets[bucket_index]);
        }
        buckets.drain(bucket_index..bucket_index + empty);
        let empty_u32 = empty as u32;
        if pos_in_span == 0 {
            if empty_u32 == spans[span_index].length {
                let offset = spans.remove(span_index).offset;
                if let Some(next) = spans.get_mut(span_index) {
                    next.offset = next
                        .offset
                        .wrapping_add(offset)
                        .wrapping_add(empty_u32 as i32);
                }
                continue;
            }
            spans[span_index].length -= empty_u32;
            spans[span_index].offset = spans[span_index].offset.wrapping_add(empty_u32 as i32);
            continue;
        }
        let split = Span {
            offset: empty_u32 as i32,
            length: spans[span_index].length - pos_in_span - empty_u32,
        };
        spans[span_index].length = pos_in_span;
        span_index += 1;
        pos_in_span = 0;
        if split.length == 0 {
            if let Some(next) = spans.get_mut(span_index) {
                next.offset = next.offset.wrapping_add(empty_u32 as i32);
            }
            continue;
        }
        spans.insert(span_index, split);
    }
}

/// Go's `emptyBucketsHere`: the number of empty buckets from `bucket_index` within the span.
fn empty_buckets_here<B: Bucket>(
    buckets: &[B],
    span_length: u32,
    bucket_index: usize,
    pos_in_span: u32,
    current: B,
) -> usize {
    let mut count = 0;
    let mut value = current;
    while (count as u32) + pos_in_span < span_length && value == B::ZERO {
        count += 1;
        match buckets.get(bucket_index + count) {
            Some(&bucket) => value = bucket,
            None => break,
        }
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delimited(families: &[dto::MetricFamily]) -> Vec<u8> {
        let mut out = Vec::new();
        for family in families {
            family.encode_length_delimited(&mut out).unwrap();
        }
        out
    }

    fn parse_all(input: &[u8]) -> Result<(), ParseError> {
        let mut parser = ProtobufParser::new(input, false, false);
        while parser.next_entry()?.is_some() {
            if parser.state == Some(Entry::Histogram) {
                parser.histogram();
            }
        }
        Ok(())
    }

    fn histogram_family(name: &str, positive_delta: Vec<i64>) -> dto::MetricFamily {
        dto::MetricFamily {
            name: name.to_owned(),
            r#type: dto::MetricType::Histogram as i32,
            metric: vec![dto::Metric {
                histogram: Some(dto::Histogram {
                    positive_span: vec![dto::BucketSpan {
                        offset: 0,
                        length: 2,
                    }],
                    positive_delta,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn rejects_malformed_input_without_panicking() {
        let valid = delimited(&[histogram_family("h", vec![1, 0])]);
        assert!(parse_all(&valid).is_ok());
        assert!(matches!(
            parse_all(&valid[..valid.len() - 1]),
            Err(ParseError::InvalidProtobuf(_))
        ));

        // Go skips the span check when a native histogram follows another one. Without it,
        // compaction here would index out of bounds.
        let second_bad = histogram_family("h", vec![1]);
        let mut family = histogram_family("h", vec![1, 0]);
        family.metric.extend(second_bad.metric);
        assert!(matches!(
            parse_all(&delimited(&[family])),
            Err(ParseError::InvalidProtobuf(message)) if message.starts_with("histogram \"h\": ")
        ));

        assert!(matches!(
            parse_all(&delimited(&[histogram_family("", vec![1, 0])])),
            Err(ParseError::InvalidMetricName { .. })
        ));
    }
}
