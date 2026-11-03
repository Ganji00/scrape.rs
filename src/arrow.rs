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

//! Layer 1 columnar Apache Arrow decoder producing OpenTelemetry Arrow Protocol metric batches.
//!
//! [`ArrowDecoder`] ingests entries from Layer 0 parsers one by one into reusable columnar buffers
//! and arena-backed string interners without per-sample heap allocations, locks, or shared
//! thread-synchronized state, then finalizes one [`OtapMetricRecords`] batch set per scrape.
//! Single-threaded `!Send` scrape loops can reuse one decoder across scrapes with bounded memory.

mod intern;
mod records;
mod schema;

use std::sync::Arc;

use arrow_array::types::{UInt8Type, UInt16Type};
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, Float64Array, Int32Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::ArrowError;

use crate::format::{Format, FormatType};
use crate::parse::{
    Entry, LabelSpan, METRIC_NAME_LABEL, MetricType, ParseError, Parser, PromParser,
    unescape_bytes_into,
};

use self::intern::StringInterner;
pub use self::records::{MetricPayloadType, OtapMetricRecords};
pub use self::schema::PROMETHEUS_TYPE_METADATA_KEY;
use self::schema::{
    AGGREGATION_TEMPORALITY_CUMULATIVE, ATTR_VALUE_TYPE_STR, KeyEncoding, OTAP_METRIC_TYPE_GAUGE,
    OTAP_METRIC_TYPE_SUM, UTC_TIME_ZONE, ValEncoding, metric_attrs_key_dict, metric_attrs_schema,
    metric_attrs_type_val_dict, number_dp_attrs_schema, number_dp_schema,
    univariate_metrics_schema,
};

/// Options controlling [`ArrowDecoder::decode`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DecodeOptions {
    /// Default timestamp in milliseconds since the Unix epoch applied to samples that omit an
    /// explicit wire timestamp. When `None`, untimestamped samples leave `time_unix_nano` unset.
    pub timestamp_ms: Option<i64>,
}

impl DecodeOptions {
    /// Creates default [`DecodeOptions`] with no fallback timestamp.
    #[must_use]
    pub const fn new() -> Self {
        Self { timestamp_ms: None }
    }

    /// Sets the fallback timestamp in milliseconds for samples without an explicit wire timestamp.
    #[must_use]
    pub const fn with_timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }
}

/// Errors returned by [`ArrowDecoder::decode`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// The input `Format` is unrecognized or not yet implemented in Layer 1.
    #[error("unsupported exposition format: {0}")]
    UnsupportedFormat(String),

    /// A metric family uses a `# TYPE` not yet supported by `ArrowDecoder`.
    #[error("unsupported metric type {metric_type:?} for metric {metric:?}")]
    UnsupportedMetricType {
        /// Metric family name declared in `# TYPE`.
        metric: String,
        /// Prometheus type name, such as `"histogram"` or `"summary"`.
        metric_type: &'static str,
    },

    /// Unexpected token while lexing or parsing the exposition grammar.
    #[error("{expected} at byte {offset}")]
    Syntax {
        /// Description of the expected grammar element.
        expected: &'static str,
        /// Byte offset of the offending token in the input.
        offset: usize,
    },

    /// Unknown `# TYPE` identifier on the wire.
    #[error("invalid metric type {0:?}")]
    InvalidMetricType(String),

    /// Invalid UTF-8 sequence in a metric name, help string, label name, or label value.
    #[error("invalid UTF-8 at byte {offset}")]
    InvalidUtf8 {
        /// Byte offset of the invalid UTF-8 sequence in the input.
        offset: usize,
    },

    /// Sample float value could not be parsed.
    #[error("invalid sample value {0:?}")]
    InvalidValue(String),

    /// Sample integer timestamp could not be parsed as `i64`.
    #[error("invalid timestamp {0:?}")]
    InvalidTimestamp(String),

    /// Series omitted a metric name or specified it twice.
    #[error("invalid metric name at byte {offset}")]
    InvalidMetricName {
        /// Byte offset of the start of the offending series line.
        offset: usize,
    },

    /// Two `# HELP` lines were declared for the same metric family.
    #[error("second HELP line for metric name {metric:?}")]
    DuplicateHelp {
        /// Metric family name.
        metric: String,
    },

    /// A second `# TYPE` line appeared, or `# TYPE` appeared after samples for the same family.
    #[error("second TYPE line for metric name {metric:?}, or TYPE reported after samples")]
    DuplicateOrLateType {
        /// Metric family name.
        metric: String,
    },

    /// A series contained duplicate label names or explicitly set the reserved `__name__` label.
    #[error("duplicate or reserved label {label:?} on metric {metric:?}")]
    DuplicateLabel {
        /// Metric family name.
        metric: String,
        /// Offending label name.
        label: String,
    },

    /// Converting a millisecond timestamp to nanoseconds overflowed `i64`.
    #[error("timestamp {timestamp_ms} ms overflows i64 nanoseconds")]
    TimestampOverflow {
        /// Millisecond timestamp that overflowed `i64` nanoseconds.
        timestamp_ms: i64,
    },

    /// Number of metrics, data points, or string bytes exceeded OTAP `u16`, `u32`, or `i32` limits.
    #[error("OTAP {table} capacity exceeded ({count})")]
    CapacityOverflow {
        /// OTAP table or arena that overflowed its index width.
        table: &'static str,
        /// Observed element or byte count.
        count: usize,
    },

    /// Constructing an Arrow `RecordBatch` failed validation.
    #[error("arrow batch construction failed: {0}")]
    Arrow(#[from] ArrowError),
}

impl DecodeError {
    fn from_parse(err: ParseError) -> Self {
        match err {
            ParseError::UnexpectedToken { expected, offset } => Self::Syntax { expected, offset },
            ParseError::InvalidMetricType(t) => Self::InvalidMetricType(t),
            ParseError::InvalidUtf8 { offset } => Self::InvalidUtf8 { offset },
            ParseError::InvalidValue(v) => Self::InvalidValue(v),
            ParseError::InvalidTimestamp(ts) => Self::InvalidTimestamp(ts),
            ParseError::InvalidMetricName { offset } => Self::InvalidMetricName { offset },
        }
    }
}

#[derive(Clone, Copy)]
struct FamilyState {
    metric_type: MetricType,
    has_type: bool,
    has_help: bool,
    help_start: u32,
    help_end: u32,
    emitted_id: Option<u16>,
}

impl Default for FamilyState {
    fn default() -> Self {
        Self {
            metric_type: MetricType::Unknown,
            has_type: false,
            has_help: false,
            help_start: 0,
            help_end: 0,
            emitted_id: None,
        }
    }
}

/// Stateful, lock-free decoder translating Prometheus exposition payloads into OTAP Arrow metric
/// [`RecordBatch`]es.
///
/// Reusing a single `ArrowDecoder` across scrapes preserves internal parser scratch spans, hash
/// tables, string arenas, and column staging buffers so steady-state scrapes allocate only the
/// output Arrow `Buffer`s returned inside [`OtapMetricRecords`].
pub struct ArrowDecoder {
    parser_spans: Vec<LabelSpan>,
    unescape_buf: Vec<u8>,

    family_names: StringInterner,
    families: Vec<FamilyState>,
    emitted_families: Vec<u32>,
    last_family_idx: Option<u32>,
    help_arena: Vec<u8>,

    key_interner: StringInterner,
    val_interner: StringInterner,

    dp_parent_ids: Vec<u16>,
    dp_values: Vec<f64>,
    dp_timestamps: Vec<i64>,
    dp_time_valid: Vec<bool>,

    dp_attr_parent_ids: Vec<u32>,
    dp_attr_pairs: Vec<(u32, u32)>,

    // Reusable staging buffers for materializing Arrow columns without reallocating.
    scratch_u8: Vec<u8>,
    scratch_u16: Vec<u16>,
    scratch_u32: Vec<u32>,
    scratch_i32: Vec<i32>,
    scratch_bytes: Vec<u8>,
}

impl Default for ArrowDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowDecoder {
    /// Creates a new [`ArrowDecoder`] with default initial buffer capacities.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(64, 256, 512)
    }

    /// Creates a new [`ArrowDecoder`] pre-sized for `metrics` families, `data_points` series, and
    /// `attrs` label pairs.
    #[must_use]
    pub fn with_capacity(metrics: usize, data_points: usize, attrs: usize) -> Self {
        Self {
            parser_spans: Vec::with_capacity(16),
            unescape_buf: Vec::new(),

            family_names: StringInterner::with_capacity(metrics, metrics.saturating_mul(32)),
            families: Vec::with_capacity(metrics),
            emitted_families: Vec::with_capacity(metrics),
            last_family_idx: None,
            help_arena: Vec::with_capacity(metrics.saturating_mul(48)),

            key_interner: StringInterner::with_capacity(64, 1024),
            val_interner: StringInterner::with_capacity(256, 4096),

            dp_parent_ids: Vec::with_capacity(data_points),
            dp_values: Vec::with_capacity(data_points),
            dp_timestamps: Vec::new(),
            dp_time_valid: Vec::new(),

            dp_attr_parent_ids: Vec::with_capacity(attrs),
            dp_attr_pairs: Vec::with_capacity(attrs),

            scratch_u8: Vec::with_capacity(attrs.max(metrics)),
            scratch_u16: Vec::with_capacity(attrs.max(metrics)),
            scratch_u32: Vec::with_capacity(data_points),
            scratch_i32: Vec::with_capacity(metrics + 1),
            scratch_bytes: Vec::new(),
        }
    }

    /// Returns the total heap buffer capacity in bytes currently retained by this decoder's
    /// reusable scratch structures.
    #[must_use]
    pub fn retained_memory_bytes(&self) -> usize {
        self.parser_spans.capacity() * size_of::<LabelSpan>()
            + self.unescape_buf.capacity()
            + self.family_names.retained_memory_bytes()
            + self.families.capacity() * size_of::<FamilyState>()
            + self.emitted_families.capacity() * size_of::<u32>()
            + self.help_arena.capacity()
            + self.key_interner.retained_memory_bytes()
            + self.val_interner.retained_memory_bytes()
            + self.dp_parent_ids.capacity() * size_of::<u16>()
            + self.dp_values.capacity() * size_of::<f64>()
            + self.dp_timestamps.capacity() * size_of::<i64>()
            + self.dp_time_valid.capacity() * size_of::<bool>()
            + self.dp_attr_parent_ids.capacity() * size_of::<u32>()
            + self.dp_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.scratch_u8.capacity()
            + self.scratch_u16.capacity() * size_of::<u16>()
            + self.scratch_u32.capacity() * size_of::<u32>()
            + self.scratch_i32.capacity() * size_of::<i32>()
            + self.scratch_bytes.capacity()
    }

    /// Decodes a single exposition scrape buffer of `format` into OTAP metric [`RecordBatch`]es.
    ///
    /// # Streaming and batching
    ///
    /// Input parsing is streaming within `input`: `decode` pulls `Entry` items one by one from
    /// `PromParser` and appends directly into reusable columnar staging vectors and arena string
    /// interners without materializing intermediate per-sample structs or per-string allocations.
    ///
    /// Output emission is batched per scrape call rather than chunked mid-scrape. Each `decode`
    /// call expects `input` to hold one complete exposition payload and returns at most one
    /// [`RecordBatch`] per [`MetricPayloadType`] table covering the entire scrape. Whole-scrape
    /// finalization is required because:
    /// - Prometheus text 0.0.4 allows interleaved samples for an earlier metric family and
    ///   trailing `# HELP` lines after samples, so `UnivariateMetrics` descriptions and parent IDs
    ///   are only complete at end-of-scrape.
    /// - OTAP schemas adaptively omit unused optional columns (`description`, `id`,
    ///   `time_unix_nano`) and choose dictionary key widths (`UInt8`, `UInt16`, or plain `Utf8`)
    ///   from whole-batch cardinality and nullability.
    pub fn decode(
        &mut self,
        input: &[u8],
        format: &Format,
        opts: &DecodeOptions,
    ) -> Result<OtapMetricRecords, DecodeError> {
        if format.format_type() != FormatType::TextPlain {
            // TODO(bwplotka): Dispatch FormatType::OpenMetrics once #6 lands and Proto* in Phase 3.
            return Err(DecodeError::UnsupportedFormat(format.as_str().to_owned()));
        }

        let default_ts_ns = match opts.timestamp_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => None,
        };

        self.reset();

        let spans = std::mem::take(&mut self.parser_spans);
        let mut parser = PromParser::with_scratch(input, spans);
        let parse_res = self.ingest_prom_entries(&mut parser, default_ts_ns);
        self.parser_spans = parser.into_scratch();
        parse_res?;

        self.finish_batches(default_ts_ns.is_some())
    }

    fn reset(&mut self) {
        self.family_names.clear();
        self.families.clear();
        self.emitted_families.clear();
        self.last_family_idx = None;
        self.help_arena.clear();

        self.key_interner.clear();
        self.val_interner.clear();

        self.dp_parent_ids.clear();
        self.dp_values.clear();
        self.dp_timestamps.clear();
        self.dp_time_valid.clear();

        self.dp_attr_parent_ids.clear();
        self.dp_attr_pairs.clear();
    }

    #[inline]
    fn resolve_family(&mut self, raw_name: &[u8], name_offset: usize) -> Result<u32, DecodeError> {
        // Consecutive `# HELP`, `# TYPE`, and series lines almost always belong to the same
        // metric family. When `raw_name` has no escapes and matches `last_family_idx` byte-for-byte,
        // it is already validated UTF-8 and needs neither unescaping nor hashing.
        if let Some(last_idx) = self.last_family_idx
            && self.family_names.get_bytes(last_idx) == raw_name
            && !raw_name.contains(&b'\\')
        {
            return Ok(last_idx);
        }

        if !raw_name.is_ascii() && std::str::from_utf8(raw_name).is_err() {
            return Err(DecodeError::InvalidUtf8 {
                offset: name_offset,
            });
        }
        let name = unescape_bytes_into(raw_name, true, &mut self.unescape_buf);
        if let Some(last_idx) = self.last_family_idx
            && self.family_names.get_bytes(last_idx) == name
        {
            return Ok(last_idx);
        }

        let idx = self
            .family_names
            .intern(name)
            .ok_or(DecodeError::CapacityOverflow {
                table: "UnivariateMetrics.name",
                count: self.family_names.len(),
            })?;
        if idx as usize == self.families.len() {
            self.families.push(FamilyState::default());
        }
        self.last_family_idx = Some(idx);
        Ok(idx)
    }

    fn ingest_prom_entries(
        &mut self,
        parser: &mut PromParser<'_>,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        while let Some(entry) = parser.next_entry().map_err(DecodeError::from_parse)? {
            match entry {
                Entry::Comment => {}
                Entry::Help => {
                    let (raw_name, raw_help) = parser.raw_help();
                    let name_offset = parser.name_offset();
                    let fam_idx = self.resolve_family(raw_name, name_offset)?;
                    // Match `prometheus/common/expfmt/text_parse.go`: a bare `# HELP name` line
                    // without docstring text is a no-op and does not count as setting HELP.
                    if raw_help.is_empty() {
                        continue;
                    }
                    if self.families[fam_idx as usize].has_help {
                        return Err(DecodeError::DuplicateHelp {
                            metric: self.family_names.get(fam_idx).to_owned(),
                        });
                    }
                    // `PromParser` already verified `raw_help` is valid UTF-8 on `Entry::Help`.
                    let unescaped = unescape_bytes_into(raw_help, false, &mut self.unescape_buf);
                    let start = u32::try_from(self.help_arena.len()).map_err(|_| {
                        DecodeError::CapacityOverflow {
                            table: "UnivariateMetrics.description",
                            count: self.help_arena.len(),
                        }
                    })?;
                    self.help_arena.extend_from_slice(unescaped);
                    let end = u32::try_from(self.help_arena.len()).map_err(|_| {
                        DecodeError::CapacityOverflow {
                            table: "UnivariateMetrics.description",
                            count: self.help_arena.len(),
                        }
                    })?;
                    let fam = &mut self.families[fam_idx as usize];
                    fam.has_help = true;
                    fam.help_start = start;
                    fam.help_end = end;
                }
                Entry::Type => {
                    let (raw_name, metric_type) = parser.metric_type();
                    let name_offset = parser.name_offset();
                    let fam_idx = self.resolve_family(raw_name, name_offset)?;
                    let fam = &self.families[fam_idx as usize];
                    if fam.has_type || fam.emitted_id.is_some() {
                        return Err(DecodeError::DuplicateOrLateType {
                            metric: self.family_names.get(fam_idx).to_owned(),
                        });
                    }
                    match metric_type {
                        MetricType::Counter | MetricType::Gauge | MetricType::Unknown => {
                            let fam = &mut self.families[fam_idx as usize];
                            fam.metric_type = metric_type;
                            fam.has_type = true;
                        }
                        // TODO(bwplotka): Assemble classic histograms and summaries in #8.
                        MetricType::Histogram => {
                            return Err(DecodeError::UnsupportedMetricType {
                                metric: self.family_names.get(fam_idx).to_owned(),
                                metric_type: "histogram",
                            });
                        }
                        MetricType::Summary => {
                            return Err(DecodeError::UnsupportedMetricType {
                                metric: self.family_names.get(fam_idx).to_owned(),
                                metric_type: "summary",
                            });
                        }
                    }
                }
                Entry::Series => {
                    let (raw_name, label_spans, full_input, sample_ts_ms, value) =
                        parser.raw_series_parts();
                    let name_offset = parser.name_offset();
                    let fam_idx = self.resolve_family(raw_name, name_offset)?;
                    let parent_id = match self.families[fam_idx as usize].emitted_id {
                        Some(id) => id,
                        None => {
                            let next_id =
                                u16::try_from(self.emitted_families.len()).map_err(|_| {
                                    DecodeError::CapacityOverflow {
                                        table: "UnivariateMetrics",
                                        count: self.emitted_families.len() + 1,
                                    }
                                })?;
                            self.families[fam_idx as usize].emitted_id = Some(next_id);
                            self.emitted_families.push(fam_idx);
                            next_id
                        }
                    };

                    let dp_index = self.dp_parent_ids.len();
                    let dp_id =
                        u32::try_from(dp_index).map_err(|_| DecodeError::CapacityOverflow {
                            table: "NumberDataPoints",
                            count: dp_index + 1,
                        })?;

                    self.append_series_labels(fam_idx, dp_id, label_spans, full_input)?;
                    self.append_data_point(parent_id, value, sample_ts_ms, default_ts_ns)?;
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn append_series_labels(
        &mut self,
        fam_idx: u32,
        dp_id: u32,
        label_spans: &[LabelSpan],
        full_input: &[u8],
    ) -> Result<(), DecodeError> {
        if label_spans.is_empty() {
            return Ok(());
        }

        let attr_start = self.dp_attr_pairs.len();
        let mut prev_key_id: Option<u32> = None;
        let mut sorted_by_key_id = true;

        for (span_idx, span) in label_spans.iter().enumerate() {
            // `PromParser` already validated UTF-8 on every `LabelSpan`.
            let raw_key = &full_input[span.name_start..span.name_end];
            let raw_val = &full_input[span.val_start..span.val_end];

            // Prometheus treats `label=""` as an absent label (`labels.Builder` deletes empty
            // values). Validate reserved and duplicate label names before skipping so unused keys
            // and `""` never enter the OTAP attribute dictionaries. `unescape_bytes_into` never
            // shrinks a non-empty slice to zero bytes, so `raw_val.is_empty()` is exact.
            if raw_val.is_empty() {
                self.validate_empty_label(fam_idx, span_idx, label_spans, full_input)?;
                continue;
            }

            let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
            if key == METRIC_NAME_LABEL.as_bytes() {
                return Err(DecodeError::DuplicateLabel {
                    metric: self.family_names.get(fam_idx).to_owned(),
                    label: METRIC_NAME_LABEL.to_owned(),
                });
            }
            let key_id =
                self.key_interner
                    .intern(key)
                    .ok_or_else(|| DecodeError::CapacityOverflow {
                        table: "NumberDpAttrs.key",
                        count: self.key_interner.len(),
                    })?;

            if let Some(prev) = prev_key_id
                && key_id <= prev
            {
                if key_id == prev {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.family_names.get(fam_idx).to_owned(),
                        label: self.key_interner.get(key_id).to_owned(),
                    });
                }
                sorted_by_key_id = false;
            }
            prev_key_id = Some(key_id);

            let val = unescape_bytes_into(raw_val, true, &mut self.unescape_buf);
            let val_id =
                self.val_interner
                    .intern(val)
                    .ok_or_else(|| DecodeError::CapacityOverflow {
                        table: "NumberDpAttrs.str",
                        count: self.val_interner.len(),
                    })?;

            self.dp_attr_parent_ids.push(dp_id);
            self.dp_attr_pairs.push((key_id, val_id));
        }

        if !sorted_by_key_id {
            let slice = &mut self.dp_attr_pairs[attr_start..];
            slice.sort_unstable_by_key(|&(k, _)| k);
            for window in slice.windows(2) {
                if window[0].0 == window[1].0 {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.family_names.get(fam_idx).to_owned(),
                        label: self.key_interner.get(window[0].0).to_owned(),
                    });
                }
            }
        }

        Ok(())
    }

    #[cold]
    fn validate_empty_label(
        &mut self,
        fam_idx: u32,
        span_idx: usize,
        label_spans: &[LabelSpan],
        full_input: &[u8],
    ) -> Result<(), DecodeError> {
        let span = &label_spans[span_idx];
        let raw_key = &full_input[span.name_start..span.name_end];
        let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
        if key == METRIC_NAME_LABEL.as_bytes() {
            return Err(DecodeError::DuplicateLabel {
                metric: self.family_names.get(fam_idx).to_owned(),
                label: METRIC_NAME_LABEL.to_owned(),
            });
        }
        for (other_idx, other) in label_spans.iter().enumerate() {
            if other_idx == span_idx {
                continue;
            }
            let other_raw_key = &full_input[other.name_start..other.name_end];
            let other_key = unescape_bytes_into(other_raw_key, true, &mut self.scratch_bytes);
            if key == other_key {
                return Err(DecodeError::DuplicateLabel {
                    metric: self.family_names.get(fam_idx).to_owned(),
                    label: String::from_utf8_lossy(key).into_owned(),
                });
            }
        }
        Ok(())
    }

    #[inline]
    fn append_data_point(
        &mut self,
        parent_id: u16,
        value: f64,
        sample_ts_ms: Option<i64>,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let dp_index = self.dp_parent_ids.len();
        self.dp_parent_ids.push(parent_id);
        self.dp_values.push(value);

        let resolved_ts_ns = match sample_ts_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => default_ts_ns,
        };
        match resolved_ts_ns {
            Some(ts_ns) => {
                if dp_index > 0 && self.dp_timestamps.is_empty() {
                    // Prior samples had no timestamp; backfill nulls so column indices align.
                    self.dp_timestamps.resize(dp_index, 0);
                    self.dp_time_valid.resize(dp_index, false);
                }
                self.dp_timestamps.push(ts_ns);
                if !self.dp_time_valid.is_empty() {
                    self.dp_time_valid.push(true);
                }
            }
            None => {
                if !self.dp_timestamps.is_empty() {
                    if self.dp_time_valid.is_empty() {
                        self.dp_time_valid.resize(dp_index, true);
                    }
                    self.dp_timestamps.push(0);
                    self.dp_time_valid.push(false);
                }
            }
        }

        Ok(())
    }

    fn finish_batches(&mut self, has_default_ts: bool) -> Result<OtapMetricRecords, DecodeError> {
        if self.emitted_families.is_empty() {
            return Ok(OtapMetricRecords::default());
        }

        let univariate_metrics = Some(self.build_univariate_metrics()?);
        let metric_attrs = Some(self.build_metric_attrs()?);
        let number_data_points = Some(self.build_number_data_points(has_default_ts)?);
        let number_dp_attrs = if self.dp_attr_pairs.is_empty() {
            None
        } else {
            Some(self.build_number_dp_attrs()?)
        };

        Ok(OtapMetricRecords {
            univariate_metrics,
            metric_attrs,
            number_data_points,
            number_dp_attrs,
        })
    }

    fn build_univariate_metrics(&mut self) -> Result<RecordBatch, DecodeError> {
        let num_metrics = self.emitted_families.len();
        let mut any_help = false;
        let mut any_counter = false;
        let mut identity_order = num_metrics == self.family_names.len();

        for (i, &fam_idx) in self.emitted_families.iter().enumerate() {
            if fam_idx as usize != i {
                identity_order = false;
            }
            let fam = &self.families[fam_idx as usize];
            any_help |= fam.has_help;
            any_counter |= fam.metric_type == MetricType::Counter;
        }

        let schema = univariate_metrics_schema(any_help, any_counter);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        self.scratch_u16.clear();
        self.scratch_u16.extend((0..num_metrics).map(|i| i as u16));
        let id_arr = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);
        cols.push(Arc::new(id_arr));

        self.scratch_u8.clear();
        for &fam_idx in &self.emitted_families {
            let otap_type = match self.families[fam_idx as usize].metric_type {
                MetricType::Counter => OTAP_METRIC_TYPE_SUM,
                _ => OTAP_METRIC_TYPE_GAUGE,
            };
            self.scratch_u8.push(otap_type);
        }
        cols.push(Arc::new(UInt8Array::new(
            scalar_from_slice(&self.scratch_u8),
            None,
        )));

        // Fast-path directly from `family_names` when emitted order matches declaration order.
        let name_arr = if identity_order {
            self.family_names.to_string_array()
        } else {
            self.scratch_i32.clear();
            self.scratch_bytes.clear();
            self.scratch_i32.push(0);
            for &fam_idx in &self.emitted_families {
                let s = self.family_names.get_bytes(fam_idx);
                self.scratch_bytes.extend_from_slice(s);
                let off = i32::try_from(self.scratch_bytes.len()).map_err(|_| {
                    DecodeError::CapacityOverflow {
                        table: "UnivariateMetrics.name",
                        count: self.scratch_bytes.len(),
                    }
                })?;
                self.scratch_i32.push(off);
            }
            string_array_from_slices(&self.scratch_i32, &self.scratch_bytes, None)
        };
        cols.push(Arc::new(name_arr));

        if any_help {
            self.scratch_i32.clear();
            self.scratch_bytes.clear();
            self.scratch_i32.push(0);
            let mut all_have_help = true;
            for &fam_idx in &self.emitted_families {
                let fam = &self.families[fam_idx as usize];
                if fam.has_help {
                    let s = &self.help_arena[fam.help_start as usize..fam.help_end as usize];
                    self.scratch_bytes.extend_from_slice(s);
                } else {
                    all_have_help = false;
                }
                let off = i32::try_from(self.scratch_bytes.len()).map_err(|_| {
                    DecodeError::CapacityOverflow {
                        table: "UnivariateMetrics.description",
                        count: self.scratch_bytes.len(),
                    }
                })?;
                self.scratch_i32.push(off);
            }
            let nulls = if all_have_help {
                None
            } else {
                Some(NullBuffer::new(BooleanBuffer::from_iter(
                    self.emitted_families
                        .iter()
                        .map(|&fam_idx| self.families[fam_idx as usize].has_help),
                )))
            };
            cols.push(Arc::new(string_array_from_slices(
                &self.scratch_i32,
                &self.scratch_bytes,
                nulls,
            )));
        }

        if any_counter {
            self.scratch_i32.clear();
            let mut all_counters = true;
            for &fam_idx in &self.emitted_families {
                if self.families[fam_idx as usize].metric_type == MetricType::Counter {
                    self.scratch_i32.push(AGGREGATION_TEMPORALITY_CUMULATIVE);
                } else {
                    self.scratch_i32.push(0);
                    all_counters = false;
                }
            }
            let counter_validity =
                BooleanBuffer::from_iter(self.emitted_families.iter().map(|&fam_idx| {
                    self.families[fam_idx as usize].metric_type == MetricType::Counter
                }));
            let nulls = if all_counters {
                None
            } else {
                Some(NullBuffer::new(counter_validity.clone()))
            };
            cols.push(Arc::new(Int32Array::new(
                scalar_from_slice(&self.scratch_i32),
                nulls.clone(),
            )));
            cols.push(Arc::new(BooleanArray::new(counter_validity, nulls)));
        }

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn build_metric_attrs(&mut self) -> Result<RecordBatch, DecodeError> {
        let num_metrics = self.emitted_families.len();

        // `scratch_u16` still holds `0..num_metrics` from `build_univariate_metrics`, so reuse it
        // before overwriting `scratch_u16` with dictionary value indices below.
        let parent_id_arr = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);

        self.scratch_u8.clear();
        self.scratch_u8.resize(num_metrics, 0);
        let key_keys = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);
        let key_dict = DictionaryArray::<UInt8Type>::new(key_keys, metric_attrs_key_dict());

        self.scratch_u8.fill(ATTR_VALUE_TYPE_STR);
        let type_arr = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);

        self.scratch_u16.clear();
        for &fam_idx in &self.emitted_families {
            let dict_key: u16 = match self.families[fam_idx as usize].metric_type {
                MetricType::Unknown => 0,
                MetricType::Counter => 1,
                MetricType::Gauge => 2,
                _ => 0,
            };
            self.scratch_u16.push(dict_key);
        }
        let str_keys = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);
        let str_dict = DictionaryArray::<UInt16Type>::new(str_keys, metric_attrs_type_val_dict());

        Ok(RecordBatch::try_new(
            metric_attrs_schema(),
            vec![
                Arc::new(parent_id_arr),
                Arc::new(key_dict),
                Arc::new(type_arr),
                Arc::new(str_dict),
            ],
        )?)
    }

    fn build_number_data_points(
        &mut self,
        has_default_ts: bool,
    ) -> Result<RecordBatch, DecodeError> {
        let num_dps = self.dp_parent_ids.len();
        let has_id = !self.dp_attr_pairs.is_empty();
        let has_time = !self.dp_timestamps.is_empty();
        let time_nullable = has_time && !has_default_ts && !self.dp_time_valid.is_empty();

        let schema = number_dp_schema(has_id, has_time, time_nullable);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        if has_id {
            self.scratch_u32.clear();
            self.scratch_u32.extend(0..num_dps as u32);
            cols.push(Arc::new(UInt32Array::new(
                scalar_from_slice(&self.scratch_u32),
                None,
            )));
        }

        cols.push(Arc::new(UInt16Array::new(
            scalar_from_slice(&self.dp_parent_ids),
            None,
        )));

        if has_time {
            let nulls = if time_nullable {
                Some(NullBuffer::new(BooleanBuffer::from_iter(
                    self.dp_time_valid.iter().copied(),
                )))
            } else {
                None
            };
            let ts_arr =
                TimestampNanosecondArray::new(scalar_from_slice(&self.dp_timestamps), nulls)
                    .with_timezone(UTC_TIME_ZONE);
            cols.push(Arc::new(ts_arr));
        }

        cols.push(Arc::new(Float64Array::new(
            scalar_from_slice(&self.dp_values),
            None,
        )));

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn build_number_dp_attrs(&mut self) -> Result<RecordBatch, DecodeError> {
        let num_attrs = self.dp_attr_pairs.len();
        let unique_keys = self.key_interner.len();
        let unique_vals = self.val_interner.len();

        let key_enc = if unique_keys <= 256 {
            KeyEncoding::DictU8
        } else if unique_keys <= 65_536 {
            KeyEncoding::DictU16
        } else {
            KeyEncoding::Utf8
        };
        let val_enc = if unique_vals <= 65_536 {
            ValEncoding::DictU16
        } else {
            ValEncoding::Utf8
        };

        let parent_id_arr: ArrayRef = Arc::new(UInt32Array::new(
            scalar_from_slice(&self.dp_attr_parent_ids),
            None,
        ));

        let key_arr: ArrayRef = match key_enc {
            KeyEncoding::DictU8 => {
                self.scratch_u8.clear();
                self.scratch_u8
                    .extend(self.dp_attr_pairs.iter().map(|&(k, _)| k as u8));
                let keys = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);
                let dict = Arc::new(self.key_interner.to_string_array());
                Arc::new(DictionaryArray::<UInt8Type>::new(keys, dict))
            }
            KeyEncoding::DictU16 => {
                self.scratch_u16.clear();
                self.scratch_u16
                    .extend(self.dp_attr_pairs.iter().map(|&(k, _)| k as u16));
                let keys = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);
                let dict = Arc::new(self.key_interner.to_string_array());
                Arc::new(DictionaryArray::<UInt16Type>::new(keys, dict))
            }
            KeyEncoding::Utf8 => Arc::new(materialize_plain_strings(
                &self.key_interner,
                self.dp_attr_pairs.iter().map(|&(k, _)| k),
                &mut self.scratch_i32,
                &mut self.scratch_bytes,
                "NumberDpAttrs.key",
            )?),
        };

        self.scratch_u8.clear();
        self.scratch_u8.resize(num_attrs, ATTR_VALUE_TYPE_STR);
        let type_arr: ArrayRef =
            Arc::new(UInt8Array::new(scalar_from_slice(&self.scratch_u8), None));

        let val_arr: ArrayRef = match val_enc {
            ValEncoding::DictU16 => {
                self.scratch_u16.clear();
                self.scratch_u16
                    .extend(self.dp_attr_pairs.iter().map(|&(_, v)| v as u16));
                let keys = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);
                let dict = Arc::new(self.val_interner.to_string_array());
                Arc::new(DictionaryArray::<UInt16Type>::new(keys, dict))
            }
            ValEncoding::Utf8 => Arc::new(materialize_plain_strings(
                &self.val_interner,
                self.dp_attr_pairs.iter().map(|&(_, v)| v),
                &mut self.scratch_i32,
                &mut self.scratch_bytes,
                "NumberDpAttrs.str",
            )?),
        };

        Ok(RecordBatch::try_new(
            number_dp_attrs_schema(key_enc, val_enc),
            vec![parent_id_arr, key_arr, type_arr, val_arr],
        )?)
    }
}

#[inline]
fn ms_to_ns(timestamp_ms: i64) -> Result<i64, DecodeError> {
    timestamp_ms
        .checked_mul(1_000_000)
        .ok_or(DecodeError::TimestampOverflow { timestamp_ms })
}

#[inline]
fn scalar_from_slice<T: arrow_buffer::ArrowNativeType>(slice: &[T]) -> ScalarBuffer<T> {
    ScalarBuffer::<T>::new(Buffer::from_slice_ref(slice), 0, slice.len())
}

#[inline]
fn string_array_from_slices(
    offsets: &[i32],
    bytes: &[u8],
    nulls: Option<NullBuffer>,
) -> StringArray {
    let offsets_buf = OffsetBuffer::new(scalar_from_slice(offsets));
    let values_buf = Buffer::from_slice_ref(bytes);
    StringArray::new(offsets_buf, values_buf, nulls)
}

#[cold]
fn materialize_plain_strings(
    interner: &StringInterner,
    ids: impl Iterator<Item = u32>,
    scratch_offsets: &mut Vec<i32>,
    scratch_bytes: &mut Vec<u8>,
    table: &'static str,
) -> Result<StringArray, DecodeError> {
    scratch_offsets.clear();
    scratch_bytes.clear();
    scratch_offsets.push(0);
    for id in ids {
        let s = interner.get_bytes(id);
        scratch_bytes.extend_from_slice(s);
        let off =
            i32::try_from(scratch_bytes.len()).map_err(|_| DecodeError::CapacityOverflow {
                table,
                count: scratch_bytes.len(),
            })?;
        scratch_offsets.push(off);
    }
    Ok(string_array_from_slices(
        scratch_offsets,
        scratch_bytes,
        None,
    ))
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_schema::{DataType, TimeUnit};

    use super::*;

    #[test]
    fn empty_and_metadata_only_inputs_produce_empty_records() {
        let mut dec = ArrowDecoder::new();
        for input in [
            b"" as &[u8],
            b"\n\n# just a comment\n",
            b"# HELP orphan_metric A metric with zero samples.\n# TYPE orphan_metric counter\n",
        ] {
            let records = dec
                .decode(input, &Format::TEXT, &DecodeOptions::default())
                .unwrap();
            assert!(records.is_empty());
            assert_eq!(records.num_metrics(), 0);
            assert_eq!(records.num_data_points(), 0);
            assert_eq!(records.iter().count(), 0);
            assert_eq!(records.into_batches().count(), 0);
        }
    }

    #[test]
    fn schema_metadata_mixed_timestamps_and_widened_key_dictionaries() {
        let mut payload = String::from(
            "# HELP req_total Total requests.\n\
             # TYPE req_total counter\n\
             req_total 10\n\
             req_total 12 1700000000500\n",
        );
        // Emit 260 distinct label keys so `NumberDpAttrs.key` widens from UInt8 to UInt16.
        payload.push_str("# TYPE wide_gauge gauge\nwide_gauge{");
        for i in 0..260 {
            if i > 0 {
                payload.push(',');
            }
            payload.push_str(&format!("k_{i}=\"v\""));
        }
        payload.push_str("} 3.5\n");

        let mut dec = ArrowDecoder::new();
        let records = dec
            .decode(payload.as_bytes(), &Format::TEXT, &DecodeOptions::new())
            .unwrap();

        let metrics = records.univariate_metrics().unwrap();
        assert_eq!(
            metrics
                .schema()
                .field_with_name("id")
                .unwrap()
                .metadata()
                .get("encoding")
                .map(String::as_str),
            Some("plain")
        );

        let dps = records.number_data_points().unwrap();
        let dps_schema = dps.schema();
        let ts_field = dps_schema.field_with_name("time_unix_nano").unwrap();
        assert!(ts_field.is_nullable());
        assert_eq!(
            ts_field.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        let ts_col = dps
            .column_by_name("time_unix_nano")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert!(ts_col.is_null(0));
        assert!(!ts_col.is_null(1));
        assert_eq!(ts_col.value(1), 1_700_000_000_500_000_000);
        assert!(ts_col.is_null(2));

        let dp_attrs = records.number_dp_attrs().unwrap();
        assert_eq!(
            dp_attrs
                .schema()
                .field_with_name("key")
                .unwrap()
                .data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
    }

    #[test]
    fn rejects_invalid_metadata_and_duplicate_labels() {
        let mut dec = ArrowDecoder::new();
        let opts = DecodeOptions::default();

        // Duplicate non-empty `# HELP`.
        let err = dec
            .decode(
                b"# HELP m first\n# HELP m second\nm 1\n",
                &Format::TEXT,
                &opts,
            )
            .unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateHelp { ref metric } if metric == "m"));

        // Duplicate `# TYPE`.
        let err = dec
            .decode(
                b"# TYPE m counter\n# TYPE m gauge\nm 1\n",
                &Format::TEXT,
                &opts,
            )
            .unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateOrLateType { ref metric } if metric == "m"));

        // `# TYPE` appearing after a sample has already been emitted.
        let err = dec
            .decode(b"m 1\n# TYPE m counter\n", &Format::TEXT, &opts)
            .unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateOrLateType { ref metric } if metric == "m"));

        // Duplicate label names (adjacent, non-adjacent, or empty-valued) and reserved `__name__`.
        for payload in [
            b"m{a=\"1\",a=\"2\"} 1\n" as &[u8],
            b"m{b=\"1\",a=\"2\",b=\"3\"} 1\n",
            b"m{a=\"\",a=\"\"} 1\n",
            b"m{a=\"1\",a=\"\"} 1\n",
            b"m{a=\"\",a=\"1\"} 1\n",
            b"m{\"a\\nb\"=\"\",\"a\nb\"=\"1\"} 1\n",
            b"m{__name__=\"override\"} 1\n",
            b"m{__name__=\"\"} 1\n",
        ] {
            let err = dec.decode(payload, &Format::TEXT, &opts).unwrap_err();
            assert!(
                matches!(err, DecodeError::DuplicateLabel { .. }),
                "expected DuplicateLabel, got {err:?}"
            );
        }

        // Unsupported metric types until #8 lands.
        let err = dec
            .decode(b"# TYPE h histogram\nh_count 1\n", &Format::TEXT, &opts)
            .unwrap_err();
        assert!(matches!(
            err,
            DecodeError::UnsupportedMetricType {
                metric_type: "histogram",
                ..
            }
        ));

        let err = dec
            .decode(b"# TYPE s summary\ns_count 1\n", &Format::TEXT, &opts)
            .unwrap_err();
        assert!(matches!(
            err,
            DecodeError::UnsupportedMetricType {
                metric_type: "summary",
                ..
            }
        ));

        // Unsupported formats.
        let err = dec.decode(b"m 1\n", &Format::UNKNOWN, &opts).unwrap_err();
        assert!(matches!(err, DecodeError::UnsupportedFormat(_)));

        // Timestamp overflow.
        let err = dec
            .decode(b"m 1 9223372036855\n", &Format::TEXT, &opts)
            .unwrap_err();
        assert!(matches!(
            err,
            DecodeError::TimestampOverflow {
                timestamp_ms: 9_223_372_036_855
            }
        ));
    }

    #[test]
    fn empty_label_values_match_missing_labels() {
        let mut dec = ArrowDecoder::new();
        let opts = DecodeOptions::default();

        let bare = dec.decode(b"m 1\n", &Format::TEXT, &opts).unwrap();
        let with_empty = dec
            .decode(b"m{a=\"\",b=\"\"} 1\n", &Format::TEXT, &opts)
            .unwrap();
        assert_eq!(with_empty, bare);
        assert!(with_empty.number_dp_attrs().is_none());

        let mixed = dec
            .decode(b"m{a=\"\",b=\"x\"} 1\n", &Format::TEXT, &opts)
            .unwrap();
        let dp_attrs = mixed.number_dp_attrs().unwrap();
        assert_eq!(dp_attrs.num_rows(), 1);
        let key_dict = dp_attrs
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .unwrap();
        let key_vals = key_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_vals.len(), 1);
        assert_eq!(key_vals.value(0), "b");

        let val_dict = dp_attrs
            .column_by_name("str")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .unwrap();
        let str_vals = val_dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(str_vals.len(), 1);
        assert_eq!(str_vals.value(0), "x");
    }
}
