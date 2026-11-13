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

//! Layer 2 conversion from Layer 1 OTAP metric records into `io.prometheus.client.MetricFamily`
//! DTOs.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float64Type, TimestampNanosecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, DictionaryArray, FixedSizeBinaryArray, ListArray,
    PrimitiveArray, RecordBatch, StringArray, StructArray, UInt16Array, UInt32Array,
};
use arrow_schema::{ArrowError, DataType};

use super::{
    Bucket, Counter, Exemplar, Gauge, Histogram, LabelPair, Metric, MetricFamily, MetricType,
    Quantile, Summary, Untyped,
};
use crate::arrow::{
    ArrowDecoder, DecodeError, DecodeOptions, OtapMetricRecords, PROMETHEUS_TYPE_METADATA_KEY,
};
use crate::format::Format;

/// Decodes an exposition payload of the given `Format` into a list of `MetricFamily` DTOs.
///
/// Use [`ArrowDecoder`] with [`arrow_to_metric_families`] directly to reuse decoder buffers across
/// scrapes.
///
/// # Errors
///
/// Returns [`DecodeError`] under the same conditions as [`ArrowDecoder::decode`].
pub fn decode_to_metric_families(
    input: &[u8],
    format: Format,
    opts: &DecodeOptions,
) -> Result<Vec<MetricFamily>, DecodeError> {
    let records = ArrowDecoder::new().decode(input, &format, opts)?;
    arrow_to_metric_families(&records)
}

/// Converts Arrow / OTAP metric records decoded by [`ArrowDecoder`] into `MetricFamily` DTOs.
///
/// Families keep exposition order and take their name from `UnivariateMetrics.name`, so
/// non-default [`crate::arrow::CounterSuffixPolicy`] and [`crate::arrow::UnitSuffixPolicy`] values
/// change it. Histogram buckets become cumulative counts ending in `+Inf`, each histogram
/// exemplar goes on the first bucket covering its value (a later exemplar replaces an earlier one
/// in the same bucket), exemplar `trace_id` and `span_id` become hex labels, and OpenMetrics
/// `info` and `stateset` families become `GAUGE`. Labels are sorted by name.
///
/// # Errors
///
/// Returns [`DecodeError::Arrow`] if a table has a missing or mistyped column, or a data point
/// `parent_id` that references no metric.
pub fn arrow_to_metric_families(
    records: &OtapMetricRecords,
) -> Result<Vec<MetricFamily>, DecodeError> {
    let Some(metrics) = records.univariate_metrics() else {
        return Ok(Vec::new());
    };

    let names = utf8(metrics, "name")?;
    let descriptions = optional_utf8(metrics, "description")?;
    let units = optional_utf8(metrics, "unit")?;
    let prom_types = metric_prometheus_types(records.metric_attrs())?;

    // `ArrowDecoder` assigns `UnivariateMetrics.id` by row index.
    let mut families = Vec::with_capacity(metrics.num_rows());
    for row in 0..metrics.num_rows() {
        let prom_type = prom_types.get(row).copied().flatten();
        families.push(MetricFamily {
            name: names.value(row).to_owned(),
            help: optional_value(descriptions, row).to_owned(),
            r#type: metric_type(prom_type) as i32,
            unit: optional_value(units, row).to_owned(),
            metric: Vec::new(),
        });
    }

    if let Some(dps) = records.number_data_points() {
        let mut labels = labels_by_parent(records.number_dp_attrs())?;
        let has_counter = families.iter().any(|f| f.r#type() == MetricType::Counter);
        let exemplars = if has_counter {
            exemplars(
                records.number_dp_exemplars(),
                records.number_dp_exemplar_attrs(),
            )?
        } else {
            Vec::new()
        };
        append_number_data_points(dps, &mut labels, &exemplars, &mut families)?;
    }
    if let Some(dps) = records.summary_data_points() {
        let mut labels = labels_by_parent(records.summary_dp_attrs())?;
        append_summary_data_points(dps, &mut labels, &mut families)?;
    }
    if let Some(dps) = records.histogram_data_points() {
        let mut labels = labels_by_parent(records.histogram_dp_attrs())?;
        let exemplars = exemplars(
            records.histogram_dp_exemplars(),
            records.histogram_dp_exemplar_attrs(),
        )?;
        append_histogram_data_points(dps, &mut labels, exemplars, &mut families)?;
    }

    Ok(families)
}

fn metric_type(prom_type: Option<&str>) -> MetricType {
    match prom_type {
        Some("counter") => MetricType::Counter,
        Some("gauge" | "info" | "stateset") => MetricType::Gauge,
        Some("histogram") => MetricType::Histogram,
        Some("gaugehistogram") => MetricType::GaugeHistogram,
        Some("summary") => MetricType::Summary,
        _ => MetricType::Untyped,
    }
}

fn metric_prometheus_types(attrs: Option<&RecordBatch>) -> Result<Vec<Option<&str>>, DecodeError> {
    let Some(attrs) = attrs else {
        return Ok(Vec::new());
    };
    let parent_ids = primitive::<UInt16Type>(attrs, "parent_id")?;
    let keys = StrColumn::new(attrs, "key")?;
    let vals = StrColumn::new(attrs, "str")?;
    let mut types = Vec::new();
    for row in 0..attrs.num_rows() {
        if keys.value(row) != Some(PROMETHEUS_TYPE_METADATA_KEY) {
            continue;
        }
        let parent = usize::from(parent_ids.value(row));
        if types.len() <= parent {
            types.resize(parent + 1, None);
        }
        types[parent] = vals.value(row);
    }
    Ok(types)
}

/// Returns sorted labels indexed by `parent_id`.
fn labels_by_parent(attrs: Option<&RecordBatch>) -> Result<Vec<Vec<LabelPair>>, DecodeError> {
    let mut labels: Vec<Vec<LabelPair>> = Vec::new();
    let Some(attrs) = attrs else {
        return Ok(labels);
    };
    let parent_ids = primitive::<UInt32Type>(attrs, "parent_id")?;
    let keys = StrColumn::new(attrs, "key")?;
    let vals = StrColumn::new(attrs, "str")?;
    for row in 0..attrs.num_rows() {
        let parent = parent_ids.value(row) as usize;
        if labels.len() <= parent {
            labels.resize_with(parent + 1, Vec::new);
        }
        labels[parent].push(LabelPair {
            name: keys.value(row).unwrap_or_default().to_owned(),
            value: vals.value(row).unwrap_or_default().to_owned(),
        });
    }
    for set in &mut labels {
        set.sort_by(|a, b| a.name.cmp(&b.name));
    }
    Ok(labels)
}

/// Returns exemplars indexed by `parent_id`.
fn exemplars(
    exemplars: Option<&RecordBatch>,
    exemplar_attrs: Option<&RecordBatch>,
) -> Result<Vec<Vec<Exemplar>>, DecodeError> {
    let mut by_dp: Vec<Vec<Exemplar>> = Vec::new();
    let Some(exemplars) = exemplars else {
        return Ok(by_dp);
    };
    let ex_ids = optional_primitive::<UInt32Type>(exemplars, "id")?;
    let parent_ids = primitive::<UInt32Type>(exemplars, "parent_id")?;
    let times = primitive::<TimestampNanosecondType>(exemplars, "time_unix_nano")?;
    let values = primitive::<Float64Type>(exemplars, "double_value")?;
    let span_ids = optional_fixed_binary(exemplars, "span_id")?;
    let trace_ids = optional_fixed_binary(exemplars, "trace_id")?;
    let mut attrs = labels_by_parent(exemplar_attrs)?;

    for row in 0..exemplars.num_rows() {
        let mut label = ex_ids
            .and_then(|ids| attrs.get_mut(ids.value(row) as usize))
            .map(std::mem::take)
            .unwrap_or_default();
        if let Some(span_id) = span_ids.filter(|c| c.is_valid(row)) {
            label.push(LabelPair {
                name: "span_id".to_owned(),
                value: hex(span_id.value(row)),
            });
        }
        if let Some(trace_id) = trace_ids.filter(|c| c.is_valid(row)) {
            label.push(LabelPair {
                name: "trace_id".to_owned(),
                value: hex(trace_id.value(row)),
            });
        }
        label.sort_by(|a, b| a.name.cmp(&b.name));

        let ts_ns = times.value(row);
        let parent = parent_ids.value(row) as usize;
        if by_dp.len() <= parent {
            by_dp.resize_with(parent + 1, Vec::new);
        }
        by_dp[parent].push(Exemplar {
            label,
            value: values.value(row),
            timestamp: (ts_ns != 0).then(|| ns_to_timestamp(ts_ns)),
        });
    }
    Ok(by_dp)
}

/// Columns shared by number, summary, and histogram data point tables.
struct DataPointColumns<'a> {
    ids: Option<&'a UInt32Array>,
    parent_ids: &'a UInt16Array,
    start_times: Option<&'a PrimitiveArray<TimestampNanosecondType>>,
    times: Option<&'a PrimitiveArray<TimestampNanosecondType>>,
}

impl<'a> DataPointColumns<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self, DecodeError> {
        Ok(Self {
            ids: optional_primitive::<UInt32Type>(batch, "id")?,
            parent_ids: primitive::<UInt16Type>(batch, "parent_id")?,
            start_times: optional_primitive::<TimestampNanosecondType>(
                batch,
                "start_time_unix_nano",
            )?,
            times: optional_primitive::<TimestampNanosecondType>(batch, "time_unix_nano")?,
        })
    }

    fn family<'f>(
        &self,
        row: usize,
        families: &'f mut [MetricFamily],
    ) -> Result<&'f mut MetricFamily, DecodeError> {
        let parent = self.parent_ids.value(row);
        families
            .get_mut(usize::from(parent))
            .ok_or_else(|| invalid(format!("data point parent_id {parent} has no metric")))
    }

    fn id(&self, row: usize) -> Option<usize> {
        self.ids.map(|ids| ids.value(row) as usize)
    }

    fn take_labels(&self, row: usize, labels: &mut [Vec<LabelPair>]) -> Vec<LabelPair> {
        self.id(row)
            .and_then(|id| labels.get_mut(id))
            .map(std::mem::take)
            .unwrap_or_default()
    }

    fn start_timestamp(&self, row: usize) -> Option<prost_types::Timestamp> {
        self.start_times
            .filter(|c| c.is_valid(row))
            .map(|c| ns_to_timestamp(c.value(row)))
    }

    fn timestamp_ms(&self, row: usize) -> i64 {
        self.times
            .filter(|c| c.is_valid(row))
            .map_or(0, |c| c.value(row).div_euclid(1_000_000))
    }
}

fn append_number_data_points(
    dps: &RecordBatch,
    labels: &mut [Vec<LabelPair>],
    exemplars: &[Vec<Exemplar>],
    families: &mut [MetricFamily],
) -> Result<(), DecodeError> {
    let cols = DataPointColumns::new(dps)?;
    let values = primitive::<Float64Type>(dps, "double_value")?;
    for row in 0..dps.num_rows() {
        let family = cols.family(row, families)?;
        let value = values.value(row);
        let mut metric = Metric {
            label: cols.take_labels(row, labels),
            timestamp_ms: cols.timestamp_ms(row),
            ..Default::default()
        };
        match family.r#type() {
            MetricType::Counter => {
                metric.counter = Some(Counter {
                    value,
                    exemplar: cols
                        .id(row)
                        .and_then(|id| exemplars.get(id))
                        .and_then(|ex| ex.first())
                        .cloned(),
                    start_timestamp: cols.start_timestamp(row),
                });
            }
            MetricType::Gauge => metric.gauge = Some(Gauge { value }),
            _ => metric.untyped = Some(Untyped { value }),
        }
        family.metric.push(metric);
    }
    Ok(())
}

fn append_summary_data_points(
    dps: &RecordBatch,
    labels: &mut [Vec<LabelPair>],
    families: &mut [MetricFamily],
) -> Result<(), DecodeError> {
    let cols = DataPointColumns::new(dps)?;
    let counts = primitive::<UInt64Type>(dps, "count")?;
    let sums = primitive::<Float64Type>(dps, "sum")?;
    let quantiles = list(dps, "quantile")?;
    let quantile_items: &StructArray = quantiles
        .values()
        .as_struct_opt()
        .ok_or_else(|| invalid("SummaryDataPoints.quantile items must be Struct".to_owned()))?;
    let q_bounds = struct_field::<Float64Type>(quantile_items, "quantile")?;
    let q_values = struct_field::<Float64Type>(quantile_items, "value")?;

    for row in 0..dps.num_rows() {
        let family = cols.family(row, families)?;
        let range = list_range(quantiles, row);
        family.metric.push(Metric {
            label: cols.take_labels(row, labels),
            summary: Some(Summary {
                sample_count: nullable_value(counts, row),
                sample_sum: nullable_value(sums, row),
                quantile: range
                    .map(|i| Quantile {
                        quantile: q_bounds.value(i),
                        value: q_values.value(i),
                    })
                    .collect(),
                start_timestamp: cols.start_timestamp(row),
            }),
            timestamp_ms: cols.timestamp_ms(row),
            ..Default::default()
        });
    }
    Ok(())
}

fn append_histogram_data_points(
    dps: &RecordBatch,
    labels: &mut [Vec<LabelPair>],
    mut exemplars: Vec<Vec<Exemplar>>,
    families: &mut [MetricFamily],
) -> Result<(), DecodeError> {
    let cols = DataPointColumns::new(dps)?;
    let counts = primitive::<UInt64Type>(dps, "count")?;
    let sums = primitive::<Float64Type>(dps, "sum")?;
    let bucket_counts = list(dps, "bucket_counts")?;
    let bucket_count_values = list_values::<UInt64Type>(bucket_counts, "bucket_counts")?;
    let bounds = list(dps, "explicit_bounds")?;
    let bound_values = list_values::<Float64Type>(bounds, "explicit_bounds")?;

    for row in 0..dps.num_rows() {
        let family = cols.family(row, families)?;

        let mut bucket = Vec::with_capacity(list_range(bucket_counts, row).len());
        let mut cumulative = 0u64;
        let mut bounds_iter = list_range(bounds, row).map(|i| bound_values.value(i));
        for i in list_range(bucket_counts, row) {
            cumulative = cumulative.saturating_add(bucket_count_values.value(i));
            bucket.push(Bucket {
                cumulative_count: cumulative,
                upper_bound: bounds_iter.next().unwrap_or(f64::INFINITY),
                ..Default::default()
            });
        }

        let dp_exemplars = cols
            .id(row)
            .and_then(|id| exemplars.get_mut(id))
            .map(std::mem::take)
            .unwrap_or_default();
        // A later exemplar replaces an earlier one in the same bucket, as in client_golang.
        for exemplar in dp_exemplars {
            if let Some(b) = bucket.iter_mut().find(|b| exemplar.value <= b.upper_bound) {
                b.exemplar = Some(exemplar);
            }
        }

        family.metric.push(Metric {
            label: cols.take_labels(row, labels),
            histogram: Some(Histogram {
                sample_count: nullable_value(counts, row),
                sample_sum: nullable_value(sums, row),
                bucket,
                start_timestamp: cols.start_timestamp(row),
                ..Default::default()
            }),
            timestamp_ms: cols.timestamp_ms(row),
            ..Default::default()
        });
    }
    Ok(())
}

/// Read access to a Utf8 or `Dictionary(UInt8 | UInt16, Utf8)` string column.
enum StrColumn<'a> {
    Plain(&'a StringArray),
    DictU8(&'a DictionaryArray<UInt8Type>, &'a StringArray),
    DictU16(&'a DictionaryArray<UInt16Type>, &'a StringArray),
}

impl<'a> StrColumn<'a> {
    fn new(batch: &'a RecordBatch, name: &str) -> Result<Self, DecodeError> {
        let col = column(batch, name)?;
        match col.data_type() {
            DataType::Utf8 => Ok(Self::Plain(col.as_string::<i32>())),
            DataType::Dictionary(key, value) if **value == DataType::Utf8 => match **key {
                DataType::UInt8 => {
                    let dict = col.as_dictionary::<UInt8Type>();
                    Ok(Self::DictU8(dict, dict.values().as_string::<i32>()))
                }
                DataType::UInt16 => {
                    let dict = col.as_dictionary::<UInt16Type>();
                    Ok(Self::DictU16(dict, dict.values().as_string::<i32>()))
                }
                _ => Err(unexpected_type(name, col)),
            },
            _ => Err(unexpected_type(name, col)),
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Plain(arr) => arr.is_valid(row).then(|| arr.value(row)),
            Self::DictU8(dict, values) => dict
                .key(row)
                .filter(|&k| values.is_valid(k))
                .map(|k| values.value(k)),
            Self::DictU16(dict, values) => dict
                .key(row)
                .filter(|&k| values.is_valid(k))
                .map(|k| values.value(k)),
        }
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, DecodeError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| invalid(format!("missing column {name:?}")))
}

fn primitive<'a, T: ArrowPrimitiveType>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a PrimitiveArray<T>, DecodeError> {
    let col = column(batch, name)?;
    col.as_primitive_opt::<T>()
        .ok_or_else(|| unexpected_type(name, col))
}

fn optional_primitive<'a, T: ArrowPrimitiveType>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a PrimitiveArray<T>>, DecodeError> {
    match batch.column_by_name(name) {
        Some(_) => primitive::<T>(batch, name).map(Some),
        None => Ok(None),
    }
}

fn utf8<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, DecodeError> {
    let col = column(batch, name)?;
    col.as_string_opt::<i32>()
        .ok_or_else(|| unexpected_type(name, col))
}

fn optional_utf8<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a StringArray>, DecodeError> {
    match batch.column_by_name(name) {
        Some(_) => utf8(batch, name).map(Some),
        None => Ok(None),
    }
}

fn optional_value(col: Option<&StringArray>, row: usize) -> &str {
    col.filter(|c| c.is_valid(row)).map_or("", |c| c.value(row))
}

fn optional_fixed_binary<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<Option<&'a FixedSizeBinaryArray>, DecodeError> {
    match batch.column_by_name(name) {
        Some(col) => col
            .as_fixed_size_binary_opt()
            .map(Some)
            .ok_or_else(|| unexpected_type(name, col)),
        None => Ok(None),
    }
}

fn list<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ListArray, DecodeError> {
    let col = column(batch, name)?;
    col.as_list_opt::<i32>()
        .ok_or_else(|| unexpected_type(name, col))
}

fn list_values<'a, T: ArrowPrimitiveType>(
    list: &'a ListArray,
    name: &str,
) -> Result<&'a PrimitiveArray<T>, DecodeError> {
    list.values()
        .as_primitive_opt::<T>()
        .ok_or_else(|| unexpected_type(name, list.values()))
}

fn list_range(list: &ListArray, row: usize) -> std::ops::Range<usize> {
    let offsets = list.value_offsets();
    offsets[row] as usize..offsets[row + 1] as usize
}

fn struct_field<'a, T: ArrowPrimitiveType>(
    items: &'a StructArray,
    name: &str,
) -> Result<&'a PrimitiveArray<T>, DecodeError> {
    let col = items
        .column_by_name(name)
        .ok_or_else(|| invalid(format!("missing struct field {name:?}")))?;
    col.as_primitive_opt::<T>()
        .ok_or_else(|| unexpected_type(name, col))
}

fn nullable_value<T>(col: &PrimitiveArray<T>, row: usize) -> T::Native
where
    T: ArrowPrimitiveType,
{
    if col.is_valid(row) {
        col.value(row)
    } else {
        T::Native::default()
    }
}

fn ns_to_timestamp(ns: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: ns.div_euclid(1_000_000_000),
        nanos: ns.rem_euclid(1_000_000_000) as i32,
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 0x0f)]));
    }
    out
}

fn invalid(message: String) -> DecodeError {
    DecodeError::Arrow(ArrowError::InvalidArgumentError(message))
}

fn unexpected_type(name: &str, col: &dyn Array) -> DecodeError {
    invalid(format!(
        "unexpected data type {} for column {name:?}",
        col.data_type()
    ))
}
