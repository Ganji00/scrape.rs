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
//! thread-synchronized state. Callers can materialize a full scrape with [`ArrowDecoder::decode`]
//! or pull bounded chunks with [`ArrowDecoder::decode_stream`] so single-threaded `!Send` scrape
//! loops keep scratch memory bounded and stay within OTAP's `u16` per-batch metric ID limit.

mod intern;
mod records;
mod schema;

use std::iter::FusedIterator;
use std::sync::Arc;

use arrow_array::types::{UInt8Type, UInt16Type};
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int32Array,
    ListArray, RecordBatch, StringArray, StructArray, TimestampNanosecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, SchemaRef};

use crate::format::{Format, FormatType};
use crate::parse::{
    Entry, LabelSpan, METRIC_NAME_LABEL, MetricType, OmScratch, OpenMetricsParser, ParseError,
    Parser, PromParser, parse_go_float, unescape_bytes_into,
};

use self::intern::StringInterner;
pub use self::records::{MetricPayloadType, OtapMetricRecords};
pub use self::schema::PROMETHEUS_TYPE_METADATA_KEY;
use self::schema::{
    AGGREGATION_TEMPORALITY_CUMULATIVE, ATTR_VALUE_TYPE_STR, KeyEncoding, OTAP_METRIC_TYPE_EMPTY,
    OTAP_METRIC_TYPE_GAUGE, OTAP_METRIC_TYPE_HISTOGRAM, OTAP_METRIC_TYPE_SUM,
    OTAP_METRIC_TYPE_SUMMARY, UTC_TIME_ZONE, ValEncoding, dp_attrs_schema, exemplars_schema,
    histogram_bucket_counts_item_field, histogram_dp_schema, histogram_explicit_bounds_item_field,
    metric_attrs_key_dict, metric_attrs_schema, metric_attrs_schema_for,
    metric_attrs_type_val_dict, number_dp_schema, summary_dp_schema, summary_quantile_item_field,
    summary_quantile_struct_fields, univariate_metrics_schema,
};

/// Default upper bound on data points emitted per [`OtapMetricRecords`] chunk in
/// [`ArrowDecoder::decode_stream`].
pub const DEFAULT_MAX_DATA_POINTS_PER_BATCH: usize = 16_384;

/// Maximum distinct `UnivariateMetrics` rows representable in a single OTAP batch (`u16` IDs).
const MAX_METRICS_PER_BATCH: usize = (u16::MAX as usize) + 1;

/// Policy controlling how counter `_total` suffixes are emitted on `UnivariateMetrics.name`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CounterSuffixPolicy {
    /// Preserve or append the `_total` suffix on counter metric names (`http_requests_total`).
    #[default]
    Keep,
    /// Strip a trailing `_total` suffix from counter metric names (`http_requests`).
    Strip,
}

/// Policy controlling how `# UNIT` suffixes are handled on `UnivariateMetrics.name`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UnitSuffixPolicy {
    /// Keep the unit suffix on the metric name (`request_duration_seconds`) while also populating
    /// `UnivariateMetrics.unit`.
    #[default]
    Keep,
    /// Strip a trailing `_<unit>` suffix from the metric name (`request_duration`) when `# UNIT`
    /// declares `<unit>`.
    Strip,
}

/// Policy controlling how OpenMetrics `info` metric families are projected into OTAP tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InfoPolicy {
    /// Project `info` series as a `Gauge` metric with `NumberDataPoints` value `1.0` and series
    /// labels in `NumberDpAttrs`.
    #[default]
    Gauge,
    /// Project `info` series labels directly onto `MetricAttrs` (`UnivariateMetrics` `Empty` type
    /// with `Metric.metadata` attributes in OTLP) without emitting `NumberDataPoints`.
    MetricAttrs,
    /// Drop `info` metric families from the decoded output.
    Drop,
}

/// Policy controlling how OpenMetrics `stateset` metric families are projected into OTAP tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatesetPolicy {
    /// Project `stateset` series as a `Gauge` metric with `NumberDataPoints` (`0.0` or `1.0`).
    #[default]
    Gauge,
    /// Project `stateset` series as a non-monotonic cumulative `Sum` metric (`is_monotonic =
    /// false`, `aggregation_temporality = Cumulative`).
    NonMonotonicSum,
    /// Drop `stateset` metric families from the decoded output.
    Drop,
}

/// Policy controlling how untyped (`unknown`) metric families are projected into OTAP tables.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UntypedPolicy {
    /// Project untyped series as a `Gauge` metric.
    #[default]
    Gauge,
    /// Project untyped series whose metric name ends with `_total` as a monotonic cumulative `Sum`
    /// (`counter`) and all other untyped series as a `Gauge`.
    InferFromSuffix,
    /// Drop untyped (`unknown`) metric families from the decoded output.
    Drop,
}

/// Options controlling [`ArrowDecoder::decode`] and [`ArrowDecoder::decode_stream`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeOptions {
    /// Default timestamp in milliseconds since the Unix epoch applied to samples that omit an
    /// explicit wire timestamp. When `None`, untimestamped samples leave `time_unix_nano` unset.
    pub timestamp_ms: Option<i64>,
    /// Maximum number of data points materialized per [`OtapMetricRecords`] chunk yielded by
    /// [`ArrowDecoder::decode_stream`]. Values of `0` are clamped to `1`. Single-batch
    /// [`ArrowDecoder::decode`] ignores this limit.
    pub max_data_points_per_batch: usize,
    /// Policy for counter `_total` suffixes on `UnivariateMetrics.name`.
    pub counter_suffix_policy: CounterSuffixPolicy,
    /// Policy for `# UNIT` suffixes on `UnivariateMetrics.name`.
    pub unit_suffix_policy: UnitSuffixPolicy,
    /// Projection policy for OpenMetrics `info` metric families.
    pub info_policy: InfoPolicy,
    /// Projection policy for OpenMetrics `stateset` metric families.
    pub stateset_policy: StatesetPolicy,
    /// Projection policy for untyped (`unknown`) metric families.
    pub untyped_policy: UntypedPolicy,
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeOptions {
    /// Creates default [`DecodeOptions`] with no fallback timestamp,
    /// [`DEFAULT_MAX_DATA_POINTS_PER_BATCH`] data points per streamed batch, and default
    /// translation policies.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timestamp_ms: None,
            max_data_points_per_batch: DEFAULT_MAX_DATA_POINTS_PER_BATCH,
            counter_suffix_policy: CounterSuffixPolicy::Keep,
            unit_suffix_policy: UnitSuffixPolicy::Keep,
            info_policy: InfoPolicy::Gauge,
            stateset_policy: StatesetPolicy::Gauge,
            untyped_policy: UntypedPolicy::Gauge,
        }
    }

    /// Sets the fallback timestamp in milliseconds for samples without an explicit wire timestamp.
    #[must_use]
    pub const fn with_timestamp_ms(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }

    /// Sets the maximum number of data points materialized per batch in
    /// [`ArrowDecoder::decode_stream`].
    #[must_use]
    pub const fn with_max_data_points_per_batch(
        mut self,
        max_data_points_per_batch: usize,
    ) -> Self {
        self.max_data_points_per_batch = max_data_points_per_batch;
        self
    }

    /// Sets the [`CounterSuffixPolicy`] for counter metric names.
    #[must_use]
    pub const fn with_counter_suffix_policy(mut self, policy: CounterSuffixPolicy) -> Self {
        self.counter_suffix_policy = policy;
        self
    }

    /// Sets the [`UnitSuffixPolicy`] for `# UNIT` suffixes on metric names.
    #[must_use]
    pub const fn with_unit_suffix_policy(mut self, policy: UnitSuffixPolicy) -> Self {
        self.unit_suffix_policy = policy;
        self
    }

    /// Sets the [`InfoPolicy`] for OpenMetrics `info` metric families.
    #[must_use]
    pub const fn with_info_policy(mut self, policy: InfoPolicy) -> Self {
        self.info_policy = policy;
        self
    }

    /// Sets the [`StatesetPolicy`] for OpenMetrics `stateset` metric families.
    #[must_use]
    pub const fn with_stateset_policy(mut self, policy: StatesetPolicy) -> Self {
        self.stateset_policy = policy;
        self
    }

    /// Sets the [`UntypedPolicy`] for untyped (`unknown`) metric families.
    #[must_use]
    pub const fn with_untyped_policy(mut self, policy: UntypedPolicy) -> Self {
        self.untyped_policy = policy;
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

    /// OpenMetrics payload did not end with `# EOF`.
    #[error("data does not end with # EOF")]
    MissingEof,

    /// OpenMetrics payload contained trailing data after `# EOF`.
    #[error("unexpected data after # EOF at byte {offset}")]
    DataAfterEof {
        /// Byte offset of the first byte after the `# EOF` line.
        offset: usize,
    },

    /// `# UNIT` declared a unit that is not a suffix of the metric name.
    #[error("unit {unit:?} is not a suffix of metric {metric:?}")]
    InvalidUnit {
        /// Metric family name declared in `# UNIT`.
        metric: String,
        /// Unit declared in `# UNIT`.
        unit: String,
    },

    /// A second `# HELP` line appeared, or `# HELP` appeared after samples for the same family.
    #[error("second HELP line for metric name {metric:?}, or HELP reported after samples")]
    DuplicateOrLateHelp {
        /// Metric family name.
        metric: String,
    },

    /// A second `# TYPE` line appeared, or `# TYPE` appeared after samples for the same family.
    #[error("second TYPE line for metric name {metric:?}, or TYPE reported after samples")]
    DuplicateOrLateType {
        /// Metric family name.
        metric: String,
    },

    /// A second `# UNIT` line appeared, or `# UNIT` appeared after samples for the same family.
    #[error("second UNIT line for metric name {metric:?}, or UNIT reported after samples")]
    DuplicateOrLateUnit {
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
            ParseError::MissingEof => Self::MissingEof,
            ParseError::DataAfterEof { offset } => Self::DataAfterEof { offset },
            ParseError::InvalidUnit { metric, unit } => Self::InvalidUnit { metric, unit },
        }
    }
}

/// Role of a series line within the active [`CurrentFamily`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SeriesRole {
    Scalar,
    CounterCreated,
    HistogramBucket,
    HistogramSum,
    HistogramCount,
    HistogramCreated,
    SummaryQuantile,
    SummarySum,
    SummaryCount,
    SummaryCreated,
}

/// Active contiguous metric-family cursor.
///
/// Tracking only the current contiguous family avoids maintaining a whole-scrape metric-name hash
/// table, lets `UnivariateMetrics` names, descriptions, and units append straight into Arrow
/// `(offsets, values)` buffers on first sample emission, and preserves active family metadata
/// across chunk flushes.
#[derive(Default)]
struct CurrentFamily {
    active: bool,
    raw_name: Vec<u8>,
    unescaped_name: Vec<u8>,
    metric_type: MetricType,
    has_type: bool,
    has_help: bool,
    has_unit: bool,
    has_samples: bool,
    saw_total_suffix: bool,
    help: Vec<u8>,
    unit: Vec<u8>,
    batch_id: Option<u16>,
}

impl CurrentFamily {
    fn with_capacity(name_cap: usize, help_cap: usize) -> Self {
        Self {
            raw_name: Vec::with_capacity(name_cap),
            unescaped_name: Vec::with_capacity(name_cap),
            help: Vec::with_capacity(help_cap),
            unit: Vec::with_capacity(16),
            ..Self::default()
        }
    }

    fn clear(&mut self) {
        self.active = false;
        self.raw_name.clear();
        self.unescaped_name.clear();
        self.metric_type = MetricType::Unknown;
        self.has_type = false;
        self.has_help = false;
        self.has_unit = false;
        self.has_samples = false;
        self.saw_total_suffix = false;
        self.help.clear();
        self.unit.clear();
        self.batch_id = None;
    }

    fn retained_memory_bytes(&self) -> usize {
        self.raw_name.capacity()
            + self.unescaped_name.capacity()
            + self.help.capacity()
            + self.unit.capacity()
    }

    #[cold]
    fn name_string(&self) -> String {
        String::from_utf8_lossy(&self.unescaped_name).into_owned()
    }

    fn start_new(&mut self, raw_name: &[u8], unescaped: &[u8]) {
        self.active = true;
        self.raw_name.clear();
        self.raw_name.extend_from_slice(raw_name);
        self.unescaped_name.clear();
        self.unescaped_name.extend_from_slice(unescaped);
        self.metric_type = MetricType::Unknown;
        self.has_type = false;
        self.has_help = false;
        self.has_unit = false;
        self.has_samples = false;
        self.saw_total_suffix = unescaped.len() > 6 && unescaped.ends_with(b"_total");
        self.help.clear();
        self.unit.clear();
        self.batch_id = None;
    }

    #[inline]
    fn classify_series(&mut self, series_name: &[u8]) -> Option<SeriesRole> {
        if !self.active {
            return None;
        }
        let fam = self.unescaped_name.as_slice();
        match self.metric_type {
            MetricType::Histogram => {
                if let Some(suffix) = series_name.strip_prefix(fam) {
                    return match suffix {
                        b"_bucket" => Some(SeriesRole::HistogramBucket),
                        b"_sum" => Some(SeriesRole::HistogramSum),
                        b"_count" => Some(SeriesRole::HistogramCount),
                        b"_created" => Some(SeriesRole::HistogramCreated),
                        _ => None,
                    };
                }
                None
            }
            MetricType::GaugeHistogram => {
                if let Some(suffix) = series_name.strip_prefix(fam) {
                    return match suffix {
                        b"_bucket" => Some(SeriesRole::HistogramBucket),
                        b"_gsum" | b"_sum" => Some(SeriesRole::HistogramSum),
                        b"_gcount" | b"_count" => Some(SeriesRole::HistogramCount),
                        b"_created" => Some(SeriesRole::HistogramCreated),
                        _ => None,
                    };
                }
                None
            }
            MetricType::Summary => {
                if series_name == fam {
                    return Some(SeriesRole::SummaryQuantile);
                }
                if let Some(suffix) = series_name.strip_prefix(fam) {
                    return match suffix {
                        b"_sum" => Some(SeriesRole::SummarySum),
                        b"_count" => Some(SeriesRole::SummaryCount),
                        b"_created" => Some(SeriesRole::SummaryCreated),
                        _ => None,
                    };
                }
                None
            }
            MetricType::Counter => {
                if series_name == fam {
                    if series_name.len() > 6 && series_name.ends_with(b"_total") {
                        self.saw_total_suffix = true;
                    }
                    return Some(SeriesRole::Scalar);
                }
                if let Some(suffix) = series_name.strip_prefix(fam) {
                    if suffix == b"_total" {
                        self.saw_total_suffix = true;
                        return Some(SeriesRole::Scalar);
                    }
                    if suffix == b"_created" {
                        return Some(SeriesRole::CounterCreated);
                    }
                }
                if let Some(base) = fam.strip_suffix(b"_total")
                    && !base.is_empty()
                    && let Some(suffix) = series_name.strip_prefix(base)
                    && suffix == b"_created"
                {
                    return Some(SeriesRole::CounterCreated);
                }
                None
            }
            MetricType::Info => {
                if series_name == fam {
                    return Some(SeriesRole::Scalar);
                }
                if let Some(b"_info") = series_name.strip_prefix(fam) {
                    return Some(SeriesRole::Scalar);
                }
                None
            }
            MetricType::Gauge | MetricType::Stateset | MetricType::Unknown => {
                (series_name == fam).then_some(SeriesRole::Scalar)
            }
        }
    }
}

/// Assembled data-point group within the active `Histogram`, `GaugeHistogram`, or `Summary` family.
#[derive(Clone, Copy, Default)]
struct PendingDpGroup {
    attr_start: u32,
    attr_len: u32,
    start_ts_ns: Option<i64>,
    ts_ns: Option<i64>,
    count: Option<u64>,
    sum: Option<f64>,
}

/// Staged exemplar within an active `Histogram` or `GaugeHistogram` family.
#[derive(Clone, Copy)]
struct PendingExemplar {
    group_idx: u32,
    ts_ns: i64,
    value: f64,
    span_id: Option<[u8; 8]>,
    trace_id: Option<[u8; 16]>,
    attr_start: u32,
    attr_len: u32,
}

type SpanAndTraceId = (Option<[u8; 8]>, Option<[u8; 16]>);

enum ActiveParser<'input> {
    Prom(PromParser<'input>),
    OpenMetrics(OpenMetricsParser<'input>),
    Invalid,
}

impl<'input> ActiveParser<'input> {
    #[inline]
    fn is_openmetrics(&self) -> bool {
        matches!(self, Self::OpenMetrics(_))
    }

    #[inline]
    fn next_entry(&mut self) -> Result<Option<Entry>, ParseError> {
        match self {
            Self::Prom(p) => p.next_entry(),
            Self::OpenMetrics(p) => p.next_entry(),
            Self::Invalid => Ok(None),
        }
    }

    #[inline]
    fn name_offset(&self) -> usize {
        match self {
            Self::Prom(p) => p.name_offset(),
            Self::OpenMetrics(p) => p.name_offset(),
            Self::Invalid => 0,
        }
    }

    #[inline]
    fn raw_help(&self) -> (&'input [u8], &'input [u8]) {
        match self {
            Self::Prom(p) => p.raw_help(),
            Self::OpenMetrics(p) => p.raw_help(),
            Self::Invalid => (b"", b""),
        }
    }

    #[inline]
    fn metric_type(&self) -> (&'input [u8], MetricType) {
        match self {
            Self::Prom(p) => p.metric_type(),
            Self::OpenMetrics(p) => p.metric_type(),
            Self::Invalid => (b"", MetricType::Unknown),
        }
    }

    #[inline]
    fn unit(&self) -> (&'input [u8], &'input [u8]) {
        match self {
            Self::Prom(p) => p.unit(),
            Self::OpenMetrics(p) => p.unit(),
            Self::Invalid => (b"", b""),
        }
    }

    #[inline]
    fn raw_series_parts(&self) -> (&'input [u8], &[LabelSpan], &'input [u8], Option<i64>, f64) {
        match self {
            Self::Prom(p) => p.raw_series_parts(),
            Self::OpenMetrics(p) => p.raw_series_parts(),
            Self::Invalid => (b"", &[], b"", None, 0.0),
        }
    }

    #[inline]
    fn raw_exemplar_parts(&self) -> Option<(f64, Option<i64>, &[LabelSpan])> {
        match self {
            Self::Prom(_) | Self::Invalid => None,
            Self::OpenMetrics(p) => p.raw_exemplar_parts(),
        }
    }

    #[inline]
    fn start_timestamp(&mut self) -> Option<i64> {
        match self {
            Self::Prom(_) | Self::Invalid => None,
            Self::OpenMetrics(p) => p.start_timestamp(),
        }
    }

    fn take_scratch_into(&mut self, decoder: &mut ArrowDecoder) {
        match self {
            Self::Prom(p) => decoder.prom_spans = p.take_scratch(),
            Self::OpenMetrics(p) => decoder.om_scratch = p.take_scratch(),
            Self::Invalid => {}
        }
    }
}

/// Stateful, lock-free decoder translating Prometheus and OpenMetrics exposition payloads into OTAP
/// Arrow metric [`RecordBatch`]es.
///
/// Reusing a single `ArrowDecoder` across scrapes preserves internal parser scratch spans, label
/// string interners, and column staging buffers so steady-state scrapes allocate only the output
/// Arrow `Buffer`s returned inside [`OtapMetricRecords`].
pub struct ArrowDecoder {
    prom_spans: Vec<LabelSpan>,
    om_scratch: OmScratch,
    unescape_buf: Vec<u8>,
    series_name_buf: Vec<u8>,
    current_family: CurrentFamily,

    um_metric_types: Vec<u8>,
    um_is_monotonic: Vec<bool>,
    um_attr_type_keys: Vec<u16>,
    um_name_offsets: Vec<i32>,
    um_name_bytes: Vec<u8>,
    um_desc_offsets: Vec<i32>,
    um_desc_bytes: Vec<u8>,
    um_desc_valid: Vec<bool>,
    um_any_help: bool,
    um_all_help: bool,
    um_unit_offsets: Vec<i32>,
    um_unit_bytes: Vec<u8>,
    um_unit_valid: Vec<bool>,
    um_any_unit: bool,
    um_all_unit: bool,
    um_any_temporality: bool,
    um_all_temporality: bool,
    um_any_sum: bool,
    um_all_sum: bool,

    extra_metric_attr_parents: Vec<u16>,
    extra_metric_attr_pairs: Vec<(u32, u32)>,

    key_interner: StringInterner,
    val_interner: StringInterner,

    // NumberDataPoints and NumberDpAttrs staging.
    dp_parent_ids: Vec<u16>,
    dp_values: Vec<f64>,
    dp_start_timestamps: Vec<i64>,
    dp_start_time_valid: Vec<bool>,
    dp_timestamps: Vec<i64>,
    dp_time_valid: Vec<bool>,
    dp_attr_parent_ids: Vec<u32>,
    dp_attr_pairs: Vec<(u32, u32)>,
    last_counter_dp_attr_start: usize,

    // NumberDpExemplars and NumberDpExemplarAttrs staging.
    num_ex_parent_ids: Vec<u32>,
    num_ex_timestamps: Vec<i64>,
    num_ex_values: Vec<f64>,
    num_ex_span_ids: Vec<[u8; 8]>,
    num_ex_span_valid: Vec<bool>,
    num_ex_any_span: bool,
    num_ex_trace_ids: Vec<[u8; 16]>,
    num_ex_trace_valid: Vec<bool>,
    num_ex_any_trace: bool,
    num_ex_attr_parent_ids: Vec<u32>,
    num_ex_attr_pairs: Vec<(u32, u32)>,

    // SummaryDataPoints and SummaryDpAttrs staging.
    sum_parent_ids: Vec<u16>,
    sum_start_timestamps: Vec<i64>,
    sum_start_time_valid: Vec<bool>,
    sum_timestamps: Vec<i64>,
    sum_time_valid: Vec<bool>,
    sum_counts: Vec<u64>,
    sum_count_valid: Vec<bool>,
    sum_sums: Vec<f64>,
    sum_sum_valid: Vec<bool>,
    sum_quantile_offsets: Vec<i32>,
    sum_quantile_bounds: Vec<f64>,
    sum_quantile_vals: Vec<f64>,
    sum_attr_parent_ids: Vec<u32>,
    sum_attr_pairs: Vec<(u32, u32)>,

    // HistogramDataPoints, HistogramDpAttrs, HistogramDpExemplars, HistogramDpExemplarAttrs.
    hist_parent_ids: Vec<u16>,
    hist_start_timestamps: Vec<i64>,
    hist_start_time_valid: Vec<bool>,
    hist_timestamps: Vec<i64>,
    hist_time_valid: Vec<bool>,
    hist_counts: Vec<u64>,
    hist_count_valid: Vec<bool>,
    hist_sums: Vec<f64>,
    hist_sum_valid: Vec<bool>,
    hist_bucket_offsets: Vec<i32>,
    hist_bucket_counts: Vec<u64>,
    hist_bounds_offsets: Vec<i32>,
    hist_bounds_vals: Vec<f64>,
    hist_attr_parent_ids: Vec<u32>,
    hist_attr_pairs: Vec<(u32, u32)>,
    hist_ex_parent_ids: Vec<u32>,
    hist_ex_timestamps: Vec<i64>,
    hist_ex_values: Vec<f64>,
    hist_ex_span_ids: Vec<[u8; 8]>,
    hist_ex_span_valid: Vec<bool>,
    hist_ex_any_span: bool,
    hist_ex_trace_ids: Vec<[u8; 16]>,
    hist_ex_trace_valid: Vec<bool>,
    hist_ex_any_trace: bool,
    hist_ex_attr_parent_ids: Vec<u32>,
    hist_ex_attr_pairs: Vec<(u32, u32)>,

    // Flat per-family assembly buffers for classic histograms and summaries.
    temp_attrs: Vec<(u32, u32)>,
    pending_groups: Vec<PendingDpGroup>,
    pending_group_attrs: Vec<(u32, u32)>,
    pending_points: Vec<(u32, f64, f64)>,
    pending_exemplars: Vec<PendingExemplar>,
    pending_exemplar_attrs: Vec<(u32, u32)>,
    pending_group_points: Vec<(f64, f64)>,
    last_group_idx: Option<usize>,

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
        let mut um_name_offsets = Vec::with_capacity(metrics + 1);
        um_name_offsets.push(0);

        Self {
            prom_spans: Vec::with_capacity(16),
            om_scratch: OmScratch::default(),
            unescape_buf: Vec::new(),
            series_name_buf: Vec::with_capacity(64),
            current_family: CurrentFamily::with_capacity(64, 128),

            um_metric_types: Vec::with_capacity(metrics),
            um_is_monotonic: Vec::with_capacity(metrics),
            um_attr_type_keys: Vec::with_capacity(metrics),
            um_name_offsets,
            um_name_bytes: Vec::with_capacity(metrics.saturating_mul(32)),
            um_desc_offsets: Vec::new(),
            um_desc_bytes: Vec::new(),
            um_desc_valid: Vec::new(),
            um_any_help: false,
            um_all_help: true,
            um_unit_offsets: Vec::new(),
            um_unit_bytes: Vec::new(),
            um_unit_valid: Vec::new(),
            um_any_unit: false,
            um_all_unit: true,
            um_any_temporality: false,
            um_all_temporality: true,
            um_any_sum: false,
            um_all_sum: true,

            extra_metric_attr_parents: Vec::new(),
            extra_metric_attr_pairs: Vec::new(),

            key_interner: StringInterner::with_capacity(64, 1024),
            val_interner: StringInterner::with_capacity(256, 4096),

            dp_parent_ids: Vec::with_capacity(data_points),
            dp_values: Vec::with_capacity(data_points),
            dp_start_timestamps: Vec::new(),
            dp_start_time_valid: Vec::new(),
            dp_timestamps: Vec::new(),
            dp_time_valid: Vec::new(),
            dp_attr_parent_ids: Vec::with_capacity(attrs),
            dp_attr_pairs: Vec::with_capacity(attrs),
            last_counter_dp_attr_start: 0,

            num_ex_parent_ids: Vec::new(),
            num_ex_timestamps: Vec::new(),
            num_ex_values: Vec::new(),
            num_ex_span_ids: Vec::new(),
            num_ex_span_valid: Vec::new(),
            num_ex_any_span: false,
            num_ex_trace_ids: Vec::new(),
            num_ex_trace_valid: Vec::new(),
            num_ex_any_trace: false,
            num_ex_attr_parent_ids: Vec::new(),
            num_ex_attr_pairs: Vec::new(),

            sum_parent_ids: Vec::new(),
            sum_start_timestamps: Vec::new(),
            sum_start_time_valid: Vec::new(),
            sum_timestamps: Vec::new(),
            sum_time_valid: Vec::new(),
            sum_counts: Vec::new(),
            sum_count_valid: Vec::new(),
            sum_sums: Vec::new(),
            sum_sum_valid: Vec::new(),
            sum_quantile_offsets: Vec::new(),
            sum_quantile_bounds: Vec::new(),
            sum_quantile_vals: Vec::new(),
            sum_attr_parent_ids: Vec::new(),
            sum_attr_pairs: Vec::new(),

            hist_parent_ids: Vec::new(),
            hist_start_timestamps: Vec::new(),
            hist_start_time_valid: Vec::new(),
            hist_timestamps: Vec::new(),
            hist_time_valid: Vec::new(),
            hist_counts: Vec::new(),
            hist_count_valid: Vec::new(),
            hist_sums: Vec::new(),
            hist_sum_valid: Vec::new(),
            hist_bucket_offsets: Vec::new(),
            hist_bucket_counts: Vec::new(),
            hist_bounds_offsets: Vec::new(),
            hist_bounds_vals: Vec::new(),
            hist_attr_parent_ids: Vec::new(),
            hist_attr_pairs: Vec::new(),
            hist_ex_parent_ids: Vec::new(),
            hist_ex_timestamps: Vec::new(),
            hist_ex_values: Vec::new(),
            hist_ex_span_ids: Vec::new(),
            hist_ex_span_valid: Vec::new(),
            hist_ex_any_span: false,
            hist_ex_trace_ids: Vec::new(),
            hist_ex_trace_valid: Vec::new(),
            hist_ex_any_trace: false,
            hist_ex_attr_parent_ids: Vec::new(),
            hist_ex_attr_pairs: Vec::new(),

            temp_attrs: Vec::new(),
            pending_groups: Vec::new(),
            pending_group_attrs: Vec::new(),
            pending_points: Vec::new(),
            pending_exemplars: Vec::new(),
            pending_exemplar_attrs: Vec::new(),
            pending_group_points: Vec::new(),
            last_group_idx: None,

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
        self.prom_spans.capacity() * size_of::<LabelSpan>()
            + self.om_scratch.retained_memory_bytes()
            + self.unescape_buf.capacity()
            + self.series_name_buf.capacity()
            + self.current_family.retained_memory_bytes()
            + self.um_metric_types.capacity()
            + self.um_is_monotonic.capacity() * size_of::<bool>()
            + self.um_attr_type_keys.capacity() * size_of::<u16>()
            + self.um_name_offsets.capacity() * size_of::<i32>()
            + self.um_name_bytes.capacity()
            + self.um_desc_offsets.capacity() * size_of::<i32>()
            + self.um_desc_bytes.capacity()
            + self.um_desc_valid.capacity() * size_of::<bool>()
            + self.um_unit_offsets.capacity() * size_of::<i32>()
            + self.um_unit_bytes.capacity()
            + self.um_unit_valid.capacity() * size_of::<bool>()
            + self.extra_metric_attr_parents.capacity() * size_of::<u16>()
            + self.extra_metric_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.key_interner.retained_memory_bytes()
            + self.val_interner.retained_memory_bytes()
            + self.dp_parent_ids.capacity() * size_of::<u16>()
            + self.dp_values.capacity() * size_of::<f64>()
            + self.dp_start_timestamps.capacity() * size_of::<i64>()
            + self.dp_start_time_valid.capacity() * size_of::<bool>()
            + self.dp_timestamps.capacity() * size_of::<i64>()
            + self.dp_time_valid.capacity() * size_of::<bool>()
            + self.dp_attr_parent_ids.capacity() * size_of::<u32>()
            + self.dp_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.num_ex_parent_ids.capacity() * size_of::<u32>()
            + self.num_ex_timestamps.capacity() * size_of::<i64>()
            + self.num_ex_values.capacity() * size_of::<f64>()
            + self.num_ex_span_ids.capacity() * size_of::<[u8; 8]>()
            + self.num_ex_span_valid.capacity() * size_of::<bool>()
            + self.num_ex_trace_ids.capacity() * size_of::<[u8; 16]>()
            + self.num_ex_trace_valid.capacity() * size_of::<bool>()
            + self.num_ex_attr_parent_ids.capacity() * size_of::<u32>()
            + self.num_ex_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.sum_parent_ids.capacity() * size_of::<u16>()
            + self.sum_start_timestamps.capacity() * size_of::<i64>()
            + self.sum_start_time_valid.capacity() * size_of::<bool>()
            + self.sum_timestamps.capacity() * size_of::<i64>()
            + self.sum_time_valid.capacity() * size_of::<bool>()
            + self.sum_counts.capacity() * size_of::<u64>()
            + self.sum_count_valid.capacity() * size_of::<bool>()
            + self.sum_sums.capacity() * size_of::<f64>()
            + self.sum_sum_valid.capacity() * size_of::<bool>()
            + self.sum_quantile_offsets.capacity() * size_of::<i32>()
            + self.sum_quantile_bounds.capacity() * size_of::<f64>()
            + self.sum_quantile_vals.capacity() * size_of::<f64>()
            + self.sum_attr_parent_ids.capacity() * size_of::<u32>()
            + self.sum_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.hist_parent_ids.capacity() * size_of::<u16>()
            + self.hist_start_timestamps.capacity() * size_of::<i64>()
            + self.hist_start_time_valid.capacity() * size_of::<bool>()
            + self.hist_timestamps.capacity() * size_of::<i64>()
            + self.hist_time_valid.capacity() * size_of::<bool>()
            + self.hist_counts.capacity() * size_of::<u64>()
            + self.hist_count_valid.capacity() * size_of::<bool>()
            + self.hist_sums.capacity() * size_of::<f64>()
            + self.hist_sum_valid.capacity() * size_of::<bool>()
            + self.hist_bucket_offsets.capacity() * size_of::<i32>()
            + self.hist_bucket_counts.capacity() * size_of::<u64>()
            + self.hist_bounds_offsets.capacity() * size_of::<i32>()
            + self.hist_bounds_vals.capacity() * size_of::<f64>()
            + self.hist_attr_parent_ids.capacity() * size_of::<u32>()
            + self.hist_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.hist_ex_parent_ids.capacity() * size_of::<u32>()
            + self.hist_ex_timestamps.capacity() * size_of::<i64>()
            + self.hist_ex_values.capacity() * size_of::<f64>()
            + self.hist_ex_span_ids.capacity() * size_of::<[u8; 8]>()
            + self.hist_ex_span_valid.capacity() * size_of::<bool>()
            + self.hist_ex_trace_ids.capacity() * size_of::<[u8; 16]>()
            + self.hist_ex_trace_valid.capacity() * size_of::<bool>()
            + self.hist_ex_attr_parent_ids.capacity() * size_of::<u32>()
            + self.hist_ex_attr_pairs.capacity() * size_of::<(u32, u32)>()
            + self.temp_attrs.capacity() * size_of::<(u32, u32)>()
            + self.pending_groups.capacity() * size_of::<PendingDpGroup>()
            + self.pending_group_attrs.capacity() * size_of::<(u32, u32)>()
            + self.pending_points.capacity() * size_of::<(u32, f64, f64)>()
            + self.pending_exemplars.capacity() * size_of::<PendingExemplar>()
            + self.pending_exemplar_attrs.capacity() * size_of::<(u32, u32)>()
            + self.pending_group_points.capacity() * size_of::<(f64, f64)>()
            + self.scratch_u8.capacity()
            + self.scratch_u16.capacity() * size_of::<u16>()
            + self.scratch_u32.capacity() * size_of::<u32>()
            + self.scratch_i32.capacity() * size_of::<i32>()
            + self.scratch_bytes.capacity()
    }

    /// Decodes a single exposition scrape buffer of `format` into OTAP metric [`RecordBatch`]es.
    ///
    /// # Contiguous families and batching
    ///
    /// Input parsing streams `Entry` items one by one from [`PromParser`] or
    /// [`OpenMetricsParser`] directly into reusable columnar staging vectors and label string
    /// interners without materializing intermediate per-sample structs or per-string allocations.
    /// Metric families are decoded as contiguous runs, with `# HELP`, `# TYPE`, and `# UNIT`
    /// lines appearing before that family's samples. Non-contiguous repetitions of an earlier
    /// metric name are not reassembled or rejected; each run starts with fresh `untyped` metadata
    /// unless preceded by its own metadata lines.
    pub fn decode(
        &mut self,
        input: &[u8],
        format: &Format,
        opts: &DecodeOptions,
    ) -> Result<OtapMetricRecords, DecodeError> {
        let default_ts_ns = match opts.timestamp_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => None,
        };

        self.reset_scrape();

        let mut parser = match format.format_type() {
            FormatType::TextPlain => {
                let spans = std::mem::take(&mut self.prom_spans);
                ActiveParser::Prom(PromParser::with_scratch(input, spans))
            }
            FormatType::OpenMetrics => {
                if format.version().starts_with("2.") {
                    // TODO(bwplotka): Dispatch OpenMetrics 2.0 in Phase 3.
                    return Err(DecodeError::UnsupportedFormat(format.as_str().to_owned()));
                }
                let scratch = std::mem::take(&mut self.om_scratch);
                ActiveParser::OpenMetrics(OpenMetricsParser::with_scratch(input, scratch))
            }
            _ => {
                // TODO(bwplotka): Dispatch Proto* in Phase 3.
                return Err(DecodeError::UnsupportedFormat(format.as_str().to_owned()));
            }
        };

        let mut pending_series = false;
        let mut done = false;
        let res = self.ingest_entries::<false>(
            &mut parser,
            opts,
            default_ts_ns,
            0,
            &mut pending_series,
            &mut done,
        );
        parser.take_scratch_into(self);
        Ok(res?.unwrap_or_default())
    }

    /// Streams `input` as a sequence of non-empty [`OtapMetricRecords`] chunks bounded by
    /// [`DecodeOptions::max_data_points_per_batch`] and OTAP's 65,536-metric per-batch `u16` ID
    /// limit.
    ///
    /// Empty or metadata-only scrapes yield zero items. When a contiguous metric family spans a
    /// chunk boundary, [`ArrowDecoder`] retains its `# HELP`, `# TYPE`, and `# UNIT` metadata
    /// across the flush and re-emits the family row in the next chunk only if further samples
    /// follow. Dropping [`DecodeStream`] early returns the internal parser scratch buffer to
    /// `self`.
    pub fn decode_stream<'dec, 'input>(
        &'dec mut self,
        input: &'input [u8],
        format: &Format,
        opts: &DecodeOptions,
    ) -> DecodeStream<'dec, 'input> {
        self.reset_scrape();
        let max_data_points = opts
            .max_data_points_per_batch
            .clamp(1, (u32::MAX as usize).saturating_add(1));

        let default_ts_ns = match opts.timestamp_ms {
            Some(ms) => match ms_to_ns(ms) {
                Ok(ns) => Some(ns),
                Err(err) => {
                    return DecodeStream {
                        decoder: self,
                        parser: ActiveParser::Invalid,
                        opts: *opts,
                        default_ts_ns: None,
                        max_data_points,
                        pending_series: false,
                        init_err: Some(err),
                        done: false,
                    };
                }
            },
            None => None,
        };

        let parser = match format.format_type() {
            FormatType::TextPlain => {
                let spans = std::mem::take(&mut self.prom_spans);
                ActiveParser::Prom(PromParser::with_scratch(input, spans))
            }
            FormatType::OpenMetrics if !format.version().starts_with("2.") => {
                let scratch = std::mem::take(&mut self.om_scratch);
                ActiveParser::OpenMetrics(OpenMetricsParser::with_scratch(input, scratch))
            }
            _ => {
                return DecodeStream {
                    decoder: self,
                    parser: ActiveParser::Invalid,
                    opts: *opts,
                    default_ts_ns,
                    max_data_points,
                    pending_series: false,
                    init_err: Some(DecodeError::UnsupportedFormat(format.as_str().to_owned())),
                    done: false,
                };
            }
        };

        DecodeStream {
            decoder: self,
            parser,
            opts: *opts,
            default_ts_ns,
            max_data_points,
            pending_series: false,
            init_err: None,
            done: false,
        }
    }

    fn reset_scrape(&mut self) {
        self.current_family.clear();
        self.clear_pending_family();
        self.reset_batch();
    }

    fn clear_pending_family(&mut self) {
        self.temp_attrs.clear();
        self.pending_groups.clear();
        self.pending_group_attrs.clear();
        self.pending_points.clear();
        self.pending_exemplars.clear();
        self.pending_exemplar_attrs.clear();
        self.pending_group_points.clear();
        self.last_group_idx = None;
    }

    fn reset_batch(&mut self) {
        self.current_family.batch_id = None;

        self.um_metric_types.clear();
        self.um_is_monotonic.clear();
        self.um_attr_type_keys.clear();
        self.um_name_offsets.clear();
        self.um_name_offsets.push(0);
        self.um_name_bytes.clear();
        self.um_desc_offsets.clear();
        self.um_desc_bytes.clear();
        self.um_desc_valid.clear();
        self.um_any_help = false;
        self.um_all_help = true;
        self.um_unit_offsets.clear();
        self.um_unit_bytes.clear();
        self.um_unit_valid.clear();
        self.um_any_unit = false;
        self.um_all_unit = true;
        self.um_any_temporality = false;
        self.um_all_temporality = true;
        self.um_any_sum = false;
        self.um_all_sum = true;

        self.extra_metric_attr_parents.clear();
        self.extra_metric_attr_pairs.clear();

        self.key_interner.clear();
        self.val_interner.clear();

        self.dp_parent_ids.clear();
        self.dp_values.clear();
        self.dp_start_timestamps.clear();
        self.dp_start_time_valid.clear();
        self.dp_timestamps.clear();
        self.dp_time_valid.clear();
        self.dp_attr_parent_ids.clear();
        self.dp_attr_pairs.clear();
        self.last_counter_dp_attr_start = 0;

        self.num_ex_parent_ids.clear();
        self.num_ex_timestamps.clear();
        self.num_ex_values.clear();
        self.num_ex_span_ids.clear();
        self.num_ex_span_valid.clear();
        self.num_ex_any_span = false;
        self.num_ex_trace_ids.clear();
        self.num_ex_trace_valid.clear();
        self.num_ex_any_trace = false;
        self.num_ex_attr_parent_ids.clear();
        self.num_ex_attr_pairs.clear();

        self.sum_parent_ids.clear();
        self.sum_start_timestamps.clear();
        self.sum_start_time_valid.clear();
        self.sum_timestamps.clear();
        self.sum_time_valid.clear();
        self.sum_counts.clear();
        self.sum_count_valid.clear();
        self.sum_sums.clear();
        self.sum_sum_valid.clear();
        self.sum_quantile_offsets.clear();
        self.sum_quantile_bounds.clear();
        self.sum_quantile_vals.clear();
        self.sum_attr_parent_ids.clear();
        self.sum_attr_pairs.clear();

        self.hist_parent_ids.clear();
        self.hist_start_timestamps.clear();
        self.hist_start_time_valid.clear();
        self.hist_timestamps.clear();
        self.hist_time_valid.clear();
        self.hist_counts.clear();
        self.hist_count_valid.clear();
        self.hist_sums.clear();
        self.hist_sum_valid.clear();
        self.hist_bucket_offsets.clear();
        self.hist_bucket_counts.clear();
        self.hist_bounds_offsets.clear();
        self.hist_bounds_vals.clear();
        self.hist_attr_parent_ids.clear();
        self.hist_attr_pairs.clear();
        self.hist_ex_parent_ids.clear();
        self.hist_ex_timestamps.clear();
        self.hist_ex_values.clear();
        self.hist_ex_span_ids.clear();
        self.hist_ex_span_valid.clear();
        self.hist_ex_any_span = false;
        self.hist_ex_trace_ids.clear();
        self.hist_ex_trace_valid.clear();
        self.hist_ex_any_trace = false;
        self.hist_ex_attr_parent_ids.clear();
        self.hist_ex_attr_pairs.clear();
    }

    #[inline]
    fn total_data_points(&self) -> usize {
        self.dp_parent_ids.len() + self.sum_parent_ids.len() + self.hist_parent_ids.len()
    }

    #[inline]
    fn emit_current_family_into_batch(&mut self, opts: &DecodeOptions) -> Result<u16, DecodeError> {
        if let Some(id) = self.current_family.batch_id {
            return Ok(id);
        }

        let m_idx = self.um_metric_types.len();
        let id = u16::try_from(m_idx).map_err(|_| DecodeError::CapacityOverflow {
            table: "UnivariateMetrics",
            count: m_idx + 1,
        })?;

        let fam_name = self.current_family.unescaped_name.as_slice();
        let unit = self.current_family.unit.as_slice();
        let inferred_counter = self.current_family.metric_type == MetricType::Unknown
            && opts.untyped_policy == UntypedPolicy::InferFromSuffix
            && fam_name.len() > 6
            && fam_name.ends_with(b"_total");
        let is_counter = self.current_family.metric_type == MetricType::Counter || inferred_counter;

        let mut base_name = if is_counter && fam_name.len() > 6 && fam_name.ends_with(b"_total") {
            &fam_name[..fam_name.len() - 6]
        } else {
            fam_name
        };

        if opts.unit_suffix_policy == UnitSuffixPolicy::Strip
            && self.current_family.has_unit
            && !unit.is_empty()
            && base_name.len() > unit.len() + 1
            && base_name.ends_with(unit)
            && base_name[base_name.len() - unit.len() - 1] == b'_'
        {
            base_name = &base_name[..base_name.len() - unit.len() - 1];
        }

        let append_total = is_counter
            && opts.counter_suffix_policy == CounterSuffixPolicy::Keep
            && self.current_family.saw_total_suffix;
        let emitted_name_len = base_name.len() + if append_total { 6 } else { 0 };

        let next_name_len = self
            .um_name_bytes
            .len()
            .checked_add(emitted_name_len)
            .and_then(|n| i32::try_from(n).ok())
            .ok_or(DecodeError::CapacityOverflow {
                table: "UnivariateMetrics.name",
                count: self.um_name_bytes.len().saturating_add(emitted_name_len),
            })?;

        let next_desc_len = if self.current_family.has_help {
            Some(
                self.um_desc_bytes
                    .len()
                    .checked_add(self.current_family.help.len())
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or(DecodeError::CapacityOverflow {
                        table: "UnivariateMetrics.description",
                        count: self
                            .um_desc_bytes
                            .len()
                            .saturating_add(self.current_family.help.len()),
                    })?,
            )
        } else {
            None
        };

        let next_unit_len = if self.current_family.has_unit {
            Some(
                self.um_unit_bytes
                    .len()
                    .checked_add(unit.len())
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or(DecodeError::CapacityOverflow {
                        table: "UnivariateMetrics.unit",
                        count: self.um_unit_bytes.len().saturating_add(unit.len()),
                    })?,
            )
        } else {
            None
        };

        self.um_name_bytes.extend_from_slice(base_name);
        if append_total {
            self.um_name_bytes.extend_from_slice(b"_total");
        }
        self.um_name_offsets.push(next_name_len);

        let (otap_type, attr_type_key, is_monotonic) = match self.current_family.metric_type {
            MetricType::Counter => (OTAP_METRIC_TYPE_SUM, 1u16, true),
            MetricType::Gauge => (OTAP_METRIC_TYPE_GAUGE, 2u16, false),
            MetricType::Histogram => (OTAP_METRIC_TYPE_HISTOGRAM, 3u16, false),
            MetricType::GaugeHistogram => (OTAP_METRIC_TYPE_HISTOGRAM, 4u16, false),
            MetricType::Summary => (OTAP_METRIC_TYPE_SUMMARY, 5u16, false),
            MetricType::Info => match opts.info_policy {
                InfoPolicy::Gauge | InfoPolicy::Drop => (OTAP_METRIC_TYPE_GAUGE, 6u16, false),
                InfoPolicy::MetricAttrs => (OTAP_METRIC_TYPE_EMPTY, 6u16, false),
            },
            MetricType::Stateset => match opts.stateset_policy {
                StatesetPolicy::Gauge | StatesetPolicy::Drop => {
                    (OTAP_METRIC_TYPE_GAUGE, 7u16, false)
                }
                StatesetPolicy::NonMonotonicSum => (OTAP_METRIC_TYPE_SUM, 7u16, false),
            },
            MetricType::Unknown => {
                if inferred_counter {
                    (OTAP_METRIC_TYPE_SUM, 0u16, true)
                } else {
                    (OTAP_METRIC_TYPE_GAUGE, 0u16, false)
                }
            }
        };

        let has_temporality =
            otap_type == OTAP_METRIC_TYPE_SUM || otap_type == OTAP_METRIC_TYPE_HISTOGRAM;
        let has_sum = otap_type == OTAP_METRIC_TYPE_SUM;

        self.um_metric_types.push(otap_type);
        self.um_is_monotonic.push(is_monotonic);
        self.um_attr_type_keys.push(attr_type_key);
        self.um_any_temporality |= has_temporality;
        self.um_all_temporality &= has_temporality;
        self.um_any_sum |= has_sum;
        self.um_all_sum &= has_sum;

        if let Some(desc_end) = next_desc_len {
            if !self.um_any_help {
                self.um_any_help = true;
                self.um_desc_offsets.resize(m_idx + 1, 0);
                if m_idx > 0 {
                    self.um_desc_valid.resize(m_idx, false);
                    self.um_desc_valid.push(true);
                }
            } else if !self.um_all_help {
                self.um_desc_valid.push(true);
            }
            self.um_desc_bytes
                .extend_from_slice(&self.current_family.help);
            self.um_desc_offsets.push(desc_end);
        } else {
            if self.um_all_help {
                self.um_all_help = false;
                if self.um_any_help {
                    self.um_desc_valid.resize(m_idx, true);
                }
            }
            if self.um_any_help {
                self.um_desc_valid.push(false);
                self.um_desc_offsets.push(self.um_desc_bytes.len() as i32);
            }
        }

        if let Some(unit_end) = next_unit_len {
            if !self.um_any_unit {
                self.um_any_unit = true;
                self.um_unit_offsets.resize(m_idx + 1, 0);
                if m_idx > 0 {
                    self.um_unit_valid.resize(m_idx, false);
                    self.um_unit_valid.push(true);
                }
            } else if !self.um_all_unit {
                self.um_unit_valid.push(true);
            }
            self.um_unit_bytes
                .extend_from_slice(&self.current_family.unit);
            self.um_unit_offsets.push(unit_end);
        } else {
            if self.um_all_unit {
                self.um_all_unit = false;
                if self.um_any_unit {
                    self.um_unit_valid.resize(m_idx, true);
                }
            }
            if self.um_any_unit {
                self.um_unit_valid.push(false);
                self.um_unit_offsets.push(self.um_unit_bytes.len() as i32);
            }
        }

        self.current_family.has_samples = true;
        self.current_family.batch_id = Some(id);
        Ok(id)
    }

    fn flush_pending_family(
        &mut self,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        if self.pending_groups.is_empty() {
            return Ok(());
        }

        let parent_id = self.emit_current_family_into_batch(opts)?;
        let num_groups = self.pending_groups.len();

        match self.current_family.metric_type {
            MetricType::Summary => {
                if self.sum_quantile_offsets.is_empty() {
                    self.sum_quantile_offsets.push(0);
                }
                for group_idx in 0..num_groups {
                    let group = self.pending_groups[group_idx];
                    let dp_index = self.sum_parent_ids.len();
                    let dp_id =
                        u32::try_from(dp_index).map_err(|_| DecodeError::CapacityOverflow {
                            table: "SummaryDataPoints",
                            count: dp_index + 1,
                        })?;

                    let attr_start = group.attr_start as usize;
                    let attr_end = attr_start + group.attr_len as usize;
                    for &(k, v) in &self.pending_group_attrs[attr_start..attr_end] {
                        self.sum_attr_parent_ids.push(dp_id);
                        self.sum_attr_pairs.push((k, v));
                    }

                    self.sum_parent_ids.push(parent_id);
                    push_optional_i64(
                        &mut self.sum_start_timestamps,
                        &mut self.sum_start_time_valid,
                        dp_index,
                        group.start_ts_ns,
                    );
                    push_optional_i64(
                        &mut self.sum_timestamps,
                        &mut self.sum_time_valid,
                        dp_index,
                        group.ts_ns.or(default_ts_ns),
                    );
                    push_optional_u64(
                        &mut self.sum_counts,
                        &mut self.sum_count_valid,
                        dp_index,
                        group.count,
                    );
                    push_optional_f64(
                        &mut self.sum_sums,
                        &mut self.sum_sum_valid,
                        dp_index,
                        group.sum,
                    );

                    self.pending_group_points.clear();
                    let g_u32 = group_idx as u32;
                    let mut sorted = true;
                    let mut prev_q = f64::NEG_INFINITY;
                    for &(g, q, val) in &self.pending_points {
                        if g == g_u32 {
                            if q < prev_q {
                                sorted = false;
                            }
                            prev_q = q;
                            self.pending_group_points.push((q, val));
                        }
                    }
                    if !sorted {
                        self.pending_group_points
                            .sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                    }
                    for &(q, val) in &self.pending_group_points {
                        self.sum_quantile_bounds.push(q);
                        self.sum_quantile_vals.push(val);
                    }
                    let q_off = i32::try_from(self.sum_quantile_bounds.len()).map_err(|_| {
                        DecodeError::CapacityOverflow {
                            table: "SummaryDataPoints.quantile",
                            count: self.sum_quantile_bounds.len(),
                        }
                    })?;
                    self.sum_quantile_offsets.push(q_off);
                }
            }
            MetricType::Histogram | MetricType::GaugeHistogram => {
                if self.hist_bucket_offsets.is_empty() {
                    self.hist_bucket_offsets.push(0);
                    self.hist_bounds_offsets.push(0);
                }
                for group_idx in 0..num_groups {
                    let group = self.pending_groups[group_idx];
                    let dp_index = self.hist_parent_ids.len();
                    let dp_id =
                        u32::try_from(dp_index).map_err(|_| DecodeError::CapacityOverflow {
                            table: "HistogramDataPoints",
                            count: dp_index + 1,
                        })?;

                    let attr_start = group.attr_start as usize;
                    let attr_end = attr_start + group.attr_len as usize;
                    for &(k, v) in &self.pending_group_attrs[attr_start..attr_end] {
                        self.hist_attr_parent_ids.push(dp_id);
                        self.hist_attr_pairs.push((k, v));
                    }

                    self.hist_parent_ids.push(parent_id);
                    push_optional_i64(
                        &mut self.hist_start_timestamps,
                        &mut self.hist_start_time_valid,
                        dp_index,
                        group.start_ts_ns,
                    );
                    push_optional_i64(
                        &mut self.hist_timestamps,
                        &mut self.hist_time_valid,
                        dp_index,
                        group.ts_ns.or(default_ts_ns),
                    );

                    self.pending_group_points.clear();
                    let g_u32 = group_idx as u32;
                    let mut sorted = true;
                    let mut prev_b = f64::NEG_INFINITY;
                    for &(g, bound, cum) in &self.pending_points {
                        if g == g_u32 {
                            if bound < prev_b {
                                sorted = false;
                            }
                            prev_b = bound;
                            self.pending_group_points.push((bound, cum));
                        }
                    }
                    if !sorted {
                        self.pending_group_points
                            .sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                    }

                    let mut prev_cum = 0u64;
                    let mut inf_cum: Option<u64> = None;
                    for &(bound, cum_f64) in &self.pending_group_points {
                        let cum = f64_to_u64_count(cum_f64);
                        if bound == f64::INFINITY {
                            inf_cum = Some(cum);
                        } else if bound.is_finite() {
                            self.hist_bounds_vals.push(bound);
                            self.hist_bucket_counts.push(cum.saturating_sub(prev_cum));
                            prev_cum = cum;
                        }
                    }
                    let final_count = group.count.or(inf_cum);
                    if !self.pending_group_points.is_empty() {
                        let total_for_inf = inf_cum.or(group.count).unwrap_or(prev_cum);
                        self.hist_bucket_counts
                            .push(total_for_inf.saturating_sub(prev_cum));
                    }

                    let bucket_off =
                        i32::try_from(self.hist_bucket_counts.len()).map_err(|_| {
                            DecodeError::CapacityOverflow {
                                table: "HistogramDataPoints.histogram_bucket_counts",
                                count: self.hist_bucket_counts.len(),
                            }
                        })?;
                    let bounds_off = i32::try_from(self.hist_bounds_vals.len()).map_err(|_| {
                        DecodeError::CapacityOverflow {
                            table: "HistogramDataPoints.histogram_explicit_bounds",
                            count: self.hist_bounds_vals.len(),
                        }
                    })?;
                    self.hist_bucket_offsets.push(bucket_off);
                    self.hist_bounds_offsets.push(bounds_off);

                    push_optional_u64(
                        &mut self.hist_counts,
                        &mut self.hist_count_valid,
                        dp_index,
                        final_count,
                    );
                    push_optional_f64(
                        &mut self.hist_sums,
                        &mut self.hist_sum_valid,
                        dp_index,
                        group.sum,
                    );

                    for ex_idx in 0..self.pending_exemplars.len() {
                        let ex = self.pending_exemplars[ex_idx];
                        if ex.group_idx != g_u32 {
                            continue;
                        }
                        let hist_ex_idx = self.hist_ex_parent_ids.len();
                        let hist_ex_id = u32::try_from(hist_ex_idx).map_err(|_| {
                            DecodeError::CapacityOverflow {
                                table: "HistogramDpExemplars",
                                count: hist_ex_idx + 1,
                            }
                        })?;
                        self.hist_ex_parent_ids.push(dp_id);
                        self.hist_ex_timestamps.push(ex.ts_ns);
                        self.hist_ex_values.push(ex.value);
                        push_optional_fixed_bytes(
                            &mut self.hist_ex_span_ids,
                            &mut self.hist_ex_span_valid,
                            &mut self.hist_ex_any_span,
                            hist_ex_idx,
                            ex.span_id,
                        );
                        push_optional_fixed_bytes(
                            &mut self.hist_ex_trace_ids,
                            &mut self.hist_ex_trace_valid,
                            &mut self.hist_ex_any_trace,
                            hist_ex_idx,
                            ex.trace_id,
                        );
                        let ex_attr_start = ex.attr_start as usize;
                        let ex_attr_end = ex_attr_start + ex.attr_len as usize;
                        for &(k, v) in &self.pending_exemplar_attrs[ex_attr_start..ex_attr_end] {
                            self.hist_ex_attr_parent_ids.push(hist_ex_id);
                            self.hist_ex_attr_pairs.push((k, v));
                        }
                    }
                }
            }
            _ => {}
        }

        self.clear_pending_family();
        Ok(())
    }

    #[inline]
    fn transition_family_for_metadata(
        &mut self,
        raw_name: &[u8],
        name_offset: usize,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        if self.current_family.active && self.current_family.raw_name.as_slice() == raw_name {
            return Ok(());
        }
        if !raw_name.is_ascii() && std::str::from_utf8(raw_name).is_err() {
            return Err(DecodeError::InvalidUtf8 {
                offset: name_offset,
            });
        }
        let unescaped = unescape_bytes_into(raw_name, true, &mut self.unescape_buf);
        if self.current_family.active && self.current_family.unescaped_name.as_slice() == unescaped
        {
            self.current_family.raw_name.clear();
            self.current_family.raw_name.extend_from_slice(raw_name);
            return Ok(());
        }

        self.series_name_buf.clear();
        self.series_name_buf.extend_from_slice(unescaped);
        self.flush_pending_family(opts, default_ts_ns)?;
        self.current_family
            .start_new(raw_name, &self.series_name_buf);
        Ok(())
    }

    // Monomorphizing on `CHUNKED` shares one entry dispatch loop between `decode` (`false`) and
    // `decode_stream` (`true`) while compiling out chunk-boundary flush checks on single-batch
    // calls.
    fn ingest_entries<const CHUNKED: bool>(
        &mut self,
        parser: &mut ActiveParser<'_>,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
        max_data_points: usize,
        pending_series: &mut bool,
        done: &mut bool,
    ) -> Result<Option<OtapMetricRecords>, DecodeError> {
        loop {
            let entry = if CHUNKED && *pending_series {
                *pending_series = false;
                Entry::Series
            } else {
                match parser.next_entry().map_err(DecodeError::from_parse)? {
                    Some(entry) => entry,
                    None => {
                        self.flush_pending_family(opts, default_ts_ns)?;
                        *done = true;
                        if CHUNKED {
                            if self.um_metric_types.is_empty() {
                                return Ok(None);
                            }
                            let batch = self.finish_batches(default_ts_ns.is_some())?;
                            self.reset_batch();
                            return Ok(Some(batch));
                        }
                        return Ok(Some(self.finish_batches(default_ts_ns.is_some())?));
                    }
                }
            };

            match entry {
                Entry::Comment => {}
                Entry::Help => self.ingest_help_entry(parser, opts, default_ts_ns)?,
                Entry::Type => self.ingest_type_entry(parser, opts, default_ts_ns)?,
                Entry::Unit => self.ingest_unit_entry(parser, opts, default_ts_ns)?,
                Entry::Series => {
                    let (raw_name, name_offset) = {
                        let (rn, _, _, _, _) = parser.raw_series_parts();
                        (rn, parser.name_offset())
                    };
                    if !raw_name.is_ascii() && std::str::from_utf8(raw_name).is_err() {
                        return Err(DecodeError::InvalidUtf8 {
                            offset: name_offset,
                        });
                    }
                    let role = if self.current_family.active
                        && self.current_family.raw_name.as_slice() == raw_name
                        && !matches!(
                            self.current_family.metric_type,
                            MetricType::Histogram | MetricType::GaugeHistogram
                        ) {
                        if self.current_family.metric_type == MetricType::Summary {
                            SeriesRole::SummaryQuantile
                        } else {
                            SeriesRole::Scalar
                        }
                    } else {
                        let unescaped = unescape_bytes_into(raw_name, true, &mut self.unescape_buf);
                        if let Some(r) = self.current_family.classify_series(unescaped) {
                            r
                        } else {
                            self.series_name_buf.clear();
                            self.series_name_buf.extend_from_slice(unescaped);
                            self.flush_pending_family(opts, default_ts_ns)?;
                            if CHUNKED && self.total_data_points() >= max_data_points {
                                self.current_family
                                    .start_new(raw_name, &self.series_name_buf);
                                *pending_series = true;
                                let batch = self.finish_batches(default_ts_ns.is_some())?;
                                self.reset_batch();
                                return Ok(Some(batch));
                            }
                            self.current_family
                                .start_new(raw_name, &self.series_name_buf);
                            SeriesRole::Scalar
                        }
                    };

                    if CHUNKED
                        && self.current_family.batch_id.is_none()
                        && self.pending_groups.is_empty()
                        && (self.um_metric_types.len() >= MAX_METRICS_PER_BATCH
                            || self.total_data_points() >= max_data_points)
                    {
                        *pending_series = true;
                        let batch = self.finish_batches(default_ts_ns.is_some())?;
                        self.reset_batch();
                        return Ok(Some(batch));
                    }

                    self.ingest_series_entry(parser, role, opts, default_ts_ns)?;

                    if CHUNKED
                        && self.pending_groups.is_empty()
                        && self.total_data_points() >= max_data_points
                    {
                        let batch = self.finish_batches(default_ts_ns.is_some())?;
                        self.reset_batch();
                        return Ok(Some(batch));
                    }
                }
            }
        }
    }

    #[inline]
    fn ingest_help_entry(
        &mut self,
        parser: &ActiveParser<'_>,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let (raw_name, raw_help) = parser.raw_help();
        let name_offset = parser.name_offset();
        // Match `prometheus/common/expfmt/text_parse.go`: a bare `# HELP name` line without
        // docstring text is a no-op and does not count as setting HELP or transitioning families.
        if raw_help.is_empty() {
            if !raw_name.is_ascii() && std::str::from_utf8(raw_name).is_err() {
                return Err(DecodeError::InvalidUtf8 {
                    offset: name_offset,
                });
            }
            return Ok(());
        }
        self.transition_family_for_metadata(raw_name, name_offset, opts, default_ts_ns)?;
        if self.current_family.has_help
            || self.current_family.has_samples
            || !self.pending_groups.is_empty()
        {
            return Err(DecodeError::DuplicateOrLateHelp {
                metric: self.current_family.name_string(),
            });
        }
        let unescaped =
            unescape_bytes_into(raw_help, parser.is_openmetrics(), &mut self.unescape_buf);
        self.current_family.help.clear();
        self.current_family.help.extend_from_slice(unescaped);
        self.current_family.has_help = true;
        Ok(())
    }

    #[inline]
    fn ingest_type_entry(
        &mut self,
        parser: &ActiveParser<'_>,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let (raw_name, metric_type) = parser.metric_type();
        let name_offset = parser.name_offset();
        self.transition_family_for_metadata(raw_name, name_offset, opts, default_ts_ns)?;
        if self.current_family.has_type
            || self.current_family.has_samples
            || !self.pending_groups.is_empty()
        {
            return Err(DecodeError::DuplicateOrLateType {
                metric: self.current_family.name_string(),
            });
        }
        self.current_family.metric_type = metric_type;
        self.current_family.has_type = true;
        Ok(())
    }

    #[inline]
    fn ingest_unit_entry(
        &mut self,
        parser: &ActiveParser<'_>,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let (raw_name, raw_unit) = parser.unit();
        let name_offset = parser.name_offset();
        if raw_unit.is_empty() {
            return Ok(());
        }
        self.transition_family_for_metadata(raw_name, name_offset, opts, default_ts_ns)?;
        if self.current_family.has_unit
            || self.current_family.has_samples
            || !self.pending_groups.is_empty()
        {
            return Err(DecodeError::DuplicateOrLateUnit {
                metric: self.current_family.name_string(),
            });
        }
        let unescaped = unescape_bytes_into(raw_unit, true, &mut self.unescape_buf);
        self.current_family.unit.clear();
        self.current_family.unit.extend_from_slice(unescaped);
        self.current_family.has_unit = true;
        Ok(())
    }

    #[inline]
    fn ingest_series_entry(
        &mut self,
        parser: &mut ActiveParser<'_>,
        role: SeriesRole,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        match role {
            SeriesRole::Scalar => self.append_scalar_series(parser, opts, default_ts_ns),
            SeriesRole::CounterCreated => self.append_counter_created_series(parser),
            SeriesRole::HistogramBucket
            | SeriesRole::HistogramSum
            | SeriesRole::HistogramCount
            | SeriesRole::HistogramCreated
            | SeriesRole::SummaryQuantile
            | SeriesRole::SummarySum
            | SeriesRole::SummaryCount
            | SeriesRole::SummaryCreated => {
                self.append_assembled_series(parser, role, default_ts_ns)
            }
        }
    }

    #[inline]
    fn append_scalar_series(
        &mut self,
        parser: &mut ActiveParser<'_>,
        opts: &DecodeOptions,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let start_ts_ms = if self.current_family.metric_type == MetricType::Counter {
            parser.start_timestamp()
        } else {
            None
        };
        let (_, label_spans, full_input, sample_ts_ms, value) = parser.raw_series_parts();
        let exemplar_parts = parser.raw_exemplar_parts();

        // Always validate series labels first so duplicate/reserved labels are caught even when a
        // translation policy drops the metric family.
        let is_dropped = match self.current_family.metric_type {
            MetricType::Info => opts.info_policy == InfoPolicy::Drop,
            MetricType::Stateset => opts.stateset_policy == StatesetPolicy::Drop,
            MetricType::Unknown => opts.untyped_policy == UntypedPolicy::Drop,
            _ => false,
        };
        if is_dropped {
            for span_idx in 0..label_spans.len() {
                self.validate_empty_label(span_idx, &label_spans[..=span_idx], full_input)?;
            }
            self.current_family.has_samples = true;
            return Ok(());
        }

        if self.current_family.metric_type == MetricType::Info
            && opts.info_policy == InfoPolicy::MetricAttrs
        {
            self.extract_series_labels_into_temp(label_spans, full_input, None)?;
            let parent_id = self.emit_current_family_into_batch(opts)?;
            for &(k, v) in &self.temp_attrs {
                self.extra_metric_attr_parents.push(parent_id);
                self.extra_metric_attr_pairs.push((k, v));
            }
            return Ok(());
        }

        let dp_index = self.dp_parent_ids.len();
        let dp_id = u32::try_from(dp_index).map_err(|_| DecodeError::CapacityOverflow {
            table: "NumberDataPoints",
            count: dp_index + 1,
        })?;

        let attr_start = self.dp_attr_pairs.len();
        self.append_series_labels(dp_id, label_spans, full_input)?;
        self.last_counter_dp_attr_start = attr_start;

        if let Some((ex_val, ex_ts_ms, ex_spans)) = exemplar_parts {
            self.append_number_exemplar(
                dp_id,
                ex_val,
                ex_ts_ms,
                ex_spans,
                full_input,
                default_ts_ns,
            )?;
        }

        let parent_id = self.emit_current_family_into_batch(opts)?;
        let start_ts_ns = match start_ts_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => None,
        };
        self.append_data_point(parent_id, value, start_ts_ns, sample_ts_ms, default_ts_ns)
    }

    fn append_counter_created_series(
        &mut self,
        parser: &ActiveParser<'_>,
    ) -> Result<(), DecodeError> {
        let (_, label_spans, full_input, _, value) = parser.raw_series_parts();
        self.extract_series_labels_into_temp(label_spans, full_input, None)?;
        if !value.is_finite() {
            return Ok(());
        }
        let created_ms = (value * 1000.0) as i64;
        let created_ns = ms_to_ns(created_ms)?;

        // Attach `_created` in Prometheus text to the preceding counter data point of the active
        // family when its base labels match.
        if let Some(batch_id) = self.current_family.batch_id
            && let Some(&last_parent) = self.dp_parent_ids.last()
            && last_parent == batch_id
            && self.dp_attr_pairs[self.last_counter_dp_attr_start..] == self.temp_attrs[..]
        {
            let dp_index = self.dp_parent_ids.len() - 1;
            set_optional_i64(
                &mut self.dp_start_timestamps,
                &mut self.dp_start_time_valid,
                self.dp_parent_ids.len(),
                dp_index,
                created_ns,
            );
        }
        Ok(())
    }

    fn append_assembled_series(
        &mut self,
        parser: &mut ActiveParser<'_>,
        role: SeriesRole,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let om_start_ts_ms = if parser.is_openmetrics() {
            parser.start_timestamp()
        } else {
            None
        };
        let (_, label_spans, full_input, sample_ts_ms, value) = parser.raw_series_parts();
        let exemplar_parts = parser.raw_exemplar_parts();

        let special_label = match role {
            SeriesRole::HistogramBucket => Some(b"le" as &[u8]),
            SeriesRole::SummaryQuantile => Some(b"quantile" as &[u8]),
            _ => None,
        };
        let special_bound =
            self.extract_series_labels_into_temp(label_spans, full_input, special_label)?;

        let group_idx = self.find_or_create_pending_group();
        let sample_ts_ns = match sample_ts_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => None,
        };
        let om_start_ts_ns = match om_start_ts_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => None,
        };

        let group = &mut self.pending_groups[group_idx];
        if group.ts_ns.is_none() && sample_ts_ns.is_some() {
            group.ts_ns = sample_ts_ns;
        }
        if group.start_ts_ns.is_none() && om_start_ts_ns.is_some() {
            group.start_ts_ns = om_start_ts_ns;
        }

        match role {
            SeriesRole::HistogramBucket | SeriesRole::SummaryQuantile => {
                if let Some(bound) = special_bound
                    && !bound.is_nan()
                {
                    self.pending_points.push((group_idx as u32, bound, value));
                }
            }
            SeriesRole::HistogramSum | SeriesRole::SummarySum => {
                group.sum = Some(value);
            }
            SeriesRole::HistogramCount | SeriesRole::SummaryCount => {
                group.count = Some(f64_to_u64_count(value));
            }
            SeriesRole::HistogramCreated | SeriesRole::SummaryCreated => {
                if value.is_finite() {
                    let created_ms = (value * 1000.0) as i64;
                    group.start_ts_ns = Some(ms_to_ns(created_ms)?);
                }
            }
            SeriesRole::Scalar | SeriesRole::CounterCreated => {}
        }

        if matches!(
            self.current_family.metric_type,
            MetricType::Histogram | MetricType::GaugeHistogram
        ) && let Some((ex_val, ex_ts_ms, ex_spans)) = exemplar_parts
        {
            self.stage_pending_histogram_exemplar(
                group_idx as u32,
                ex_val,
                ex_ts_ms,
                ex_spans,
                full_input,
                default_ts_ns,
            )?;
        }

        Ok(())
    }

    #[inline]
    fn find_or_create_pending_group(&mut self) -> usize {
        if let Some(last_idx) = self.last_group_idx
            && last_idx < self.pending_groups.len()
        {
            let g = self.pending_groups[last_idx];
            let start = g.attr_start as usize;
            let end = start + g.attr_len as usize;
            if self.pending_group_attrs[start..end] == self.temp_attrs[..] {
                return last_idx;
            }
        }

        for (idx, g) in self.pending_groups.iter().enumerate() {
            let start = g.attr_start as usize;
            let end = start + g.attr_len as usize;
            if self.pending_group_attrs[start..end] == self.temp_attrs[..] {
                self.last_group_idx = Some(idx);
                return idx;
            }
        }

        let attr_start = self.pending_group_attrs.len() as u32;
        let attr_len = self.temp_attrs.len() as u32;
        self.pending_group_attrs.extend_from_slice(&self.temp_attrs);
        let idx = self.pending_groups.len();
        self.pending_groups.push(PendingDpGroup {
            attr_start,
            attr_len,
            ..PendingDpGroup::default()
        });
        self.last_group_idx = Some(idx);
        idx
    }

    #[inline]
    fn extract_series_labels_into_temp(
        &mut self,
        label_spans: &[LabelSpan],
        full_input: &[u8],
        special_label: Option<&[u8]>,
    ) -> Result<Option<f64>, DecodeError> {
        self.temp_attrs.clear();
        if label_spans.is_empty() {
            return Ok(None);
        }

        let mut prev_key_id: Option<u32> = None;
        let mut sorted_by_key_id = true;
        let mut seen_special = false;
        let mut special_val: Option<f64> = None;

        for (span_idx, span) in label_spans.iter().enumerate() {
            let raw_key = &full_input[span.name_start..span.name_end];
            let raw_val = &full_input[span.val_start..span.val_end];

            if let Some(target_key) = special_label {
                let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
                if key == target_key {
                    if seen_special {
                        return Err(DecodeError::DuplicateLabel {
                            metric: self.current_family.name_string(),
                            label: String::from_utf8_lossy(target_key).into_owned(),
                        });
                    }
                    seen_special = true;
                    if !raw_val.is_empty() {
                        let val = unescape_bytes_into(raw_val, true, &mut self.unescape_buf);
                        special_val = std::str::from_utf8(val).ok().and_then(parse_go_float);
                    }
                    continue;
                }
            }

            if raw_val.is_empty() {
                self.validate_empty_label(span_idx, label_spans, full_input)?;
                continue;
            }

            let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
            if key == METRIC_NAME_LABEL.as_bytes() {
                return Err(DecodeError::DuplicateLabel {
                    metric: self.current_family.name_string(),
                    label: METRIC_NAME_LABEL.to_owned(),
                });
            }
            let key_id =
                self.key_interner
                    .intern(key)
                    .ok_or_else(|| DecodeError::CapacityOverflow {
                        table: "DpAttrs.key",
                        count: self.key_interner.len(),
                    })?;

            if let Some(prev) = prev_key_id
                && key_id <= prev
            {
                if key_id == prev {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.current_family.name_string(),
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
                        table: "DpAttrs.str",
                        count: self.val_interner.len(),
                    })?;

            self.temp_attrs.push((key_id, val_id));
        }

        if !sorted_by_key_id {
            self.temp_attrs.sort_unstable_by_key(|&(k, _)| k);
            for window in self.temp_attrs.windows(2) {
                if window[0].0 == window[1].0 {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.current_family.name_string(),
                        label: self.key_interner.get(window[0].0).to_owned(),
                    });
                }
            }
        }

        Ok(special_val)
    }

    #[inline]
    fn append_series_labels(
        &mut self,
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
            let raw_key = &full_input[span.name_start..span.name_end];
            let raw_val = &full_input[span.val_start..span.val_end];

            // Prometheus treats `label=""` as an absent label (`labels.Builder` deletes empty
            // values). Validate reserved and duplicate label names before skipping so unused keys
            // and `""` never enter the OTAP attribute dictionaries. `unescape_bytes_into` never
            // shrinks a non-empty slice to zero bytes, so `raw_val.is_empty()` is exact.
            if raw_val.is_empty() {
                self.validate_empty_label(span_idx, label_spans, full_input)?;
                continue;
            }

            let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
            if key == METRIC_NAME_LABEL.as_bytes() {
                return Err(DecodeError::DuplicateLabel {
                    metric: self.current_family.name_string(),
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
                        metric: self.current_family.name_string(),
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
                        metric: self.current_family.name_string(),
                        label: self.key_interner.get(window[0].0).to_owned(),
                    });
                }
            }
        }

        Ok(())
    }

    fn append_number_exemplar(
        &mut self,
        dp_id: u32,
        ex_val: f64,
        ex_ts_ms: Option<i64>,
        ex_spans: &[LabelSpan],
        full_input: &[u8],
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let ex_idx = self.num_ex_parent_ids.len();
        let ex_id = u32::try_from(ex_idx).map_err(|_| DecodeError::CapacityOverflow {
            table: "NumberDpExemplars",
            count: ex_idx + 1,
        })?;
        let ts_ns = match ex_ts_ms {
            Some(ms) => ms_to_ns(ms)?,
            None => default_ts_ns.unwrap_or(0),
        };
        let (span_id, trace_id) =
            self.extract_exemplar_labels(ex_id, ex_spans, full_input, true)?;
        self.num_ex_parent_ids.push(dp_id);
        self.num_ex_timestamps.push(ts_ns);
        self.num_ex_values.push(ex_val);
        push_optional_fixed_bytes(
            &mut self.num_ex_span_ids,
            &mut self.num_ex_span_valid,
            &mut self.num_ex_any_span,
            ex_idx,
            span_id,
        );
        push_optional_fixed_bytes(
            &mut self.num_ex_trace_ids,
            &mut self.num_ex_trace_valid,
            &mut self.num_ex_any_trace,
            ex_idx,
            trace_id,
        );
        Ok(())
    }

    fn stage_pending_histogram_exemplar(
        &mut self,
        group_idx: u32,
        ex_val: f64,
        ex_ts_ms: Option<i64>,
        ex_spans: &[LabelSpan],
        full_input: &[u8],
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let ts_ns = match ex_ts_ms {
            Some(ms) => ms_to_ns(ms)?,
            None => default_ts_ns.unwrap_or(0),
        };
        let attr_start = self.pending_exemplar_attrs.len() as u32;
        let (span_id, trace_id) = self.extract_exemplar_labels(0, ex_spans, full_input, false)?;
        let attr_len = (self.pending_exemplar_attrs.len() as u32) - attr_start;
        self.pending_exemplars.push(PendingExemplar {
            group_idx,
            ts_ns,
            value: ex_val,
            span_id,
            trace_id,
            attr_start,
            attr_len,
        });
        Ok(())
    }

    fn extract_exemplar_labels(
        &mut self,
        ex_id: u32,
        ex_spans: &[LabelSpan],
        full_input: &[u8],
        into_number_exemplar_attrs: bool,
    ) -> Result<SpanAndTraceId, DecodeError> {
        let mut span_id: Option<[u8; 8]> = None;
        let mut trace_id: Option<[u8; 16]> = None;
        let mut seen_span_id = false;
        let mut seen_trace_id = false;

        let attr_start = if into_number_exemplar_attrs {
            self.num_ex_attr_pairs.len()
        } else {
            self.pending_exemplar_attrs.len()
        };
        let mut prev_key_id: Option<u32> = None;
        let mut sorted_by_key_id = true;

        for (span_idx, span) in ex_spans.iter().enumerate() {
            let raw_key = &full_input[span.name_start..span.name_end];
            let raw_val = &full_input[span.val_start..span.val_end];

            if raw_val.is_empty() {
                self.validate_empty_label(span_idx, ex_spans, full_input)?;
                continue;
            }

            let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
            if key == b"span_id" {
                if seen_span_id {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.current_family.name_string(),
                        label: "span_id".to_owned(),
                    });
                }
                seen_span_id = true;
                let val = unescape_bytes_into(raw_val, true, &mut self.unescape_buf);
                if let Some(parsed) = parse_hex_array::<8>(val) {
                    span_id = Some(parsed);
                    continue;
                }
            } else if key == b"trace_id" {
                if seen_trace_id {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.current_family.name_string(),
                        label: "trace_id".to_owned(),
                    });
                }
                seen_trace_id = true;
                let val = unescape_bytes_into(raw_val, true, &mut self.unescape_buf);
                if let Some(parsed) = parse_hex_array::<16>(val) {
                    trace_id = Some(parsed);
                    continue;
                }
            }

            let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
            let key_id =
                self.key_interner
                    .intern(key)
                    .ok_or_else(|| DecodeError::CapacityOverflow {
                        table: "ExemplarAttrs.key",
                        count: self.key_interner.len(),
                    })?;

            if let Some(prev) = prev_key_id
                && key_id <= prev
            {
                if key_id == prev {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.current_family.name_string(),
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
                        table: "ExemplarAttrs.str",
                        count: self.val_interner.len(),
                    })?;

            if into_number_exemplar_attrs {
                self.num_ex_attr_parent_ids.push(ex_id);
                self.num_ex_attr_pairs.push((key_id, val_id));
            } else {
                self.pending_exemplar_attrs.push((key_id, val_id));
            }
        }

        if !sorted_by_key_id {
            let slice = if into_number_exemplar_attrs {
                &mut self.num_ex_attr_pairs[attr_start..]
            } else {
                &mut self.pending_exemplar_attrs[attr_start..]
            };
            slice.sort_unstable_by_key(|&(k, _)| k);
            for window in slice.windows(2) {
                if window[0].0 == window[1].0 {
                    return Err(DecodeError::DuplicateLabel {
                        metric: self.current_family.name_string(),
                        label: self.key_interner.get(window[0].0).to_owned(),
                    });
                }
            }
        }

        Ok((span_id, trace_id))
    }

    #[cold]
    fn validate_empty_label(
        &mut self,
        span_idx: usize,
        label_spans: &[LabelSpan],
        full_input: &[u8],
    ) -> Result<(), DecodeError> {
        let span = &label_spans[span_idx];
        let raw_key = &full_input[span.name_start..span.name_end];
        let key = unescape_bytes_into(raw_key, true, &mut self.unescape_buf);
        if key == METRIC_NAME_LABEL.as_bytes() {
            return Err(DecodeError::DuplicateLabel {
                metric: self.current_family.name_string(),
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
                    metric: self.current_family.name_string(),
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
        start_ts_ns: Option<i64>,
        sample_ts_ms: Option<i64>,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let dp_index = self.dp_parent_ids.len();
        self.dp_parent_ids.push(parent_id);
        self.dp_values.push(value);

        push_optional_i64(
            &mut self.dp_start_timestamps,
            &mut self.dp_start_time_valid,
            dp_index,
            start_ts_ns,
        );

        let resolved_ts_ns = match sample_ts_ms {
            Some(ms) => Some(ms_to_ns(ms)?),
            None => default_ts_ns,
        };
        push_optional_i64(
            &mut self.dp_timestamps,
            &mut self.dp_time_valid,
            dp_index,
            resolved_ts_ns,
        );

        Ok(())
    }

    fn finish_batches(&mut self, has_default_ts: bool) -> Result<OtapMetricRecords, DecodeError> {
        if self.um_metric_types.is_empty() {
            return Ok(OtapMetricRecords::default());
        }

        let univariate_metrics = Some(self.build_univariate_metrics()?);
        let metric_attrs = Some(self.build_metric_attrs()?);

        let number_data_points = if self.dp_parent_ids.is_empty() {
            None
        } else {
            Some(self.build_number_data_points(has_default_ts)?)
        };

        let summary_data_points = if self.sum_parent_ids.is_empty() {
            None
        } else {
            Some(self.build_summary_data_points(has_default_ts)?)
        };

        let histogram_data_points = if self.hist_parent_ids.is_empty() {
            None
        } else {
            Some(self.build_histogram_data_points(has_default_ts)?)
        };

        let any_dp_attrs = !self.dp_attr_pairs.is_empty()
            || !self.sum_attr_pairs.is_empty()
            || !self.hist_attr_pairs.is_empty()
            || !self.num_ex_attr_pairs.is_empty()
            || !self.hist_ex_attr_pairs.is_empty();

        let (key_enc, val_enc, key_dict, val_dict) = if any_dp_attrs {
            let unique_keys = self.key_interner.len();
            let unique_vals = self.val_interner.len();
            let ke = if unique_keys <= 256 {
                KeyEncoding::DictU8
            } else if unique_keys <= 65_536 {
                KeyEncoding::DictU16
            } else {
                KeyEncoding::Utf8
            };
            let ve = if unique_vals <= 65_536 {
                ValEncoding::DictU16
            } else {
                ValEncoding::Utf8
            };
            let kd: Option<ArrayRef> = (ke != KeyEncoding::Utf8)
                .then(|| Arc::new(self.key_interner.to_string_array()) as ArrayRef);
            let vd: Option<ArrayRef> = (ve != ValEncoding::Utf8)
                .then(|| Arc::new(self.val_interner.to_string_array()) as ArrayRef);
            (ke, ve, kd, vd)
        } else {
            (KeyEncoding::DictU8, ValEncoding::DictU16, None, None)
        };

        let number_dp_attrs = if self.dp_attr_pairs.is_empty() {
            None
        } else {
            let parents = std::mem::take(&mut self.dp_attr_parent_ids);
            let pairs = std::mem::take(&mut self.dp_attr_pairs);
            let res = self.build_u32_attrs_batch(
                &parents,
                &pairs,
                key_enc,
                val_enc,
                key_dict.as_ref(),
                val_dict.as_ref(),
            );
            self.dp_attr_parent_ids = parents;
            self.dp_attr_pairs = pairs;
            Some(res?)
        };

        let summary_dp_attrs = if self.sum_attr_pairs.is_empty() {
            None
        } else {
            let parents = std::mem::take(&mut self.sum_attr_parent_ids);
            let pairs = std::mem::take(&mut self.sum_attr_pairs);
            let res = self.build_u32_attrs_batch(
                &parents,
                &pairs,
                key_enc,
                val_enc,
                key_dict.as_ref(),
                val_dict.as_ref(),
            );
            self.sum_attr_parent_ids = parents;
            self.sum_attr_pairs = pairs;
            Some(res?)
        };

        let histogram_dp_attrs = if self.hist_attr_pairs.is_empty() {
            None
        } else {
            let parents = std::mem::take(&mut self.hist_attr_parent_ids);
            let pairs = std::mem::take(&mut self.hist_attr_pairs);
            let res = self.build_u32_attrs_batch(
                &parents,
                &pairs,
                key_enc,
                val_enc,
                key_dict.as_ref(),
                val_dict.as_ref(),
            );
            self.hist_attr_parent_ids = parents;
            self.hist_attr_pairs = pairs;
            Some(res?)
        };

        let number_dp_exemplars = if self.num_ex_parent_ids.is_empty() {
            None
        } else {
            Some(self.build_number_dp_exemplars()?)
        };

        let histogram_dp_exemplars = if self.hist_ex_parent_ids.is_empty() {
            None
        } else {
            Some(self.build_histogram_dp_exemplars()?)
        };

        let number_dp_exemplar_attrs = if self.num_ex_attr_pairs.is_empty() {
            None
        } else {
            let parents = std::mem::take(&mut self.num_ex_attr_parent_ids);
            let pairs = std::mem::take(&mut self.num_ex_attr_pairs);
            let res = self.build_u32_attrs_batch(
                &parents,
                &pairs,
                key_enc,
                val_enc,
                key_dict.as_ref(),
                val_dict.as_ref(),
            );
            self.num_ex_attr_parent_ids = parents;
            self.num_ex_attr_pairs = pairs;
            Some(res?)
        };

        let histogram_dp_exemplar_attrs = if self.hist_ex_attr_pairs.is_empty() {
            None
        } else {
            let parents = std::mem::take(&mut self.hist_ex_attr_parent_ids);
            let pairs = std::mem::take(&mut self.hist_ex_attr_pairs);
            let res = self.build_u32_attrs_batch(
                &parents,
                &pairs,
                key_enc,
                val_enc,
                key_dict.as_ref(),
                val_dict.as_ref(),
            );
            self.hist_ex_attr_parent_ids = parents;
            self.hist_ex_attr_pairs = pairs;
            Some(res?)
        };

        Ok(OtapMetricRecords {
            univariate_metrics,
            metric_attrs,
            number_data_points,
            summary_data_points,
            histogram_data_points,
            number_dp_attrs,
            summary_dp_attrs,
            histogram_dp_attrs,
            number_dp_exemplars,
            histogram_dp_exemplars,
            number_dp_exemplar_attrs,
            histogram_dp_exemplar_attrs,
        })
    }

    fn build_univariate_metrics(&mut self) -> Result<RecordBatch, DecodeError> {
        let num_metrics = self.um_metric_types.len();
        let schema = univariate_metrics_schema(
            self.um_any_help,
            self.um_any_unit,
            self.um_any_temporality,
            self.um_any_sum,
        );
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        self.scratch_u16.clear();
        self.scratch_u16.extend((0..num_metrics).map(|i| i as u16));
        cols.push(Arc::new(UInt16Array::new(
            scalar_from_slice(&self.scratch_u16),
            None,
        )));

        cols.push(Arc::new(UInt8Array::new(
            scalar_from_slice(&self.um_metric_types),
            None,
        )));

        cols.push(Arc::new(string_array_from_slices(
            &self.um_name_offsets,
            &self.um_name_bytes,
            None,
        )));

        if self.um_any_help {
            let nulls = if self.um_all_help {
                None
            } else {
                Some(NullBuffer::new(BooleanBuffer::from_iter(
                    self.um_desc_valid.iter().copied(),
                )))
            };
            cols.push(Arc::new(string_array_from_slices(
                &self.um_desc_offsets,
                &self.um_desc_bytes,
                nulls,
            )));
        }

        if self.um_any_unit {
            let nulls = if self.um_all_unit {
                None
            } else {
                Some(NullBuffer::new(BooleanBuffer::from_iter(
                    self.um_unit_valid.iter().copied(),
                )))
            };
            cols.push(Arc::new(string_array_from_slices(
                &self.um_unit_offsets,
                &self.um_unit_bytes,
                nulls,
            )));
        }

        if self.um_any_temporality {
            self.scratch_i32.clear();
            self.scratch_i32
                .extend(self.um_metric_types.iter().map(|&t| {
                    if t == OTAP_METRIC_TYPE_SUM || t == OTAP_METRIC_TYPE_HISTOGRAM {
                        AGGREGATION_TEMPORALITY_CUMULATIVE
                    } else {
                        0
                    }
                }));
            let temp_nulls = if self.um_all_temporality {
                None
            } else {
                Some(NullBuffer::new(BooleanBuffer::from_iter(
                    self.um_metric_types
                        .iter()
                        .map(|&t| t == OTAP_METRIC_TYPE_SUM || t == OTAP_METRIC_TYPE_HISTOGRAM),
                )))
            };
            cols.push(Arc::new(Int32Array::new(
                scalar_from_slice(&self.scratch_i32),
                temp_nulls,
            )));
        }

        if self.um_any_sum {
            let monotonic_values = BooleanBuffer::from_iter(self.um_is_monotonic.iter().copied());
            let sum_nulls = if self.um_all_sum {
                None
            } else {
                Some(NullBuffer::new(BooleanBuffer::from_iter(
                    self.um_metric_types
                        .iter()
                        .map(|&t| t == OTAP_METRIC_TYPE_SUM),
                )))
            };
            cols.push(Arc::new(BooleanArray::new(monotonic_values, sum_nulls)));
        }

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn build_metric_attrs(&mut self) -> Result<RecordBatch, DecodeError> {
        let num_metrics = self.um_metric_types.len();

        if self.extra_metric_attr_pairs.is_empty() {
            // Fast path: only `prometheus.type` is present on each metric row, using pre-built
            // static dictionary arrays.
            let parent_id_arr = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);

            self.scratch_u8.clear();
            self.scratch_u8.resize(num_metrics, 0);
            let key_keys = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);
            let key_dict = DictionaryArray::<UInt8Type>::new(key_keys, metric_attrs_key_dict());

            self.scratch_u8.fill(ATTR_VALUE_TYPE_STR);
            let type_arr = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);

            let str_keys = UInt16Array::new(scalar_from_slice(&self.um_attr_type_keys), None);
            let str_dict =
                DictionaryArray::<UInt16Type>::new(str_keys, metric_attrs_type_val_dict());

            return Ok(RecordBatch::try_new(
                metric_attrs_schema(),
                vec![
                    Arc::new(parent_id_arr),
                    Arc::new(key_dict),
                    Arc::new(type_arr),
                    Arc::new(str_dict),
                ],
            )?);
        }

        // Dynamic path when `InfoPolicy::MetricAttrs` attaches `info` labels to `MetricAttrs`.
        let prom_type_key_id = self
            .key_interner
            .intern(PROMETHEUS_TYPE_METADATA_KEY.as_bytes())
            .ok_or_else(|| DecodeError::CapacityOverflow {
                table: "MetricAttrs.key",
                count: self.key_interner.len(),
            })?;
        const TYPE_NAMES: [&[u8]; 8] = [
            b"unknown",
            b"counter",
            b"gauge",
            b"histogram",
            b"gaugehistogram",
            b"summary",
            b"info",
            b"stateset",
        ];
        let mut type_val_ids = [0u32; 8];
        for (i, name) in TYPE_NAMES.iter().enumerate() {
            type_val_ids[i] =
                self.val_interner
                    .intern(name)
                    .ok_or_else(|| DecodeError::CapacityOverflow {
                        table: "MetricAttrs.str",
                        count: self.val_interner.len(),
                    })?;
        }

        let total_rows = num_metrics + self.extra_metric_attr_pairs.len();
        let mut parents = Vec::with_capacity(total_rows);
        let mut pairs = Vec::with_capacity(total_rows);
        for (m_idx, &t_idx) in self.um_attr_type_keys.iter().enumerate() {
            parents.push(m_idx as u16);
            pairs.push((prom_type_key_id, type_val_ids[t_idx as usize]));
        }
        parents.extend_from_slice(&self.extra_metric_attr_parents);
        pairs.extend_from_slice(&self.extra_metric_attr_pairs);

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
        let key_dict: Option<ArrayRef> = (key_enc != KeyEncoding::Utf8)
            .then(|| Arc::new(self.key_interner.to_string_array()) as ArrayRef);
        let val_dict: Option<ArrayRef> = (val_enc != ValEncoding::Utf8)
            .then(|| Arc::new(self.val_interner.to_string_array()) as ArrayRef);

        let parent_id_arr: ArrayRef = Arc::new(UInt16Array::new(scalar_from_slice(&parents), None));
        let (key_arr, type_arr, val_arr) = self.build_attr_columns(
            &pairs,
            key_enc,
            val_enc,
            key_dict.as_ref(),
            val_dict.as_ref(),
            "MetricAttrs",
        )?;

        Ok(RecordBatch::try_new(
            metric_attrs_schema_for(key_enc, val_enc),
            vec![parent_id_arr, key_arr, type_arr, val_arr],
        )?)
    }

    fn build_number_data_points(
        &mut self,
        has_default_ts: bool,
    ) -> Result<RecordBatch, DecodeError> {
        let num_dps = self.dp_parent_ids.len();
        let has_id = !self.dp_attr_pairs.is_empty() || !self.num_ex_parent_ids.is_empty();
        let has_start_time = !self.dp_start_timestamps.is_empty();
        let start_time_nullable = has_start_time && !self.dp_start_time_valid.is_empty();
        let has_time = !self.dp_timestamps.is_empty();
        let time_nullable = has_time && !has_default_ts && !self.dp_time_valid.is_empty();

        let schema = number_dp_schema(
            has_id,
            has_start_time,
            start_time_nullable,
            has_time,
            time_nullable,
        );
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

        if has_start_time {
            cols.push(build_timestamp_col(
                &self.dp_start_timestamps,
                &self.dp_start_time_valid,
                start_time_nullable,
            ));
        }

        if has_time {
            cols.push(build_timestamp_col(
                &self.dp_timestamps,
                &self.dp_time_valid,
                time_nullable,
            ));
        }

        cols.push(Arc::new(Float64Array::new(
            scalar_from_slice(&self.dp_values),
            None,
        )));

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn build_summary_data_points(
        &mut self,
        has_default_ts: bool,
    ) -> Result<RecordBatch, DecodeError> {
        let num_dps = self.sum_parent_ids.len();
        let has_id = !self.sum_attr_pairs.is_empty();
        let has_start_time = !self.sum_start_timestamps.is_empty();
        let start_time_nullable = has_start_time && !self.sum_start_time_valid.is_empty();
        let has_time = !self.sum_timestamps.is_empty();
        let time_nullable = has_time && !has_default_ts && !self.sum_time_valid.is_empty();
        let count_nullable = !self.sum_count_valid.is_empty();
        let sum_nullable = !self.sum_sum_valid.is_empty();

        let schema = summary_dp_schema(
            has_id,
            has_start_time,
            start_time_nullable,
            has_time,
            time_nullable,
            count_nullable,
            sum_nullable,
        );
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
            scalar_from_slice(&self.sum_parent_ids),
            None,
        )));

        if has_start_time {
            cols.push(build_timestamp_col(
                &self.sum_start_timestamps,
                &self.sum_start_time_valid,
                start_time_nullable,
            ));
        }

        if has_time {
            cols.push(build_timestamp_col(
                &self.sum_timestamps,
                &self.sum_time_valid,
                time_nullable,
            ));
        }

        let count_nulls = count_nullable.then(|| {
            NullBuffer::new(BooleanBuffer::from_iter(
                self.sum_count_valid.iter().copied(),
            ))
        });
        cols.push(Arc::new(UInt64Array::new(
            scalar_from_slice(&self.sum_counts),
            count_nulls,
        )));

        let sum_nulls = sum_nullable
            .then(|| NullBuffer::new(BooleanBuffer::from_iter(self.sum_sum_valid.iter().copied())));
        cols.push(Arc::new(Float64Array::new(
            scalar_from_slice(&self.sum_sums),
            sum_nulls,
        )));

        let q_struct = StructArray::try_new(
            summary_quantile_struct_fields(),
            vec![
                Arc::new(Float64Array::new(
                    scalar_from_slice(&self.sum_quantile_bounds),
                    None,
                )),
                Arc::new(Float64Array::new(
                    scalar_from_slice(&self.sum_quantile_vals),
                    None,
                )),
            ],
            None,
        )?;
        let q_list = ListArray::new(
            summary_quantile_item_field(),
            OffsetBuffer::new(scalar_from_slice(&self.sum_quantile_offsets)),
            Arc::new(q_struct),
            None,
        );
        cols.push(Arc::new(q_list));

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn build_histogram_data_points(
        &mut self,
        has_default_ts: bool,
    ) -> Result<RecordBatch, DecodeError> {
        let num_dps = self.hist_parent_ids.len();
        let has_id = !self.hist_attr_pairs.is_empty() || !self.hist_ex_parent_ids.is_empty();
        let has_start_time = !self.hist_start_timestamps.is_empty();
        let start_time_nullable = has_start_time && !self.hist_start_time_valid.is_empty();
        let has_time = !self.hist_timestamps.is_empty();
        let time_nullable = has_time && !has_default_ts && !self.hist_time_valid.is_empty();
        let count_nullable = !self.hist_count_valid.is_empty();
        let sum_nullable = !self.hist_sum_valid.is_empty();

        let schema = histogram_dp_schema(
            has_id,
            has_start_time,
            start_time_nullable,
            has_time,
            time_nullable,
            count_nullable,
            sum_nullable,
        );
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
            scalar_from_slice(&self.hist_parent_ids),
            None,
        )));

        if has_start_time {
            cols.push(build_timestamp_col(
                &self.hist_start_timestamps,
                &self.hist_start_time_valid,
                start_time_nullable,
            ));
        }

        if has_time {
            cols.push(build_timestamp_col(
                &self.hist_timestamps,
                &self.hist_time_valid,
                time_nullable,
            ));
        }

        let count_nulls = count_nullable.then(|| {
            NullBuffer::new(BooleanBuffer::from_iter(
                self.hist_count_valid.iter().copied(),
            ))
        });
        cols.push(Arc::new(UInt64Array::new(
            scalar_from_slice(&self.hist_counts),
            count_nulls,
        )));

        let sum_nulls = sum_nullable.then(|| {
            NullBuffer::new(BooleanBuffer::from_iter(
                self.hist_sum_valid.iter().copied(),
            ))
        });
        cols.push(Arc::new(Float64Array::new(
            scalar_from_slice(&self.hist_sums),
            sum_nulls,
        )));

        let bucket_counts_list = ListArray::new(
            histogram_bucket_counts_item_field(),
            OffsetBuffer::new(scalar_from_slice(&self.hist_bucket_offsets)),
            Arc::new(UInt64Array::new(
                scalar_from_slice(&self.hist_bucket_counts),
                None,
            )),
            None,
        );
        cols.push(Arc::new(bucket_counts_list));

        let explicit_bounds_list = ListArray::new(
            histogram_explicit_bounds_item_field(),
            OffsetBuffer::new(scalar_from_slice(&self.hist_bounds_offsets)),
            Arc::new(Float64Array::new(
                scalar_from_slice(&self.hist_bounds_vals),
                None,
            )),
            None,
        );
        cols.push(Arc::new(explicit_bounds_list));

        Ok(RecordBatch::try_new(schema, cols)?)
    }

    fn build_number_dp_exemplars(&mut self) -> Result<RecordBatch, DecodeError> {
        let has_id = !self.num_ex_attr_pairs.is_empty();
        let schema = exemplars_schema(has_id, self.num_ex_any_span, self.num_ex_any_trace);
        build_exemplars_batch(
            schema,
            has_id,
            &self.num_ex_parent_ids,
            &self.num_ex_timestamps,
            &self.num_ex_values,
            self.num_ex_any_span,
            &self.num_ex_span_ids,
            &self.num_ex_span_valid,
            self.num_ex_any_trace,
            &self.num_ex_trace_ids,
            &self.num_ex_trace_valid,
            &mut self.scratch_u32,
            &mut self.scratch_bytes,
        )
    }

    fn build_histogram_dp_exemplars(&mut self) -> Result<RecordBatch, DecodeError> {
        let has_id = !self.hist_ex_attr_pairs.is_empty();
        let schema = exemplars_schema(has_id, self.hist_ex_any_span, self.hist_ex_any_trace);
        build_exemplars_batch(
            schema,
            has_id,
            &self.hist_ex_parent_ids,
            &self.hist_ex_timestamps,
            &self.hist_ex_values,
            self.hist_ex_any_span,
            &self.hist_ex_span_ids,
            &self.hist_ex_span_valid,
            self.hist_ex_any_trace,
            &self.hist_ex_trace_ids,
            &self.hist_ex_trace_valid,
            &mut self.scratch_u32,
            &mut self.scratch_bytes,
        )
    }

    fn build_u32_attrs_batch(
        &mut self,
        parent_ids: &[u32],
        pairs: &[(u32, u32)],
        key_enc: KeyEncoding,
        val_enc: ValEncoding,
        key_dict: Option<&ArrayRef>,
        val_dict: Option<&ArrayRef>,
    ) -> Result<RecordBatch, DecodeError> {
        let parent_id_arr: ArrayRef =
            Arc::new(UInt32Array::new(scalar_from_slice(parent_ids), None));
        let (key_arr, type_arr, val_arr) =
            self.build_attr_columns(pairs, key_enc, val_enc, key_dict, val_dict, "DpAttrs")?;
        Ok(RecordBatch::try_new(
            dp_attrs_schema(key_enc, val_enc),
            vec![parent_id_arr, key_arr, type_arr, val_arr],
        )?)
    }

    fn build_attr_columns(
        &mut self,
        pairs: &[(u32, u32)],
        key_enc: KeyEncoding,
        val_enc: ValEncoding,
        key_dict: Option<&ArrayRef>,
        val_dict: Option<&ArrayRef>,
        table: &'static str,
    ) -> Result<(ArrayRef, ArrayRef, ArrayRef), DecodeError> {
        let num_attrs = pairs.len();

        let key_arr: ArrayRef = match key_enc {
            KeyEncoding::DictU8 => {
                self.scratch_u8.clear();
                self.scratch_u8.extend(pairs.iter().map(|&(k, _)| k as u8));
                let keys = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);
                let dict = Arc::clone(key_dict.expect("key_dict present for DictU8"));
                Arc::new(DictionaryArray::<UInt8Type>::new(keys, dict))
            }
            KeyEncoding::DictU16 => {
                self.scratch_u16.clear();
                self.scratch_u16
                    .extend(pairs.iter().map(|&(k, _)| k as u16));
                let keys = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);
                let dict = Arc::clone(key_dict.expect("key_dict present for DictU16"));
                Arc::new(DictionaryArray::<UInt16Type>::new(keys, dict))
            }
            KeyEncoding::Utf8 => Arc::new(materialize_plain_strings(
                &self.key_interner,
                pairs.iter().map(|&(k, _)| k),
                &mut self.scratch_i32,
                &mut self.scratch_bytes,
                table,
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
                    .extend(pairs.iter().map(|&(_, v)| v as u16));
                let keys = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);
                let dict = Arc::clone(val_dict.expect("val_dict present for DictU16"));
                Arc::new(DictionaryArray::<UInt16Type>::new(keys, dict))
            }
            ValEncoding::Utf8 => Arc::new(materialize_plain_strings(
                &self.val_interner,
                pairs.iter().map(|&(_, v)| v),
                &mut self.scratch_i32,
                &mut self.scratch_bytes,
                table,
            )?),
        };

        Ok((key_arr, type_arr, val_arr))
    }
}

/// Lending-style streaming iterator yielding bounded, non-empty [`OtapMetricRecords`] chunks from
/// a single exposition scrape.
///
/// Created by [`ArrowDecoder::decode_stream`]. Each `next()` call advances the underlying parser
/// until [`DecodeOptions::max_data_points_per_batch`], OTAP's 65,536-metric `u16` batch limit, or
/// end-of-input is reached, then finalizes a batch and clears per-batch staging buffers so peak
/// decoder scratch memory stays proportional to one chunk.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct DecodeStream<'dec, 'input> {
    decoder: &'dec mut ArrowDecoder,
    parser: ActiveParser<'input>,
    opts: DecodeOptions,
    default_ts_ns: Option<i64>,
    max_data_points: usize,
    pending_series: bool,
    init_err: Option<DecodeError>,
    done: bool,
}

impl Iterator for DecodeStream<'_, '_> {
    type Item = Result<OtapMetricRecords, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        if let Some(err) = self.init_err.take() {
            self.done = true;
            return Some(Err(err));
        }
        match self.decoder.ingest_entries::<true>(
            &mut self.parser,
            &self.opts,
            self.default_ts_ns,
            self.max_data_points,
            &mut self.pending_series,
            &mut self.done,
        ) {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => None,
            Err(err) => {
                self.done = true;
                self.decoder.reset_scrape();
                Some(Err(err))
            }
        }
    }
}

impl FusedIterator for DecodeStream<'_, '_> {}

impl Drop for DecodeStream<'_, '_> {
    fn drop(&mut self) {
        self.parser.take_scratch_into(self.decoder);
    }
}

#[inline]
fn ms_to_ns(timestamp_ms: i64) -> Result<i64, DecodeError> {
    timestamp_ms
        .checked_mul(1_000_000)
        .ok_or(DecodeError::TimestampOverflow { timestamp_ms })
}

#[inline]
fn f64_to_u64_count(value: f64) -> u64 {
    if value.is_sign_positive() && value.is_finite() {
        value as u64
    } else {
        0
    }
}

#[inline]
fn push_optional_i64(
    values: &mut Vec<i64>,
    valid: &mut Vec<bool>,
    row_index: usize,
    val: Option<i64>,
) {
    match val {
        Some(v) => {
            if row_index > 0 && values.is_empty() {
                values.resize(row_index, 0);
                valid.resize(row_index, false);
            }
            values.push(v);
            if !valid.is_empty() {
                valid.push(true);
            }
        }
        None => {
            if !values.is_empty() {
                if valid.is_empty() {
                    valid.resize(row_index, true);
                }
                values.push(0);
                valid.push(false);
            }
        }
    }
}

#[inline]
fn set_optional_i64(
    values: &mut Vec<i64>,
    valid: &mut Vec<bool>,
    total_rows: usize,
    row_index: usize,
    val: i64,
) {
    if values.is_empty() {
        values.resize(total_rows, 0);
        if total_rows > 1 {
            valid.resize(total_rows, false);
        }
    }
    values[row_index] = val;
    if !valid.is_empty() {
        valid[row_index] = true;
    }
}

#[inline]
fn push_optional_u64(
    values: &mut Vec<u64>,
    valid: &mut Vec<bool>,
    row_index: usize,
    val: Option<u64>,
) {
    match val {
        Some(v) => {
            values.push(v);
            if !valid.is_empty() {
                valid.push(true);
            }
        }
        None => {
            if valid.is_empty() {
                valid.resize(row_index, true);
            }
            values.push(0);
            valid.push(false);
        }
    }
}

#[inline]
fn push_optional_f64(
    values: &mut Vec<f64>,
    valid: &mut Vec<bool>,
    row_index: usize,
    val: Option<f64>,
) {
    match val {
        Some(v) => {
            values.push(v);
            if !valid.is_empty() {
                valid.push(true);
            }
        }
        None => {
            if valid.is_empty() {
                valid.resize(row_index, true);
            }
            values.push(0.0);
            valid.push(false);
        }
    }
}

#[inline]
fn push_optional_fixed_bytes<const N: usize>(
    values: &mut Vec<[u8; N]>,
    valid: &mut Vec<bool>,
    any_present: &mut bool,
    row_index: usize,
    val: Option<[u8; N]>,
) {
    match val {
        Some(v) => {
            if !*any_present {
                *any_present = true;
                values.resize(row_index, [0u8; N]);
                if row_index > 0 {
                    valid.resize(row_index, false);
                    valid.push(true);
                }
            } else if !valid.is_empty() {
                valid.push(true);
            }
            values.push(v);
        }
        None => {
            if *any_present {
                if valid.is_empty() {
                    valid.resize(row_index, true);
                }
                values.push([0u8; N]);
                valid.push(false);
            }
        }
    }
}

fn parse_hex_array<const N: usize>(raw: &[u8]) -> Option<[u8; N]> {
    if raw.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, &[hi_b, lo_b]) in raw.as_chunks::<2>().0.iter().enumerate() {
        let hi = hex_nibble(hi_b)?;
        let lo = hex_nibble(lo_b)?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

#[inline]
const fn hex_nibble(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[inline]
fn build_timestamp_col(values: &[i64], valid: &[bool], nullable: bool) -> ArrayRef {
    let nulls = if nullable {
        Some(NullBuffer::new(BooleanBuffer::from_iter(
            valid.iter().copied(),
        )))
    } else {
        None
    };
    let arr = TimestampNanosecondArray::new(scalar_from_slice(values), nulls)
        .with_timezone(UTC_TIME_ZONE);
    Arc::new(arr)
}

#[allow(clippy::too_many_arguments)]
fn build_exemplars_batch(
    schema: SchemaRef,
    has_id: bool,
    parent_ids: &[u32],
    timestamps: &[i64],
    values: &[f64],
    has_span_id: bool,
    span_ids: &[[u8; 8]],
    span_valid: &[bool],
    has_trace_id: bool,
    trace_ids: &[[u8; 16]],
    trace_valid: &[bool],
    scratch_u32: &mut Vec<u32>,
    scratch_bytes: &mut Vec<u8>,
) -> Result<RecordBatch, DecodeError> {
    let num_rows = parent_ids.len();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    if has_id {
        scratch_u32.clear();
        scratch_u32.extend(0..num_rows as u32);
        cols.push(Arc::new(UInt32Array::new(
            scalar_from_slice(scratch_u32),
            None,
        )));
    }

    cols.push(Arc::new(UInt32Array::new(
        scalar_from_slice(parent_ids),
        None,
    )));

    let ts_arr = TimestampNanosecondArray::new(scalar_from_slice(timestamps), None)
        .with_timezone(UTC_TIME_ZONE);
    cols.push(Arc::new(ts_arr));

    cols.push(Arc::new(Float64Array::new(scalar_from_slice(values), None)));

    if has_span_id {
        scratch_bytes.clear();
        for id in span_ids {
            scratch_bytes.extend_from_slice(id);
        }
        let nulls = (!span_valid.is_empty())
            .then(|| NullBuffer::new(BooleanBuffer::from_iter(span_valid.iter().copied())));
        let arr =
            FixedSizeBinaryArray::try_new(8, Buffer::from_slice_ref(&scratch_bytes[..]), nulls)?;
        cols.push(Arc::new(arr));
    }

    if has_trace_id {
        scratch_bytes.clear();
        for id in trace_ids {
            scratch_bytes.extend_from_slice(id);
        }
        let nulls = (!trace_valid.is_empty())
            .then(|| NullBuffer::new(BooleanBuffer::from_iter(trace_valid.iter().copied())));
        let arr =
            FixedSizeBinaryArray::try_new(16, Buffer::from_slice_ref(&scratch_bytes[..]), nulls)?;
        cols.push(Arc::new(arr));
    }

    Ok(RecordBatch::try_new(schema, cols)?)
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
    fn description_column_handles_mixed_and_uniform_help_presence() {
        let mut dec = ArrowDecoder::new();
        let opts = DecodeOptions::default();

        // Case 1: first metric has no HELP, second has HELP, third has no HELP.
        let records = dec
            .decode(
                b"m0 1\n# HELP m1 With help.\nm1 2\nm2 3\n",
                &Format::TEXT,
                &opts,
            )
            .unwrap();
        let metrics = records.univariate_metrics().unwrap();
        let desc = metrics
            .column_by_name("description")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(desc.is_null(0));
        assert!(!desc.is_null(1));
        assert_eq!(desc.value(1), "With help.");
        assert!(desc.is_null(2));

        // Case 2: all metrics have HELP, so no null bitmap is materialized on `description`.
        let records = dec
            .decode(
                b"# HELP a First.\na 1\n# HELP b Second.\nb 2\n",
                &Format::TEXT,
                &opts,
            )
            .unwrap();
        let metrics = records.univariate_metrics().unwrap();
        let desc = metrics
            .column_by_name("description")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(desc.null_count(), 0);
        assert_eq!(desc.value(0), "First.");
        assert_eq!(desc.value(1), "Second.");

        // Case 3: no metric has HELP, so the optional `description` column is omitted.
        let records = dec.decode(b"a 1\nb 2\n", &Format::TEXT, &opts).unwrap();
        let metrics = records.univariate_metrics().unwrap();
        assert!(metrics.column_by_name("description").is_none());
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
        assert!(matches!(err, DecodeError::DuplicateOrLateHelp { ref metric } if metric == "m"));

        // `# HELP` appearing after a sample has already been emitted for the family.
        let err = dec
            .decode(b"m 1\n# HELP m late\n", &Format::TEXT, &opts)
            .unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateOrLateHelp { ref metric } if metric == "m"));

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

        // Duplicate `# UNIT` in OpenMetrics.
        let err = dec
            .decode(
                b"# UNIT m_seconds seconds\n# UNIT m_seconds seconds\nm_seconds 1\n# EOF\n",
                &Format::OPEN_METRICS_1_0_0,
                &opts,
            )
            .unwrap_err();
        assert!(
            matches!(err, DecodeError::DuplicateOrLateUnit { ref metric } if metric == "m_seconds")
        );

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
            b"# TYPE h histogram\nh_bucket{le=\"1\",le=\"2\"} 1\n",
            b"# TYPE s summary\ns{quantile=\"0.5\",quantile=\"0.9\"} 1\n",
        ] {
            let err = dec.decode(payload, &Format::TEXT, &opts).unwrap_err();
            assert!(
                matches!(err, DecodeError::DuplicateLabel { .. }),
                "expected DuplicateLabel, got {err:?}"
            );
        }

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

        // Dropped families (`UntypedPolicy::Drop`) validate duplicate/reserved labels without
        // interning their keys or values into `NumberDpAttrs` dictionaries.
        let drop_opts = DecodeOptions::new().with_untyped_policy(UntypedPolicy::Drop);
        let with_dropped = dec
            .decode(
                b"dropped_metric{dropped_k=\"dropped_v\"} 1\n# TYPE kept gauge\nkept{b=\"x\"} 2\n",
                &Format::TEXT,
                &drop_opts,
            )
            .unwrap();
        let kept_attrs = with_dropped.number_dp_attrs().unwrap();
        let kept_keys = kept_attrs
            .column_by_name("key")
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<UInt8Type>>()
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(kept_keys.len(), 1);
        assert_eq!(kept_keys.value(0), "b");

        let dup_dropped = dec
            .decode(
                b"dropped_metric{a=\"1\",a=\"2\"} 1\n",
                &Format::TEXT,
                &drop_opts,
            )
            .unwrap_err();
        assert!(matches!(dup_dropped, DecodeError::DuplicateLabel { .. }));
    }

    #[test]
    fn decode_stream_splits_mid_family_preserves_metadata_and_handles_edges() {
        let mut dec = ArrowDecoder::new();

        // Empty and metadata-only scrapes yield zero chunks.
        for empty in [
            b"" as &[u8],
            b"# comment\n",
            b"# HELP unused Help.\n# TYPE unused counter\n",
        ] {
            assert_eq!(
                dec.decode_stream(empty, &Format::TEXT, &DecodeOptions::new())
                    .count(),
                0
            );
        }

        // Split a 4-sample counter family into chunks of 2, followed by a 1-sample gauge family
        // and a trailing metadata-only family. Also verify `max_data_points_per_batch = 0` clamps.
        let zero_opts = DecodeOptions::new().with_max_data_points_per_batch(0);
        let one_by_one: Vec<_> = dec
            .decode_stream(b"a 1\na 2\n", &Format::TEXT, &zero_opts)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(one_by_one.len(), 2);
        assert_eq!(one_by_one[0].num_data_points(), 1);
        assert_eq!(one_by_one[1].num_data_points(), 1);

        let payload = concat!(
            "# HELP req_total Total HTTP requests.\n",
            "# TYPE req_total counter\n",
            "req_total{shard=\"0\"} 10\n",
            "req_total{shard=\"1\"} 20\n",
            "req_total{shard=\"2\"} 30\n",
            "req_total{shard=\"3\"} 40\n",
            "# TYPE mem_bytes gauge\n",
            "mem_bytes 512\n",
            "# HELP trailing_empty Unused.\n",
            "# TYPE trailing_empty gauge\n",
        );
        let opts = DecodeOptions::new()
            .with_timestamp_ms(1_700_000_000_000)
            .with_max_data_points_per_batch(2);

        let chunks: Vec<OtapMetricRecords> = dec
            .decode_stream(payload.as_bytes(), &Format::TEXT, &opts)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(chunks.len(), 3);

        // Chunks 0 and 1 both carry `req_total` with its original `# HELP` and counter semantics.
        for chunk in &chunks[..2] {
            assert_eq!(chunk.num_metrics(), 1);
            assert_eq!(chunk.num_data_points(), 2);
            let um = chunk.univariate_metrics().unwrap();
            let names = um
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let descs = um
                .column_by_name("description")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(names.value(0), "req_total");
            assert_eq!(descs.value(0), "Total HTTP requests.");
            assert!(um.column_by_name("is_monotonic").is_some());
        }

        // Chunk 2 contains only `mem_bytes`; `req_total` was not re-emitted with zero samples.
        assert_eq!(chunks[2].num_metrics(), 1);
        assert_eq!(chunks[2].num_data_points(), 1);
        let um2 = chunks[2].univariate_metrics().unwrap();
        let names2 = um2
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names2.value(0), "mem_bytes");
        assert!(um2.column_by_name("description").is_none());
        assert!(um2.column_by_name("is_monotonic").is_none());

        // Late `# HELP` or `# TYPE` immediately after a chunk boundary is still rejected.
        let late_help = b"m{a=\"1\"} 1\nm{a=\"2\"} 2\n# HELP m too late\nm{a=\"3\"} 3\n";
        let mut stream = dec.decode_stream(late_help, &Format::TEXT, &opts);
        assert!(stream.next().unwrap().is_ok());
        let err = stream.next().unwrap().unwrap_err();
        assert!(matches!(err, DecodeError::DuplicateOrLateHelp { ref metric } if metric == "m"));
        assert!(stream.next().is_none());
        drop(stream);

        // Dropping a stream early returns the scratch label span vector to the decoder.
        let labeled = b"m{k1=\"1\",k2=\"2\",k3=\"3\",k4=\"4\"} 1\nm{k1=\"5\"} 2\n";
        {
            let mut stream = dec.decode_stream(labeled, &Format::TEXT, &zero_opts);
            assert!(stream.next().unwrap().is_ok());
        }
        assert!(dec.prom_spans.capacity() >= 4);

        // Consecutive assembled families (histograms/summaries) preceded by `# TYPE` flush at
        // `max_data_points_per_batch` when the next family begins.
        let assembled_stream = concat!(
            "# TYPE h1 histogram\n",
            "h1_bucket{le=\"+Inf\"} 1\n",
            "h1_sum 1\n",
            "h1_count 1\n",
            "# TYPE s1 summary\n",
            "s1{quantile=\"0.5\"} 2\n",
            "s1_sum 2\n",
            "s1_count 1\n",
        );
        let assembled_chunks: Vec<_> = dec
            .decode_stream(assembled_stream.as_bytes(), &Format::TEXT, &zero_opts)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(assembled_chunks.len(), 2);
        assert_eq!(assembled_chunks[0].num_data_points(), 1);
        assert!(assembled_chunks[0].histogram_data_points().is_some());
        assert_eq!(assembled_chunks[1].num_data_points(), 1);
        assert!(assembled_chunks[1].summary_data_points().is_some());

        // Init errors surface on the first `next()` and fuse the iterator.
        let mut bad_fmt = dec.decode_stream(b"m 1\n", &Format::UNKNOWN, &opts);
        assert!(matches!(
            bad_fmt.next(),
            Some(Err(DecodeError::UnsupportedFormat(_)))
        ));
        assert!(bad_fmt.next().is_none());
    }

    #[test]
    fn interleaved_family_starts_fresh_untyped_run() {
        let mut dec = ArrowDecoder::new();
        let opts = DecodeOptions::default();
        let payload = b"# TYPE a counter\na{x=\"1\"} 1\nb 2\na{x=\"2\"} 3\n";

        let records = dec.decode(payload, &Format::TEXT, &opts).unwrap();
        assert_eq!(records.num_metrics(), 3);
        assert_eq!(records.num_data_points(), 3);

        let um = records.univariate_metrics().unwrap();
        let names = um
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let metric_types = um
            .column_by_name("metric_type")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let is_monotonic = um
            .column_by_name("is_monotonic")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();

        assert_eq!(names.value(0), "a");
        assert_eq!(metric_types.value(0), OTAP_METRIC_TYPE_SUM);
        assert!(!is_monotonic.is_null(0));
        assert!(is_monotonic.value(0));

        assert_eq!(names.value(1), "b");
        assert_eq!(metric_types.value(1), OTAP_METRIC_TYPE_GAUGE);
        assert!(is_monotonic.is_null(1));

        // The second run of `a` is non-contiguous and has no `# TYPE` of its own, so it decodes as
        // a separate untyped (`Gauge`) metric rather than failing the scrape.
        assert_eq!(names.value(2), "a");
        assert_eq!(metric_types.value(2), OTAP_METRIC_TYPE_GAUGE);
        assert!(is_monotonic.is_null(2));

        let streamed: Vec<_> = dec
            .decode_stream(payload, &Format::TEXT, &opts)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(streamed, vec![records]);
    }
}
