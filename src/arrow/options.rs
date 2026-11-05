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

//! Configuration policies, [`DecodeOptions`], and [`DecodeError`] for [`super::ArrowDecoder`].

use arrow_schema::ArrowError;

use crate::parse::ParseError;

/// Default upper bound on data points emitted per [`super::OtapMetricRecords`] chunk in
/// [`super::ArrowDecoder::decode_stream`].
pub const DEFAULT_MAX_DATA_POINTS_PER_BATCH: usize = 16_384;

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

/// Options controlling [`super::ArrowDecoder::decode`] and [`super::ArrowDecoder::decode_stream`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeOptions {
    /// Default timestamp in milliseconds since the Unix epoch applied to samples that omit an
    /// explicit wire timestamp. When `None`, untimestamped samples leave `time_unix_nano` unset.
    pub timestamp_ms: Option<i64>,
    /// Maximum number of data points materialized per [`super::OtapMetricRecords`] chunk yielded by
    /// [`super::ArrowDecoder::decode_stream`]. Values of `0` are clamped to `1`. Single-batch
    /// [`super::ArrowDecoder::decode`] ignores this limit.
    pub max_data_points_per_batch: usize,
    /// Policy for counter `_total` suffixes on `UnivariateMetrics.name`.
    pub counter_suffix_policy: CounterSuffixPolicy,
    /// Policy for `# UNIT` suffixes on `UnivariateMetrics.name`.
    pub unit_suffix_policy: UnitSuffixPolicy,
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
    /// [`super::ArrowDecoder::decode_stream`].
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

    /// Sets the [`UntypedPolicy`] for untyped (`unknown`) metric families.
    #[must_use]
    pub const fn with_untyped_policy(mut self, policy: UntypedPolicy) -> Self {
        self.untyped_policy = policy;
        self
    }
}

/// Errors returned by [`super::ArrowDecoder::decode`].
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
    pub(super) fn from_parse(err: ParseError) -> Self {
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
