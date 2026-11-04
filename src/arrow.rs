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

/// Default upper bound on data points emitted per [`OtapMetricRecords`] chunk in
/// [`ArrowDecoder::decode_stream`].
pub const DEFAULT_MAX_DATA_POINTS_PER_BATCH: usize = 16_384;

/// Maximum distinct `UnivariateMetrics` rows representable in a single OTAP batch (`u16` IDs).
const MAX_METRICS_PER_BATCH: usize = (u16::MAX as usize) + 1;

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
}

impl Default for DecodeOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodeOptions {
    /// Creates default [`DecodeOptions`] with no fallback timestamp and
    /// [`DEFAULT_MAX_DATA_POINTS_PER_BATCH`] data points per streamed batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            timestamp_ms: None,
            max_data_points_per_batch: DEFAULT_MAX_DATA_POINTS_PER_BATCH,
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

/// Active contiguous metric-family cursor.
///
/// Tracking only the current contiguous family avoids maintaining a whole-scrape metric-name hash
/// table, lets `UnivariateMetrics` names and descriptions append straight into Arrow `(offsets,
/// values)` buffers on first sample emission, and preserves active family metadata across chunk
/// flushes.
#[derive(Default)]
struct CurrentFamily {
    active: bool,
    raw_name: Vec<u8>,
    unescaped_name: Vec<u8>,
    metric_type: MetricType,
    has_type: bool,
    has_help: bool,
    has_samples: bool,
    help: Vec<u8>,
    batch_id: Option<u16>,
}

impl CurrentFamily {
    fn with_capacity(name_cap: usize, help_cap: usize) -> Self {
        Self {
            raw_name: Vec::with_capacity(name_cap),
            unescaped_name: Vec::with_capacity(name_cap),
            help: Vec::with_capacity(help_cap),
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
        self.has_samples = false;
        self.help.clear();
        self.batch_id = None;
    }

    fn retained_memory_bytes(&self) -> usize {
        self.raw_name.capacity() + self.unescaped_name.capacity() + self.help.capacity()
    }

    #[cold]
    fn name_string(&self) -> String {
        String::from_utf8_lossy(&self.unescaped_name).into_owned()
    }

    #[inline]
    fn transition_if_needed(
        &mut self,
        raw_name: &[u8],
        name_offset: usize,
        unescape_buf: &mut Vec<u8>,
    ) -> Result<(), DecodeError> {
        // Consecutive `# HELP`, `# TYPE`, and series lines belong to the same metric family.
        // Matching `raw_name` against the already validated active name avoids UTF-8 checks,
        // unescaping, and hashing on every subsequent line of that family.
        if self.active && self.raw_name.as_slice() == raw_name {
            return Ok(());
        }

        if !raw_name.is_ascii() && std::str::from_utf8(raw_name).is_err() {
            return Err(DecodeError::InvalidUtf8 {
                offset: name_offset,
            });
        }
        let unescaped = unescape_bytes_into(raw_name, true, unescape_buf);
        if self.active && self.unescaped_name.as_slice() == unescaped {
            self.raw_name.clear();
            self.raw_name.extend_from_slice(raw_name);
            return Ok(());
        }

        // NOTE: Previously seen family names are intentionally not tracked across transitions so
        // `decode_stream` stays `O(chunk)` in memory and untyped `.nometa` scrapes with interleaved
        // `_sum` and `_count` lines still decode. A non-contiguous repetition of an earlier name
        // starts a fresh untyped family run unless preceded by its own `# TYPE` or `# HELP`.
        self.active = true;
        self.raw_name.clear();
        self.raw_name.extend_from_slice(raw_name);
        self.unescaped_name.clear();
        self.unescaped_name.extend_from_slice(unescaped);
        self.metric_type = MetricType::Unknown;
        self.has_type = false;
        self.has_help = false;
        self.has_samples = false;
        self.help.clear();
        self.batch_id = None;
        Ok(())
    }
}

/// Stateful, lock-free decoder translating Prometheus exposition payloads into OTAP Arrow metric
/// [`RecordBatch`]es.
///
/// Reusing a single `ArrowDecoder` across scrapes preserves internal parser scratch spans, label
/// string interners, and column staging buffers so steady-state scrapes allocate only the output
/// Arrow `Buffer`s returned inside [`OtapMetricRecords`].
pub struct ArrowDecoder {
    parser_spans: Vec<LabelSpan>,
    unescape_buf: Vec<u8>,
    current_family: CurrentFamily,

    um_metric_types: Vec<u8>,
    um_attr_type_keys: Vec<u16>,
    um_name_offsets: Vec<i32>,
    um_name_bytes: Vec<u8>,
    um_desc_offsets: Vec<i32>,
    um_desc_bytes: Vec<u8>,
    um_desc_valid: Vec<bool>,
    um_any_help: bool,
    um_all_help: bool,
    um_any_counter: bool,
    um_all_counter: bool,

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
        let mut um_name_offsets = Vec::with_capacity(metrics + 1);
        um_name_offsets.push(0);
        Self {
            parser_spans: Vec::with_capacity(16),
            unescape_buf: Vec::new(),
            current_family: CurrentFamily::with_capacity(64, 128),

            um_metric_types: Vec::with_capacity(metrics),
            um_attr_type_keys: Vec::with_capacity(metrics),
            um_name_offsets,
            um_name_bytes: Vec::with_capacity(metrics.saturating_mul(32)),
            um_desc_offsets: Vec::new(),
            um_desc_bytes: Vec::new(),
            um_desc_valid: Vec::new(),
            um_any_help: false,
            um_all_help: true,
            um_any_counter: false,
            um_all_counter: true,

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
            + self.current_family.retained_memory_bytes()
            + self.um_metric_types.capacity()
            + self.um_attr_type_keys.capacity() * size_of::<u16>()
            + self.um_name_offsets.capacity() * size_of::<i32>()
            + self.um_name_bytes.capacity()
            + self.um_desc_offsets.capacity() * size_of::<i32>()
            + self.um_desc_bytes.capacity()
            + self.um_desc_valid.capacity() * size_of::<bool>()
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
    /// # Contiguous families and batching
    ///
    /// Input parsing streams `Entry` items one by one from `PromParser` directly into reusable
    /// columnar staging vectors and label string interners without materializing intermediate
    /// per-sample structs or per-string allocations. Metric families are decoded as contiguous
    /// runs, with `# HELP` and `# TYPE` lines appearing before that family's samples.
    /// Non-contiguous repetitions of an earlier metric name are not reassembled or rejected; each
    /// run starts with fresh `untyped` metadata unless preceded by its own `# HELP` or `# TYPE`.
    ///
    /// `decode` finalizes `input` into a single [`OtapMetricRecords`] batch set, adaptively
    /// omitting unused optional OTAP columns (`description`, `id`, `time_unix_nano`) and
    /// selecting dictionary key widths (`UInt8`, `UInt16`, or plain `Utf8`) from the batch's
    /// cardinality and nullability.
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

        self.reset_scrape();

        let spans = std::mem::take(&mut self.parser_spans);
        let mut parser = PromParser::with_scratch(input, spans);
        let mut pending_series = false;
        let mut done = false;
        let res = self.ingest_prom_entries::<false>(
            &mut parser,
            default_ts_ns,
            0,
            &mut pending_series,
            &mut done,
        );
        self.parser_spans = parser.into_scratch();
        Ok(res?.unwrap_or_default())
    }

    /// Streams `input` as a sequence of non-empty [`OtapMetricRecords`] chunks bounded by
    /// [`DecodeOptions::max_data_points_per_batch`] and OTAP's 65,536-metric per-batch `u16` ID
    /// limit.
    ///
    /// Empty or metadata-only scrapes yield zero items. When a contiguous metric family spans a
    /// chunk boundary, [`ArrowDecoder`] retains its `# HELP` and `# TYPE` metadata across the
    /// flush and re-emits the family row in the next chunk only if further samples follow.
    /// Dropping [`DecodeStream`] early returns the internal parser scratch buffer to `self`.
    pub fn decode_stream<'dec, 'input>(
        &'dec mut self,
        input: &'input [u8],
        format: &Format,
        opts: &DecodeOptions,
    ) -> DecodeStream<'dec, 'input> {
        self.reset_scrape();
        let spans = std::mem::take(&mut self.parser_spans);
        let max_data_points = opts
            .max_data_points_per_batch
            .clamp(1, (u32::MAX as usize).saturating_add(1));

        if format.format_type() != FormatType::TextPlain {
            // TODO(bwplotka): Dispatch FormatType::OpenMetrics once #6 lands and Proto* in Phase 3.
            return DecodeStream {
                decoder: self,
                parser: PromParser::with_scratch(b"", spans),
                default_ts_ns: None,
                max_data_points,
                pending_series: false,
                init_err: Some(DecodeError::UnsupportedFormat(format.as_str().to_owned())),
                done: false,
            };
        }

        let default_ts_ns = match opts.timestamp_ms {
            Some(ms) => match ms_to_ns(ms) {
                Ok(ns) => Some(ns),
                Err(err) => {
                    return DecodeStream {
                        decoder: self,
                        parser: PromParser::with_scratch(b"", spans),
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

        DecodeStream {
            decoder: self,
            parser: PromParser::with_scratch(input, spans),
            default_ts_ns,
            max_data_points,
            pending_series: false,
            init_err: None,
            done: false,
        }
    }

    fn reset_scrape(&mut self) {
        self.current_family.clear();
        self.reset_batch();
    }

    fn reset_batch(&mut self) {
        self.current_family.batch_id = None;

        self.um_metric_types.clear();
        self.um_attr_type_keys.clear();
        self.um_name_offsets.clear();
        self.um_name_offsets.push(0);
        self.um_name_bytes.clear();
        self.um_desc_offsets.clear();
        self.um_desc_bytes.clear();
        self.um_desc_valid.clear();
        self.um_any_help = false;
        self.um_all_help = true;
        self.um_any_counter = false;
        self.um_all_counter = true;

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
    fn emit_current_family_into_batch(&mut self) -> Result<u16, DecodeError> {
        if let Some(id) = self.current_family.batch_id {
            return Ok(id);
        }

        let m_idx = self.um_metric_types.len();
        let id = u16::try_from(m_idx).map_err(|_| DecodeError::CapacityOverflow {
            table: "UnivariateMetrics",
            count: m_idx + 1,
        })?;

        let next_name_len = self
            .um_name_bytes
            .len()
            .checked_add(self.current_family.unescaped_name.len())
            .and_then(|n| i32::try_from(n).ok())
            .ok_or(DecodeError::CapacityOverflow {
                table: "UnivariateMetrics.name",
                count: self
                    .um_name_bytes
                    .len()
                    .saturating_add(self.current_family.unescaped_name.len()),
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

        self.um_name_bytes
            .extend_from_slice(&self.current_family.unescaped_name);
        self.um_name_offsets.push(next_name_len);

        let (otap_type, attr_type_key, is_counter) = match self.current_family.metric_type {
            MetricType::Counter => (OTAP_METRIC_TYPE_SUM, 1u16, true),
            MetricType::Gauge => (OTAP_METRIC_TYPE_GAUGE, 2u16, false),
            _ => (OTAP_METRIC_TYPE_GAUGE, 0u16, false),
        };
        self.um_metric_types.push(otap_type);
        self.um_attr_type_keys.push(attr_type_key);
        self.um_any_counter |= is_counter;
        self.um_all_counter &= is_counter;

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

        self.current_family.has_samples = true;
        self.current_family.batch_id = Some(id);
        Ok(id)
    }

    // Monomorphizing on `CHUNKED` shares one entry dispatch loop between `decode` (`false`) and
    // `decode_stream` (`true`) while compiling out chunk-boundary flush checks on single-batch
    // calls.
    fn ingest_prom_entries<const CHUNKED: bool>(
        &mut self,
        parser: &mut PromParser<'_>,
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
                // `PromParser` never emits `Entry::Unit` (`# UNIT` is parsed as `Entry::Comment`
                // in Prometheus text 0.0.4).
                Entry::Comment | Entry::Unit => {}
                Entry::Help => self.ingest_help_entry(parser)?,
                Entry::Type => self.ingest_type_entry(parser)?,
                Entry::Series => {
                    let (raw_name, label_spans, full_input, sample_ts_ms, value) =
                        parser.raw_series_parts();
                    let name_offset = parser.name_offset();
                    self.current_family.transition_if_needed(
                        raw_name,
                        name_offset,
                        &mut self.unescape_buf,
                    )?;
                    if CHUNKED
                        && self.current_family.batch_id.is_none()
                        && self.um_metric_types.len() >= MAX_METRICS_PER_BATCH
                    {
                        *pending_series = true;
                        let batch = self.finish_batches(default_ts_ns.is_some())?;
                        self.reset_batch();
                        return Ok(Some(batch));
                    }
                    self.append_current_series(
                        label_spans,
                        full_input,
                        sample_ts_ms,
                        value,
                        default_ts_ns,
                    )?;
                    if CHUNKED && self.dp_parent_ids.len() >= max_data_points {
                        let batch = self.finish_batches(default_ts_ns.is_some())?;
                        self.reset_batch();
                        return Ok(Some(batch));
                    }
                }
            }
        }
    }

    #[inline]
    fn ingest_help_entry(&mut self, parser: &PromParser<'_>) -> Result<(), DecodeError> {
        let (raw_name, raw_help) = parser.raw_help();
        let name_offset = parser.name_offset();
        self.current_family
            .transition_if_needed(raw_name, name_offset, &mut self.unescape_buf)?;
        // Match `prometheus/common/expfmt/text_parse.go`: a bare `# HELP name` line without
        // docstring text is a no-op and does not count as setting HELP.
        if raw_help.is_empty() {
            return Ok(());
        }
        if self.current_family.has_help || self.current_family.has_samples {
            return Err(DecodeError::DuplicateOrLateHelp {
                metric: self.current_family.name_string(),
            });
        }
        // `PromParser` already verified `raw_help` is valid UTF-8 on `Entry::Help`.
        let unescaped = unescape_bytes_into(raw_help, false, &mut self.unescape_buf);
        self.current_family.help.clear();
        self.current_family.help.extend_from_slice(unescaped);
        self.current_family.has_help = true;
        Ok(())
    }

    #[inline]
    fn ingest_type_entry(&mut self, parser: &PromParser<'_>) -> Result<(), DecodeError> {
        let (raw_name, metric_type) = parser.metric_type();
        let name_offset = parser.name_offset();
        self.current_family
            .transition_if_needed(raw_name, name_offset, &mut self.unescape_buf)?;
        if self.current_family.has_type || self.current_family.has_samples {
            return Err(DecodeError::DuplicateOrLateType {
                metric: self.current_family.name_string(),
            });
        }
        match metric_type {
            MetricType::Counter | MetricType::Gauge | MetricType::Unknown => {
                self.current_family.metric_type = metric_type;
                self.current_family.has_type = true;
                Ok(())
            }
            // TODO(bwplotka): Assemble classic histograms, summaries, and OpenMetrics types in #8
            // and #9.
            MetricType::Histogram
            | MetricType::Summary
            | MetricType::GaugeHistogram
            | MetricType::Info
            | MetricType::Stateset => Err(DecodeError::UnsupportedMetricType {
                metric: self.current_family.name_string(),
                metric_type: metric_type.as_str(),
            }),
        }
    }

    #[inline]
    fn append_current_series(
        &mut self,
        label_spans: &[LabelSpan],
        full_input: &[u8],
        sample_ts_ms: Option<i64>,
        value: f64,
        default_ts_ns: Option<i64>,
    ) -> Result<(), DecodeError> {
        let dp_index = self.dp_parent_ids.len();
        let dp_id = u32::try_from(dp_index).map_err(|_| DecodeError::CapacityOverflow {
            table: "NumberDataPoints",
            count: dp_index + 1,
        })?;

        self.append_series_labels(dp_id, label_spans, full_input)?;
        let parent_id = self.emit_current_family_into_batch()?;
        self.append_data_point(parent_id, value, sample_ts_ms, default_ts_ns)
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
            // `PromParser` already validated UTF-8 on every `LabelSpan`.
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
        if self.um_metric_types.is_empty() {
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
            summary_data_points: None,
            histogram_data_points: None,
            number_dp_attrs,
            summary_dp_attrs: None,
            histogram_dp_attrs: None,
            number_dp_exemplars: None,
            histogram_dp_exemplars: None,
            number_dp_exemplar_attrs: None,
            histogram_dp_exemplar_attrs: None,
        })
    }

    fn build_univariate_metrics(&mut self) -> Result<RecordBatch, DecodeError> {
        let num_metrics = self.um_metric_types.len();
        let schema = univariate_metrics_schema(
            self.um_any_help,
            false,
            self.um_any_counter,
            self.um_any_counter,
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

        if self.um_any_counter {
            self.scratch_i32.clear();
            self.scratch_i32
                .extend(self.um_metric_types.iter().map(|&t| {
                    if t == OTAP_METRIC_TYPE_SUM {
                        AGGREGATION_TEMPORALITY_CUMULATIVE
                    } else {
                        0
                    }
                }));
            let counter_validity = BooleanBuffer::from_iter(
                self.um_metric_types
                    .iter()
                    .map(|&t| t == OTAP_METRIC_TYPE_SUM),
            );
            let nulls = if self.um_all_counter {
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
        let num_metrics = self.um_metric_types.len();

        // `scratch_u16` still holds `0..num_metrics` from `build_univariate_metrics`.
        let parent_id_arr = UInt16Array::new(scalar_from_slice(&self.scratch_u16), None);

        self.scratch_u8.clear();
        self.scratch_u8.resize(num_metrics, 0);
        let key_keys = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);
        let key_dict = DictionaryArray::<UInt8Type>::new(key_keys, metric_attrs_key_dict());

        self.scratch_u8.fill(ATTR_VALUE_TYPE_STR);
        let type_arr = UInt8Array::new(scalar_from_slice(&self.scratch_u8), None);

        let str_keys = UInt16Array::new(scalar_from_slice(&self.um_attr_type_keys), None);
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

        let schema = number_dp_schema(has_id, false, false, has_time, time_nullable);
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
    parser: PromParser<'input>,
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
        match self.decoder.ingest_prom_entries::<true>(
            &mut self.parser,
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
        self.decoder.parser_spans = self.parser.take_scratch();
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
        assert!(dec.parser_spans.capacity() >= 4);

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
