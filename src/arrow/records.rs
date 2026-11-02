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

//! Output container and payload discriminants for OTAP metric `RecordBatch` sets.

use arrow_array::RecordBatch;

/// OTAP `ArrowPayloadType` wire discriminants produced by [`super::ArrowDecoder`].
///
/// Discriminant values match `opentelemetry.proto.experimental.arrow.v1.ArrowPayloadType`
/// in [`open-telemetry/otel-arrow`](https://github.com/open-telemetry/otel-arrow).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
#[non_exhaustive]
pub enum MetricPayloadType {
    /// Metric descriptor table (`ArrowPayloadType::UnivariateMetrics = 10`).
    UnivariateMetrics = 10,
    /// Scalar gauge, counter, and untyped data points (`ArrowPayloadType::NumberDataPoints = 11`).
    NumberDataPoints = 11,
    /// Key-value label attributes attached to [`Self::NumberDataPoints`]
    /// (`ArrowPayloadType::NumberDpAttrs = 15`).
    NumberDpAttrs = 15,
    /// Metric-level metadata attributes (`ArrowPayloadType::MetricAttrs = 26`).
    MetricAttrs = 26,
}

impl MetricPayloadType {
    /// Returns the protobuf `ArrowPayloadType` `i32` wire value.
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}

/// Collection of non-empty OTAP metric [`RecordBatch`] tables decoded from a single exposition
/// payload.
///
/// Tables with zero rows are omitted (`None`) to match OTAP batch store invariants.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OtapMetricRecords {
    pub(super) univariate_metrics: Option<RecordBatch>,
    pub(super) metric_attrs: Option<RecordBatch>,
    pub(super) number_data_points: Option<RecordBatch>,
    pub(super) number_dp_attrs: Option<RecordBatch>,
}

impl OtapMetricRecords {
    /// Returns the `UnivariateMetrics` table (`ArrowPayloadType = 10`), if non-empty.
    #[must_use]
    pub fn univariate_metrics(&self) -> Option<&RecordBatch> {
        self.univariate_metrics.as_ref()
    }

    /// Returns the `MetricAttrs` table (`ArrowPayloadType = 26`), if non-empty.
    #[must_use]
    pub fn metric_attrs(&self) -> Option<&RecordBatch> {
        self.metric_attrs.as_ref()
    }

    /// Returns the `NumberDataPoints` table (`ArrowPayloadType = 11`), if non-empty.
    #[must_use]
    pub fn number_data_points(&self) -> Option<&RecordBatch> {
        self.number_data_points.as_ref()
    }

    /// Returns the `NumberDpAttrs` table (`ArrowPayloadType = 15`), if non-empty.
    #[must_use]
    pub fn number_dp_attrs(&self) -> Option<&RecordBatch> {
        self.number_dp_attrs.as_ref()
    }

    /// Looks up a decoded [`RecordBatch`] by its [`MetricPayloadType`].
    #[must_use]
    pub fn get(&self, payload_type: MetricPayloadType) -> Option<&RecordBatch> {
        match payload_type {
            MetricPayloadType::UnivariateMetrics => self.univariate_metrics.as_ref(),
            MetricPayloadType::MetricAttrs => self.metric_attrs.as_ref(),
            MetricPayloadType::NumberDataPoints => self.number_data_points.as_ref(),
            MetricPayloadType::NumberDpAttrs => self.number_dp_attrs.as_ref(),
        }
    }

    /// Iterates over all non-empty `(MetricPayloadType, &RecordBatch)` pairs in wire order.
    pub fn iter(&self) -> impl Iterator<Item = (MetricPayloadType, &RecordBatch)> {
        [
            (
                MetricPayloadType::UnivariateMetrics,
                self.univariate_metrics.as_ref(),
            ),
            (
                MetricPayloadType::NumberDataPoints,
                self.number_data_points.as_ref(),
            ),
            (
                MetricPayloadType::NumberDpAttrs,
                self.number_dp_attrs.as_ref(),
            ),
            (MetricPayloadType::MetricAttrs, self.metric_attrs.as_ref()),
        ]
        .into_iter()
        .filter_map(|(kind, batch)| batch.map(|b| (kind, b)))
    }

    /// Consumes `self` and yields all non-empty `(MetricPayloadType, RecordBatch)` pairs.
    pub fn into_batches(self) -> impl Iterator<Item = (MetricPayloadType, RecordBatch)> {
        [
            (
                MetricPayloadType::UnivariateMetrics,
                self.univariate_metrics,
            ),
            (MetricPayloadType::NumberDataPoints, self.number_data_points),
            (MetricPayloadType::NumberDpAttrs, self.number_dp_attrs),
            (MetricPayloadType::MetricAttrs, self.metric_attrs),
        ]
        .into_iter()
        .filter_map(|(kind, batch)| batch.map(|b| (kind, b)))
    }

    /// Returns `true` when the payload produced zero metric families.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.univariate_metrics.is_none()
    }

    /// Returns the number of emitted metric families in `UnivariateMetrics`.
    #[must_use]
    pub fn num_metrics(&self) -> usize {
        self.univariate_metrics
            .as_ref()
            .map_or(0, RecordBatch::num_rows)
    }

    /// Returns the number of scalar data points in `NumberDataPoints`.
    #[must_use]
    pub fn num_data_points(&self) -> usize {
        self.number_data_points
            .as_ref()
            .map_or(0, RecordBatch::num_rows)
    }
}
