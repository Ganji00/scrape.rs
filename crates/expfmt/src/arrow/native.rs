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

//! Conversion of parsed native histograms into OTAP histogram columns.
//!
//! Each conversion makes one pass over the sparse buckets, writes straight into the reused
//! column buffers, and applies the checks of Go's
//! [`Histogram.Validate`](https://github.com/prometheus/prometheus/blob/v0.315.1/model/histogram/histogram.go#L426)
//! and `FloatHistogram.Validate` on the way. Prometheus runs `Validate` when it appends a
//! scraped histogram, and an invalid histogram fails the whole scrape, so a check failure here
//! is a [`DecodeError::InvalidHistogram`].

use crate::parse::{Histogram, HistogramRef, Span};

use super::columns::{HistogramDpTable, f64_to_u64_count};
use super::family::CurrentFamily;
use super::options::DecodeError;

/// Go's `histogram.CustomBucketsSchema`, the schema of a native histogram with custom buckets
/// (NHCB).
pub(super) const CUSTOM_BUCKETS_SCHEMA: i32 = -53;

/// Go's `histogram.ExponentialSchemaMin` and `histogram.ExponentialSchemaMax`.
pub(super) const EXPONENTIAL_SCHEMAS: std::ops::RangeInclusive<i32> = -4..=8;

/// Bucket counts of one side of a native histogram.
#[derive(Clone, Copy)]
pub(super) enum Buckets<'h> {
    /// Delta-encoded integer counts, Go's `Histogram`.
    Delta(&'h [i64]),
    /// Absolute float counts, Go's `FloatHistogram`.
    Absolute(&'h [f64]),
}

impl Buckets<'_> {
    fn len(self) -> usize {
        match self {
            Self::Delta(buckets) => buckets.len(),
            Self::Absolute(buckets) => buckets.len(),
        }
    }
}

/// Integer and float native histograms behind one shape, with counts as `u64`.
pub(super) struct NativeView<'h> {
    pub(super) schema: i32,
    pub(super) zero_threshold: f64,
    pub(super) sum: f64,
    pub(super) count: u64,
    pub(super) zero_count: u64,
    /// `true` for a float histogram, which Go does not check against its bucket counts.
    pub(super) is_float: bool,
    pub(super) positive_spans: &'h [Span],
    pub(super) positive: Buckets<'h>,
    pub(super) negative_spans: &'h [Span],
    pub(super) negative: Buckets<'h>,
    pub(super) custom_values: &'h [f64],
}

impl<'h> NativeView<'h> {
    /// Returns the view of `histogram`, or the reason Go's `FloatHistogram.Validate` rejects a
    /// negative float count.
    pub(super) fn new(histogram: HistogramRef<'h>) -> Result<Self, String> {
        match histogram {
            HistogramRef::Int(h) => Ok(Self::with_counts(
                h,
                h.count,
                h.zero_count,
                false,
                Buckets::Delta(&h.positive_buckets),
                Buckets::Delta(&h.negative_buckets),
            )),
            HistogramRef::Float(h) => {
                if h.count < 0.0 {
                    return Err(format!("histogram has negative count: {}", h.count));
                }
                if h.zero_count < 0.0 {
                    return Err(format!(
                        "histogram has negative zero count: {}",
                        h.zero_count
                    ));
                }
                Ok(Self::with_counts(
                    h,
                    f64_to_u64_count(h.count),
                    f64_to_u64_count(h.zero_count),
                    true,
                    Buckets::Absolute(&h.positive_buckets),
                    Buckets::Absolute(&h.negative_buckets),
                ))
            }
        }
    }

    fn with_counts<C, B>(
        h: &'h Histogram<C, B>,
        count: u64,
        zero_count: u64,
        is_float: bool,
        positive: Buckets<'h>,
        negative: Buckets<'h>,
    ) -> Self {
        Self {
            schema: h.schema,
            zero_threshold: h.zero_threshold,
            sum: h.sum,
            count,
            zero_count,
            is_float,
            positive_spans: &h.positive_spans,
            positive,
            negative_spans: &h.negative_spans,
            negative,
            custom_values: &h.custom_values,
        }
    }

    /// Go's count check: the buckets must add up to `count`, or to at most `count` when the
    /// sum is NaN. Float histograms skip it, like Go, because of float precision.
    pub(super) fn check_count(&self, bucket_total: u64) -> Result<(), String> {
        if self.is_float {
            return Ok(());
        }
        if self.sum.is_nan() {
            if bucket_total > self.count {
                return Err(format!(
                    "{} observations found in buckets, but the Count field is {}: histogram's \
                     observation count should be at least the number of observations found in \
                     the buckets",
                    bucket_total, self.count
                ));
            }
        } else if bucket_total != self.count {
            return Err(format!(
                "{} observations found in buckets, but the Count field is {}: histogram's \
                 observation count should equal the number of observations found in the \
                 buckets (in absence of NaN)",
                bucket_total, self.count
            ));
        }
        Ok(())
    }
}

/// Walks the buckets of one side in index order and calls `visit(index, count)` with absolute
/// counts. Indexes follow Go: the first span's offset is the index of its first bucket, later
/// offsets are gaps.
///
/// Applies Go's `checkHistogramSpans` and `checkHistogramBuckets`, and returns the total count
/// of the side, wrapping like Go's `uint64`.
pub(super) fn walk_buckets(
    side: &str,
    spans: &[Span],
    buckets: Buckets<'_>,
    mut visit: impl FnMut(i64, u64),
) -> Result<u64, String> {
    let mut span_buckets = 0u64;
    for (n, span) in spans.iter().enumerate() {
        if n > 0 && span.offset < 0 {
            return Err(format!(
                "{side} side: span number {} with offset {}: histogram has a span whose offset \
                 is negative",
                n + 1,
                span.offset
            ));
        }
        span_buckets += u64::from(span.length);
    }
    if span_buckets != buckets.len() as u64 {
        return Err(format!(
            "{side} side: spans need {span_buckets} buckets, have {} buckets: histogram spans \
             specify different number of buckets than provided",
            buckets.len()
        ));
    }

    let mut total = 0u64;
    let mut index = 0i64;
    let mut bucket = 0usize;
    let mut current = 0i64;
    for span in spans {
        index += i64::from(span.offset);
        for _ in 0..span.length {
            let count = match buckets {
                Buckets::Delta(deltas) => {
                    current = current.wrapping_add(deltas[bucket]);
                    if current < 0 {
                        return Err(negative_bucket(side, bucket, current as f64));
                    }
                    current as u64
                }
                Buckets::Absolute(counts) => {
                    let count = counts[bucket];
                    if count < 0.0 {
                        return Err(negative_bucket(side, bucket, count));
                    }
                    f64_to_u64_count(count)
                }
            };
            visit(index, count);
            total = total.wrapping_add(count);
            index += 1;
            bucket += 1;
        }
    }
    Ok(total)
}

#[cold]
fn negative_bucket(side: &str, bucket: usize, count: f64) -> String {
    format!(
        "{side} side: bucket number {} has observation count of {count}: histogram has a bucket \
         whose observation count is negative",
        bucket + 1
    )
}

#[cold]
pub(super) fn invalid_histogram(family: &CurrentFamily, reason: String) -> DecodeError {
    DecodeError::InvalidHistogram {
        metric: family.name_string(),
        reason,
    }
}

/// Appends a native histogram with custom buckets (NHCB) to `dp` as an explicit-bucket
/// histogram data point: the custom values become the explicit bounds, followed by one dense
/// count per bucket including `+Inf`.
///
/// `dp_index` is the row of the data point, whose base columns the caller appends.
pub(super) fn push_custom_buckets(
    dp: &mut HistogramDpTable,
    dp_index: usize,
    histogram: &NativeView<'_>,
    family: &CurrentFamily,
) -> Result<(), DecodeError> {
    check_custom_buckets(histogram).map_err(|reason| invalid_histogram(family, reason))?;

    if dp.bucket_offsets.is_empty() {
        dp.bucket_offsets.push(0);
        dp.bounds_offsets.push(0);
    }
    let bounds = histogram.custom_values;
    let start = dp.bucket_counts.len();
    dp.bucket_counts.resize(start + bounds.len() + 1, 0);
    let counts = &mut dp.bucket_counts[start..];
    // `check_custom_buckets` keeps every index within `0..=bounds.len()`.
    walk_buckets(
        "positive",
        histogram.positive_spans,
        histogram.positive,
        |index, count| counts[index as usize] = count,
    )
    .and_then(|total| histogram.check_count(total))
    .map_err(|reason| invalid_histogram(family, reason))?;

    dp.bounds_vals.extend_from_slice(bounds);
    let bucket_off =
        i32::try_from(dp.bucket_counts.len()).map_err(|_| DecodeError::CapacityOverflow {
            table: "HistogramDataPoints.histogram_bucket_counts",
            count: dp.bucket_counts.len(),
        })?;
    let bounds_off =
        i32::try_from(dp.bounds_vals.len()).map_err(|_| DecodeError::CapacityOverflow {
            table: "HistogramDataPoints.histogram_explicit_bounds",
            count: dp.bounds_vals.len(),
        })?;
    dp.bucket_offsets.push(bucket_off);
    dp.bounds_offsets.push(bounds_off);
    dp.counts.push(dp_index, Some(histogram.count));
    dp.sums.push(dp_index, Some(histogram.sum));
    Ok(())
}

/// The custom-bucket checks of Go's `Validate`, except the per-bucket checks that
/// [`walk_buckets`] does.
fn check_custom_buckets(histogram: &NativeView<'_>) -> Result<(), String> {
    check_custom_bounds(histogram.custom_values, histogram.positive_spans)?;
    if histogram.zero_count != 0 {
        return Err("custom buckets: must have zero count of 0".to_owned());
    }
    if histogram.zero_threshold != 0.0 {
        return Err("custom buckets: must have zero threshold of 0".to_owned());
    }
    if !histogram.negative_spans.is_empty() {
        return Err("custom buckets: must not have negative spans".to_owned());
    }
    if histogram.negative.len() != 0 {
        return Err("custom buckets: must not have negative buckets".to_owned());
    }
    Ok(())
}

/// Go's `checkHistogramCustomBounds`, without the span and bucket count check that
/// [`walk_buckets`] does.
fn check_custom_bounds(bounds: &[f64], spans: &[Span]) -> Result<(), String> {
    let mut previous = f64::NEG_INFINITY;
    for &bound in bounds {
        if bound.is_nan() {
            return Err("custom buckets: must not have NaN bounds".to_owned());
        }
        if bound <= previous {
            return Err(format!(
                "previous bound is {previous} and current is {bound}: custom buckets: must be \
                 in strictly increasing order"
            ));
        }
        previous = bound;
    }
    if previous == f64::INFINITY {
        return Err("custom buckets: last +Inf bound must not be explicitly defined".to_owned());
    }
    let mut total_span_length = 0i64;
    for (n, span) in spans.iter().enumerate() {
        if span.offset < 0 {
            return Err(format!(
                "span number {} with offset {}: histogram has a span whose offset is negative",
                n + 1,
                span.offset
            ));
        }
        total_span_length += i64::from(span.offset) + i64::from(span.length);
    }
    if total_span_length > bounds.len() as i64 + 1 {
        return Err(format!(
            "spans need at least {total_span_length} custom bounds, have {}: custom buckets: \
             must have enough bounds for the spans",
            bounds.len() + 1
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(offset: i32, length: u32) -> Span {
        Span { offset, length }
    }

    fn walk(spans: &[Span], buckets: Buckets<'_>) -> Result<(Vec<(i64, u64)>, u64), String> {
        let mut seen = Vec::new();
        let total = walk_buckets("positive", spans, buckets, |index, count| {
            seen.push((index, count));
        })?;
        Ok((seen, total))
    }

    #[test]
    fn walk_buckets_decodes_deltas_and_gaps() {
        let spans = [span(-2, 2), span(3, 1)];
        let (seen, total) = walk(&spans, Buckets::Delta(&[1, 2, -1])).unwrap();
        assert_eq!(seen, [(-2, 1), (-1, 3), (3, 2)]);
        assert_eq!(total, 6);
    }

    #[test]
    fn walk_buckets_applies_go_checks() {
        let err = walk(&[span(0, 1), span(-1, 1)], Buckets::Delta(&[1, 1])).unwrap_err();
        assert!(err.contains("offset is negative"), "{err}");
        let err = walk(&[span(0, 2)], Buckets::Delta(&[1])).unwrap_err();
        assert!(err.contains("different number of buckets"), "{err}");
        let err = walk(&[span(0, 2)], Buckets::Delta(&[1, -2])).unwrap_err();
        assert!(err.contains("count is negative"), "{err}");
        let err = walk(&[span(0, 1)], Buckets::Absolute(&[-0.5])).unwrap_err();
        assert!(err.contains("count is negative"), "{err}");
    }

    #[test]
    fn check_custom_bounds_matches_go() {
        assert!(check_custom_bounds(&[1.0, 2.0], &[span(0, 3)]).is_ok());
        assert!(check_custom_bounds(&[1.0, 1.0], &[]).is_err());
        assert!(check_custom_bounds(&[f64::NAN], &[]).is_err());
        assert!(check_custom_bounds(&[f64::NEG_INFINITY], &[]).is_err());
        assert!(check_custom_bounds(&[1.0, f64::INFINITY], &[]).is_err());
        assert!(check_custom_bounds(&[1.0], &[span(-1, 1)]).is_err());
        assert!(check_custom_bounds(&[1.0], &[span(1, 2)]).is_err());
    }

    fn nhcb(count: u64, sum: f64, spans: Vec<Span>, deltas: Vec<i64>) -> Histogram<u64, i64> {
        Histogram {
            schema: CUSTOM_BUCKETS_SCHEMA,
            count,
            sum,
            positive_spans: spans,
            positive_buckets: deltas,
            custom_values: vec![1.0, 2.0, 5.0],
            ..Histogram::default()
        }
    }

    fn family() -> CurrentFamily {
        let mut family = CurrentFamily::default();
        family.start_new(b"h", b"h");
        family
    }

    fn push(dp: &mut HistogramDpTable, h: &Histogram<u64, i64>) -> Result<(), DecodeError> {
        let view = NativeView::new(HistogramRef::Int(h)).unwrap();
        push_custom_buckets(dp, dp.base.len(), &view, &family())
    }

    #[test]
    fn push_custom_buckets_writes_dense_counts() {
        let mut dp = HistogramDpTable::default();
        // Buckets 0 and 3 (+Inf) only, compacted with a gap like the protobuf parser does.
        let h = nhcb(5, 7.5, vec![span(0, 1), span(2, 1)], vec![2, 1]);
        push(&mut dp, &h).unwrap();
        assert_eq!(dp.bucket_counts, [2, 0, 0, 3]);
        assert_eq!(dp.bounds_vals, [1.0, 2.0, 5.0]);
        assert_eq!(dp.bucket_offsets, [0, 4]);
        assert_eq!(dp.bounds_offsets, [0, 3]);
    }

    #[test]
    fn push_custom_buckets_checks_count() {
        let mut dp = HistogramDpTable::default();
        let h = nhcb(4, 1.0, vec![span(0, 1)], vec![5]);
        let err = push(&mut dp, &h).unwrap_err().to_string();
        assert!(err.contains("should equal"), "{err}");

        let h = nhcb(6, f64::NAN, vec![span(0, 1)], vec![5]);
        push(&mut HistogramDpTable::default(), &h).unwrap();
    }

    #[test]
    fn push_custom_buckets_rejects_zero_and_negative_buckets() {
        let mut h = nhcb(1, 1.0, vec![span(0, 1)], vec![1]);
        h.zero_count = 1;
        h.count = 2;
        assert!(push(&mut HistogramDpTable::default(), &h).is_err());

        let mut h = nhcb(2, 1.0, vec![span(0, 1)], vec![1]);
        h.negative_spans = vec![span(0, 1)];
        h.negative_buckets = vec![1];
        assert!(push(&mut HistogramDpTable::default(), &h).is_err());
    }

    #[test]
    fn float_view_rejects_negative_counts() {
        let h = Histogram::<f64, f64> {
            count: -1.0,
            ..Histogram::default()
        };
        assert!(NativeView::new(HistogramRef::Float(&h)).is_err());
    }
}
