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

//! Decoding delimited protobuf input with [`ArrowDecoder`].

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float64Type, TimestampNanosecondType, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, RecordBatch, StringArray};
use prost::Message;

use super::*;
use crate::dto;

fn encode(families: &[dto::MetricFamily]) -> Vec<u8> {
    let mut buf = Vec::new();
    for family in families {
        family.encode_length_delimited(&mut buf).unwrap();
    }
    buf
}

fn family(
    name: &str,
    help: &str,
    metric_type: dto::MetricType,
    metric: Vec<dto::Metric>,
) -> dto::MetricFamily {
    dto::MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        r#type: metric_type as i32,
        metric,
        unit: String::new(),
    }
}

fn labels(pairs: &[(&str, &str)]) -> Vec<dto::LabelPair> {
    pairs
        .iter()
        .map(|&(name, value)| dto::LabelPair {
            name: name.to_owned(),
            value: value.to_owned(),
        })
        .collect()
}

fn timestamp(millis: i64) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: millis.div_euclid(1000),
        nanos: (millis.rem_euclid(1000) * 1_000_000) as i32,
    }
}

fn counter(pairs: &[(&str, &str)], value: f64, timestamp_ms: i64) -> dto::Metric {
    dto::Metric {
        label: labels(pairs),
        counter: Some(dto::Counter {
            value,
            ..dto::Counter::default()
        }),
        timestamp_ms,
        ..dto::Metric::default()
    }
}

fn gauge(pairs: &[(&str, &str)], value: f64) -> dto::Metric {
    dto::Metric {
        label: labels(pairs),
        gauge: Some(dto::Gauge { value }),
        ..dto::Metric::default()
    }
}

/// Classic histogram with `(upper_bound, cumulative_count)` buckets, which the protobuf parser
/// converts to NHCB.
fn classic_histogram(
    pairs: &[(&str, &str)],
    buckets: &[(f64, u64)],
    count: u64,
    sum: f64,
) -> dto::Metric {
    dto::Metric {
        label: labels(pairs),
        histogram: Some(dto::Histogram {
            sample_count: count,
            sample_sum: sum,
            bucket: buckets
                .iter()
                .map(|&(upper_bound, cumulative_count)| dto::Bucket {
                    upper_bound,
                    cumulative_count,
                    ..dto::Bucket::default()
                })
                .collect(),
            ..dto::Histogram::default()
        }),
        timestamp_ms: 1_700_000_000_000,
        ..dto::Metric::default()
    }
}

fn decode(input: &[u8], opts: &DecodeOptions) -> Result<OtapMetricRecords, DecodeError> {
    ArrowDecoder::new().decode(input, &Format::PROTO_DELIM, opts)
}

/// Values of a plain or dictionary-encoded string column.
fn strings(batch: &RecordBatch, column: &str) -> Vec<String> {
    let array = batch.column_by_name(column).unwrap();
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return (0..array.len())
            .map(|i| array.value(i).to_owned())
            .collect();
    }
    let dictionary = array.as_any_dictionary();
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    dictionary
        .normalized_keys()
        .into_iter()
        .map(|key| values.value(key).to_owned())
        .collect()
}

fn u64s(batch: &RecordBatch, column: &str) -> Vec<u64> {
    let array = batch.column_by_name(column).unwrap();
    array.as_primitive::<UInt64Type>().values().to_vec()
}

fn f64s(batch: &RecordBatch, column: &str) -> Vec<f64> {
    let array = batch.column_by_name(column).unwrap();
    array.as_primitive::<Float64Type>().values().to_vec()
}

fn timestamps(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
    let array = batch.column_by_name(column).unwrap();
    let array = array.as_primitive::<TimestampNanosecondType>();
    (0..array.len())
        .map(|i| array.is_valid(i).then(|| array.value(i)))
        .collect()
}

#[test]
fn scalars_labels_exemplars_and_timestamps() {
    let mut requests = counter(&[("path", "/a"), ("code", "200")], 5.0, 1_700_000_000_123);
    let requests_counter = requests.counter.as_mut().unwrap();
    requests_counter.start_timestamp = Some(timestamp(1_600_000_000_000));
    requests_counter.exemplar = Some(dto::Exemplar {
        label: labels(&[
            ("trace_id", "0102030405060708090a0b0c0d0e0f10"),
            ("span_id", "0102030405060708"),
            ("user", "x"),
        ]),
        value: 0.25,
        timestamp: Some(timestamp(1_700_000_000_100)),
    });
    // A zero start timestamp is unset, like in Go. An empty label value drops the label.
    let mut other = counter(&[("path", "/b"), ("code", "")], 7.0, 0);
    other.counter.as_mut().unwrap().start_timestamp = Some(timestamp(0));

    let input = encode(&[
        family(
            "requests_total",
            "Requests.",
            dto::MetricType::Counter,
            vec![requests, other],
        ),
        family(
            "temperature",
            "",
            dto::MetricType::Gauge,
            vec![gauge(&[("room", "a\\b\"c")], -1.5)],
        ),
        family(
            "mystery",
            "",
            dto::MetricType::Untyped,
            vec![dto::Metric {
                untyped: Some(dto::Untyped { value: 3.0 }),
                ..dto::Metric::default()
            }],
        ),
    ]);
    let opts = DecodeOptions::default().with_timestamp_ms(1_800_000_000_000);
    let records = decode(&input, &opts).unwrap();

    let metrics = records.univariate_metrics().unwrap();
    assert_eq!(
        strings(metrics, "name"),
        ["requests_total", "temperature", "mystery"]
    );
    let points = records.number_data_points().unwrap();
    assert_eq!(f64s(points, "double_value"), [5.0, 7.0, -1.5, 3.0]);
    assert_eq!(
        timestamps(points, "time_unix_nano"),
        [
            Some(1_700_000_000_123_000_000),
            Some(1_800_000_000_000_000_000),
            Some(1_800_000_000_000_000_000),
            Some(1_800_000_000_000_000_000),
        ]
    );
    assert_eq!(
        timestamps(points, "start_time_unix_nano"),
        [Some(1_600_000_000_000_000_000), None, None, None]
    );

    // Protobuf label values are taken as they are, without text unescaping. Attributes are
    // sorted by interned key ID, like the text decoder does.
    let attrs = records.number_dp_attrs().unwrap();
    assert_eq!(strings(attrs, "key"), ["path", "code", "path", "room"]);
    assert_eq!(strings(attrs, "str"), ["/a", "200", "/b", "a\\b\"c"]);

    let exemplars = records.number_dp_exemplars().unwrap();
    assert_eq!(f64s(exemplars, "double_value"), [0.25]);
    assert_eq!(
        timestamps(exemplars, "time_unix_nano"),
        [Some(1_700_000_000_100_000_000)]
    );
    let span_ids = exemplars
        .column_by_name("span_id")
        .unwrap()
        .as_fixed_size_binary();
    assert_eq!(span_ids.value(0), [1, 2, 3, 4, 5, 6, 7, 8]);
    let exemplar_attrs = records.number_dp_exemplar_attrs().unwrap();
    assert_eq!(strings(exemplar_attrs, "key"), ["user"]);
}

#[test]
fn summaries_become_one_data_point_per_metric() {
    let summary = dto::Metric {
        label: labels(&[("job", "a")]),
        summary: Some(dto::Summary {
            sample_count: 10,
            sample_sum: 4.5,
            quantile: vec![
                dto::Quantile {
                    quantile: 0.9,
                    value: 2.0,
                },
                dto::Quantile {
                    quantile: 0.5,
                    value: 1.0,
                },
                dto::Quantile {
                    quantile: f64::NAN,
                    value: 9.0,
                },
            ],
            start_timestamp: Some(timestamp(1_000)),
        }),
        ..dto::Metric::default()
    };
    let empty = dto::Metric::default();
    let input = encode(&[family(
        "latency",
        "Latency.",
        dto::MetricType::Summary,
        vec![summary, empty],
    )]);
    let records = decode(&input, &DecodeOptions::default()).unwrap();

    assert!(records.number_data_points().is_none());
    let points = records.summary_data_points().unwrap();
    assert_eq!(points.num_rows(), 2);
    assert_eq!(u64s(points, "count"), [10, 0]);
    assert_eq!(f64s(points, "sum"), [4.5, 0.0]);
    assert_eq!(
        timestamps(points, "start_time_unix_nano"),
        [Some(1_000_000_000), None]
    );
    let quantiles = points.column_by_name("quantile").unwrap().as_list::<i32>();
    assert_eq!(quantiles.value_offsets(), [0, 2, 2]);
    let quantile_values = quantiles.values().as_struct();
    assert_eq!(
        quantile_values
            .column_by_name("quantile")
            .unwrap()
            .as_primitive::<Float64Type>()
            .values(),
        &[0.5, 0.9]
    );
    let summary_attrs = records.summary_dp_attrs().unwrap();
    assert_eq!(
        summary_attrs
            .column_by_name("parent_id")
            .unwrap()
            .as_primitive::<UInt32Type>()
            .values(),
        &[0]
    );
}

#[test]
fn classic_histograms_become_explicit_bucket_points() {
    let input = encode(&[family(
        "rpc_seconds",
        "RPC latency.",
        dto::MetricType::Histogram,
        vec![
            // Empty buckets in the middle are compacted away by the NHCB conversion and come
            // back as zero counts.
            classic_histogram(
                &[("method", "get")],
                &[(0.1, 1), (0.5, 1), (1.0, 1), (2.0, 1), (5.0, 3)],
                4,
                9.0,
            ),
            classic_histogram(&[("method", "put")], &[], 0, 0.0),
        ],
    )]);
    let records = decode(&input, &DecodeOptions::default()).unwrap();
    let points = records.histogram_data_points().unwrap();
    assert_eq!(u64s(points, "count"), [4, 0]);
    assert_eq!(f64s(points, "sum"), [9.0, 0.0]);
    let buckets = points
        .column_by_name("bucket_counts")
        .unwrap()
        .as_list::<i32>();
    assert_eq!(buckets.value_offsets(), [0, 6, 7]);
    assert_eq!(
        buckets.values().as_primitive::<UInt64Type>().values(),
        &[1, 0, 0, 0, 2, 1, 0]
    );
    let bounds = points
        .column_by_name("explicit_bounds")
        .unwrap()
        .as_list::<i32>();
    assert_eq!(bounds.value_offsets(), [0, 5, 5]);
    assert_eq!(
        bounds.values().as_primitive::<Float64Type>().values(),
        &[0.1, 0.5, 1.0, 2.0, 5.0]
    );
}

#[test]
fn text_and_protobuf_decode_to_the_same_records() {
    let text = "\
# HELP requests_total Requests.
# TYPE requests_total counter
requests_total{code=\"200\",path=\"/a\"} 5 1700000000000
requests_total{code=\"500\",path=\"/a\"} 1 1700000000000
# TYPE temperature gauge
temperature{room=\"a\"} 21.5 1700000000000
# HELP latency Latency.
# TYPE latency summary
latency{job=\"a\",quantile=\"0.5\"} 1 1700000000000
latency{job=\"a\",quantile=\"0.9\"} 2 1700000000000
latency_sum{job=\"a\"} 4.5 1700000000000
latency_count{job=\"a\"} 10 1700000000000
# HELP rpc_seconds RPC latency.
# TYPE rpc_seconds histogram
rpc_seconds_bucket{method=\"get\",le=\"0.1\"} 1 1700000000000
rpc_seconds_bucket{method=\"get\",le=\"0.5\"} 1 1700000000000
rpc_seconds_bucket{method=\"get\",le=\"1\"} 3 1700000000000
rpc_seconds_bucket{method=\"get\",le=\"+Inf\"} 4 1700000000000
rpc_seconds_sum{method=\"get\"} 9 1700000000000
rpc_seconds_count{method=\"get\"} 4 1700000000000
";
    let at = |mut metric: dto::Metric| {
        metric.timestamp_ms = 1_700_000_000_000;
        metric
    };
    let summary = dto::Metric {
        label: labels(&[("job", "a")]),
        summary: Some(dto::Summary {
            sample_count: 10,
            sample_sum: 4.5,
            quantile: vec![
                dto::Quantile {
                    quantile: 0.5,
                    value: 1.0,
                },
                dto::Quantile {
                    quantile: 0.9,
                    value: 2.0,
                },
            ],
            start_timestamp: None,
        }),
        ..dto::Metric::default()
    };
    let proto = encode(&[
        family(
            "requests_total",
            "Requests.",
            dto::MetricType::Counter,
            vec![
                counter(&[("code", "200"), ("path", "/a")], 5.0, 1_700_000_000_000),
                counter(&[("code", "500"), ("path", "/a")], 1.0, 1_700_000_000_000),
            ],
        ),
        family(
            "temperature",
            "",
            dto::MetricType::Gauge,
            vec![at(gauge(&[("room", "a")], 21.5))],
        ),
        family(
            "latency",
            "Latency.",
            dto::MetricType::Summary,
            vec![at(summary)],
        ),
        family(
            "rpc_seconds",
            "RPC latency.",
            dto::MetricType::Histogram,
            vec![classic_histogram(
                &[("method", "get")],
                &[(0.1, 1), (0.5, 1), (1.0, 3)],
                4,
                9.0,
            )],
        ),
    ]);
    let opts = DecodeOptions::default();
    let from_text = ArrowDecoder::new()
        .decode(text.as_bytes(), &Format::TEXT, &opts)
        .unwrap();
    let from_proto = decode(&proto, &opts).unwrap();
    for (payload_type, batch) in from_text.iter() {
        assert_eq!(
            Some(batch),
            from_proto.get(payload_type),
            "{payload_type:?} differs"
        );
    }
    assert_eq!(from_text, from_proto);
}

#[test]
fn repeated_family_names_start_new_metric_rows() {
    let input = encode(&[
        family(
            "up",
            "Up.",
            dto::MetricType::Gauge,
            vec![gauge(&[("job", "a")], 1.0)],
        ),
        family(
            "up",
            "Up.",
            dto::MetricType::Gauge,
            vec![gauge(&[("job", "b")], 0.0)],
        ),
        // Go skips families without metrics.
        family("nothing", "", dto::MetricType::Counter, vec![]),
    ]);
    let records = decode(&input, &DecodeOptions::default()).unwrap();
    assert_eq!(
        strings(records.univariate_metrics().unwrap(), "name"),
        ["up", "up"]
    );
    let points = records.number_data_points().unwrap();
    assert_eq!(
        points
            .column_by_name("parent_id")
            .unwrap()
            .as_primitive::<UInt16Type>()
            .values(),
        &[0, 1]
    );
}

#[test]
fn label_errors_match_the_text_decoder() {
    for pairs in [
        &[("__name__", "x")][..],
        &[("__name__", "")][..],
        &[("a", "1"), ("a", "2")][..],
        &[("a", ""), ("a", "2")][..],
    ] {
        let input = encode(&[family(
            "g",
            "",
            dto::MetricType::Gauge,
            vec![gauge(pairs, 1.0)],
        )]);
        let err = decode(&input, &DecodeOptions::default()).unwrap_err();
        assert!(
            matches!(err, DecodeError::DuplicateLabel { .. }),
            "{pairs:?}: {err:?}"
        );
    }

    let untyped = |pairs| dto::Metric {
        label: labels(pairs),
        untyped: Some(dto::Untyped { value: 1.0 }),
        ..dto::Metric::default()
    };
    let opts = DecodeOptions::default().with_untyped_policy(UntypedPolicy::Drop);
    let input = encode(&[family(
        "u",
        "",
        dto::MetricType::Untyped,
        vec![untyped(&[("a", "1")])],
    )]);
    assert!(decode(&input, &opts).unwrap().is_empty());
    let input = encode(&[family(
        "u",
        "",
        dto::MetricType::Untyped,
        vec![untyped(&[("a", "1"), ("a", "2")])],
    )]);
    assert!(matches!(
        decode(&input, &opts),
        Err(DecodeError::DuplicateLabel { .. })
    ));
}

#[test]
fn invalid_histograms_fail_the_scrape() {
    let native = |schema| dto::Metric {
        histogram: Some(dto::Histogram {
            sample_count: 1,
            schema,
            zero_threshold: 1e-128,
            zero_count: 1,
            ..dto::Histogram::default()
        }),
        ..dto::Metric::default()
    };
    for schema in [9, -5] {
        let input = encode(&[family(
            "h",
            "",
            dto::MetricType::Histogram,
            vec![native(schema)],
        )]);
        let err = decode(&input, &DecodeOptions::default()).unwrap_err();
        assert!(
            matches!(&err, DecodeError::InvalidHistogram { metric, reason }
                if metric == "h" && reason.contains("invalid schema")),
            "{err:?}"
        );
    }
}

#[test]
fn chunked_and_budgeted_decoding_matches_single_batch() {
    let mut metrics = Vec::new();
    for i in 0..5 {
        metrics.push(classic_histogram(
            &[("i", &i.to_string())],
            &[(1.0, i)],
            i,
            0.5,
        ));
    }
    let input = encode(&[
        family(
            "a",
            "A.",
            dto::MetricType::Gauge,
            (0..5).map(|i| gauge(&[], f64::from(i))).collect(),
        ),
        family("h", "H.", dto::MetricType::Histogram, metrics),
    ]);
    let opts = DecodeOptions::default().with_max_data_points_per_batch(2);
    let mut dec = ArrowDecoder::new();
    let chunks: Vec<_> = dec
        .decode_stream(&input, &Format::PROTO_DELIM, &opts)
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        chunks
            .iter()
            .map(OtapMetricRecords::num_data_points)
            .collect::<Vec<_>>(),
        [2, 2, 2, 2, 2]
    );
    // The family row is repeated in every chunk its points land in.
    assert_eq!(
        strings(chunks[2].univariate_metrics().unwrap(), "name"),
        ["a", "h"]
    );
    assert_eq!(
        strings(chunks[3].univariate_metrics().unwrap(), "description"),
        ["H."]
    );

    // A budget of one entry per step pauses at every entry, which round-trips the parser
    // state through the cursor, and still yields the same chunks.
    let mut cursor = dec
        .start_decode_stream(&Format::PROTO_DELIM, &opts)
        .unwrap();
    let mut stepped = Vec::new();
    let mut pending = 0;
    loop {
        match dec
            .step_decode(
                &mut cursor,
                &input,
                DecodeBudget::default().with_max_entries(1),
            )
            .unwrap()
        {
            DecodeStep::Pending => pending += 1,
            DecodeStep::Batch(batch) => stepped.push(batch),
            DecodeStep::Done => break,
        }
    }
    assert!(pending >= 5, "{pending}");
    assert_eq!(stepped, chunks);

    let whole = dec
        .decode(&input, &Format::PROTO_DELIM, &DecodeOptions::default())
        .unwrap();
    assert_eq!(whole.num_data_points(), 10);
}

#[test]
fn protobuf_scratch_is_reused_across_scrapes() {
    let input = encode(&[family(
        "h",
        "",
        dto::MetricType::Histogram,
        vec![classic_histogram(&[], &[(1.0, 1), (2.0, 2)], 2, 1.0)],
    )]);
    let mut dec = ArrowDecoder::new();
    dec.decode(&input, &Format::PROTO_DELIM, &DecodeOptions::default())
        .unwrap();
    let retained = dec.retained_memory_bytes();
    assert!(dec.proto.retained_memory_bytes() > 0);
    for _ in 0..3 {
        dec.decode(&input, &Format::PROTO_DELIM, &DecodeOptions::default())
            .unwrap();
    }
    assert_eq!(dec.retained_memory_bytes(), retained);
}
