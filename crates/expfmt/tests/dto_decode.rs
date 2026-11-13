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

//! Layer 2 `dto::decode_to_metric_families` and `dto::arrow_to_metric_families` tests against the
//! synced `testdata/` corpus and hand-written OpenMetrics 1.0 payloads.

#![cfg(all(feature = "arrow", feature = "dto"))]

use std::fs;
use std::path::{Path, PathBuf};

use prometheus_expfmt::arrow::{ArrowDecoder, DecodeOptions};
use prometheus_expfmt::dto::{
    Bucket, Counter, Exemplar, Gauge, Histogram, LabelPair, Metric, MetricFamily, MetricType,
    Quantile, Summary, Untyped, arrow_to_metric_families, decode_to_metric_families,
};
use prometheus_expfmt::format::{Format, FormatType};
use prost::Message;

fn testdata() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
}

fn text_format() -> Format {
    Format::from_type(FormatType::TextPlain)
}

fn om_format() -> Format {
    Format::new_open_metrics("1.0.0").expect("OpenMetrics 1.0.0 format should be valid")
}

fn labels(pairs: &[(&str, &str)]) -> Vec<LabelPair> {
    pairs
        .iter()
        .map(|&(name, value)| LabelPair {
            name: name.to_owned(),
            value: value.to_owned(),
        })
        .collect()
}

fn timestamp(seconds: i64, nanos: i32) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos })
}

fn family(name: &str, help: &str, unit: &str, t: MetricType, metric: Vec<Metric>) -> MetricFamily {
    MetricFamily {
        name: name.to_owned(),
        help: help.to_owned(),
        r#type: t as i32,
        unit: unit.to_owned(),
        metric,
    }
}

fn sorted_by_name(mut families: Vec<MetricFamily>) -> Vec<MetricFamily> {
    families.sort_by(|a, b| a.name.cmp(&b.name));
    families
}

/// `prometheus/common/expfmt/testdata/text` and `protobuf` hold the same metric families, so
/// decoding the text fixture must reproduce the protobuf fixture.
#[test]
fn text_fixture_matches_protobuf_fixture() {
    let root = testdata().join("expfmt");
    let text = fs::read(root.join("text")).expect("text fixture should be readable");
    let got = decode_to_metric_families(&text, text_format(), &DecodeOptions::new())
        .expect("text fixture should decode");

    let raw = fs::read(root.join("protobuf")).expect("protobuf fixture should be readable");
    let mut cursor: &[u8] = &raw;
    let mut want = Vec::new();
    while !cursor.is_empty() {
        want.push(
            MetricFamily::decode_length_delimited(&mut cursor)
                .expect("protobuf fixture should decode"),
        );
    }

    assert_eq!(want.len(), 37);
    assert_eq!(sorted_by_name(got), sorted_by_name(want));
}

#[test]
fn textparse_fixtures_keep_every_family_and_data_point() {
    let root = testdata().join("textparse");
    let fixtures = [
        ("TestPromParse.prom.txt", text_format()),
        ("alltypes.237mfs.prom.txt", text_format()),
        ("alltypes.237mfs.nometa.prom.txt", text_format()),
        ("TestOpenMetricsParse.om.txt", om_format()),
        ("TestOpenMetricsParse_UTF8.om.txt", om_format()),
        ("1histogram.om.txt", om_format()),
        ("alltypes.5mfs.om.txt", om_format()),
        ("alltypes.bench.om.txt", om_format()),
        ("ct.bench.om.txt", om_format()),
        ("histograms.bench.om.txt", om_format()),
    ];
    let opts = DecodeOptions::new();
    let mut decoder = ArrowDecoder::new();

    for (name, format) in fixtures {
        let input = fs::read(root.join(name)).unwrap_or_else(|e| panic!("read {name}: {e}"));
        let records = decoder
            .decode(&input, &format, &opts)
            .unwrap_or_else(|e| panic!("ArrowDecoder failed on {name}: {e}"));
        let families = arrow_to_metric_families(&records)
            .unwrap_or_else(|e| panic!("arrow_to_metric_families failed on {name}: {e}"));

        assert_eq!(families.len(), records.num_metrics(), "{name}: families");
        let metrics: usize = families.iter().map(|f| f.metric.len()).sum();
        assert_eq!(metrics, records.num_data_points(), "{name}: data points");
    }
}

#[test]
fn openmetrics_all_types() {
    let input = br#"# TYPE request_seconds histogram
# HELP request_seconds Request latency.
# UNIT request_seconds seconds
request_seconds_bucket{path="/",le="0.1"} 1 # {trace_id="0123456789ABCDEF0123456789abcdef",span_id="0123456789abcdef",pod="a"} 0.05 123.456
request_seconds_bucket{path="/",le="1"} 3 # {pod="b"} 0.5
request_seconds_bucket{path="/",le="+Inf"} 4
request_seconds_count{path="/"} 4
request_seconds_sum{path="/"} 2.5
request_seconds_created{path="/"} 1000.5
# TYPE rpc summary
rpc{quantile="0.99"} 2
rpc{quantile="0.5"} 1
rpc_sum 10
rpc_count 5
# TYPE jobs counter
jobs_total{queue="b",job="x"} 7 1700000000.5 # {id="1"} 1 1.5
jobs_created{queue="b",job="x"} 10
# TYPE build info
build_info{version="1.0"} 1
# TYPE state stateset
state{state="on"} 1
# TYPE queue_size gaugehistogram
queue_size_bucket{le="1"} 2
queue_size_bucket{le="+Inf"} 3
queue_size_gcount 3
queue_size_gsum 1
# TYPE temperature gauge
temperature -1.5
# TYPE mystery unknown
mystery 1
# EOF
"#;

    let got = decode_to_metric_families(input, om_format(), &DecodeOptions::new())
        .expect("OpenMetrics payload should decode");

    let want = vec![
        family(
            "request_seconds",
            "Request latency.",
            "seconds",
            MetricType::Histogram,
            vec![Metric {
                label: labels(&[("path", "/")]),
                histogram: Some(Histogram {
                    sample_count: 4,
                    sample_sum: 2.5,
                    bucket: vec![
                        Bucket {
                            cumulative_count: 1,
                            upper_bound: 0.1,
                            exemplar: Some(Exemplar {
                                label: labels(&[
                                    ("pod", "a"),
                                    ("span_id", "0123456789abcdef"),
                                    ("trace_id", "0123456789abcdef0123456789abcdef"),
                                ]),
                                value: 0.05,
                                timestamp: timestamp(123, 456_000_000),
                            }),
                            ..Default::default()
                        },
                        Bucket {
                            cumulative_count: 3,
                            upper_bound: 1.0,
                            exemplar: Some(Exemplar {
                                label: labels(&[("pod", "b")]),
                                value: 0.5,
                                timestamp: None,
                            }),
                            ..Default::default()
                        },
                        Bucket {
                            cumulative_count: 4,
                            upper_bound: f64::INFINITY,
                            ..Default::default()
                        },
                    ],
                    start_timestamp: timestamp(1000, 500_000_000),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        ),
        family(
            "rpc",
            "",
            "",
            MetricType::Summary,
            vec![Metric {
                summary: Some(Summary {
                    sample_count: 5,
                    sample_sum: 10.0,
                    quantile: vec![
                        Quantile {
                            quantile: 0.5,
                            value: 1.0,
                        },
                        Quantile {
                            quantile: 0.99,
                            value: 2.0,
                        },
                    ],
                    start_timestamp: None,
                }),
                ..Default::default()
            }],
        ),
        family(
            "jobs_total",
            "",
            "",
            MetricType::Counter,
            vec![Metric {
                label: labels(&[("job", "x"), ("queue", "b")]),
                counter: Some(Counter {
                    value: 7.0,
                    exemplar: Some(Exemplar {
                        label: labels(&[("id", "1")]),
                        value: 1.0,
                        timestamp: timestamp(1, 500_000_000),
                    }),
                    start_timestamp: timestamp(10, 0),
                }),
                timestamp_ms: 1_700_000_000_500,
                ..Default::default()
            }],
        ),
        family(
            "build",
            "",
            "",
            MetricType::Gauge,
            vec![Metric {
                label: labels(&[("version", "1.0")]),
                gauge: Some(Gauge { value: 1.0 }),
                ..Default::default()
            }],
        ),
        family(
            "state",
            "",
            "",
            MetricType::Gauge,
            vec![Metric {
                label: labels(&[("state", "on")]),
                gauge: Some(Gauge { value: 1.0 }),
                ..Default::default()
            }],
        ),
        family(
            "queue_size",
            "",
            "",
            MetricType::GaugeHistogram,
            vec![Metric {
                histogram: Some(Histogram {
                    sample_count: 3,
                    sample_sum: 1.0,
                    bucket: vec![
                        Bucket {
                            cumulative_count: 2,
                            upper_bound: 1.0,
                            ..Default::default()
                        },
                        Bucket {
                            cumulative_count: 3,
                            upper_bound: f64::INFINITY,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        ),
        family(
            "temperature",
            "",
            "",
            MetricType::Gauge,
            vec![Metric {
                gauge: Some(Gauge { value: -1.5 }),
                ..Default::default()
            }],
        ),
        family(
            "mystery",
            "",
            "",
            MetricType::Untyped,
            vec![Metric {
                untyped: Some(Untyped { value: 1.0 }),
                ..Default::default()
            }],
        ),
    ];

    assert_eq!(got, want);
}

#[test]
fn histogram_exemplars_attach_to_first_covering_bucket() {
    let input = br#"# TYPE h histogram
h_bucket{le="1"} 2 # {id="a"} 0.5
h_bucket{le="2"} 3 # {id="b"} 0.9
h_bucket{le="+Inf"} 4 # {id="c"} 5
h_count 4
h_sum 7
# EOF
"#;
    let got = decode_to_metric_families(input, om_format(), &DecodeOptions::new())
        .expect("OpenMetrics payload should decode");
    let ids: Vec<Option<String>> = got[0].metric[0]
        .histogram
        .as_ref()
        .expect("histogram should be set")
        .bucket
        .iter()
        .map(|b| b.exemplar.as_ref().map(|e| e.label[0].value.clone()))
        .collect();

    // `b` falls into the `le="1"` bucket and replaces `a`, as in client_golang.
    assert_eq!(ids, vec![Some("b".to_owned()), None, Some("c".to_owned())]);
}

#[test]
fn histogram_without_buckets() {
    let input = b"# TYPE h histogram\nh_count 3\nh_sum 1.5\n";
    let got = decode_to_metric_families(input, text_format(), &DecodeOptions::new())
        .expect("text payload should decode");

    assert_eq!(
        got[0].metric[0].histogram,
        Some(Histogram {
            sample_count: 3,
            sample_sum: 1.5,
            ..Default::default()
        })
    );
}

#[test]
fn default_timestamp_applies_to_untimestamped_samples() {
    let input = b"# TYPE a gauge\na 1\na{x=\"y\"} 2 5000\n";
    let opts = DecodeOptions::new().with_timestamp_ms(1_000);
    let got =
        decode_to_metric_families(input, text_format(), &opts).expect("text payload should decode");

    let timestamps: Vec<i64> = got[0].metric.iter().map(|m| m.timestamp_ms).collect();
    assert_eq!(timestamps, vec![1_000, 5_000]);
}

#[test]
fn empty_input_yields_no_families() {
    let got = decode_to_metric_families(b"", text_format(), &DecodeOptions::new())
        .expect("empty payload should decode");
    assert!(got.is_empty());
}
