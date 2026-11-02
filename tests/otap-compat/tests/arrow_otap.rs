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

//! Integration tests verifying `expfmt::arrow::ArrowDecoder` against `otel-arrow-dfe-pdata`
//! schema checks, transport-optimized ID encoding and decoding, and OTLP semantic equivalence.

use std::fs;
use std::path::Path;

use expfmt::arrow::{
    ArrowDecoder, DecodeOptions, MetricPayloadType, OtapMetricRecords, PROMETHEUS_TYPE_METADATA_KEY,
};
use expfmt::format::Format;
use otel_arrow_dfe_pdata::otap::{Metrics, OtapArrowRecords, OtapBatchStore};
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{AnyValue, KeyValue, any_value};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    AggregationTemporality, Gauge, Metric, NumberDataPoint, Sum, metric, number_data_point,
};
use otel_arrow_dfe_pdata::schema::{
    consts, is_id_plain_encoded, is_parent_id_plain_encoded, payloads,
};
use otel_arrow_dfe_pdata::testing::equiv::assert_equivalent;
use otel_arrow_dfe_pdata::testing::round_trip::{otap_to_otlp, to_metrics_data};

fn assert_matches_otap_schema(records: &OtapMetricRecords) {
    for (payload_type, batch) in records.iter() {
        let wire_type = ArrowPayloadType::try_from(payload_type.as_i32())
            .expect("MetricPayloadType matches ArrowPayloadType wire discriminant");
        payloads::get(wire_type)
            .check_match(batch)
            .unwrap_or_else(|err| panic!("upstream OTAP schema rejected {payload_type:?}: {err}"));

        let schema = batch.schema();
        if schema.index_of(consts::ID).is_ok() {
            assert!(
                is_id_plain_encoded(batch),
                "{payload_type:?} id column must carry plain encoding metadata"
            );
        }
        if schema.index_of(consts::PARENT_ID).is_ok() {
            assert!(
                is_parent_id_plain_encoded(batch),
                "{payload_type:?} parent_id column must carry plain encoding metadata"
            );
        }
    }
}

fn to_otap_arrow_records(records: &OtapMetricRecords) -> OtapArrowRecords {
    assert_matches_otap_schema(records);
    let mut store = Metrics::new();
    for (payload_type, batch) in records.iter() {
        let wire_type = ArrowPayloadType::try_from(payload_type.as_i32())
            .expect("MetricPayloadType matches ArrowPayloadType wire discriminant");
        store
            .set(wire_type, batch.clone())
            .unwrap_or_else(|err| panic!("OTAP batch store rejected {payload_type:?}: {err}"));
    }
    OtapArrowRecords::Metrics(store)
}

fn kv_str(key: &str, val: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(val.to_owned())),
        }),
    }
}

fn number_dp(attrs: &[(&str, &str)], time_unix_nano: u64, val: f64) -> NumberDataPoint {
    NumberDataPoint {
        attributes: attrs.iter().map(|&(k, v)| kv_str(k, v)).collect(),
        start_time_unix_nano: 0,
        time_unix_nano,
        exemplars: Vec::new(),
        flags: 0,
        value: Some(number_data_point::Value::AsDouble(val)),
    }
}

#[test]
fn metric_payload_type_discriminants_match_otap_proto() {
    let cases = [
        (
            MetricPayloadType::UnivariateMetrics,
            ArrowPayloadType::UnivariateMetrics,
        ),
        (
            MetricPayloadType::NumberDataPoints,
            ArrowPayloadType::NumberDataPoints,
        ),
        (
            MetricPayloadType::NumberDpAttrs,
            ArrowPayloadType::NumberDpAttrs,
        ),
        (
            MetricPayloadType::MetricAttrs,
            ArrowPayloadType::MetricAttrs,
        ),
    ];
    for (local, upstream) in cases {
        assert_eq!(local.as_i32(), upstream as i32);
        assert_eq!(ArrowPayloadType::try_from(local.as_i32()), Ok(upstream));
    }
}

#[test]
fn adaptive_schema_variants_match_upstream_otap_schema_definitions() {
    let mut decoder = ArrowDecoder::new();

    // Minimal schema: no description, no counter columns, no dp id, no timestamps.
    let minimal = decoder
        .decode(b"bare_gauge 1\n", &Format::TEXT, &DecodeOptions::new())
        .expect("minimal payload decodes");
    assert_matches_otap_schema(&minimal);

    // Mixed explicit and omitted timestamps (nullable `time_unix_nano` when fallback is `None`).
    let mixed_ts = decoder
        .decode(
            b"m_ts{a=\"1\"} 1 1700000000000\nm_ts{a=\"2\"} 2\n",
            &Format::TEXT,
            &DecodeOptions::new(),
        )
        .expect("mixed timestamp payload decodes");
    assert_matches_otap_schema(&mixed_ts);

    // >256 distinct label keys upgrades `NumberDpAttrs.key` from `Dict(UInt8)` to `Dict(UInt16)`.
    let mut wide_keys = String::from("# TYPE wide_total counter\n");
    for i in 0..260 {
        wide_keys.push_str(&format!("wide_total{{k_{i}=\"v\"}} {i}\n"));
    }
    let wide = decoder
        .decode(wide_keys.as_bytes(), &Format::TEXT, &DecodeOptions::new())
        .expect("wide key payload decodes");
    assert_matches_otap_schema(&wide);
}

#[test]
fn counter_gauge_and_untyped_round_trip_through_otap_and_otlp() {
    let payload = concat!(
        "# HELP http_requests_total Total HTTP requests.\\nLine two\\\\end.\n",
        "# TYPE http_requests_total counter\n",
        "http_requests_total{status=\"200\",method=\"GET\"} 42 1700000000123\n",
        "http_requests_total{method=\"POST\",status=\"500\",empty=\"\"} 3\n",
        "http_requests_total{method=\"DELETE\",status=\"204\"} 1 1700000000999\n",
        "# HELP memory_bytes Resident memory in bytes.\n",
        "# TYPE memory_bytes gauge\n",
        "memory_bytes{empty=\"\"} 1048576\n",
        "# HELP bare_empty_help\n",
        "# HELP bare_empty_help \n",
        "# TYPE explicit_untyped untyped\n",
        "explicit_untyped{env=\"prod\"} -12.5 0\n",
        "implicit_untyped +Inf\n",
        "# HELP \"utf8.metric\\\"name\\\\\\n\" Quoted UTF-8 metric with escapes.\n",
        "# TYPE \"utf8.metric\\\"name\\\\\\n\" counter\n",
        "{ \"utf8.metric\\\"name\\\\\\n\", \"k.1©\\\"\"=\"v\\\"\\\\\\n1\" } 7.5\n",
        // Metadata-only family with zero samples must be omitted.
        "# HELP zero_samples_total Unused family.\n",
        "# TYPE zero_samples_total counter\n",
    );

    let mut decoder = ArrowDecoder::new();
    let opts = DecodeOptions::new().with_timestamp_ms(1_700_000_000_000);
    let decoded = decoder
        .decode(payload.as_bytes(), &Format::TEXT, &opts)
        .expect("decode succeeds");

    assert_eq!(decoded.num_metrics(), 5);
    assert_eq!(decoded.num_data_points(), 7);
    assert!(decoded.get(MetricPayloadType::UnivariateMetrics).is_some());
    assert!(decoded.get(MetricPayloadType::MetricAttrs).is_some());
    assert!(decoded.get(MetricPayloadType::NumberDataPoints).is_some());
    assert!(decoded.get(MetricPayloadType::NumberDpAttrs).is_some());

    let mut otap = to_otap_arrow_records(&decoded);
    let plain_otlp = otap_to_otlp(&otap);

    // Verify transport-optimized delta and quasi-delta ID encoding and decoding round-trip.
    otap.encode_transport_optimized()
        .expect("transport optimize succeeds");
    otap.decode_transport_optimized_ids()
        .expect("transport decode IDs succeeds");
    let round_tripped_otlp = otap_to_otlp(&otap);

    let default_ns = 1_700_000_000_000_000_000_u64;
    let expected = OtlpProtoMessage::Metrics(to_metrics_data(vec![
        Metric {
            name: "http_requests_total".to_owned(),
            description: "Total HTTP requests.\nLine two\\end.".to_owned(),
            unit: String::new(),
            metadata: vec![kv_str(PROMETHEUS_TYPE_METADATA_KEY, "counter")],
            data: Some(metric::Data::Sum(Sum {
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
                data_points: vec![
                    number_dp(
                        &[("method", "GET"), ("status", "200")],
                        1_700_000_000_123_000_000,
                        42.0,
                    ),
                    number_dp(&[("method", "POST"), ("status", "500")], default_ns, 3.0),
                    number_dp(
                        &[("method", "DELETE"), ("status", "204")],
                        1_700_000_000_999_000_000,
                        1.0,
                    ),
                ],
            })),
        },
        Metric {
            name: "memory_bytes".to_owned(),
            description: "Resident memory in bytes.".to_owned(),
            unit: String::new(),
            metadata: vec![kv_str(PROMETHEUS_TYPE_METADATA_KEY, "gauge")],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_dp(&[], default_ns, 1_048_576.0)],
            })),
        },
        Metric {
            name: "explicit_untyped".to_owned(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![kv_str(PROMETHEUS_TYPE_METADATA_KEY, "unknown")],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_dp(&[("env", "prod")], 0, -12.5)],
            })),
        },
        Metric {
            name: "implicit_untyped".to_owned(),
            description: String::new(),
            unit: String::new(),
            metadata: vec![kv_str(PROMETHEUS_TYPE_METADATA_KEY, "unknown")],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_dp(&[], default_ns, f64::INFINITY)],
            })),
        },
        Metric {
            name: "utf8.metric\"name\\\n".to_owned(),
            description: "Quoted UTF-8 metric with escapes.".to_owned(),
            unit: String::new(),
            metadata: vec![kv_str(PROMETHEUS_TYPE_METADATA_KEY, "counter")],
            data: Some(metric::Data::Sum(Sum {
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
                data_points: vec![number_dp(&[("k.1©\"", "v\"\\\n1")], default_ns, 7.5)],
            })),
        },
    ]));

    assert_equivalent(std::slice::from_ref(&expected), &[plain_otlp]);
    assert_equivalent(&[expected], &[round_tripped_otlp]);
}

#[test]
fn special_floats_and_label_free_batches_validate_in_otap() {
    let payload = "g_inf +Inf\ng_neginf -Inf\ng_nan NaN\n";
    let mut decoder = ArrowDecoder::new();
    let decoded = decoder
        .decode(payload.as_bytes(), &Format::TEXT, &DecodeOptions::default())
        .expect("decode succeeds");

    assert_eq!(decoded.num_metrics(), 3);
    assert_eq!(decoded.num_data_points(), 3);
    // Label-free scrapes omit `NumberDpAttrs` rather than emitting an empty 0-row batch.
    assert!(decoded.number_dp_attrs().is_none());

    let mut otap = to_otap_arrow_records(&decoded);
    otap.encode_transport_optimized()
        .expect("transport optimize succeeds without NumberDpAttrs");
    otap.decode_transport_optimized_ids()
        .expect("transport decode succeeds");

    let OtlpProtoMessage::Metrics(metrics_data) = otap_to_otlp(&otap) else {
        panic!("expected OTLP MetricsData");
    };
    let metrics = &metrics_data.resource_metrics[0].scope_metrics[0].metrics;
    assert_eq!(metrics.len(), 3);
    // `encode_transport_optimized` reorders rows by metric name, so inspect values by name.
    for m in metrics {
        let Some(metric::Data::Gauge(g)) = &m.data else {
            panic!("expected Gauge for {}", m.name);
        };
        let Some(number_data_point::Value::AsDouble(v)) = g.data_points[0].value else {
            panic!("expected double value for {}", m.name);
        };
        match m.name.as_str() {
            "g_inf" => assert_eq!(v, f64::INFINITY),
            "g_neginf" => assert_eq!(v, f64::NEG_INFINITY),
            "g_nan" => assert!(v.is_nan()),
            other => panic!("unexpected metric {other}"),
        }
    }
}

/// Filters `alltypes.237mfs.prom.txt` down to its counter, gauge, and untyped metric families
/// until `#8` adds classic histogram and summary assembly.
fn filter_counter_gauge_families(raw: &str) -> Vec<u8> {
    let mut out = String::with_capacity(raw.len());
    let mut skip_family: Option<String> = None;

    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let mut parts = rest.split_whitespace();
            let name = parts.next().unwrap_or_default();
            let kind = parts.next().unwrap_or_default();
            if matches!(kind, "histogram" | "summary") {
                skip_family = Some(name.to_owned());
                continue;
            }
            skip_family = None;
        } else if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = rest.split_whitespace().next().unwrap_or_default();
            let type_decl_hist = raw.lines().any(|l| {
                l == format!("# TYPE {name} histogram") || l == format!("# TYPE {name} summary")
            });
            if type_decl_hist {
                skip_family = Some(name.to_owned());
                continue;
            }
            skip_family = None;
        } else if let Some(skipped) = &skip_family {
            let sample_name = line.split(['{', ' ', '\t']).next().unwrap_or_default();
            if sample_name == skipped
                || sample_name == format!("{skipped}_bucket")
                || sample_name == format!("{skipped}_sum")
                || sample_name == format!("{skipped}_count")
            {
                continue;
            }
            skip_family = None;
        }
        out.push_str(line);
        out.push('\n');
    }
    out.into_bytes()
}

#[test]
fn decodes_upstream_237mfs_fixtures_into_valid_otap() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/textparse");
    let nometa = fs::read(root.join("alltypes.237mfs.nometa.prom.txt")).unwrap();
    let full = fs::read_to_string(root.join("alltypes.237mfs.prom.txt")).unwrap();
    let typed_simple = filter_counter_gauge_families(&full);

    let mut decoder = ArrowDecoder::new();
    let opts = DecodeOptions::new().with_timestamp_ms(0);

    // 1. Full unmodified `alltypes.237mfs.nometa.prom.txt` (1,857 untyped samples plus `# EOF`).
    let nometa_records = decoder
        .decode(&nometa, &Format::TEXT, &opts)
        .expect("nometa fixture decodes");
    assert_eq!(nometa_records.num_data_points(), 1857);

    let mut nometa_otap = to_otap_arrow_records(&nometa_records);
    nometa_otap
        .encode_transport_optimized()
        .expect("nometa transport optimize succeeds");
    nometa_otap
        .decode_transport_optimized_ids()
        .expect("nometa transport decode IDs succeeds");
    let _ = otap_to_otlp(&nometa_otap);

    // 2. All 209 counter and gauge families from `alltypes.237mfs.prom.txt`.
    let typed_records = decoder
        .decode(&typed_simple, &Format::TEXT, &opts)
        .expect("typed counter+gauge fixture decodes");
    assert_eq!(typed_records.num_metrics(), 209);

    let mut typed_otap = to_otap_arrow_records(&typed_records);
    typed_otap
        .encode_transport_optimized()
        .expect("typed transport optimize succeeds");
    typed_otap
        .decode_transport_optimized_ids()
        .expect("typed transport decode IDs succeeds");
    let _ = otap_to_otlp(&typed_otap);
}
