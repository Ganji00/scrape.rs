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

//! Round-trip tests against `testdata/roundtrip/`. The Go harness encodes canonical `MetricFamily`
//! inputs with `prometheus/common/expfmt` for Prometheus text 0.0.4 and OpenMetrics 1.0 across all
//! escaping schemes, and writes the families a decoder must return next to each payload.

#![cfg(all(feature = "arrow", feature = "dto"))]

use std::fs;
use std::path::{Path, PathBuf};

use prometheus_expfmt::arrow::{ArrowDecoder, DecodeOptions};
use prometheus_expfmt::dto::{MetricFamily, arrow_to_metric_families, decode_to_metric_families};
use prometheus_expfmt::format::{EscapingScheme, Format, FormatType};
use prost::Message;

const SCHEMES: [EscapingScheme; 4] = [
    EscapingScheme::AllowUtf8,
    EscapingScheme::EscapeUnderscores,
    EscapingScheme::EscapeDots,
    EscapingScheme::EscapeValues,
];

fn roundtrip_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/roundtrip")
}

/// Returns the fixture name prefix and `Format` of every case the Go harness writes.
fn cases() -> Vec<(String, Format)> {
    let text = Format::from_type(FormatType::TextPlain);
    let om = Format::new_open_metrics("1.0.0").expect("OpenMetrics 1.0.0 format should be valid");
    let mut cases = Vec::new();
    for (name, format) in [("text-0.0.4", text), ("openmetrics-1.0.0", om)] {
        for scheme in SCHEMES {
            cases.push((
                format!("{name}.{scheme}"),
                format.with_escaping_scheme(scheme),
            ));
        }
    }
    cases
}

fn read_payload(case: &str) -> Vec<u8> {
    let path = roundtrip_dir().join(format!("{case}.txt"));
    fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn read_expected(case: &str) -> Vec<MetricFamily> {
    let path = roundtrip_dir().join(format!("{case}.pb"));
    let raw = fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut cursor: &[u8] = &raw;
    let mut families = Vec::new();
    while !cursor.is_empty() {
        families.push(
            MetricFamily::decode_length_delimited(&mut cursor)
                .unwrap_or_else(|e| panic!("{case}: decode expected families: {e}")),
        );
    }
    families
}

#[test]
fn decode_to_metric_families_round_trips_go_encoder_output() {
    for (case, format) in cases() {
        let got = decode_to_metric_families(&read_payload(&case), format, &DecodeOptions::new())
            .unwrap_or_else(|e| panic!("{case}: decode_to_metric_families failed: {e}"));
        assert_eq!(got, read_expected(&case), "{case}");
    }
}

#[test]
fn arrow_decoder_round_trips_go_encoder_output() {
    let opts = DecodeOptions::new();
    let mut decoder = ArrowDecoder::new();
    for (case, format) in cases() {
        let records = decoder
            .decode(&read_payload(&case), &format, &opts)
            .unwrap_or_else(|e| panic!("{case}: ArrowDecoder failed: {e}"));
        let got = arrow_to_metric_families(&records)
            .unwrap_or_else(|e| panic!("{case}: arrow_to_metric_families failed: {e}"));
        assert_eq!(got, read_expected(&case), "{case}");
    }
}
