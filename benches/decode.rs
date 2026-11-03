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

//! Throughput benchmarks for `expfmt::arrow::ArrowDecoder` over the upstream Prometheus
//! `model/textparse` fixture corpus.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench --bench decode
//! ```

use std::fs;
use std::hint::black_box;
use std::path::Path;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use expfmt::arrow::{ArrowDecoder, DecodeOptions};
use expfmt::format::Format;

/// Filters `alltypes.237mfs.prom.txt` down to its 209 counter, gauge, and untyped metric families
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

fn bench_arrow_decode(c: &mut Criterion) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/textparse");
    let nometa = fs::read(root.join("alltypes.237mfs.nometa.prom.txt")).unwrap();
    let full = fs::read_to_string(root.join("alltypes.237mfs.prom.txt")).unwrap();
    let typed_simple = filter_counter_gauge_families(&full);

    let opts = DecodeOptions::new().with_timestamp_ms(0);
    let stream_256_opts = opts.with_max_data_points_per_batch(256);
    let mut group = c.benchmark_group("ParsePromText");

    group.throughput(Throughput::Bytes(nometa.len() as u64));
    group.bench_function("alltypes.237mfs.nometa", |b| {
        let mut decoder = ArrowDecoder::new();
        b.iter(|| {
            let records = decoder
                .decode(black_box(&nometa), &Format::TEXT, &opts)
                .unwrap();
            black_box(records);
        });
    });
    group.bench_function("alltypes.237mfs.nometa.stream_16k", |b| {
        let mut decoder = ArrowDecoder::new();
        b.iter(|| {
            for chunk in decoder.decode_stream(black_box(&nometa), &Format::TEXT, &opts) {
                black_box(chunk.unwrap());
            }
        });
    });
    group.bench_function("alltypes.237mfs.nometa.stream_256", |b| {
        let mut decoder = ArrowDecoder::new();
        b.iter(|| {
            for chunk in decoder.decode_stream(black_box(&nometa), &Format::TEXT, &stream_256_opts)
            {
                black_box(chunk.unwrap());
            }
        });
    });

    group.throughput(Throughput::Bytes(typed_simple.len() as u64));
    group.bench_function("alltypes.209mfs.counter_gauge", |b| {
        let mut decoder = ArrowDecoder::new();
        b.iter(|| {
            let records = decoder
                .decode(black_box(&typed_simple), &Format::TEXT, &opts)
                .unwrap();
            black_box(records);
        });
    });
    group.bench_function("alltypes.209mfs.counter_gauge.stream_16k", |b| {
        let mut decoder = ArrowDecoder::new();
        b.iter(|| {
            for chunk in decoder.decode_stream(black_box(&typed_simple), &Format::TEXT, &opts) {
                black_box(chunk.unwrap());
            }
        });
    });
    group.bench_function("alltypes.209mfs.counter_gauge.stream_256", |b| {
        let mut decoder = ArrowDecoder::new();
        b.iter(|| {
            for chunk in
                decoder.decode_stream(black_box(&typed_simple), &Format::TEXT, &stream_256_opts)
            {
                black_box(chunk.unwrap());
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_arrow_decode);
criterion_main!(benches);
