# scrape.rs

> **Status: Experimental (Work in Progress)**

Official Rust libraries for Prometheus and OpenMetrics metric collection pipelines: wire exposition format decoding (`expfmt`), target and metric relabeling, standard service discoveries, and scrape management.

## Overview

`scrape.rs` provides the building blocks required to discover targets, relabel labels, scrape HTTP endpoints, and decode Prometheus and OpenMetrics payloads in Rust:

```text
Service discoveries
  -> Target relabeling
  -> Scrape manager / HTTP fetch
  -> Streaming expfmt parser
  -> Stage 1: family `__name__` filter pushdown
  -> Metric family assembly & string interning
  -> Stage 2: dictionary-vectorized relabeling & `honor_labels`
  -> Columnar Apache Arrow (OTAP) batches or Protobuf DTOs
```

The project serves two main use cases:

1. Columnar scraping for OpenTelemetry Arrow. [`open-telemetry/otel-arrow#3277`](https://github.com/open-telemetry/otel-arrow/issues/3277) defines an OTAP-native `prometheus_scrape` receiver in Rust. `scrape.rs` streams scrape bytes through early `__name__` family filtering, metric family assembly, and dictionary-vectorized relabeling directly into Apache Arrow (`OtapArrowRecords`) tables on thread-per-core `!Send` runtimes with bounded memory.
2. Standalone Rust Prometheus tooling. Validation harnesses such as OpenTelemetry Weaver live-checking `/metrics` endpoints against semantic conventions, CLIs, and custom collectors can use individual components (such as `expfmt` DTO decoding or `relabel`) without pulling in the full scrape runtime.

## Components and scope

| Component | Status | Specification | Scope |
| :--- | :--- | :--- | :--- |
| `expfmt` | In progress | [specs/SPEC.md](./specs/SPEC.md) | `Content-Type` and `Accept` negotiation, zero-copy streaming parsers for Prometheus Text 0.0.4, OpenMetrics 1.0, OpenMetrics 2.0, and Prometheus Protobuf, primary Apache Arrow / OTAP decoder (`ArrowDecoder`), and [`MetricFamily`](https://github.com/prometheus/prometheus/blob/main/prompb/io/prometheus/client/metrics.proto) protobuf DTO decoder. |
| `relabel` | In progress | [specs/SPEC.md](./specs/SPEC.md#target-and-metric-relabeling-architecture) | Prometheus-compatible target and metric relabeling (`replace`, `keep`, `drop`, `hashmod`, `labelmap`, `labeldrop`, `labelkeep`, `lowercase`, `uppercase`, `keepequal`, `dropequal`), Stage 1 `__name__` filter pushdown, and Stage 2 dictionary-vectorized relabeling and `honor_labels` resolution. |
| `discovery` | Planned | TBD | Standard Prometheus service discovery mechanisms (static targets, file-based SD, HTTP SD, DNS SD, Kubernetes SD) emitting target groups and `__meta_*` labels. |
| `scrape` | Planned | TBD | Bounded HTTP scrape scheduling, target pool lifecycle, compression and content negotiation, stale marker handling, target state reporting, and self-telemetry. |

## Development and testing

Run the unit and fixture test suite:

```sh
cargo test --all-features
```

Run the [`otel-arrow-dfe-pdata`](https://github.com/open-telemetry/otel-arrow/tree/main/rust/otap-dataflow/crates/pdata) compatibility test suite:

```sh
cargo test --manifest-path tests/otap-compat/Cargo.toml
```

Run the Go differential fixture extractor and tests (requires Go):

```sh
cd tests/go-harness && go test ./...
```

Run the decoding and relabeling benchmarks in Go `benchfmt` format for [`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
cargo bench --bench decode -- -bench '^BenchmarkDecode' -benchtime 1s -count 6 -cpu 2 -benchmem | tee decode.txt
cargo bench --bench relabel -- -bench '^BenchmarkRelabel' -benchtime 1s -count 6 -cpu 2 -benchmem | tee relabel.txt
```
