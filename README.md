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
  -> Metric relabeling / drop hook
  -> Columnar Apache Arrow (OTAP) batches or Protobuf DTOs
```

The project serves two main use cases:

1. Columnar scraping for OpenTelemetry Arrow. [`open-telemetry/otel-arrow#3277`](https://github.com/open-telemetry/otel-arrow/issues/3277) defines an OTAP-native `prometheus_scrape` receiver in Rust. `scrape.rs` streams scrape bytes through pre-allocation relabeling and metric family assembly directly into Apache Arrow (`OtapArrowRecords`) tables on thread-per-core `!Send` runtimes with bounded memory.
2. Standalone Rust Prometheus tooling. Validation harnesses such as OpenTelemetry Weaver live-checking `/metrics` endpoints against semantic conventions, CLIs, and custom collectors can use individual components (such as `expfmt` DTO decoding or `relabel`) without pulling in the full scrape runtime.

## Components and scope

| Component | Status | Specification | Scope |
| :--- | :--- | :--- | :--- |
| `expfmt` | In progress | [specs/expfmt/SPEC.md](./specs/expfmt/SPEC.md) | `Content-Type` and `Accept` negotiation, zero-copy streaming parsers for Prometheus Text 0.0.4, OpenMetrics 1.0, OpenMetrics 2.0, and Prometheus Protobuf, primary Apache Arrow / OTAP decoder (`ArrowDecoder`), and [`MetricFamily`](https://github.com/prometheus/prometheus/blob/main/prompb/io/prometheus/client/metrics.proto) protobuf DTO decoder. |
| `relabel` | Planned | TBD | Prometheus-compatible target and metric relabeling (`replace`, `keep`, `drop`, `hashmod`, `labelmap`, `labeldrop`, `labelkeep`, `lowercase`, `uppercase`, `keepequal`, `dropequal`), including pre-allocation filtering on borrowed parser label buffers. |
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

Run the Criterion decoding benchmarks:

```sh
cargo bench --bench decode
```
