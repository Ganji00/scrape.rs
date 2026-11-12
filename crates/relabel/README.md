# prometheus-relabel

Prometheus-compatible target and metric label relabeling (`relabel_configs`, `metric_relabel_configs`, and `honor_labels` key resolution) in Rust, ported from Prometheus [`model/relabel`](https://github.com/prometheus/prometheus/tree/main/model/relabel) and [`scrape/target.go`](https://github.com/prometheus/prometheus/blob/main/scrape/target.go).

Part of the [`prometheus/scrape.rs`](https://github.com/prometheus/scrape.rs) workspace:

- Pre-scrape target discovery label population (`populate_discovered_labels`, `populate_labels`) and all 11 Prometheus relabel actions (`replace`, `keep`, `drop`, `hashmod`, `labelmap`, `labeldrop`, `labelkeep`, `lowercase`, `uppercase`, `keepequal`, `dropequal`).
- Stage 1 series `__name__` filter pushdown (`split_metric_name_pushdown`, `keep_metric_name`).
- Stage 2 dictionary-vectorized attribute relabeling and `honor_labels` conflict resolution for columnar metric pipelines.
