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

//! Throughput and allocation benchmarks for `expfmt::relabel` target discovery relabeling,
//! per-sample scalar metric relabeling, and two-stage dictionary-vectorized metric relabeling.
//!
//! Recommended CLI invocation:
//! ```sh
//! cargo bench --bench relabel -- -bench '^BenchmarkRelabel' -benchtime 1s -count 6 -cpu 2 -benchmem | tee ${bench}.txt
//! ```

use std::collections::BTreeMap;
use std::hint::black_box;

use benchfmt::{CountingAlloc, Suite};
use expfmt::relabel::{
    Action, Config, DictionaryValueTransform, LabelSet, METRIC_NAME_LABEL, Regexp,
    TargetPopulateConfig, ValidationScheme, eval_key_keep_mask, eval_single_label_keep_mask,
    eval_single_label_value_transform, keep_metric_name, merge_target_and_metric_labels,
    populate_discovered_labels, populate_labels, process_in_place,
    resolve_conflicting_exposed_keys, split_metric_name_pushdown,
};

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new();

const K8S_RULES_JSON: &str = r#"[
    {
        "source_labels": ["__meta_kubernetes_pod_container_port_name"],
        "regex": ".*-metrics",
        "action": "keep"
    },
    {
        "source_labels": ["__meta_kubernetes_pod_label_name"],
        "action": "drop",
        "regex": ""
    },
    {
        "source_labels": ["__meta_kubernetes_pod_phase"],
        "regex": "Succeeded|Failed",
        "action": "drop"
    },
    {
        "source_labels": ["__meta_kubernetes_pod_annotation_prometheus_io_scrape"],
        "regex": "false",
        "action": "drop"
    },
    {
        "source_labels": ["__meta_kubernetes_pod_annotation_prometheus_io_scheme"],
        "target_label": "__scheme__",
        "regex": "(https?)",
        "replacement": "$1",
        "action": "replace"
    },
    {
        "source_labels": ["__meta_kubernetes_pod_annotation_prometheus_io_path"],
        "target_label": "__metrics_path__",
        "regex": "(.+)",
        "replacement": "$1",
        "action": "replace"
    },
    {
        "source_labels": ["__address__", "__meta_kubernetes_pod_annotation_prometheus_io_port"],
        "target_label": "__address__",
        "regex": "(.+?)(\\:\\d+)?;(\\d+)",
        "replacement": "$1:$3",
        "action": "replace"
    },
    {
        "regex": "__meta_kubernetes_pod_annotation_prometheus_io_param_(.+)",
        "replacement": "__param_$1",
        "action": "labelmap"
    },
    {
        "regex": "__meta_kubernetes_pod_label_prometheus_io_label_(.+)",
        "action": "labelmap"
    },
    {
        "regex": "__meta_kubernetes_pod_annotation_prometheus_io_label_(.+)",
        "action": "labelmap"
    },
    {
        "source_labels": ["__meta_kubernetes_namespace", "__meta_kubernetes_pod_label_name"],
        "separator": "/",
        "target_label": "job",
        "replacement": "$1",
        "action": "replace"
    },
    {
        "source_labels": ["__meta_kubernetes_namespace"],
        "target_label": "namespace",
        "action": "replace"
    },
    {
        "source_labels": ["__meta_kubernetes_pod_name"],
        "target_label": "pod",
        "action": "replace"
    },
    {
        "source_labels": ["__meta_kubernetes_pod_container_name"],
        "target_label": "container",
        "action": "replace"
    },
    {
        "source_labels": [
            "__meta_kubernetes_pod_name",
            "__meta_kubernetes_pod_container_name",
            "__meta_kubernetes_pod_container_port_name"
        ],
        "separator": ":",
        "target_label": "instance",
        "action": "replace"
    },
    {
        "target_label": "cluster",
        "replacement": "dev-us-central-0"
    },
    {
        "source_labels": ["__meta_kubernetes_namespace"],
        "regex": "hosted-grafana",
        "action": "drop"
    },
    {
        "source_labels": ["__address__"],
        "target_label": "__tmp_hash",
        "modulus": 3,
        "action": "hashmod"
    },
    {
        "source_labels": ["__tmp_hash"],
        "regex": "^1$",
        "action": "keep"
    },
    {
        "regex": "__tmp_hash",
        "action": "labeldrop"
    }
]"#;

/// Compact interned representation of a scraped series for benchmarking Stage 1 + Stage 2
/// dictionary-vectorized relabeling against per-sample `LabelSet` relabeling.
#[derive(Clone)]
struct InternedSeries {
    /// Pair of `(key_id, val_id)` attributes on this series.
    attrs: Vec<(usize, usize)>,
    /// Index of the `"env"` label value in `val_dict` (`None` if omitted).
    env_val_id: Option<usize>,
    /// Index of the `"pod"` label value in `val_dict` (`None` if omitted).
    pod_val_id: Option<usize>,
}

struct ScrapeFixture {
    target_labels: LabelSet,
    metric_relabel_configs: Vec<Config>,
    family_names: Vec<String>,
    /// `200` families x `10` series per family = `2,000` scalar `LabelSet`s.
    scalar_families: Vec<Vec<LabelSet>>,
    /// Deduplicated key dictionary (`32` unique keys).
    key_dict: Vec<String>,
    /// Deduplicated value dictionary (`128` unique values).
    val_dict: Vec<String>,
    /// `200` families x `10` interned series per family.
    interned_families: Vec<Vec<InternedSeries>>,
}

fn build_scrape_fixture() -> ScrapeFixture {
    let target_labels = LabelSet::from([
        ("__address__".to_owned(), "10.0.0.1:9090".to_owned()),
        ("job".to_owned(), "api-server".to_owned()),
        ("instance".to_owned(), "10.0.0.1:9090".to_owned()),
        ("cluster".to_owned(), "prod-eu-west-1".to_owned()),
    ]);

    let mut metric_relabel_configs = vec![
        Config {
            source_labels: vec![METRIC_NAME_LABEL.to_owned()],
            regex: Regexp::must_new("(http|rpc|process)_.*"),
            action: Action::Keep,
            ..Config::default()
        },
        Config {
            regex: Regexp::must_new("ephemeral_.*"),
            action: Action::LabelDrop,
            ..Config::default()
        },
        Config {
            source_labels: vec!["env".to_owned()],
            regex: Regexp::must_new("prod|staging"),
            action: Action::Keep,
            ..Config::default()
        },
        Config {
            source_labels: vec!["pod".to_owned()],
            regex: Regexp::must_new("([a-z0-9-]+)-[0-9a-f]+"),
            target_label: "app".to_owned(),
            replacement: "$1".to_owned(),
            action: Action::Replace,
            ..Config::default()
        },
    ];
    for cfg in &mut metric_relabel_configs {
        cfg.validate(ValidationScheme::Utf8).unwrap();
    }

    // 32 unique label keys in the scrape batch.
    let mut key_dict = vec![
        "env".to_owned(),
        "pod".to_owned(),
        "method".to_owned(),
        "status".to_owned(),
        "ephemeral_debug".to_owned(),
        "ephemeral_trace".to_owned(),
        "app".to_owned(),
    ];
    while key_dict.len() < 32 {
        key_dict.push(format!("dim_key_{}", key_dict.len()));
    }

    // 128 unique label values in the scrape batch.
    let envs = ["prod", "staging", "dev"];
    let methods = ["GET", "POST", "PUT", "DELETE"];
    let statuses = ["200", "201", "400", "404", "500"];
    let mut val_dict = Vec::with_capacity(128);
    for e in envs {
        val_dict.push(e.to_owned());
    }
    for m in methods {
        val_dict.push(m.to_owned());
    }
    for s in statuses {
        val_dict.push(s.to_owned());
    }
    for i in 0..64 {
        val_dict.push(format!("checkout-service-{i:02}-{:04x}", 0x7f00 + i));
    }
    while val_dict.len() < 128 {
        val_dict.push(format!("val_{}", val_dict.len()));
    }

    let intern_val = |s: &str, dict: &[String]| -> usize {
        dict.iter()
            .position(|v| v == s)
            .expect("value present in dictionary")
    };

    let prefixes = ["http", "go", "rpc", "node"];
    let mut family_names = Vec::with_capacity(200);
    let mut scalar_families = Vec::with_capacity(200);
    let mut interned_families = Vec::with_capacity(200);

    for fam_idx in 0..200 {
        let prefix = prefixes[fam_idx % prefixes.len()];
        let family_name = format!("{prefix}_requests_family_{fam_idx}_total");
        family_names.push(family_name.clone());

        let mut scalar_series = Vec::with_capacity(10);
        let mut interned_series = Vec::with_capacity(10);

        for s_idx in 0..10 {
            let env_str = envs[(fam_idx + s_idx) % envs.len()];
            let pod_str = &val_dict[12 + ((fam_idx * 10 + s_idx) % 64)];
            let method_str = methods[s_idx % methods.len()];
            let status_str = statuses[(fam_idx + s_idx) % statuses.len()];
            let eph_str = &val_dict[76 + ((fam_idx + s_idx) % 40)];

            let mut ls = LabelSet::new();
            ls.insert(METRIC_NAME_LABEL.to_owned(), family_name.clone());
            ls.insert("env".to_owned(), env_str.to_owned());
            ls.insert("pod".to_owned(), pod_str.clone());
            ls.insert("method".to_owned(), method_str.to_owned());
            ls.insert("status".to_owned(), status_str.to_owned());
            ls.insert("ephemeral_debug".to_owned(), eph_str.clone());
            scalar_series.push(ls);

            let env_val_id = intern_val(env_str, &val_dict);
            let pod_val_id = intern_val(pod_str, &val_dict);
            let method_val_id = intern_val(method_str, &val_dict);
            let status_val_id = intern_val(status_str, &val_dict);
            let eph_val_id = intern_val(eph_str, &val_dict);

            interned_series.push(InternedSeries {
                attrs: vec![
                    (0, env_val_id),
                    (1, pod_val_id),
                    (2, method_val_id),
                    (3, status_val_id),
                    (4, eph_val_id),
                ],
                env_val_id: Some(env_val_id),
                pod_val_id: Some(pod_val_id),
            });
        }

        scalar_families.push(scalar_series);
        interned_families.push(interned_series);
    }

    ScrapeFixture {
        target_labels,
        metric_relabel_configs,
        family_names,
        scalar_families,
        key_dict,
        val_dict,
        interned_families,
    }
}

fn main() {
    let mut k8s_cfgs: Vec<Config> = serde_json::from_str(K8S_RULES_JSON).unwrap();
    for cfg in &mut k8s_cfgs {
        cfg.validate(ValidationScheme::Utf8).unwrap();
    }
    let k8s_target = LabelSet::from([
        ("__address__".to_owned(), "10.132.183.40:80".to_owned()),
        (
            "__meta_kubernetes_namespace".to_owned(),
            "loki-boltdb-shipper".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_container_init".to_owned(),
            "false".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_container_name".to_owned(),
            "promtail".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_container_port_name".to_owned(),
            "http-metrics".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_container_port_number".to_owned(),
            "80".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_container_port_protocol".to_owned(),
            "TCP".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_label_name".to_owned(),
            "promtail-loki-boltdb-shipper".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_name".to_owned(),
            "promtail-loki-boltdb-shipper-jgtr7".to_owned(),
        ),
        (
            "__meta_kubernetes_pod_phase".to_owned(),
            "Running".to_owned(),
        ),
        ("__meta_kubernetes_pod_ready".to_owned(), "true".to_owned()),
    ]);
    let k8s_pop_cfg = TargetPopulateConfig {
        job_name: "kubernetes-pods".to_owned(),
        scrape_interval: "15s".to_owned(),
        scrape_timeout: "10s".to_owned(),
        metrics_path: "/metrics".to_owned(),
        scheme: "http".to_owned(),
        params: BTreeMap::new(),
    };
    let k8s_discovered = populate_discovered_labels(&k8s_target, &BTreeMap::new(), &k8s_pop_cfg);

    let fixture = build_scrape_fixture();

    let mut suite = Suite::from_env("expfmt")
        .with_alloc(&ALLOC)
        .bench("Relabel/case=kubernetes_target/mode=populate_labels", |b| {
            let empty_group = BTreeMap::new();
            b.report_allocs();
            while b.keep_running() {
                let res = populate_labels(
                    black_box(&k8s_target),
                    black_box(&empty_group),
                    black_box(&k8s_pop_cfg),
                    black_box(&k8s_cfgs),
                )
                .unwrap();
                black_box(res);
            }
        })
        .bench(
            "Relabel/case=kubernetes_target/mode=process_in_place",
            |b| {
                let mut labels = k8s_discovered.clone();
                b.report_allocs();
                while b.keep_running() {
                    labels.clone_from(black_box(&k8s_discovered));
                    let kept = process_in_place(&mut labels, black_box(&k8s_cfgs));
                    black_box(kept);
                }
            },
        )
        .bench(
            "Relabel/case=scrape_2000_series/mode=scalar_per_sample",
            |b| {
                let mut work = LabelSet::new();
                b.report_allocs();
                while b.keep_running() {
                    let mut kept_series = 0usize;
                    for family in black_box(&fixture.scalar_families) {
                        for sample in family {
                            work.clone_from(sample);
                            merge_target_and_metric_labels(
                                &mut work,
                                black_box(&fixture.target_labels),
                                false,
                            );
                            if process_in_place(
                                &mut work,
                                black_box(&fixture.metric_relabel_configs),
                            ) {
                                kept_series += 1;
                            }
                        }
                    }
                    black_box(kept_series);
                }
            },
        )
        .bench(
            "Relabel/case=scrape_2000_series/mode=two_stage_dictionary",
            |b| {
                let mut renamed_keys = Vec::new();
                let mut key_keep_mask = Vec::new();
                let mut val_keep_mask = Vec::new();
                let mut val_transforms = Vec::new();

                // Warm scratch capacities once to measure steady-state scrape reuse.
                let (_stage1, stage2) = split_metric_name_pushdown(&fixture.metric_relabel_configs);
                resolve_conflicting_exposed_keys(
                    &fixture.key_dict,
                    &fixture.target_labels,
                    false,
                    &mut renamed_keys,
                );
                eval_key_keep_mask(&fixture.key_dict, &stage2[0], &mut key_keep_mask);
                eval_single_label_keep_mask(&fixture.val_dict, &stage2[1], &mut val_keep_mask);
                eval_single_label_value_transform(
                    &fixture.val_dict,
                    &stage2[2],
                    &mut val_transforms,
                );

                b.report_allocs();
                while b.keep_running() {
                    let (stage1, stage2) =
                        split_metric_name_pushdown(black_box(&fixture.metric_relabel_configs));

                    // Stage 2 dictionary setup across unique keys (32) and unique values (128).
                    resolve_conflicting_exposed_keys(
                        black_box(&fixture.key_dict),
                        black_box(&fixture.target_labels),
                        false,
                        &mut renamed_keys,
                    );
                    eval_key_keep_mask(
                        black_box(&fixture.key_dict),
                        &stage2[0],
                        &mut key_keep_mask,
                    )
                    .unwrap();
                    let env_filter = eval_single_label_keep_mask(
                        black_box(&fixture.val_dict),
                        &stage2[1],
                        &mut val_keep_mask,
                    )
                    .unwrap();
                    let pod_absent = eval_single_label_value_transform(
                        black_box(&fixture.val_dict),
                        &stage2[2],
                        &mut val_transforms,
                    )
                    .unwrap();

                    let mut kept_series = 0usize;
                    let mut kept_attrs = 0usize;
                    for (fam_idx, family_name) in
                        black_box(&fixture.family_names).iter().enumerate()
                    {
                        // Stage 1: evaluate `__name__` filter once per contiguous family transition.
                        if !keep_metric_name(family_name, stage1) {
                            continue;
                        }
                        for series in &fixture.interned_families[fam_idx] {
                            let env_kept = match series.env_val_id {
                                Some(vid) => val_keep_mask[vid],
                                None => env_filter.keep_when_absent,
                            };
                            if !env_kept {
                                continue;
                            }
                            kept_series += 1;
                            for &(kid, _vid) in &series.attrs {
                                if key_keep_mask[kid] {
                                    kept_attrs += 1;
                                }
                            }
                            let xform = match series.pod_val_id {
                                Some(vid) => &val_transforms[vid],
                                None => &pod_absent,
                            };
                            if matches!(xform, DictionaryValueTransform::SetTarget(_)) {
                                kept_attrs += 1;
                            }
                        }
                    }
                    black_box((kept_series, kept_attrs));
                }
            },
        );

    suite.run();
}
