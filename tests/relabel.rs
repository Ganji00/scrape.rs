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

//! Integration tests for Prometheus target and metric relabeling (`model/relabel` and
//! `scrape/target.go`).

#![cfg(feature = "relabel")]

use std::collections::BTreeMap;

use expfmt::relabel::{
    Action, Config, DEFAULT_REPLACEMENT, LabelSet, Regexp, TargetPopulateConfig, ValidationScheme,
    is_valid_legacy_target_label, merge_target_and_metric_labels, populate_discovered_labels,
    populate_labels, process,
};

fn label_set(pairs: &[(&str, &str)]) -> LabelSet {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

fn strs(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

#[test]
fn relabel_matches_prometheus_relabel_test_suite() {
    // Ported from TestRelabel in model/relabel/relabel_test.go.
    let base_input = label_set(&[("a", "foo"), ("b", "bar"), ("c", "baz")]);

    // 1. No relabelings.
    assert_eq!(process(&base_input, &[]), Some(base_input.clone()));

    // 2. No matching rule.
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a", "b", "c"]),
                regex: Regexp::must_new("bar"),
                target_label: "d".to_owned(),
                replacement: "foo".to_owned(),
                action: Action::Replace,
                ..Config::default()
            }]
        ),
        Some(base_input.clone())
    );

    // 3. Matching rule without target label replacement ($1 empty).
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a", "b", "c"]),
                regex: Regexp::must_new("foo;bar;baz"),
                target_label: "d".to_owned(),
                action: Action::Replace,
                ..Config::default()
            }]
        ),
        Some(base_input.clone())
    );

    // 4. Matching rule with capture groups and new target label.
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a"]),
                regex: Regexp::must_new("f(.*)"),
                target_label: "d".to_owned(),
                replacement: "ch${1}ch${1}".to_owned(),
                action: Action::Replace,
                ..Config::default()
            }]
        ),
        Some(label_set(&[
            ("a", "foo"),
            ("b", "bar"),
            ("c", "baz"),
            ("d", "choochoo"),
        ]))
    );

    // 5. Matching rules chaining into existing and subsequent source labels.
    assert_eq!(
        process(
            &base_input,
            &[
                Config {
                    source_labels: strs(&["a", "b"]),
                    regex: Regexp::must_new("f(.*);(.*)r"),
                    target_label: "a".to_owned(),
                    replacement: "b${1}${2}m".to_owned(),
                    action: Action::Replace,
                    ..Config::default()
                },
                Config {
                    source_labels: strs(&["c", "a"]),
                    regex: Regexp::must_new("(b).*b(.*)ba(.*)"),
                    target_label: "d".to_owned(),
                    replacement: "$1$2$2$3".to_owned(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ]
        ),
        Some(label_set(&[
            ("a", "boobam"),
            ("b", "bar"),
            ("c", "baz"),
            ("d", "boooom"),
        ]))
    );

    // 6. Replacement deleting an existing label when replacement expands to empty string.
    assert_eq!(
        process(
            &base_input,
            &[
                Config {
                    source_labels: strs(&["a"]),
                    regex: Regexp::must_new("foo"),
                    target_label: "a".to_owned(),
                    replacement: String::new(),
                    action: Action::Replace,
                    ..Config::default()
                },
                Config {
                    source_labels: strs(&["b"]),
                    regex: Regexp::must_new(".*"),
                    target_label: "b".to_owned(),
                    replacement: "$1".to_owned(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ]
        ),
        Some(label_set(&[("c", "baz")]))
    );

    // 7. Match with custom separator + Drop / Keep rules.
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a", "b"]),
                separator: ";".to_owned(),
                regex: Regexp::must_new("foo;bar"),
                action: Action::Drop,
                ..Config::default()
            }]
        ),
        None
    );
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a", "b"]),
                separator: ".*".to_owned(),
                regex: Regexp::must_new("foo;bar"),
                action: Action::Drop,
                ..Config::default()
            }]
        ),
        Some(base_input.clone())
    );
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a"]),
                regex: Regexp::must_new("no-match"),
                action: Action::Keep,
                ..Config::default()
            }]
        ),
        None
    );
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["a"]),
                regex: Regexp::must_new("f.*"),
                action: Action::Keep,
                ..Config::default()
            }]
        ),
        Some(base_input.clone())
    );

    // 8. Uppercase and Lowercase actions.
    assert_eq!(
        process(
            &base_input,
            &[
                Config {
                    source_labels: strs(&["a"]),
                    target_label: "a_upper".to_owned(),
                    action: Action::Uppercase,
                    ..Config::default()
                },
                Config {
                    source_labels: strs(&["a_upper"]),
                    target_label: "a_lower".to_owned(),
                    action: Action::Lowercase,
                    ..Config::default()
                },
            ]
        ),
        Some(label_set(&[
            ("a", "foo"),
            ("a_lower", "foo"),
            ("a_upper", "FOO"),
            ("b", "bar"),
            ("c", "baz"),
        ]))
    );

    // 9. HashMod action (exact Prometheus test vectors: "baz" % 1000 == 976, "foo\nbar" % 1000 == 734).
    assert_eq!(
        process(
            &base_input,
            &[Config {
                source_labels: strs(&["c"]),
                modulus: 1000,
                target_label: "d".to_owned(),
                action: Action::HashMod,
                ..Config::default()
            }]
        ),
        Some(label_set(&[
            ("a", "foo"),
            ("b", "bar"),
            ("c", "baz"),
            ("d", "976"),
        ]))
    );
    assert_eq!(
        process(
            &label_set(&[("a", "foo\nbar")]),
            &[Config {
                source_labels: strs(&["a"]),
                modulus: 1000,
                target_label: "b".to_owned(),
                action: Action::HashMod,
                ..Config::default()
            }]
        ),
        Some(label_set(&[("a", "foo\nbar"), ("b", "734")]))
    );

    // 10. Multi-line value matching with default `(?s:...)` dot-matches-newline flag.
    assert_eq!(
        process(
            &label_set(&[("a", "line1\nline2"), ("b", "bar"), ("c", "baz")]),
            &[Config {
                source_labels: strs(&["a"]),
                regex: Regexp::must_new("line1.*line2"),
                target_label: "d".to_owned(),
                replacement: "match${1}".to_owned(),
                action: Action::Replace,
                ..Config::default()
            }]
        ),
        Some(label_set(&[
            ("a", "line1\nline2"),
            ("b", "bar"),
            ("c", "baz"),
            ("d", "match"),
        ]))
    );

    // 11. LabelMap, LabelDrop, LabelKeep.
    assert_eq!(
        process(
            &base_input,
            &[
                Config {
                    regex: Regexp::must_new("a|b"),
                    replacement: "mapped_${0}".to_owned(),
                    action: Action::LabelMap,
                    ..Config::default()
                },
                Config {
                    regex: Regexp::must_new("c"),
                    action: Action::LabelDrop,
                    ..Config::default()
                },
                Config {
                    regex: Regexp::must_new("mapped_.*"),
                    action: Action::LabelKeep,
                    ..Config::default()
                },
            ]
        ),
        Some(label_set(&[("mapped_a", "foo"), ("mapped_b", "bar")]))
    );

    // 12. KeepEqual and DropEqual.
    let eq_input = label_set(&[
        ("__tmp_port", "1234"),
        ("__port1", "1234"),
        ("__port2", "5678"),
    ]);
    assert_eq!(
        process(
            &eq_input,
            &[Config {
                source_labels: strs(&["__tmp_port"]),
                target_label: "__port1".to_owned(),
                action: Action::DropEqual,
                ..Config::default()
            }]
        ),
        None
    );
    assert_eq!(
        process(
            &eq_input,
            &[Config {
                source_labels: strs(&["__tmp_port"]),
                target_label: "__port1".to_owned(),
                action: Action::KeepEqual,
                ..Config::default()
            }]
        ),
        Some(eq_input.clone())
    );
    assert_eq!(
        process(
            &eq_input,
            &[Config {
                source_labels: strs(&["__tmp_port"]),
                target_label: "__port2".to_owned(),
                action: Action::DropEqual,
                ..Config::default()
            }]
        ),
        Some(eq_input.clone())
    );
    assert_eq!(
        process(
            &eq_input,
            &[Config {
                source_labels: strs(&["__tmp_port"]),
                target_label: "__port2".to_owned(),
                action: Action::KeepEqual,
                ..Config::default()
            }]
        ),
        None
    );

    // 13. UTF-8 and named capture groups (`(?<second>line2)`) with `$1`, `${1}`, `$second`, `${second}`.
    assert_eq!(
        process(
            &label_set(&[("a", "line1\nline2"), ("b", "bar"), ("c", "baz")]),
            &[
                Config {
                    source_labels: strs(&["a"]),
                    regex: Regexp::must_new("(line1).*(?<second>line2)"),
                    target_label: "label1_${1}".to_owned(),
                    replacement: "val_${second}".to_owned(),
                    action: Action::Replace,
                    ..Config::default()
                },
                Config {
                    source_labels: strs(&["a"]),
                    regex: Regexp::must_new("(line1).*(?<second>line2)"),
                    target_label: "label2_$1".to_owned(),
                    replacement: "val_$second".to_owned(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ]
        ),
        Some(label_set(&[
            ("a", "line1\nline2"),
            ("b", "bar"),
            ("c", "baz"),
            ("label1_line1", "val_line2"),
            ("label2_line1", "val_line2"),
        ]))
    );
}

#[test]
fn relabel_validate_matches_prometheus() {
    // Ported from TestRelabelValidate in model/relabel/relabel_test.go.
    let mut empty_action = Config {
        action: Action::Empty,
        ..Config::default()
    };
    assert_eq!(
        empty_action
            .validate(ValidationScheme::Utf8)
            .unwrap_err()
            .to_string(),
        "relabel action cannot be empty"
    );

    let mut replace_no_target = Config {
        action: Action::Replace,
        target_label: String::new(),
        ..Config::default()
    };
    assert!(
        replace_no_target
            .validate(ValidationScheme::Utf8)
            .unwrap_err()
            .to_string()
            .contains("requires 'target_label' value")
    );

    let mut lower_no_target = Config {
        action: Action::Lowercase,
        target_label: String::new(),
        ..Config::default()
    };
    assert!(
        lower_no_target
            .validate(ValidationScheme::Utf8)
            .unwrap_err()
            .to_string()
            .contains("requires 'target_label' value")
    );

    let mut lower_utf8_var = Config {
        action: Action::Lowercase,
        replacement: DEFAULT_REPLACEMENT.to_owned(),
        target_label: "${3}".to_owned(),
        name_validation_scheme: ValidationScheme::Utf8,
        ..Config::default()
    };
    assert!(lower_utf8_var.validate(ValidationScheme::Utf8).is_ok());

    let mut lower_legacy_var = Config {
        action: Action::Lowercase,
        replacement: DEFAULT_REPLACEMENT.to_owned(),
        target_label: "${3}".to_owned(),
        name_validation_scheme: ValidationScheme::Legacy,
        ..Config::default()
    };
    assert_eq!(
        lower_legacy_var
            .validate(ValidationScheme::Utf8)
            .unwrap_err()
            .to_string(),
        "\"${3}\" is invalid 'target_label' for lowercase action"
    );

    let mut replace_utf8_targets = [
        Config {
            source_labels: strs(&["a"]),
            regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
            action: Action::Replace,
            replacement: "${1}".to_owned(),
            target_label: "${3}".to_owned(),
            name_validation_scheme: ValidationScheme::Utf8,
            ..Config::default()
        },
        Config {
            source_labels: strs(&["a"]),
            regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
            action: Action::Replace,
            replacement: "${1}".to_owned(),
            target_label: "0${3}".to_owned(),
            name_validation_scheme: ValidationScheme::Utf8,
            ..Config::default()
        },
        Config {
            source_labels: strs(&["a"]),
            regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
            action: Action::Replace,
            replacement: "${1}".to_owned(),
            target_label: "-${3}".to_owned(),
            name_validation_scheme: ValidationScheme::Utf8,
            ..Config::default()
        },
        Config {
            regex: Regexp::must_new("__meta_kubernetes_pod_label_(strimzi_io_.+)"),
            action: Action::LabelMap,
            replacement: "$1".to_owned(),
            name_validation_scheme: ValidationScheme::Legacy,
            ..Config::default()
        },
        Config {
            regex: Regexp::must_new("__meta_(.+)"),
            action: Action::LabelMap,
            replacement: "${1}".to_owned(),
            name_validation_scheme: ValidationScheme::Legacy,
            ..Config::default()
        },
    ];
    for cfg in &mut replace_utf8_targets {
        cfg.validate(ValidationScheme::Utf8).unwrap();
    }

    // KeepEqual / DropEqual forbid non-default regex, modulus, separator, replacement.
    let mut bad_keepequal = Config {
        source_labels: strs(&["a"]),
        target_label: "b".to_owned(),
        action: Action::KeepEqual,
        separator: ":".to_owned(),
        ..Config::default()
    };
    assert_eq!(
        bad_keepequal
            .validate(ValidationScheme::Utf8)
            .unwrap_err()
            .to_string(),
        "keepequal action requires only 'source_labels' and `target_label`, and no other fields"
    );

    // LabelDrop / LabelKeep forbid source_labels, target_label, modulus, separator, replacement.
    let mut bad_labeldrop = Config {
        source_labels: strs(&["a"]),
        regex: Regexp::must_new("temp.*"),
        action: Action::LabelDrop,
        ..Config::default()
    };
    assert_eq!(
        bad_labeldrop
            .validate(ValidationScheme::Utf8)
            .unwrap_err()
            .to_string(),
        "labeldrop action requires only 'regex', and no other fields"
    );
}

#[test]
fn target_label_legacy_validity_matches_prometheus() {
    // Ported from TestTargetLabelLegacyValidity in model/relabel/relabel_test.go.
    let cases = [
        ("-label", false),
        ("label", true),
        ("label${1}", true),
        ("${1}label", true),
        ("${1}", true),
        ("${", false),
        ("$", false),
        ("${}", false),
        ("foo${", false),
        ("$1", true),
        ("asd$2asd", true),
        ("-foo${1}bar-", false),
        ("bar.foo${1}bar", false),
        ("_${1}_", true),
        ("foo${bar}foo", true),
    ];
    for (input, expected) in cases {
        assert_eq!(
            is_valid_legacy_target_label(input),
            expected,
            "input: {input:?}"
        );
    }
}

#[test]
fn kubernetes_relabel_pipeline_and_serde_roundtrip() {
    // Ported from BenchmarkRelabel ("kubernetes" pipeline) and TestRegexp_JSONUnmarshalThenMarshal
    // in model/relabel/relabel_test.go.
    let k8s_rules_json = r#"[
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

    let mut cfgs: Vec<Config> = serde_json::from_str(k8s_rules_json).unwrap();
    for cfg in &mut cfgs {
        cfg.validate(ValidationScheme::Utf8).unwrap();
    }

    let discovered_target = label_set(&[
        ("__address__", "10.132.183.40:80"),
        ("__meta_kubernetes_namespace", "loki-boltdb-shipper"),
        ("__meta_kubernetes_pod_container_init", "false"),
        ("__meta_kubernetes_pod_container_name", "promtail"),
        ("__meta_kubernetes_pod_container_port_name", "http-metrics"),
        ("__meta_kubernetes_pod_container_port_number", "80"),
        ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
        (
            "__meta_kubernetes_pod_label_name",
            "promtail-loki-boltdb-shipper",
        ),
        (
            "__meta_kubernetes_pod_name",
            "promtail-loki-boltdb-shipper-jgtr7",
        ),
        ("__meta_kubernetes_pod_phase", "Running"),
        ("__meta_kubernetes_pod_ready", "true"),
    ]);

    let pop_cfg = TargetPopulateConfig {
        job_name: "kubernetes-pods".to_owned(),
        scrape_interval: "15s".to_owned(),
        scrape_timeout: "10s".to_owned(),
        metrics_path: "/metrics".to_owned(),
        scheme: "http".to_owned(),
        params: BTreeMap::new(),
    };

    let populated = populate_labels(&discovered_target, &BTreeMap::new(), &pop_cfg, &cfgs)
        .unwrap()
        .expect("target should be kept (10.132.183.40:80 hashmod 3 == 0)");
    assert_eq!(
        populated.public_labels(),
        label_set(&[
            ("cluster", "dev-us-central-0"),
            ("container", "promtail"),
            (
                "instance",
                "promtail-loki-boltdb-shipper-jgtr7:promtail:http-metrics"
            ),
            ("job", "loki-boltdb-shipper/promtail-loki-boltdb-shipper"),
            ("namespace", "loki-boltdb-shipper"),
            ("pod", "promtail-loki-boltdb-shipper-jgtr7"),
        ])
    );
}

#[test]
fn populate_labels_and_honor_labels_merging_match_prometheus() {
    let target = label_set(&[
        ("__address__", "127.0.0.1:9090"),
        ("__meta_dns_name", "prom.local"),
    ]);
    let group_labels = label_set(&[("env", "prod"), ("team", "obs")]);
    let pop_cfg = TargetPopulateConfig {
        job_name: "prometheus".to_owned(),
        scrape_interval: "30s".to_owned(),
        scrape_timeout: "10s".to_owned(),
        metrics_path: "/metrics".to_owned(),
        scheme: "http".to_owned(),
        params: BTreeMap::from([(
            "module".to_owned(),
            vec!["http_2xx".to_owned(), "ignored".to_owned()],
        )]),
    };

    let discovered = populate_discovered_labels(&target, &group_labels, &pop_cfg);
    assert_eq!(
        discovered.get("__param_module"),
        Some(&"http_2xx".to_owned())
    );
    assert_eq!(
        discovered.get("__meta_dns_name"),
        Some(&"prom.local".to_owned())
    );

    let populated = populate_labels(&target, &group_labels, &pop_cfg, &[])
        .unwrap()
        .unwrap();
    // `__meta_*` is stripped and `instance` defaults to `__address__`.
    assert!(!populated.labels.contains_key("__meta_dns_name"));
    assert_eq!(
        populated.labels.get("instance"),
        Some(&"127.0.0.1:9090".to_owned())
    );
    assert_eq!(
        populated.public_labels(),
        label_set(&[
            ("env", "prod"),
            ("instance", "127.0.0.1:9090"),
            ("job", "prometheus"),
            ("team", "obs"),
        ])
    );

    // honor_labels = false: conflicting sample labels are renamed to `exported_<name>`.
    let mut sample_labels = label_set(&[
        ("job", "sample-job"),
        ("exported_job", "existing-exported"),
        ("method", "GET"),
    ]);
    merge_target_and_metric_labels(&mut sample_labels, &populated.labels, false);
    assert_eq!(
        sample_labels,
        label_set(&[
            ("env", "prod"),
            ("exported_exported_job", "sample-job"),
            ("exported_job", "existing-exported"),
            ("instance", "127.0.0.1:9090"),
            ("job", "prometheus"),
            ("method", "GET"),
            ("team", "obs"),
        ])
    );

    // honor_labels = true: existing sample labels win over target labels.
    let mut honored_labels = label_set(&[("job", "sample-job"), ("method", "GET")]);
    merge_target_and_metric_labels(&mut honored_labels, &populated.labels, true);
    assert_eq!(
        honored_labels,
        label_set(&[
            ("env", "prod"),
            ("instance", "127.0.0.1:9090"),
            ("job", "sample-job"),
            ("method", "GET"),
            ("team", "obs"),
        ])
    );
}
