use runspk::{
    config::TransferLimits,
    model::*,
    policy::{Policy, SchedulingPolicy},
    store::Store,
};
use serde_json::json;
use std::collections::BTreeMap;

const POLICIES: [Policy; 4] = [
    Policy::Fifo,
    Policy::BestFit,
    Policy::Priority,
    Policy::Easy,
];

fn job(seq: i64, cpu: u32, timeout: u64) -> Job {
    serde_json::from_value(json!({
        "id": seq.to_string(), "seq": seq, "state": "queued", "submitted_ms": 0,
        "spec": {"command": ["true"], "resources": {"cpu": cpu, "mem_mib": 64},
            "timeout_secs": timeout, "trusted": true, "cwd": null}
    }))
    .unwrap()
}

fn labels(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn worker(id: &str, cpu: u32, entries: &[(&str, &str)]) -> Worker {
    serde_json::from_value(json!({
        "registration": {
            "auth": {"id": id, "session": uuid::Uuid::new_v4().to_string()},
            "capacity": {"cpu": cpu, "mem_mib": 1024},
            "physical": {"cpu": cpu, "mem_mib": 1024},
            "allow_native": true, "labels": labels(entries)
        },
        "state": "online", "last_seen_ms": now_ms(), "reserved": {"cpu": 0, "mem_mib": 0}
    }))
    .unwrap()
}

fn running(mut job: Job, worker: &str) -> Job {
    job.state = JobState::Running;
    job.started_ms = Some(0);
    job.worker_id = Some(worker.to_string());
    job
}

#[test]
fn every_policy_requires_all_exact_labels() {
    let mut targeted = job(1, 1, 10);
    targeted.spec.required_labels = labels(&[("arch", "arm64"), ("zone", "east")]);
    for policy in POLICIES {
        for entries in [
            vec![],
            vec![("arch", "arm64")],
            vec![("arch", "x86_64"), ("zone", "east")],
            vec![("arch", "ARM64"), ("zone", "east")],
            vec![("Arch", "arm64"), ("zone", "east")],
        ] {
            assert!(
                policy
                    .plan(&[targeted.clone()], &[worker("a", 4, &entries)], 0)
                    .is_empty(),
                "{policy:?}: {entries:?}"
            );
        }
        let workers = [
            worker("a", 4, &[("arch", "x86_64"), ("zone", "east")]),
            worker(
                "b",
                4,
                &[("arch", "arm64"), ("zone", "east"), ("disk", "ssd")],
            ),
        ];
        let mut second = targeted.clone();
        second.id = "2".into();
        second.seq = 2;
        let placements = policy.plan(&[targeted.clone(), second], &workers, 0);
        assert_eq!(
            placements.len(),
            2,
            "{policy:?}: labels must not be consumed"
        );
        assert!(placements.iter().all(|p| p.worker_id == "b"));
    }
}

#[test]
fn every_policy_skips_a_head_that_cannot_ever_match_labels() {
    let mut head = job(1, 1, 10);
    head.spec.required_labels = labels(&[("arch", "arm64")]);
    for policy in POLICIES {
        let placements = policy.plan(&[head.clone(), job(2, 1, 10)], &[worker("a", 4, &[])], 0);
        assert_eq!(placements.len(), 1, "{policy:?}");
        assert_eq!(placements[0].job_id, "2", "{policy:?}");
    }
}

#[test]
fn easy_reserves_only_matching_workers_and_checks_backfill_requirements() {
    let workers = [
        worker("a", 4, &[("arch", "x86_64")]),
        worker("z", 4, &[("arch", "arm64")]),
    ];
    let mut head = job(2, 4, 60);
    head.spec.required_labels = labels(&[("arch", "arm64")]);
    let mut long = job(3, 2, 40);
    long.spec.required_labels = head.spec.required_labels.clone();
    let mut impossible = job(4, 1, 1);
    impossible.spec.required_labels = labels(&[("arch", "riscv64")]);
    let mut short = job(5, 2, 20);
    short.spec.required_labels = head.spec.required_labels.clone();
    let jobs = [
        running(job(0, 2, 10), "a"),
        running(job(1, 2, 30), "z"),
        head,
        long,
        impossible,
        short,
    ];
    let placements = Policy::Easy.plan(&jobs, &workers, 0);
    assert_eq!(placements.len(), 1);
    assert_eq!(placements[0].job_id, "5");
    assert_eq!(placements[0].worker_id, "z");
}

#[test]
fn old_job_and_worker_json_defaults_to_untargeted_metadata() {
    let old_job = job(1, 1, 1);
    assert_eq!(old_job.spec.name, None);
    assert!(old_job.spec.tags.is_empty());
    assert!(old_job.spec.required_labels.is_empty());
    assert_eq!(old_job.spec.transfer_limits, None);
    old_job.spec.validate().unwrap();
    let mut old_worker = serde_json::to_value(worker("a", 1, &[])).unwrap();
    old_worker["registration"]
        .as_object_mut()
        .unwrap()
        .remove("labels");
    old_worker["registration"]
        .as_object_mut()
        .unwrap()
        .remove("transfer_limits");
    let old_worker: Worker = serde_json::from_value(old_worker).unwrap();
    assert!(old_worker.registration.labels.is_empty());
    assert_eq!(old_worker.registration.transfer_limits, None);
    for policy in POLICIES {
        assert_eq!(
            policy
                .plan(
                    std::slice::from_ref(&old_job),
                    std::slice::from_ref(&old_worker),
                    0
                )
                .len(),
            1
        );
    }
}

#[test]
fn every_policy_checks_project_transfer_capabilities() {
    let mut project = job(1, 1, 1);
    project.spec.workspace_id = Some("a".repeat(64));
    project.spec.transfer_limits = Some(TransferLimits::hard());
    project.spec.validate().unwrap();
    let mut old = worker("a", 4, &[]);
    old.registration.project_staging = true;
    let mut upgraded = worker("b", 4, &[]);
    upgraded.registration.project_staging = true;
    upgraded.registration.transfer_limits = Some(TransferLimits::hard());
    for policy in POLICIES {
        assert!(
            policy
                .plan(&[project.clone()], &[old.clone()], 0)
                .is_empty()
        );
        let placements = policy.plan(&[project.clone()], &[old.clone(), upgraded.clone()], 0);
        assert_eq!(placements.len(), 1, "{policy:?}");
        assert_eq!(placements[0].worker_id, "b", "{policy:?}");
        let placements = policy.plan(&[project.clone(), job(2, 1, 1)], &[old.clone()], 0);
        assert_eq!(placements.len(), 1, "{policy:?}");
        assert_eq!(placements[0].job_id, "2", "{policy:?}");
        let mut native = project.clone();
        native.spec.workspace_id = None;
        assert_eq!(policy.plan(&[native], &[old.clone()], 0).len(), 1);
    }
}

#[test]
fn easy_reservations_respect_project_transfer_capabilities() {
    let mut old = worker("a", 4, &[]);
    old.registration.project_staging = true;
    let mut upgraded = worker("z", 4, &[]);
    upgraded.registration.project_staging = true;
    upgraded.registration.transfer_limits = Some(TransferLimits::hard());
    let mut head = job(2, 4, 60);
    head.spec.workspace_id = Some("a".repeat(64));
    head.spec.transfer_limits = Some(TransferLimits::hard());
    let mut long = job(3, 2, 40);
    long.spec.workspace_id = head.spec.workspace_id.clone();
    long.spec.transfer_limits = head.spec.transfer_limits;
    let mut short = long.clone();
    short.id = "4".into();
    short.seq = 4;
    short.spec.timeout_secs = 20;
    let jobs = [
        running(job(0, 2, 10), "a"),
        running(job(1, 2, 30), "z"),
        head,
        long,
        short,
    ];
    let placements = Policy::Easy.plan(&jobs, &[old, upgraded], 0);
    assert_eq!(placements.len(), 1);
    assert_eq!(placements[0].job_id, "4");
    assert_eq!(placements[0].worker_id, "z");
}

#[test]
fn job_and_registration_validate_optional_transfer_limits() {
    let invalid = TransferLimits {
        max_files: 0,
        ..Default::default()
    };
    let mut spec = job(1, 1, 1).spec;
    spec.transfer_limits = Some(invalid);
    assert!(spec.validate().is_err());
    let mut registration = worker("a", 1, &[]).registration;
    registration.transfer_limits = Some(invalid);
    assert!(registration.validate_metadata().is_err());
}

#[test]
fn job_names_and_tags_reject_empty_oversized_and_control_metadata() {
    for name in [
        String::new(),
        "x".repeat(257),
        "é".repeat(129),
        "line\nbreak".into(),
        "tab\t".into(),
        "nul\0".into(),
        "control\u{85}".into(),
    ] {
        let mut spec = job(1, 1, 1).spec;
        spec.name = Some(name.clone());
        assert!(spec.validate().is_err(), "name {name:?}");
    }
    for tags in [
        vec![String::new()],
        vec!["x".repeat(65)],
        vec!["tag".into(); 65],
        vec!["line\nbreak".into()],
        vec!["control\u{7f}".into()],
        vec!["control\u{85}".into()],
    ] {
        let mut spec = job(1, 1, 1).spec;
        spec.tags = tags.clone();
        assert!(spec.validate().is_err(), "tags {tags:?}");
    }
}

#[test]
fn labels_reject_invalid_keys_values_and_counts_for_jobs_and_workers() {
    let mut invalid = Vec::new();
    for key in [
        String::new(),
        "x".repeat(65),
        "space key".into(),
        "key=value".into(),
        "key\0".into(),
        "é".into(),
    ] {
        invalid.push(BTreeMap::from([(key, "value".into())]));
    }
    for value in [
        String::new(),
        "x".repeat(257),
        "line\nbreak".into(),
        "control\u{7f}".into(),
        "control\u{85}".into(),
    ] {
        invalid.push(BTreeMap::from([("key".into(), value)]));
    }
    invalid.push(
        (0..65)
            .map(|i| (format!("key{i}"), "value".into()))
            .collect(),
    );
    let mut store = Store::open(":memory:").unwrap();
    for entries in invalid {
        assert!(validate_labels(&entries).is_err(), "{entries:?}");
        let mut spec = job(1, 1, 1).spec;
        spec.required_labels = entries.clone();
        assert!(store.submit(spec).is_err(), "job: {entries:?}");
        let mut registration = worker("a", 1, &[]).registration;
        registration.labels = entries.clone();
        assert!(store.register(registration).is_err(), "worker: {entries:?}");
    }
}

#[test]
fn cli_label_parser_reuses_metadata_validation() {
    assert_eq!(
        parse_label("arch=arm64").unwrap(),
        ("arch".into(), "arm64".into())
    );
    assert_eq!(
        parse_label("setting=a=b").unwrap(),
        ("setting".into(), "a=b".into())
    );
    for invalid in ["arch", "=arm64", "arch=", "bad key=value", "arch=arm64\n"] {
        assert!(parse_label(invalid).is_err(), "{invalid:?}");
    }
    assert!(parse_label(&format!("arch={}", "x".repeat(257))).is_err());
}

#[test]
fn metadata_boundaries_and_unicode_values_are_valid() {
    let mut spec = job(1, 1, 1).spec;
    spec.name = Some("é".repeat(128));
    spec.tags = vec!["é".repeat(32); 64];
    spec.required_labels = (0..64)
        .map(|i| (format!("{i:064}"), "é".repeat(128)))
        .collect();
    spec.validate().unwrap();
    validate_labels(&labels(&[(
        "vendor.example/accelerator_type-1",
        "GPU model A",
    )]))
    .unwrap();
}

#[test]
fn metadata_survives_storage_and_changes_submission_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.sqlite");
    let mut store = Store::open(&path).unwrap();
    let registration = worker("a", 1, &[("arch", "arm64")]).registration;
    let auth = registration.auth.clone();
    store.register(registration).unwrap();
    let mut spec = job(1, 1, 1).spec;
    spec.name = Some("Training α".into());
    spec.tags = vec!["training".into()];
    spec.required_labels = labels(&[("arch", "arm64")]);
    spec.submission_key = Some("targeted-job".into());
    let submitted = store.submit(spec.clone()).unwrap();
    assert_eq!(store.submit(spec.clone()).unwrap().id, submitted.id);
    for field in ["name", "tags", "required_labels"] {
        let mut changed = spec.clone();
        match field {
            "name" => changed.name = Some("different".into()),
            "tags" => changed.tags.push("different".into()),
            _ => changed.required_labels = labels(&[("arch", "x86_64")]),
        }
        assert!(
            store.submit(changed).is_err(),
            "{field} must participate in deduplication"
        );
    }
    drop(store);
    let store = Store::open(&path).unwrap();
    assert_eq!(store.get(&submitted.id).unwrap().spec, spec);
    assert_eq!(
        store.worker(&auth).unwrap().registration.labels,
        spec.required_labels
    );
}

#[test]
fn queue_explanation_counts_submission_order_without_including_terminal_history() {
    let mut store = runspk::store::Store::open(":memory:").unwrap();
    let spec:JobSpec=serde_json::from_value(serde_json::json!({"command":["true"],"resources":{"cpu":1,"mem_mib":64},"timeout_secs":10,"trusted":true})).unwrap();
    let first = store.submit(spec.clone()).unwrap();
    let second = store.submit(spec).unwrap();
    assert_eq!(store.explain(&second.id).unwrap()["queue_position"], 2);
    store.cancel(&first.id).unwrap();
    assert_eq!(store.explain(&second.id).unwrap()["queue_position"], 1);
    assert!(store.explain(&first.id).unwrap()["queue_position"].is_null());
}
