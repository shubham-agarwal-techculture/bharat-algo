//! End-to-end paper run: one strategy per segment, then a byte-for-byte
//! replay of the journal.

use atp_core::catalog::sample_legs;
use atp_core::replay::{diff_states, replay_from_snapshot, replay_records, snapshot_from};
use atp_runtime::demo::run_demo;
use std::path::PathBuf;

fn config(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../config")
        .join(name)
}

#[test]
fn seven_segments_fill_and_replay() {
    let dir = std::env::temp_dir().join(format!("atp-demo-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let journal = dir.join("demo.jsonl");
    let (engine, report) =
        run_demo(&journal, &config("instruments.toml"), &config("risk.toml")).unwrap();
    assert!(report.ok(), "{}", atp_runtime::format_report(&report));
    assert_eq!(report.mode, "paper");
    assert_eq!(report.gateway, "simulator");
    assert_eq!(engine.broker.submitted.len(), sample_legs().len() * 2);
    assert!(engine
        .broker
        .submitted
        .iter()
        .all(|order| !order.algo_id.is_empty()));
    for leg in sample_legs() {
        for suffix in ["e", "x"] {
            let id = format!("{}-{suffix}", leg.strategy_id);
            let order = engine.writer.state.orders.get(&id).unwrap();
            assert_eq!(order.state.as_str(), "FILLED", "{id}");
            assert_eq!(order.algo_id, leg.algo_id);
            assert!(
                order
                    .hops
                    .iter()
                    .any(|h| format!("{:?}", h.stage).contains("Gateway"))
                    || order.hops.len() >= 3
            );
            let _ = order.correlation_id;
        }
    }

    let snap = snapshot_from(&engine.writer.state, engine.writer.head_hash());
    let rebuilt = replay_from_snapshot(&snap, engine.writer.records(), None).unwrap();
    assert!(diff_states(&engine.writer.state, &rebuilt).is_empty());
    let mid = engine.writer.state.last_seq / 2;
    let early = replay_records("ACC1", engine.writer.records(), Some(mid)).unwrap();
    assert!(early.last_seq <= mid);
    assert!(!diff_states(&early, &engine.writer.state).is_empty());

    let mut cfg = engine.writer.state.risk.clone().unwrap();
    let mut engine = engine;
    cfg.version += 1;
    cfg.stale_feed_ms = 4_000;
    engine.apply_config(cfg).unwrap();
    assert_eq!(engine.writer.state.risk.as_ref().unwrap().version, 2);
    assert_eq!(
        engine.writer.state.risk.as_ref().unwrap().stale_feed_ms,
        4_000
    );
    let mut same = engine.writer.state.risk.clone().unwrap();
    same.stale_feed_ms = 3_000;
    assert!(engine.apply_config(same).is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn two_demos_produce_the_same_state_hash() {
    let dir = std::env::temp_dir().join(format!("atp-demo-det-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let a = run_demo(
        &dir.join("a.jsonl"),
        &config("instruments.toml"),
        &config("risk.toml"),
    )
    .unwrap()
    .1
    .state_hash;
    let b = run_demo(
        &dir.join("b.jsonl"),
        &config("instruments.toml"),
        &config("risk.toml"),
    )
    .unwrap()
    .1
    .state_hash;
    assert_eq!(a, b);
    let _ = std::fs::remove_dir_all(&dir);
}
