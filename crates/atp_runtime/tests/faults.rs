//! Fault injection through the simulator. These tests prove the runtime does
//! not send a second live order when a query shows the first one working, and
//! that UNKNOWN is resolved from the broker book rather than by guessing.

use atp_core::calendar::ymd_hms_ist_to_unix_ms;
use atp_core::{OrderIntent, OrderState, OrderType, Product, Side, Validity, PRICE_SCALE};
use atp_runtime::broker::Broker;
use atp_runtime::engine::{Engine, RunMode};
use atp_runtime::kite::{KiteAdapter, NOT_YET_LIVE};
use atp_runtime::simulator::{serve, Fault, Simulator};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn clock() -> i64 {
    ymd_hms_ist_to_unix_ms(2026, 1, 7, 10, 0, 0)
}

fn px(s: &str) -> i64 {
    atp_core::parse_scaled(s).unwrap()
}

fn boot() -> Engine<Simulator> {
    Engine::paper_memory(clock()).expect("boot")
}

fn arm(engine: &mut Engine<Simulator>) {
    engine
        .publish_market(
            "NSE:CM:RELIANCE",
            px("2500"),
            px("2500"),
            px("2500"),
            1,
            "md-reliance",
        )
        .unwrap();
}

fn intent(engine: &Engine<Simulator>, id: &str, qty: i64) -> OrderIntent {
    OrderIntent {
        intent_id: id.into(),
        account_id: "ACC1".into(),
        strategy_id: "manual".into(),
        instrument_id: "NSE:CM:RELIANCE".into(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        product: Product::Cnc,
        validity: Validity::Day,
        qty,
        price: px("2500"),
        algo_id: "ALGOMANUAL1".into(),
        correlation_id: format!("corr-{id}"),
        wall_ms: engine.now(),
        mono_ns: 1,
    }
}

fn state_of(engine: &Engine<Simulator>, id: &str) -> OrderState {
    engine.writer.state.orders.get(id).unwrap().state
}

#[test]
fn disconnect_before_accept_resends_once_with_the_same_id() {
    let mut engine = boot();
    arm(&mut engine);
    engine
        .broker
        .script("manual-e", Fault::DisconnectBeforeAccept);
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Filled);
    assert_eq!(engine.broker.accept_count["manual-e"], 1);
    assert_eq!(engine.broker.attempt_count["manual-e"], 2);
    assert_eq!(engine.broker.submitted.len(), 2);
    assert!(engine
        .broker
        .submitted
        .iter()
        .all(|o| o.client_order_id == "manual-e" && o.algo_id == "ALGOMANUAL1"));
    assert_eq!(engine.broker.books().unwrap().trades.len(), 1);
    assert!(engine.writer.state.halt.is_none());
}

#[test]
fn disconnect_after_accept_queries_and_does_not_resend() {
    let dir = std::env::temp_dir().join(format!("atp-recover-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("journal.jsonl");
    let mut engine = Engine::boot(
        Simulator::new(0, 0),
        RunMode::Paper,
        Some(&path),
        "ACC1",
        clock(),
    )
    .unwrap();
    engine
        .load_instruments(atp_core::catalog::sample_catalog())
        .unwrap();
    engine
        .apply_config(atp_core::config::demo_risk_config())
        .unwrap();
    arm(&mut engine);
    engine
        .broker
        .script("manual-e", Fault::DisconnectAfterAccept);
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Sent);
    assert_eq!(engine.broker.accept_count["manual-e"], 1);

    let Engine { broker, .. } = engine;
    let mut recovered = Engine::boot(broker, RunMode::Paper, Some(&path), "ACC1", clock()).unwrap();
    recovered.recover().unwrap();
    assert_eq!(state_of(&recovered, "manual-e"), OrderState::Acked);
    assert_eq!(recovered.broker.accept_count["manual-e"], 1);
    assert_eq!(recovered.broker.attempt_count["manual-e"], 1);
    assert_eq!(recovered.broker.submitted.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn duplicate_fill_is_applied_once() {
    let mut engine = boot();
    arm(&mut engine);
    engine.broker.script("manual-e", Fault::DuplicateFill);
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    let order = engine.writer.state.orders.get("manual-e").unwrap();
    assert_eq!(order.state, OrderState::Filled);
    assert_eq!(order.filled_qty, 1);
    assert_eq!(order.fill_trade_ids.len(), 1);
    assert!(engine.writer.records().iter().any(|r| {
        r.events
            .iter()
            .any(|e| matches!(e, atp_core::Event::DuplicateFillIgnored { .. }))
    }));
    assert!(engine.reconcile().unwrap());
}

#[test]
fn delayed_ack_resolves_unknown_without_a_second_send() {
    let mut engine = boot();
    arm(&mut engine);
    engine.broker.fill_delay_ms = 60_000;
    let ready = engine.now() + 60_000;
    engine
        .broker
        .script("manual-e", Fault::DelayAck { ready_at: ready });
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Sent);
    engine.advance(500);
    engine.timer().unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Acked);
    assert_eq!(engine.broker.attempt_count["manual-e"], 1);
    engine.advance(60_000);
    engine.poll_broker().unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Filled);
    assert_eq!(engine.broker.submitted.len(), 1);
    assert!(engine.writer.state.halt.is_none());
}

#[test]
fn broker_reject_does_not_open_a_position() {
    let mut engine = boot();
    arm(&mut engine);
    engine.broker.script("manual-e", Fault::Reject);
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Rejected);
    assert!(engine
        .writer
        .state
        .positions
        .values()
        .all(|p| p.net_qty == 0));
    assert!(engine.reconcile().unwrap());
}

#[test]
fn excess_fill_halts_and_does_not_invent_quantity() {
    let mut engine = boot();
    arm(&mut engine);
    engine.broker.script("manual-e", Fault::ExcessFill);
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    let order = engine.writer.state.orders.get("manual-e").unwrap();
    assert_eq!(order.filled_qty, 0);
    assert!(engine.writer.state.halt.is_some());
    assert_eq!(engine.broker.accept_count["manual-e"], 1);
}

#[test]
fn partial_fill_stays_inside_the_order_qty() {
    let mut engine = boot();
    arm(&mut engine);
    engine
        .broker
        .script("manual-e", Fault::Partial { first: 4 });
    engine
        .submit_intent(intent(&engine, "manual-e", 10))
        .unwrap();
    let order = engine.writer.state.orders.get("manual-e").unwrap();
    assert_eq!(order.state, OrderState::PartiallyFilled);
    assert_eq!(order.filled_qty, 4);
    assert!(order.filled_qty <= order.qty);
    assert!(engine.reconcile().unwrap());
}

#[test]
fn fill_before_ack_still_records_the_broker_id() {
    let mut engine = boot();
    arm(&mut engine);
    engine.broker.script("manual-e", Fault::FillBeforeAck);
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::Filled);
    engine.advance(5);
    engine.poll_broker().unwrap();
    assert!(engine
        .writer
        .state
        .orders
        .get("manual-e")
        .unwrap()
        .broker_order_id
        .is_some());
    assert!(engine.writer.state.halt.is_none());
}

#[test]
fn kill_switch_blocks_new_risk_and_sends_nothing() {
    let mut engine = boot();
    arm(&mut engine);
    engine.kill(atp_core::KillScope::Global, "test").unwrap();
    engine
        .submit_intent(intent(&engine, "manual-e", 1))
        .unwrap();
    assert_eq!(state_of(&engine, "manual-e"), OrderState::RiskRejected);
    assert!(engine.broker.submitted.is_empty());
}

#[test]
fn segment_rate_limit_is_not_exceeded() {
    let mut engine = boot();
    arm(&mut engine);
    for n in 0..25 {
        let id = format!("m{n:02}");
        engine.submit_intent(intent(&engine, &id, 1)).unwrap();
    }
    assert_eq!(engine.broker.submitted.len(), 20);
    let rejected = engine
        .writer
        .state
        .orders
        .values()
        .filter(|o| o.state == OrderState::RiskRejected)
        .count();
    assert_eq!(rejected, 5);
    assert!(engine.broker.accept_count.values().all(|n| *n == 1));
}

#[test]
fn live_kite_skeleton_refuses_to_start() {
    let kite = KiteAdapter::skeleton("key", "token", "AB1234");
    let err = match Engine::boot(kite, RunMode::Live, None, "ACC1", clock()) {
        Err(err) => err,
        Ok(_) => panic!("live kite must refuse to start"),
    };
    assert!(err.to_string().contains("not certified"), "{err}");
    assert!(NOT_YET_LIVE.contains("not certified"));
}

#[test]
fn paper_refuses_kite_gateway() {
    let kite = KiteAdapter::skeleton("key", "token", "AB1234");
    let err = match Engine::boot(kite, RunMode::Paper, None, "ACC1", clock()) {
        Err(err) => err,
        Ok(_) => panic!("paper mode must refuse kite"),
    };
    assert!(err.to_string().contains("simulator"), "{err}");
}

#[test]
fn killswitch_uses_the_broker_when_the_engine_is_down() {
    let addr = "127.0.0.1:18424";
    let sim = Arc::new(Mutex::new(Simulator::new(0, 0)));
    let background = sim.clone();
    std::thread::spawn(move || {
        let _ = serve(addr, background);
    });
    let mut client = wait_connect(addr);
    let order = atp_core::OutboundOrder {
        client_order_id: "kill-me".into(),
        account_id: "ACC1".into(),
        strategy_id: "manual".into(),
        instrument_id: "NSE:CM:RELIANCE".into(),
        exchange: atp_core::Exchange::Nse,
        segment: atp_core::Segment::Cm,
        symbol: "RELIANCE".into(),
        side: Side::Buy,
        order_type: OrderType::Limit,
        product: Product::Cnc,
        validity: Validity::Day,
        qty: 1,
        price: 2500 * PRICE_SCALE,
        algo_id: "ALGOMANUAL1".into(),
        correlation_id: "kill".into(),
    };
    client.submit(&order).unwrap();
    let via = atp_runtime::http::issue_kill(
        "http://127.0.0.1:18499/kill",
        addr,
        atp_core::KillScope::Global,
        "engine down",
    )
    .unwrap();
    assert_eq!(via, "broker");
    client.set_now(1);
    client.poll();
    match client.query("kill-me").unwrap() {
        atp_core::BrokerView::Cancelled { .. } => {}
        other => panic!("expected cancel, got {other:?}"),
    }
}

fn wait_connect(addr: &str) -> atp_runtime::broker::TcpBroker {
    for _ in 0..50 {
        if let Ok(client) = atp_runtime::broker::TcpBroker::connect(addr) {
            return client;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("simbroker did not accept a connection");
}
