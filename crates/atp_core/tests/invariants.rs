//! Behaviour the platform must not regress: risk cannot be skipped, fills
//! cannot exceed quantity, rate limits hold, and a journal replays to the
//! same state.

use atp_core::calendar::ymd_hms_ist_to_unix_ms;
use atp_core::catalog::sample_catalog;
use atp_core::config::{demo_risk_config, RiskConfig};
use atp_core::events::{Command, Effect, JournalMeta};
use atp_core::journal::{content_hash, PartitionWriter};
use atp_core::replay::{
    diff_states, replay_from_snapshot, replay_records, snapshot_from, state_hash,
};
use atp_core::types::{
    parse_scaled, BrokerView, KillScope, OrderIntent, OrderState, OrderType, Product, Side,
    Validity,
};
use atp_core::MarginModel;

fn meta(wall: i64, n: u64) -> JournalMeta {
    JournalMeta {
        correlation_id: format!("c{n}"),
        wall_ms: wall,
        mono_ns: n.saturating_mul(1_000_000),
    }
}

fn open_ms() -> i64 {
    ymd_hms_ist_to_unix_ms(2026, 1, 7, 10, 0, 0)
}

fn boot(cfg: RiskConfig) -> PartitionWriter {
    let mut w = PartitionWriter::memory("ACC1");
    w.handle(Command::LoadInstruments {
        instruments: sample_catalog(),
        meta: meta(0, 1),
    })
    .unwrap();
    w.handle(Command::ApplyConfig {
        config: cfg,
        meta: meta(0, 2),
    })
    .unwrap();
    w
}

fn feed(w: &mut PartitionWriter, id: &str, px: &str, seq: u64, wall: i64) {
    let price = parse_scaled(px).unwrap();
    w.handle(Command::MarketData {
        instrument_id: id.into(),
        ltp: price,
        bid: price,
        ask: price,
        source_seq: seq,
        exchange_wall_ms: wall,
        resync: false,
        meta: meta(wall, seq + 10),
    })
    .unwrap();
}

fn intent(id: &str, instrument: &str, side: Side, qty: i64, price: i64, wall: i64) -> OrderIntent {
    OrderIntent {
        intent_id: id.into(),
        account_id: "ACC1".into(),
        strategy_id: "manual".into(),
        instrument_id: instrument.into(),
        side,
        order_type: OrderType::Limit,
        product: Product::Cnc,
        validity: Validity::Day,
        qty,
        price,
        algo_id: "ALGOMANUAL1".into(),
        correlation_id: format!("sig-{id}"),
        wall_ms: wall,
        mono_ns: 50_000,
    }
}

fn place(w: &mut PartitionWriter, id: &str, qty: i64, px: &str, wall: i64) -> Vec<Effect> {
    w.handle(Command::OrderIntent(intent(
        id,
        "NSE:CM:RELIANCE",
        Side::Buy,
        qty,
        parse_scaled(px).unwrap(),
        wall,
    )))
    .unwrap()
}

#[test]
fn approved_order_is_sent_only_after_risk_and_replay_matches() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    let effects = place(&mut w, "ord1", 1, "2500", wall);
    assert!(effects.iter().any(|e| matches!(e, Effect::Send(_))));
    let order = w.state.orders.get("ord1").unwrap();
    assert_eq!(order.state, OrderState::RiskApproved);
    assert!(order.risk_approved);
    assert!(order.algo_id == "ALGOMANUAL1");

    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 20),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].state, OrderState::Sent);
    // Idempotent mark: a retry must not bump send attempts.
    let attempts = w.state.orders["ord1"].send_attempts;
    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 21),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].send_attempts, attempts);

    w.handle(Command::BrokerAck {
        client_order_id: "ord1".into(),
        broker_order_id: "B1".into(),
        meta: meta(wall + 5, 22),
    })
    .unwrap();
    w.handle(Command::Fill {
        client_order_id: "ord1".into(),
        trade_id: "T1".into(),
        qty: 1,
        price: parse_scaled("2500").unwrap(),
        meta: meta(wall + 6, 23),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].state, OrderState::Filled);
    assert_eq!(w.state.orders["ord1"].filled_qty, 1);

    let again = w.handle(Command::Fill {
        client_order_id: "ord1".into(),
        trade_id: "T1".into(),
        qty: 1,
        price: parse_scaled("2500").unwrap(),
        meta: meta(wall + 7, 24),
    });
    assert!(again.is_ok());
    assert_eq!(w.state.orders["ord1"].filled_qty, 1);
    assert_eq!(w.state.orders["ord1"].state, OrderState::Filled);

    let replayed = replay_records("ACC1", w.records(), None).unwrap();
    assert_eq!(state_hash(&replayed), state_hash(&w.state));
    assert!(diff_states(&replayed, &w.state).is_empty());

    let mut second = boot(demo_risk_config());
    feed(&mut second, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut second, "ord1", 1, "2500", wall);
    second
        .handle(Command::MarkDispatched {
            client_order_id: "ord1".into(),
            meta: meta(wall, 20),
        })
        .unwrap();
    second
        .handle(Command::MarkDispatched {
            client_order_id: "ord1".into(),
            meta: meta(wall, 21),
        })
        .unwrap();
    second
        .handle(Command::BrokerAck {
            client_order_id: "ord1".into(),
            broker_order_id: "B1".into(),
            meta: meta(wall + 5, 22),
        })
        .unwrap();
    second
        .handle(Command::Fill {
            client_order_id: "ord1".into(),
            trade_id: "T1".into(),
            qty: 1,
            price: parse_scaled("2500").unwrap(),
            meta: meta(wall + 6, 23),
        })
        .unwrap();
    second
        .handle(Command::Fill {
            client_order_id: "ord1".into(),
            trade_id: "T1".into(),
            qty: 1,
            price: parse_scaled("2500").unwrap(),
            meta: meta(wall + 7, 24),
        })
        .unwrap();
    assert_eq!(
        serde_json::to_string(w.records()).unwrap(),
        serde_json::to_string(second.records()).unwrap()
    );
}

#[test]
fn snapshot_replay_and_tamper_detection() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 1, "2500", wall);
    let mid = w.state.last_seq;
    let snap = snapshot_from(&w.state, &content_hash(&w.records()[mid as usize - 1]));
    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 30),
    })
    .unwrap();
    let from_snap = replay_from_snapshot(&snap, w.records(), None).unwrap();
    assert_eq!(state_hash(&from_snap), state_hash(&w.state));
    let partial = replay_records("ACC1", w.records(), Some(mid)).unwrap();
    assert!(diff_states(&partial, &snap.state).is_empty());

    let mut tampered = w.records().to_vec();
    tampered[1].prev_hash = "deadbeef".into();
    assert!(replay_records("ACC1", &tampered, None).is_err());
}

#[test]
fn duplicate_intent_does_not_create_a_second_order() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 1, "2500", wall);
    let n = w.state.orders.len();
    let seq = w.state.last_seq;
    let effects = place(&mut w, "ord1", 1, "2500", wall);
    assert!(effects.is_empty());
    assert_eq!(w.state.orders.len(), n);
    assert_eq!(w.state.last_seq, seq);
}

#[test]
fn excess_fill_halts_without_increasing_qty_and_blocks_new_risk() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 2, "2500", wall);
    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 40),
    })
    .unwrap();
    w.handle(Command::Fill {
        client_order_id: "ord1".into(),
        trade_id: "Tbig".into(),
        qty: 5,
        price: parse_scaled("2500").unwrap(),
        meta: meta(wall, 41),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].filled_qty, 0);
    assert!(w.state.halt.is_some());
    let rejected = place(&mut w, "ord2", 1, "2500", wall);
    assert!(rejected.is_empty() || w.state.orders["ord2"].state == OrderState::RiskRejected);
    assert!(w
        .state
        .orders
        .values()
        .all(|o| o.state != OrderState::Sent || o.risk_approved));
}

#[test]
fn partial_fills_never_exceed_order_qty() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 5, "2500", wall);
    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 50),
    })
    .unwrap();
    for (i, qty) in [2i64, 2, 1, 1].iter().enumerate() {
        w.handle(Command::Fill {
            client_order_id: "ord1".into(),
            trade_id: format!("P{i}"),
            qty: *qty,
            price: parse_scaled("2500").unwrap(),
            meta: meta(wall + i as i64, 60 + i as u64),
        })
        .unwrap();
        let order = &w.state.orders["ord1"];
        assert!(order.filled_qty <= order.qty);
    }
    assert_eq!(w.state.orders["ord1"].state, OrderState::Filled);
    assert_eq!(w.state.orders["ord1"].filled_qty, 5);
}

#[test]
fn rate_limit_is_never_exceeded() {
    let mut cfg = demo_risk_config();
    cfg.account_ops = 1;
    cfg.segment_ops = 1;
    cfg.strategies.iter_mut().for_each(|s| s.max_ops = 1);
    let wall = open_ms();
    let mut w = boot(cfg);
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "a1", 1, "2500", wall);
    place(&mut w, "a2", 1, "2500", wall);
    assert_eq!(w.state.orders["a1"].state, OrderState::RiskApproved);
    assert_eq!(w.state.orders["a2"].state, OrderState::RiskRejected);
    assert_eq!(
        w.state.orders["a2"].reject_rule.as_deref(),
        Some("ORDER_RATE")
    );
    assert!(atp_core::rms::window_ok(&w.state.rates.account, 1));
}

#[test]
fn self_trade_stale_feed_gap_hours_and_fat_finger_reject() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "bid1", 1, "2500", wall);
    w.handle(Command::OrderIntent(intent(
        "ask1",
        "NSE:CM:RELIANCE",
        Side::Sell,
        1,
        parse_scaled("2500").unwrap(),
        wall,
    )))
    .unwrap();
    assert_eq!(w.state.orders["ask1"].state, OrderState::RiskRejected);
    assert_eq!(
        w.state.orders["ask1"].reject_rule.as_deref(),
        Some("SELF_TRADE")
    );

    let stale = wall + 6_000;
    w.handle(Command::OrderIntent(intent(
        "stale1",
        "NSE:CM:RELIANCE",
        Side::Buy,
        1,
        parse_scaled("2490").unwrap(),
        stale,
    )))
    .unwrap();
    assert_eq!(
        w.state.orders["stale1"].reject_rule.as_deref(),
        Some("STALE_FEED")
    );

    feed(&mut w, "NSE:CM:INFY", "1500", 1, wall);
    feed(&mut w, "NSE:CM:INFY", "1500", 3, wall);
    assert!(w.state.market["NSE:CM:INFY"].gap);
    w.handle(Command::OrderIntent(intent(
        "gap1",
        "NSE:CM:INFY",
        Side::Buy,
        1,
        parse_scaled("1500").unwrap(),
        wall,
    )))
    .unwrap();
    assert_eq!(
        w.state.orders["gap1"].reject_rule.as_deref(),
        Some("FEED_GAP")
    );

    let weekend = ymd_hms_ist_to_unix_ms(2026, 1, 10, 10, 0, 0);
    feed(&mut w, "NSE:CM:TCS", "3000", 1, weekend);
    w.handle(Command::OrderIntent(intent(
        "wknd",
        "NSE:CM:TCS",
        Side::Buy,
        1,
        parse_scaled("3000").unwrap(),
        weekend,
    )))
    .unwrap();
    assert_eq!(
        w.state.orders["wknd"].reject_rule.as_deref(),
        Some("TRADING_HOURS")
    );

    w.handle(Command::OrderIntent(intent(
        "fat1",
        "NSE:CM:RELIANCE",
        Side::Buy,
        1,
        parse_scaled("2350").unwrap(),
        wall,
    )))
    .unwrap();
    assert_eq!(
        w.state.orders["fat1"].reject_rule.as_deref(),
        Some("FAT_FINGER_PRICE")
    );
}

#[test]
fn mcx_evening_and_currency_hours_are_enforced() {
    let mut w = boot(demo_risk_config());
    let evening = ymd_hms_ist_to_unix_ms(2026, 1, 7, 18, 0, 0);
    feed(&mut w, "MCX:CO:CRUDEOIL25NOVFUT", "6500", 1, evening);
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, evening);
    w.handle(Command::OrderIntent(OrderIntent {
        product: Product::Nrml,
        ..intent(
            "crude1",
            "MCX:CO:CRUDEOIL25NOVFUT",
            Side::Buy,
            1,
            parse_scaled("6500").unwrap(),
            evening,
        )
    }))
    .unwrap();
    assert_eq!(w.state.orders["crude1"].state, OrderState::RiskApproved);
    w.handle(Command::OrderIntent(intent(
        "cash-eve",
        "NSE:CM:RELIANCE",
        Side::Buy,
        1,
        parse_scaled("2500").unwrap(),
        evening,
    )))
    .unwrap();
    assert_eq!(
        w.state.orders["cash-eve"].reject_rule.as_deref(),
        Some("TRADING_HOURS")
    );

    let early = ymd_hms_ist_to_unix_ms(2026, 1, 7, 8, 30, 0);
    feed(&mut w, "NSE:CD:USDINR25OCTFUT", "84", 1, early);
    w.handle(Command::OrderIntent(OrderIntent {
        product: Product::Nrml,
        ..intent(
            "fx-early",
            "NSE:CD:USDINR25OCTFUT",
            Side::Buy,
            1,
            parse_scaled("84").unwrap(),
            early,
        )
    }))
    .unwrap();
    assert_eq!(
        w.state.orders["fx-early"].reject_rule.as_deref(),
        Some("TRADING_HOURS")
    );
}

#[test]
fn freeze_qty_is_sliced_and_each_slice_is_risk_checked() {
    let mut cfg = demo_risk_config();
    cfg.max_order_value = parse_scaled("500000000").unwrap();
    cfg.max_exposure = parse_scaled("500000000").unwrap();
    cfg.fat_finger_notional = parse_scaled("500000000").unwrap();
    cfg.max_margin = parse_scaled("500000000").unwrap();
    cfg.account_daily_loss = parse_scaled("500000000").unwrap();
    for s in &mut cfg.strategies {
        s.daily_loss = parse_scaled("500000000").unwrap();
        s.max_order_qty = 100_000;
        s.max_ops = 20;
    }
    let wall = open_ms();
    let mut w = boot(cfg);
    feed(&mut w, "NSE:FO:NIFTY25OCTFUT", "25000", 1, wall);
    let effects = w
        .handle(Command::OrderIntent(OrderIntent {
            product: Product::Nrml,
            ..intent(
                "nifty",
                "NSE:FO:NIFTY25OCTFUT",
                Side::Buy,
                3675,
                parse_scaled("25000").unwrap(),
                wall,
            )
        }))
        .unwrap();
    let sends = effects
        .iter()
        .filter(|e| matches!(e, Effect::Send(_)))
        .count();
    assert_eq!(sends, 3);
    assert!(w.state.orders.contains_key("nifty-1"));
    assert!(w.state.orders.contains_key("nifty-2"));
    assert!(w.state.orders.contains_key("nifty-3"));
    assert!(w
        .state
        .orders
        .values()
        .filter(|o| o.intent_id == "nifty")
        .all(|o| o.risk_approved && o.qty <= 1800));
}

#[test]
fn kill_switch_withdraws_unsent_orders_and_blocks_new_ones() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "live1", 1, "2500", wall);
    let effects = w
        .handle(Command::Kill {
            scope: KillScope::Strategy {
                strategy_id: "manual".into(),
            },
            reason: "operator".into(),
            meta: meta(wall, 70),
        })
        .unwrap();
    assert!(effects.iter().all(|e| !matches!(e, Effect::Send(_))));
    assert_eq!(w.state.orders["live1"].state, OrderState::Cancelled);
    place(&mut w, "after", 1, "2500", wall);
    assert_eq!(w.state.orders["after"].state, OrderState::RiskRejected);
    assert_eq!(
        w.state.orders["after"].reject_rule.as_deref(),
        Some("KILL_SWITCH")
    );
}

#[test]
fn unknown_not_found_resends_once_with_the_same_id() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 1, "2500", wall);
    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 80),
    })
    .unwrap();
    w.handle(Command::BrokerDisconnect {
        meta: meta(wall + 1, 81),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].state, OrderState::Unknown);
    let effects = w
        .handle(Command::ResolveUnknown {
            client_order_id: "ord1".into(),
            view: BrokerView::NotFound,
            meta: meta(wall + 2, 82),
        })
        .unwrap();
    assert_eq!(w.state.orders["ord1"].state, OrderState::Sent);
    assert_eq!(w.state.orders["ord1"].send_attempts, 2);
    match &effects[0] {
        Effect::Send(o) => assert_eq!(o.client_order_id, "ord1"),
        other => panic!("expected resend, got {other:?}"),
    }
    w.handle(Command::DispatchFailed {
        client_order_id: "ord1".into(),
        detail: "socket reset".into(),
        meta: meta(wall + 3, 83),
    })
    .unwrap();
    w.handle(Command::ResolveUnknown {
        client_order_id: "ord1".into(),
        view: BrokerView::NotFound,
        meta: meta(wall + 4, 84),
    })
    .unwrap();
    assert!(w.state.halt.is_some(), "second resend must fail closed");
    assert_eq!(w.state.orders["ord1"].client_order_id, "ord1");
}

#[test]
fn config_reload_requires_a_higher_version() {
    let mut w = boot(demo_risk_config());
    let mut next = demo_risk_config();
    next.version = 1;
    assert!(w
        .handle(Command::ApplyConfig {
            config: next.clone(),
            meta: meta(1, 1),
        })
        .is_err());
    assert_eq!(w.state.risk.as_ref().unwrap().version, 1);
    next.version = 2;
    next.account_ops = 7;
    w.handle(Command::ApplyConfig {
        config: next,
        meta: meta(2, 2),
    })
    .unwrap();
    assert_eq!(w.state.risk.as_ref().unwrap().account_ops, 7);
}

#[test]
fn margin_hook_fail_closed_when_unavailable() {
    let mut cfg = demo_risk_config();
    cfg.margin = MarginModel::Unavailable;
    let wall = open_ms();
    let mut w = boot(cfg);
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "m1", 1, "2500", wall);
    assert_eq!(
        w.state.orders["m1"].reject_rule.as_deref(),
        Some("MARGIN_UNAVAILABLE")
    );
}

#[test]
fn replace_and_cancel_follow_the_table() {
    let wall = open_ms();
    let mut w = boot(demo_risk_config());
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 2, "2500", wall);
    w.handle(Command::MarkDispatched {
        client_order_id: "ord1".into(),
        meta: meta(wall, 90),
    })
    .unwrap();
    w.handle(Command::BrokerAck {
        client_order_id: "ord1".into(),
        broker_order_id: "B9".into(),
        meta: meta(wall, 91),
    })
    .unwrap();
    w.handle(Command::RequestReplace {
        client_order_id: "ord1".into(),
        new_qty: 2,
        new_price: parse_scaled("2510").unwrap(),
        meta: meta(wall, 92),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].state, OrderState::ReplacePending);
    w.handle(Command::ReplaceAck {
        client_order_id: "ord1".into(),
        new_broker_order_id: "B10".into(),
        meta: meta(wall, 93),
    })
    .unwrap();
    assert_eq!(w.state.orders["ord1"].state, OrderState::Replaced);
    assert!(w.state.orders["ord1"].state.is_terminal());
    let successor = w.state.orders["ord1"].replaced_by.clone().unwrap();
    assert_eq!(w.state.orders[&successor].state, OrderState::Acked);
    assert!(w.state.orders[&successor].risk_approved);

    w.handle(Command::RequestCancel {
        client_order_id: successor.clone(),
        meta: meta(wall, 94),
    })
    .unwrap();
    w.handle(Command::CancelReject {
        client_order_id: successor.clone(),
        reason: "exchange busy".into(),
        meta: meta(wall, 95),
    })
    .unwrap();
    assert_eq!(w.state.orders[&successor].state, OrderState::Acked);
    w.handle(Command::RequestCancel {
        client_order_id: successor.clone(),
        meta: meta(wall, 96),
    })
    .unwrap();
    w.handle(Command::CancelAck {
        client_order_id: successor.clone(),
        meta: meta(wall, 97),
    })
    .unwrap();
    assert_eq!(w.state.orders[&successor].state, OrderState::Cancelled);
    assert!(w
        .handle(Command::RequestCancel {
            client_order_id: successor,
            meta: meta(wall, 98),
        })
        .is_err());
}

#[test]
fn option_contract_passes_risk_with_strike_metadata() {
    let wall = open_ms();
    let mut cfg = demo_risk_config();
    cfg.fat_finger_price_bps = 10_000;
    let mut w = boot(cfg);
    let inst = w
        .state
        .instruments
        .get("NSE:FO:NIFTY25OCT24000CE")
        .unwrap()
        .clone();
    assert_eq!(inst.option_type, Some(atp_core::OptionType::Call));
    assert_eq!(inst.strike, Some(parse_scaled("24000").unwrap()));
    feed(&mut w, "NSE:FO:NIFTY25OCT24000CE", "100", 1, wall);
    w.handle(Command::OrderIntent(OrderIntent {
        product: Product::Nrml,
        ..intent(
            "ce1",
            "NSE:FO:NIFTY25OCT24000CE",
            Side::Buy,
            75,
            parse_scaled("100").unwrap(),
            wall,
        )
    }))
    .unwrap();
    assert_eq!(w.state.orders["ce1"].state, OrderState::RiskApproved);
    assert_eq!(w.state.orders["ce1"].segment, atp_core::Segment::Fo);
}

#[test]
fn journal_file_roundtrip() {
    let wall = open_ms();
    let path = std::env::temp_dir().join(format!("atp-journal-{}-round.jsonl", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let mut w = PartitionWriter::open(&path, "ACC1").unwrap();
    w.handle(Command::LoadInstruments {
        instruments: sample_catalog(),
        meta: meta(0, 1),
    })
    .unwrap();
    w.handle(Command::ApplyConfig {
        config: demo_risk_config(),
        meta: meta(0, 2),
    })
    .unwrap();
    feed(&mut w, "NSE:CM:RELIANCE", "2500", 1, wall);
    place(&mut w, "ord1", 1, "2500", wall);
    let hash = state_hash(&w.state);
    drop(w);
    let reopened = PartitionWriter::open(&path, "ACC1").unwrap();
    assert_eq!(state_hash(&reopened.state), hash);
    let _ = std::fs::remove_file(&path);
}
