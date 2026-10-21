//! Pre-trade risk. Pure: the only clock is the intent's event time.
//! A failed check rejects the whole intent (no partial slices).

use crate::calendar::is_instrument_open;
use crate::pnl::unrealized;
use crate::state::{is_working, PartitionState};
use crate::types::{
    notional, MarginModel, OrderIntent, OrderType, RiskRule, Side, MAX_SLICES, RATE_WINDOW_MS,
};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RiskOutcome {
    Approve { slices: Vec<i64> },
    Reject { rule: RiskRule, detail: String },
}

fn reject(rule: RiskRule, detail: impl Into<String>) -> RiskOutcome {
    RiskOutcome::Reject {
        rule,
        detail: detail.into(),
    }
}

pub fn live_count(stamps: &[i64], now: i64) -> usize {
    let cutoff = now.checked_sub(RATE_WINDOW_MS).unwrap_or(i64::MIN);
    stamps.iter().filter(|t| **t > cutoff && **t <= now).count()
}

pub fn can_reserve(stamps: &[i64], now: i64, max: u32, n: u32) -> bool {
    if stamps.iter().any(|t| *t > now) {
        return false;
    }
    n <= max && live_count(stamps, now) as u32 + n <= max
}

/// Drop stamps outside the window and append `n` copies of `now` when the
/// reservation fits. Returns false without appending when it does not.
pub fn reserve(stamps: &mut Vec<i64>, now: i64, max: u32, n: u32) -> bool {
    let cutoff = now.checked_sub(RATE_WINDOW_MS).unwrap_or(i64::MIN);
    stamps.retain(|t| *t > cutoff);
    if !can_reserve(stamps, now, max, n) {
        return false;
    }
    stamps.extend(std::iter::repeat_n(now, n as usize));
    true
}

#[allow(clippy::too_many_arguments)]
pub fn reserve_all(
    account: &mut Vec<i64>,
    segment: &mut Vec<i64>,
    strategy: &mut Vec<i64>,
    now: i64,
    account_max: u32,
    segment_max: u32,
    strategy_max: u32,
    n: u32,
) -> bool {
    if !can_reserve(account, now, account_max, n)
        || !can_reserve(segment, now, segment_max, n)
        || !can_reserve(strategy, now, strategy_max, n)
    {
        return false;
    }
    let ok_a = reserve(account, now, account_max, n);
    let ok_s = reserve(segment, now, segment_max, n);
    let ok_t = reserve(strategy, now, strategy_max, n);
    debug_assert!(ok_a && ok_s && ok_t);
    true
}

pub fn plan_slices(qty: i64, freeze_qty: i64) -> Result<Vec<i64>, RiskOutcome> {
    if freeze_qty <= 0 || qty <= freeze_qty {
        return Ok(vec![qty]);
    }
    let mut out = Vec::new();
    let mut rem = qty;
    while rem > 0 {
        let q = rem.min(freeze_qty);
        out.push(q);
        rem -= q;
        if out.len() > MAX_SLICES {
            return Err(reject(
                RiskRule::SliceOverflow,
                format!("more than {MAX_SLICES} slices"),
            ));
        }
    }
    Ok(out)
}

pub fn margin_required(
    model: &MarginModel,
    segment_bps: Option<u32>,
    price: i64,
    qty: i64,
) -> Option<i128> {
    let _ = model;
    let bps = segment_bps? as i128;
    Some(price as i128 * qty as i128 * bps / 10_000)
}

fn prices_cross(
    side: Side,
    order_type: OrderType,
    price: i64,
    other_side: Side,
    other_type: OrderType,
    other_price: i64,
) -> bool {
    if side == other_side {
        return false;
    }
    if order_type == OrderType::Market || other_type == OrderType::Market {
        return true;
    }
    let (buy, sell) = if side == Side::Buy {
        (price, other_price)
    } else {
        (other_price, price)
    };
    buy >= sell
}

pub fn assess(state: &PartitionState, intent: &OrderIntent) -> RiskOutcome {
    let Some(cfg) = state.risk.as_ref() else {
        return reject(RiskRule::ConfigNotLoaded, "risk config is not loaded");
    };
    if intent.account_id != state.account_id || intent.account_id != cfg.account_id {
        return reject(
            RiskRule::AccountMismatch,
            "intent account does not match the partition",
        );
    }
    if state.halt.is_some() {
        return reject(
            RiskRule::PartitionHalted,
            state.halt.clone().unwrap_or_default(),
        );
    }
    if state.is_killed(
        &intent.strategy_id,
        &intent.instrument_id,
        &intent.account_id,
    ) {
        return reject(RiskRule::KillSwitch, "kill switch active for this scope");
    }
    let Some(reg) = cfg.strategy(&intent.strategy_id) else {
        return reject(RiskRule::StrategyNotRegistered, &intent.strategy_id);
    };
    if intent.algo_id.is_empty() || intent.algo_id != reg.algo_id {
        return reject(
            RiskRule::AlgoId,
            format!("algo id must equal registered {}", reg.algo_id),
        );
    }
    let Some(inst) = state.instruments.get(&intent.instrument_id) else {
        return reject(RiskRule::Instrument, "unknown instrument");
    };
    if state.has_unknown_order() {
        return reject(
            RiskRule::UncertainState,
            "an order is UNKNOWN; new risk is closed until reconciliation",
        );
    }
    if intent.qty <= 0 || intent.qty % inst.lot_size != 0 {
        return reject(
            RiskRule::LotSize,
            format!(
                "qty {} is not a positive multiple of lot {}",
                intent.qty, inst.lot_size
            ),
        );
    }
    if intent.qty > cfg.max_order_qty || intent.qty > reg.max_order_qty {
        return reject(RiskRule::MaxQty, "intent qty exceeds max order qty");
    }
    if intent.qty > cfg.fat_finger_qty {
        return reject(RiskRule::FatFingerQty, "qty exceeds fat-finger limit");
    }
    if !is_instrument_open(inst, intent.wall_ms, &cfg.holidays) {
        return reject(RiskRule::TradingHours, "segment is closed");
    }
    let Some(md) = state.market.get(&intent.instrument_id) else {
        return reject(RiskRule::MarketDataMissing, "no market data");
    };
    if md.gap {
        return reject(RiskRule::FeedGap, "market-data sequence gap");
    }
    if md.ltp <= 0 {
        return reject(RiskRule::MarketDataMissing, "ltp is not positive");
    }
    let age = intent.wall_ms - md.exchange_wall_ms;
    if age > cfg.stale_feed_ms {
        return reject(
            RiskRule::StaleFeed,
            format!("feed age {age}ms exceeds {}", cfg.stale_feed_ms),
        );
    }

    let limit_price = match intent.order_type {
        OrderType::Market => md.ltp,
        OrderType::Limit => intent.price,
    };
    if intent.order_type == OrderType::Limit {
        if intent.price <= 0 {
            return reject(RiskRule::InvalidPrice, "limit price must be positive");
        }
        if intent.price % inst.tick_size != 0 {
            return reject(RiskRule::TickSize, "price is off tick");
        }
        if intent.price < inst.band_low || intent.price > inst.band_high {
            return reject(RiskRule::PriceBand, "outside exchange price band");
        }
        if inst.circuit_bps > 0 {
            let delta = md.ltp as i128 * inst.circuit_bps as i128 / 10_000;
            let low = md.ltp as i128 - delta;
            let high = md.ltp as i128 + delta;
            if (intent.price as i128) < low || (intent.price as i128) > high {
                return reject(RiskRule::PriceBand, "outside LTP circuit collar");
            }
        }
        let diff = (intent.price - md.ltp).abs() as i128;
        let bps = diff * 10_000 / md.ltp as i128;
        if bps > cfg.fat_finger_price_bps as i128 {
            return reject(
                RiskRule::FatFingerPrice,
                format!("{bps} bps from LTP exceeds {}", cfg.fat_finger_price_bps),
            );
        }
    }

    let Some(intent_notional) = notional(limit_price, intent.qty) else {
        return reject(RiskRule::Overflow, "notional overflow");
    };
    if intent_notional > cfg.max_order_value as i128 {
        return reject(RiskRule::MaxOrderValue, "order value exceeds limit");
    }
    if intent_notional > cfg.fat_finger_notional as i128 {
        return reject(
            RiskRule::FatFingerNotional,
            "notional exceeds fat-finger limit",
        );
    }

    let slices = match plan_slices(intent.qty, inst.freeze_qty) {
        Ok(s) => s,
        Err(outcome) => return outcome,
    };
    for q in &slices {
        if *q > cfg.max_order_qty || *q > reg.max_order_qty {
            return reject(RiskRule::MaxQty, "slice exceeds max order qty");
        }
    }

    if let Err(outcome) = check_position_exposure_margin(state, intent, limit_price, intent.qty) {
        return outcome;
    }
    if let Err(outcome) = check_daily_loss(state, intent) {
        return outcome;
    }
    if let Err(outcome) = check_self_trade(state, intent) {
        return outcome;
    }

    let n = slices.len() as u32;
    let seg_key = inst.segment.as_str();
    let seg_stamps = state
        .rates
        .by_segment
        .get(seg_key)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let strat_stamps = state
        .rates
        .by_strategy
        .get(&intent.strategy_id)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if !can_reserve(&state.rates.account, intent.wall_ms, cfg.account_ops, n)
        || !can_reserve(seg_stamps, intent.wall_ms, cfg.segment_ops, n)
        || !can_reserve(strat_stamps, intent.wall_ms, reg.max_ops, n)
    {
        return reject(RiskRule::OrderRate, "order-rate window would be exceeded");
    }

    RiskOutcome::Approve { slices }
}

fn check_position_exposure_margin(
    state: &PartitionState,
    intent: &OrderIntent,
    price: i64,
    qty: i64,
) -> Result<(), RiskOutcome> {
    let cfg = state.risk.as_ref().expect("config checked");
    let key = crate::types::pos_key(&intent.strategy_id, &intent.instrument_id);
    let net = state.positions.get(&key).map(|p| p.net_qty).unwrap_or(0);
    let mut pending_signed: i64 = 0;
    for order in state.orders.values() {
        if order.instrument_id == intent.instrument_id && is_working(order.state) {
            pending_signed =
                pending_signed.saturating_add(order.side.sign().saturating_mul(order.leaves_qty()));
        }
    }
    let projected = net as i128 + pending_signed as i128 + intent.side.sign() as i128 * qty as i128;
    if projected.abs() > cfg.max_position_qty as i128 {
        return Err(reject(
            RiskRule::PositionLimit,
            "projected position exceeds limit",
        ));
    }

    let mut exposure = 0i128;
    for pos in state.positions.values() {
        if pos.net_qty == 0 {
            continue;
        }
        let Some(md) = state.market.get(&pos.instrument_id) else {
            return Err(reject(
                RiskRule::ExposureLimit,
                format!("cannot mark {}", pos.instrument_id),
            ));
        };
        exposure += md.ltp as i128 * pos.net_qty.unsigned_abs() as i128;
    }
    for order in state.orders.values() {
        if !is_working(order.state) {
            continue;
        }
        let px = if order.order_type == OrderType::Market {
            state
                .market
                .get(&order.instrument_id)
                .map(|m| m.ltp)
                .unwrap_or(0)
        } else {
            order.price
        };
        if px <= 0 {
            return Err(reject(RiskRule::ExposureLimit, "open order has no price"));
        }
        exposure += px as i128 * order.leaves_qty() as i128;
    }
    let Some(add) = notional(price, qty) else {
        return Err(reject(RiskRule::Overflow, "exposure overflow"));
    };
    if exposure + add > cfg.max_exposure as i128 {
        return Err(reject(RiskRule::ExposureLimit, "exposure limit"));
    }

    let model = &cfg.margin;
    let mut used = 0i128;
    for pos in state.positions.values() {
        if pos.net_qty == 0 {
            continue;
        }
        let Some(inst) = state.instruments.get(&pos.instrument_id) else {
            return Err(reject(
                RiskRule::MarginUnavailable,
                "position instrument missing",
            ));
        };
        let Some(md) = state.market.get(&pos.instrument_id) else {
            return Err(reject(RiskRule::MarginUnavailable, "cannot mark margin"));
        };
        let Some(m) = margin_required(
            model,
            model.segment_bps(inst.segment),
            md.ltp,
            pos.net_qty.abs(),
        ) else {
            return Err(reject(
                RiskRule::MarginUnavailable,
                "margin model unavailable",
            ));
        };
        used += m;
    }
    for order in state.orders.values() {
        if !is_working(order.state) {
            continue;
        }
        let Some(inst) = state.instruments.get(&order.instrument_id) else {
            return Err(reject(
                RiskRule::MarginUnavailable,
                "order instrument missing",
            ));
        };
        let px = if order.price > 0 { order.price } else { price };
        let Some(m) = margin_required(
            model,
            model.segment_bps(inst.segment),
            px,
            order.leaves_qty(),
        ) else {
            return Err(reject(
                RiskRule::MarginUnavailable,
                "margin model unavailable",
            ));
        };
        used += m;
    }
    let Some(inst) = state.instruments.get(&intent.instrument_id) else {
        return Err(reject(RiskRule::Instrument, "missing"));
    };
    let Some(add_m) = margin_required(model, model.segment_bps(inst.segment), price, qty) else {
        return Err(reject(
            RiskRule::MarginUnavailable,
            "margin model unavailable",
        ));
    };
    if used + add_m > cfg.max_margin as i128 {
        return Err(reject(RiskRule::Margin, "margin limit"));
    }
    Ok(())
}

fn check_daily_loss(state: &PartitionState, intent: &OrderIntent) -> Result<(), RiskOutcome> {
    let cfg = state.risk.as_ref().expect("config");
    let reg = cfg
        .strategy(intent.strategy_id.as_str())
        .expect("registered");
    let mut strat_pnl = 0i128;
    let mut account_pnl = 0i128;
    for pos in state.positions.values() {
        let ltp = state
            .market
            .get(&pos.instrument_id)
            .map(|m| m.ltp)
            .unwrap_or(0);
        let Some(u) = unrealized(pos, ltp) else {
            return Err(reject(
                RiskRule::StrategyDailyLoss,
                format!("cannot mark {}", pos.instrument_id),
            ));
        };
        let pnl = pos.daily_realized + u;
        account_pnl += pnl;
        if pos.strategy_id == intent.strategy_id {
            strat_pnl += pnl;
        }
    }
    let strat_loss = if strat_pnl < 0 { -strat_pnl } else { 0 };
    let acct_loss = if account_pnl < 0 { -account_pnl } else { 0 };
    if strat_loss >= reg.daily_loss as i128 {
        return Err(reject(
            RiskRule::StrategyDailyLoss,
            "strategy daily loss limit",
        ));
    }
    if acct_loss >= cfg.account_daily_loss as i128 {
        return Err(reject(
            RiskRule::AccountDailyLoss,
            "account daily loss limit",
        ));
    }
    Ok(())
}

fn check_self_trade(state: &PartitionState, intent: &OrderIntent) -> Result<(), RiskOutcome> {
    for order in state.orders.values() {
        if order.instrument_id != intent.instrument_id || !is_working(order.state) {
            continue;
        }
        if prices_cross(
            intent.side,
            intent.order_type,
            intent.price,
            order.side,
            order.order_type,
            order.price,
        ) {
            return Err(reject(
                RiskRule::SelfTrade,
                format!("would cross resting {}", order.client_order_id),
            ));
        }
    }
    Ok(())
}

/// Used by tests that inspect per-window counts after a series of reservations.
pub fn window_ok(stamps: &[i64], max: u32) -> bool {
    for t in stamps {
        if live_count(stamps, *t) as u32 > max {
            return false;
        }
    }
    true
}

pub fn segment_rates(map: &BTreeMap<String, Vec<i64>>) -> &BTreeMap<String, Vec<i64>> {
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn reservations_never_exceed_the_window(max in 1u32..8, steps in proptest::collection::vec((0i64..20_000, 1u32..4), 1..40)) {
            let mut stamps = Vec::new();
            let mut now = 0i64;
            for (delta, n) in steps {
                now += delta;
                let before = stamps.clone();
                let ok = reserve(&mut stamps, now, max, n);
                if ok {
                    prop_assert!(window_ok(&stamps, max));
                    prop_assert!(live_count(&stamps, now) as u32 <= max);
                } else {
                    prop_assert_eq!(stamps.iter().filter(|t| **t > now - 1000).count(), before.iter().filter(|t| **t > now - 1000).count());
                }
            }
        }
    }

    #[test]
    fn window_boundary_frees_a_slot() {
        let mut stamps = Vec::new();
        assert!(reserve(&mut stamps, 0, 1, 1));
        assert!(!reserve(&mut stamps, 999, 1, 1));
        assert!(reserve(&mut stamps, 1000, 1, 1));
        assert_eq!(stamps, vec![1000]);
    }
}
