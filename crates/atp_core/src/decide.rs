//! `decide` proposes events. `apply` folds them. Neither performs I/O.

use crate::config::RiskConfig;
use crate::events::{Command, Event};
use crate::instrument::Instrument;
use crate::oms::{next_state, requires_prior_risk_approval, IllegalTransition, Trigger};
use crate::pnl::{self, Position};
use crate::rms::{self, reserve_all, RiskOutcome};
use crate::state::{is_working, PartitionState};
use crate::types::{
    pos_key, BrokerView, CancelRequest, Hop, HopStage, KillScope, Order, OrderIntent, OrderState,
    OrderType, OutboundOrder, ReplaceRequest, RiskRule, Side, Validity, MAX_SLICES,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("{0}")]
pub struct CoreError(pub String);

impl CoreError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl From<IllegalTransition> for CoreError {
    fn from(value: IllegalTransition) -> Self {
        Self(value.to_string())
    }
}

pub fn decide(state: &PartitionState, cmd: &Command) -> Result<Vec<Event>, CoreError> {
    match cmd {
        Command::LoadInstruments { instruments, .. } => decide_instruments(instruments),
        Command::ApplyConfig { config, .. } => decide_config(state, config),
        Command::Compliance {
            static_ip,
            audit_retention_days,
            algo_ids,
            ..
        } => Ok(vec![Event::ComplianceNoted {
            static_ip: static_ip.clone(),
            audit_retention_days: *audit_retention_days,
            algo_ids: {
                let mut ids = algo_ids.clone();
                ids.sort();
                ids.dedup();
                ids
            },
        }]),
        Command::MarketData {
            instrument_id,
            ltp,
            bid,
            ask,
            source_seq,
            exchange_wall_ms,
            resync,
            ..
        } => decide_md(
            state,
            instrument_id,
            *ltp,
            *bid,
            *ask,
            *source_seq,
            *exchange_wall_ms,
            *resync,
        ),
        Command::Signal {
            strategy_id,
            name,
            instrument_id,
            note,
            meta,
        } => Ok(vec![Event::SignalRecorded {
            strategy_id: strategy_id.clone(),
            name: name.clone(),
            instrument_id: instrument_id.clone(),
            note: note.clone(),
            correlation_id: meta.correlation_id.clone(),
        }]),
        Command::OrderIntent(intent) => decide_intent(state, intent),
        Command::MarkDispatched {
            client_order_id,
            meta,
        } => decide_mark(state, client_order_id, meta.wall_ms, meta.mono_ns),
        Command::DispatchFailed {
            client_order_id,
            detail,
            meta,
        } => decide_unknown_from(
            state,
            client_order_id,
            detail,
            meta.wall_ms,
            &[OrderState::RiskApproved, OrderState::Sent],
        ),
        Command::BrokerAck {
            client_order_id,
            broker_order_id,
            meta,
        } => decide_ack(
            state,
            client_order_id,
            broker_order_id,
            meta.wall_ms,
            meta.mono_ns,
        ),
        Command::BrokerReject {
            client_order_id,
            reason,
            ..
        } => decide_broker_reject(state, client_order_id, reason),
        Command::Fill {
            client_order_id,
            trade_id,
            qty,
            price,
            meta,
        } => decide_fill(
            state,
            client_order_id,
            trade_id,
            *qty,
            *price,
            meta.wall_ms,
            meta.mono_ns,
        ),
        Command::CancelAck {
            client_order_id, ..
        } => decide_cancel_ack(state, client_order_id),
        Command::CancelReject {
            client_order_id,
            reason,
            ..
        } => decide_cancel_reject(state, client_order_id, reason),
        Command::ReplaceAck {
            client_order_id,
            new_broker_order_id,
            meta,
        } => decide_replace_ack(
            state,
            client_order_id,
            new_broker_order_id,
            meta.wall_ms,
            meta.mono_ns,
        ),
        Command::ReplaceReject {
            client_order_id,
            reason,
            ..
        } => decide_replace_reject(state, client_order_id, reason),
        Command::Expired {
            client_order_id, ..
        } => decide_expire(state, client_order_id),
        Command::RequestCancel {
            client_order_id,
            meta,
        } => decide_request_cancel(state, client_order_id, &meta.correlation_id),
        Command::RequestReplace {
            client_order_id,
            new_qty,
            new_price,
            meta,
        } => decide_request_replace(
            state,
            client_order_id,
            *new_qty,
            *new_price,
            &meta.correlation_id,
            meta.wall_ms,
        ),
        Command::Timer { meta } => decide_timer(state, meta.wall_ms),
        Command::Kill { scope, reason, .. } => Ok(decide_kill(state, scope, reason)),
        Command::ClearKill { scope, reason, .. } => Ok(vec![Event::KillCleared {
            scope: scope.clone(),
            reason: reason.clone(),
        }]),
        Command::Recon { report, .. } => Ok(decide_recon(state, report)),
        Command::SessionRoll { day, .. } => Ok(decide_session(state, *day)),
        Command::RecordHop {
            client_order_id,
            stage,
            meta,
        } => {
            let _ = require_order(state, client_order_id)?;
            Ok(vec![Event::HopRecorded {
                client_order_id: client_order_id.clone(),
                stage: *stage,
                wall_ms: meta.wall_ms,
                mono_ns: meta.mono_ns,
            }])
        }
        Command::ResolveUnknown {
            client_order_id,
            view,
            meta,
        } => decide_resolve(state, client_order_id, view, meta.wall_ms, meta.mono_ns),
        Command::BrokerDisconnect { meta } => Ok(decide_disconnect(state, meta.wall_ms)),
        Command::Resume { reason, .. } => {
            if state.halt.is_none() {
                Ok(vec![])
            } else {
                Ok(vec![Event::PartitionResumed {
                    reason: reason.clone(),
                }])
            }
        }
        Command::QueryFailed {
            client_order_id,
            meta,
        } => {
            let order = require_order(state, client_order_id)?;
            if order.state != OrderState::Unknown && !order.needs_query {
                return Err(CoreError::new(
                    "query failed for an order that is not awaiting a query",
                ));
            }
            Ok(vec![Event::QueryFailedNoted {
                client_order_id: client_order_id.clone(),
                wall_ms: meta.wall_ms,
            }])
        }
    }
}

pub fn apply(state: &mut PartitionState, event: &Event) -> Result<(), CoreError> {
    match event {
        Event::InstrumentsLoaded { instruments } => {
            let mut map = BTreeMap::new();
            for inst in instruments {
                inst.validate().map_err(CoreError::new)?;
                if map.insert(inst.id.clone(), inst.clone()).is_some() {
                    return Err(CoreError::new("duplicate instrument in event"));
                }
            }
            state.instruments = map;
        }
        Event::MarketDataUpdated {
            instrument_id,
            ltp,
            bid,
            ask,
            source_seq,
            exchange_wall_ms,
            gap,
        } => {
            state.market.insert(
                instrument_id.clone(),
                crate::state::MarketSnapshot {
                    instrument_id: instrument_id.clone(),
                    ltp: *ltp,
                    bid: *bid,
                    ask: *ask,
                    source_seq: *source_seq,
                    exchange_wall_ms: *exchange_wall_ms,
                    gap: *gap,
                },
            );
        }
        Event::SignalRecorded {
            strategy_id,
            name,
            instrument_id,
            note,
            correlation_id,
        } => {
            state.last_signals.insert(
                strategy_id.clone(),
                crate::state::SignalRecord {
                    strategy_id: strategy_id.clone(),
                    name: name.clone(),
                    instrument_id: instrument_id.clone(),
                    note: note.clone(),
                    correlation_id: correlation_id.clone(),
                },
            );
        }
        Event::OrderRegistered { order } => {
            if order.state != OrderState::New
                || order.filled_qty != 0
                || order.risk_approved
                || order.qty <= 0
                || !valid_clord(&order.client_order_id)
            {
                return Err(CoreError::new("refusing to register a malformed order"));
            }
            if state.orders.contains_key(&order.client_order_id) {
                return Err(CoreError::new("duplicate client order id"));
            }
            state.intent_ids.insert(order.intent_id.clone());
            state
                .orders
                .insert(order.client_order_id.clone(), order.clone());
        }
        Event::RiskRejected {
            client_order_id,
            rule,
            detail,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.state = next_state(order.state, Trigger::RiskReject)?;
            order.risk_approved = false;
            order.reject_rule = Some(rule.as_str().to_string());
            order.reject_detail = Some(detail.clone());
            let wall = order.created_wall_ms;
            let mono = order.created_mono_ns;
            order.push_hop(HopStage::Risk, wall, mono);
        }
        Event::RiskApproved {
            client_order_id,
            outbound,
        } => {
            if outbound.client_order_id != *client_order_id {
                return Err(CoreError::new("outbound id mismatch"));
            }
            let (segment, strategy, now) = {
                let order = require_order(state, client_order_id)?;
                (
                    order.segment.as_str().to_string(),
                    order.strategy_id.clone(),
                    order.created_wall_ms,
                )
            };
            consume_rate(state, &segment, &strategy, now)?;
            let order = order_mut(state, client_order_id)?;
            order.state = next_state(order.state, Trigger::RiskApprove)?;
            order.risk_approved = true;
            let wall = order.created_wall_ms;
            let mono = order.created_mono_ns;
            order.push_hop(HopStage::Risk, wall, mono);
        }
        Event::OrderSent {
            client_order_id,
            wall_ms,
            mono_ns,
        } => {
            let order = order_mut(state, client_order_id)?;
            if !order.risk_approved {
                return Err(CoreError::new("dispatch without risk approval"));
            }
            order.state = next_state(order.state, Trigger::Dispatch)?;
            order.send_attempts = order.send_attempts.saturating_add(1);
            order.push_hop(HopStage::Oms, *wall_ms, *mono_ns);
        }
        Event::OrderUnknown {
            client_order_id,
            reason: _,
            wall_ms,
        } => {
            let order = order_mut(state, client_order_id)?;
            let trigger = match order.state {
                OrderState::RiskApproved => Trigger::DispatchUncertain,
                OrderState::Sent | OrderState::CancelPending | OrderState::ReplacePending => {
                    Trigger::TimeoutUnknown
                }
                OrderState::Acked | OrderState::PartiallyFilled => Trigger::DisconnectUnknown,
                _ => return Err(CoreError::new("cannot mark this order unknown")),
            };
            order.state = next_state(order.state, trigger)?;
            order.needs_query = true;
            order.last_wall_ms = *wall_ms;
        }
        Event::BrokerAck {
            client_order_id,
            broker_order_id,
            wall_ms,
            mono_ns,
        } => apply_ack(state, client_order_id, broker_order_id, *wall_ms, *mono_ns)?,
        Event::BrokerReject {
            client_order_id,
            reason,
        } => {
            let order = order_mut(state, client_order_id)?;
            if order.filled_qty > 0 {
                return Err(CoreError::new("reject after fills"));
            }
            order.state = next_state(order.state, Trigger::BrokerReject)?;
            order.reject_detail = Some(reason.clone());
            order.needs_query = false;
        }
        Event::FillApplied {
            client_order_id,
            trade_id,
            qty,
            price,
            wall_ms,
            mono_ns,
        } => apply_fill(
            state,
            client_order_id,
            trade_id,
            *qty,
            *price,
            *wall_ms,
            *mono_ns,
        )?,
        Event::DuplicateFillIgnored {
            client_order_id,
            trade_id,
        } => {
            let order = require_order(state, client_order_id)?;
            if !order.fill_trade_ids.contains(trade_id) {
                return Err(CoreError::new("duplicate-fill event for an unseen trade"));
            }
        }
        Event::CancelRequested {
            client_order_id,
            request,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.state_before_pending = Some(order.state);
            order.state = next_state(order.state, Trigger::CancelRequest)?;
            order.correlation_id = request.correlation_id.clone();
        }
        Event::CancelConfirmed { client_order_id } => {
            let order = order_mut(state, client_order_id)?;
            let trigger = match order.state {
                OrderState::CancelPending | OrderState::Unknown => Trigger::CancelConfirm,
                _ => {
                    return Err(CoreError::new(
                        "cancel confirm in a state that is not pending",
                    ))
                }
            };
            order.state = next_state(order.state, trigger)?;
            order.needs_query = false;
        }
        Event::CancelRejected {
            client_order_id,
            reason,
        } => {
            let order = order_mut(state, client_order_id)?;
            let trigger = if order.filled_qty > 0 {
                Trigger::CancelRejectPartial
            } else {
                match order.state_before_pending {
                    Some(OrderState::Sent) => Trigger::CancelRejectSent,
                    Some(OrderState::Acked) | Some(OrderState::PartiallyFilled) | None => {
                        Trigger::CancelRejectAcked
                    }
                    Some(_) => Trigger::CancelRejectAcked,
                }
            };
            order.state = next_state(order.state, trigger)?;
            order.reject_detail = Some(reason.clone());
            order.state_before_pending = None;
        }
        Event::ReplaceRequested {
            client_order_id,
            new_qty,
            new_price,
            wall_ms,
            ..
        } => {
            let (segment, strategy) = {
                let order = require_order(state, client_order_id)?;
                (
                    order.segment.as_str().to_string(),
                    order.strategy_id.clone(),
                )
            };
            consume_rate(state, &segment, &strategy, *wall_ms)?;
            let order = order_mut(state, client_order_id)?;
            order.state_before_pending = Some(order.state);
            order.pending_replace_qty = Some(*new_qty);
            order.pending_replace_price = Some(*new_price);
            order.state = next_state(order.state, Trigger::ReplaceRequest)?;
        }
        Event::ReplaceConfirmed {
            client_order_id,
            successor,
        } => {
            if state.orders.contains_key(&successor.client_order_id) {
                return Err(CoreError::new("successor id already exists"));
            }
            if successor.state != OrderState::Acked
                || !successor.risk_approved
                || successor.filled_qty != 0
            {
                return Err(CoreError::new("bad successor"));
            }
            let order = order_mut(state, client_order_id)?;
            order.state = next_state(order.state, Trigger::ReplaceConfirm)?;
            order.replaced_by = Some(successor.client_order_id.clone());
            order.pending_replace_qty = None;
            order.pending_replace_price = None;
            order.needs_query = false;
            state.intent_ids.insert(successor.intent_id.clone());
            state
                .orders
                .insert(successor.client_order_id.clone(), successor.clone());
        }
        Event::ReplaceRejected {
            client_order_id,
            reason,
        } => {
            let order = order_mut(state, client_order_id)?;
            let trigger = if order.filled_qty > 0 {
                Trigger::ReplaceRejectPartial
            } else {
                Trigger::ReplaceRejectAcked
            };
            order.state = next_state(order.state, trigger)?;
            order.pending_replace_qty = None;
            order.pending_replace_price = None;
            order.reject_detail = Some(reason.clone());
            order.state_before_pending = None;
        }
        Event::ReplaceRiskRejected {
            client_order_id,
            rule,
            detail,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.reject_rule = Some(rule.as_str().to_string());
            order.reject_detail = Some(detail.clone());
        }
        Event::OrderExpired { client_order_id } => {
            let order = order_mut(state, client_order_id)?;
            order.state = next_state(order.state, Trigger::Expire)?;
            order.needs_query = false;
        }
        Event::LocalWithdraw {
            client_order_id,
            reason,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.state = next_state(order.state, Trigger::Withdraw)?;
            order.reject_detail = Some(reason.clone());
        }
        Event::UnknownResolved {
            client_order_id,
            resolved_to,
            broker_order_id,
            detail,
        } => {
            let trigger = recon_trigger(*resolved_to).ok_or_else(|| {
                CoreError::new(format!("cannot resolve UNKNOWN to {resolved_to}"))
            })?;
            let order = order_mut(state, client_order_id)?;
            order.state = next_state(order.state, trigger)?;
            if let Some(id) = broker_order_id {
                order.broker_order_id = Some(id.clone());
            }
            order.needs_query = false;
            order.pending_resend = false;
            order.reject_detail = Some(detail.clone());
        }
        Event::ResendAuthorized {
            client_order_id,
            outbound,
            wall_ms,
        } => {
            if outbound.client_order_id != *client_order_id {
                return Err(CoreError::new("resend id mismatch"));
            }
            let (segment, strategy, attempts) = {
                let order = require_order(state, client_order_id)?;
                (
                    order.segment.as_str().to_string(),
                    order.strategy_id.clone(),
                    order.send_attempts,
                )
            };
            if attempts == 0 || !require_order(state, client_order_id)?.risk_approved {
                return Err(CoreError::new("resend without a prior risk-approved send"));
            }
            consume_rate(state, &segment, &strategy, *wall_ms)?;
            let order = order_mut(state, client_order_id)?;
            if order.filled_qty != 0 {
                return Err(CoreError::new(
                    "refusing to resend an order that already filled",
                ));
            }
            order.state = next_state(order.state, Trigger::Resend)?;
            order.send_attempts = order.send_attempts.saturating_add(1);
            order.pending_resend = false;
            order.needs_query = false;
            order.last_wall_ms = *wall_ms;
        }
        Event::ResendDeferred { client_order_id } => {
            let order = order_mut(state, client_order_id)?;
            if order.state != OrderState::Unknown {
                return Err(CoreError::new("resend deferred but order is not UNKNOWN"));
            }
            order.pending_resend = true;
            order.needs_query = false;
        }
        Event::QueryRequested {
            client_order_id,
            wall_ms,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.needs_query = true;
            order.last_wall_ms = *wall_ms;
        }
        Event::QueryFailedNoted {
            client_order_id,
            wall_ms,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.needs_query = true;
            order.last_wall_ms = *wall_ms;
        }
        Event::ConfigApplied { config } => {
            config.validate().map_err(CoreError::new)?;
            state.risk = Some(config.clone());
        }
        Event::KillApplied { scope, .. } => apply_kill_flag(&mut state.kills, scope, true),
        Event::KillCleared { scope, .. } => apply_kill_flag(&mut state.kills, scope, false),
        Event::PartitionHalted { reason } => {
            if state.halt.is_none() {
                state.halt = Some(reason.clone());
            }
        }
        Event::PartitionResumed { .. } => state.halt = None,
        Event::ReconOk { .. } => {}
        Event::SessionRolled { day } => {
            state.session_day = *day;
            for pos in state.positions.values_mut() {
                pos.daily_realized = 0;
            }
        }
        Event::HopRecorded {
            client_order_id,
            stage,
            wall_ms,
            mono_ns,
        } => {
            let order = order_mut(state, client_order_id)?;
            order.push_hop(*stage, *wall_ms, *mono_ns);
        }
        Event::ComplianceNoted {
            static_ip,
            audit_retention_days,
            algo_ids,
        } => {
            state.compliance = Some(crate::state::ComplianceRecord {
                static_ip: static_ip.clone(),
                audit_retention_days: *audit_retention_days,
                algo_ids: algo_ids.clone(),
            });
        }
    }
    Ok(())
}

pub fn check_invariants(state: &PartitionState) -> Result<(), CoreError> {
    for order in state.orders.values() {
        if order.filled_qty < 0 || order.filled_qty > order.qty || order.qty <= 0 {
            return Err(CoreError::new(format!(
                "{} filled qty {} / {}",
                order.client_order_id, order.filled_qty, order.qty
            )));
        }
        if order.state == OrderState::Filled && order.filled_qty != order.qty {
            return Err(CoreError::new("FILLED order is not fully filled"));
        }
        if order.filled_qty == order.qty && order.state != OrderState::Filled {
            return Err(CoreError::new(format!(
                "{} fully filled but state is {}",
                order.client_order_id, order.state
            )));
        }
        if order.state == OrderState::PartiallyFilled
            && (order.filled_qty == 0 || order.filled_qty >= order.qty)
        {
            return Err(CoreError::new("partial state has a bad quantity"));
        }
        if matches!(
            order.state,
            OrderState::New | OrderState::RiskApproved | OrderState::Sent | OrderState::Acked
        ) && order.filled_qty != 0
        {
            return Err(CoreError::new("unfilled state has fills"));
        }
        if order.state == OrderState::Rejected && order.filled_qty != 0 {
            return Err(CoreError::new("rejected order has fills"));
        }
        if requires_prior_risk_approval(order.state) && !order.risk_approved {
            return Err(CoreError::new(format!(
                "{} reached {} without risk approval",
                order.client_order_id, order.state
            )));
        }
        if order.state == OrderState::RiskRejected && order.risk_approved {
            return Err(CoreError::new("risk-rejected order is marked approved"));
        }
        if !state.intent_ids.contains(&order.intent_id) {
            return Err(CoreError::new("order intent id missing from the set"));
        }
        if order.filled_qty > 0 {
            let avg = (order.filled_notional / i128::from(order.filled_qty)) as i64;
            if avg != order.avg_fill_price {
                return Err(CoreError::new("average fill price drifted"));
            }
        }
    }
    if let Some(cfg) = &state.risk {
        if !rms::window_ok(&state.rates.account, cfg.account_ops) {
            return Err(CoreError::new("account order rate exceeded"));
        }
        for stamps in state.rates.by_segment.values() {
            if !rms::window_ok(stamps, cfg.segment_ops) {
                return Err(CoreError::new("segment order rate exceeded"));
            }
        }
        for (id, stamps) in &state.rates.by_strategy {
            if let Some(reg) = cfg.strategy(id) {
                if !rms::window_ok(stamps, reg.max_ops) {
                    return Err(CoreError::new("strategy order rate exceeded"));
                }
            }
        }
    }
    Ok(())
}

fn decide_instruments(instruments: &[Instrument]) -> Result<Vec<Event>, CoreError> {
    let mut ordered = instruments.to_vec();
    ordered.sort_by(|a, b| a.id.cmp(&b.id));
    let mut seen = BTreeSet::new();
    for inst in &ordered {
        inst.validate().map_err(CoreError::new)?;
        if !seen.insert(inst.id.clone()) {
            return Err(CoreError::new(format!("duplicate instrument {}", inst.id)));
        }
    }
    Ok(vec![Event::InstrumentsLoaded {
        instruments: ordered,
    }])
}

fn decide_config(state: &PartitionState, config: &RiskConfig) -> Result<Vec<Event>, CoreError> {
    config.validate().map_err(CoreError::new)?;
    if config.account_id != state.account_id {
        return Err(CoreError::new("config account does not match partition"));
    }
    if let Some(prev) = &state.risk {
        if config.version <= prev.version {
            return Err(CoreError::new(
                "config version must increase; keeping the previous limits",
            ));
        }
    }
    let mut config = config.clone();
    config
        .strategies
        .sort_by(|a, b| a.strategy_id.cmp(&b.strategy_id));
    config.holidays.sort_unstable();
    config.holidays.dedup();
    Ok(vec![Event::ConfigApplied { config }])
}

#[allow(clippy::too_many_arguments)]
fn decide_md(
    state: &PartitionState,
    instrument_id: &str,
    ltp: i64,
    bid: i64,
    ask: i64,
    source_seq: u64,
    exchange_wall_ms: i64,
    resync: bool,
) -> Result<Vec<Event>, CoreError> {
    if !state.instruments.contains_key(instrument_id) {
        return Err(CoreError::new(format!(
            "unknown instrument {instrument_id}"
        )));
    }
    if ltp <= 0 {
        return Err(CoreError::new("ltp must be positive"));
    }
    let gap = if resync {
        false
    } else if let Some(prev) = state.market.get(instrument_id) {
        prev.gap || prev.source_seq.saturating_add(1) != source_seq
    } else {
        false
    };
    Ok(vec![Event::MarketDataUpdated {
        instrument_id: instrument_id.to_string(),
        ltp,
        bid,
        ask,
        source_seq,
        exchange_wall_ms,
        gap,
    }])
}

fn decide_intent(state: &PartitionState, intent: &OrderIntent) -> Result<Vec<Event>, CoreError> {
    if state.intent_ids.contains(&intent.intent_id) {
        return Ok(vec![]);
    }
    if !valid_clord(&intent.intent_id) {
        return Err(CoreError::new("intent id must be 1-20 chars [A-Za-z0-9-]"));
    }
    match rms::assess(state, intent) {
        RiskOutcome::Reject { rule, detail } => {
            let segment = state
                .instruments
                .get(&intent.instrument_id)
                .map(|inst| inst.segment)
                .unwrap_or(crate::types::Segment::Cm);
            let order = new_order(intent, &intent.intent_id, 0, 1, intent.qty, segment);
            Ok(vec![
                Event::OrderRegistered { order },
                Event::RiskRejected {
                    client_order_id: intent.intent_id.clone(),
                    rule,
                    detail,
                },
            ])
        }
        RiskOutcome::Approve { slices } => {
            if slices.len() > MAX_SLICES {
                return Err(CoreError::new("slice plan too large"));
            }
            let mut events = Vec::with_capacity(slices.len() * 2);
            for (index, qty) in slices.iter().copied().enumerate() {
                let clord = if slices.len() == 1 {
                    intent.intent_id.clone()
                } else {
                    format!("{}-{}", intent.intent_id, index + 1)
                };
                if !valid_clord(&clord) {
                    return Err(CoreError::new(format!(
                        "client order id {clord} is not valid"
                    )));
                }
                let segment = state
                    .instruments
                    .get(&intent.instrument_id)
                    .ok_or_else(|| CoreError::new("instrument disappeared"))?
                    .segment;
                let order = new_order(
                    intent,
                    &clord,
                    index as u32,
                    slices.len() as u32,
                    qty,
                    segment,
                );
                let outbound = outbound_from(&order, state)?;
                events.push(Event::OrderRegistered { order });
                events.push(Event::RiskApproved {
                    client_order_id: clord,
                    outbound,
                });
            }
            Ok(events)
        }
    }
}

fn decide_mark(
    state: &PartitionState,
    id: &str,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if order.state == OrderState::RiskApproved {
        return Ok(vec![Event::OrderSent {
            client_order_id: id.to_string(),
            wall_ms,
            mono_ns,
        }]);
    }
    if order.send_attempts > 0 {
        return Ok(vec![]);
    }
    Err(CoreError::new(format!(
        "cannot mark {id} dispatched from {}",
        order.state
    )))
}

fn decide_unknown_from(
    state: &PartitionState,
    id: &str,
    reason: &str,
    wall_ms: i64,
    allowed: &[OrderState],
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if order.state == OrderState::Unknown {
        return Ok(vec![]);
    }
    if !allowed.contains(&order.state) {
        return Err(CoreError::new(format!(
            "cannot move {} from {} to UNKNOWN",
            id, order.state
        )));
    }
    Ok(vec![Event::OrderUnknown {
        client_order_id: id.to_string(),
        reason: reason.to_string(),
        wall_ms,
    }])
}

fn decide_ack(
    state: &PartitionState,
    id: &str,
    broker_order_id: &str,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if broker_order_id.is_empty() {
        return Err(CoreError::new("empty broker order id"));
    }
    if let Some(existing) = &order.broker_order_id {
        if existing != broker_order_id {
            return Ok(halt(format!(
                "broker id conflict on {id}: {existing} vs {broker_order_id}"
            )));
        }
    }
    match order.state {
        OrderState::Sent
        | OrderState::Unknown
        | OrderState::Acked
        | OrderState::PartiallyFilled
        | OrderState::Filled
        | OrderState::CancelPending
        | OrderState::ReplacePending => Ok(vec![Event::BrokerAck {
            client_order_id: id.to_string(),
            broker_order_id: broker_order_id.to_string(),
            wall_ms,
            mono_ns,
        }]),
        _ => Ok(halt(format!("unsolicited ack for {id} in {}", order.state))),
    }
}

fn decide_broker_reject(
    state: &PartitionState,
    id: &str,
    reason: &str,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if order.filled_qty > 0 {
        return Ok(halt(format!("broker reject after fills on {id}: {reason}")));
    }
    match order.state {
        OrderState::Sent | OrderState::Unknown => Ok(vec![Event::BrokerReject {
            client_order_id: id.to_string(),
            reason: reason.to_string(),
        }]),
        OrderState::Rejected => Ok(vec![]),
        _ => Ok(halt(format!("broker reject for {id} in {}", order.state))),
    }
}

fn decide_fill(
    state: &PartitionState,
    id: &str,
    trade_id: &str,
    qty: i64,
    price: i64,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if trade_id.is_empty() {
        return Ok(halt("empty trade id"));
    }
    if order.fill_trade_ids.contains(trade_id) {
        return Ok(vec![Event::DuplicateFillIgnored {
            client_order_id: id.to_string(),
            trade_id: trade_id.to_string(),
        }]);
    }
    if state.seen_trade_ids.contains(trade_id) {
        return Ok(halt(format!("trade id {trade_id} already used")));
    }
    if qty <= 0 || price <= 0 {
        return Ok(halt(format!("bad fill qty/price on {id}")));
    }
    if order.filled_qty.saturating_add(qty) > order.qty {
        return Ok(halt(format!(
            "excess fill on {id}: have {} plus {qty} over {}",
            order.filled_qty, order.qty
        )));
    }
    let complete = order.filled_qty + qty == order.qty;
    let allowed = if complete {
        next_state(order.state, Trigger::FillComplete).is_ok()
    } else if matches!(
        order.state,
        OrderState::CancelPending | OrderState::ReplacePending
    ) {
        true
    } else {
        next_state(order.state, Trigger::FillPartial).is_ok()
    };
    if !allowed {
        return Ok(halt(format!(
            "fill not applicable to {id} in {}",
            order.state
        )));
    }
    Ok(vec![Event::FillApplied {
        client_order_id: id.to_string(),
        trade_id: trade_id.to_string(),
        qty,
        price,
        wall_ms,
        mono_ns,
    }])
}

fn decide_cancel_ack(state: &PartitionState, id: &str) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    match order.state {
        OrderState::CancelPending | OrderState::Unknown => Ok(vec![Event::CancelConfirmed {
            client_order_id: id.to_string(),
        }]),
        OrderState::Cancelled => Ok(vec![]),
        _ => Ok(halt(format!("cancel ack for {id} in {}", order.state))),
    }
}

fn decide_cancel_reject(
    state: &PartitionState,
    id: &str,
    reason: &str,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if order.state != OrderState::CancelPending {
        return Ok(halt(format!("cancel reject for {id} in {}", order.state)));
    }
    Ok(vec![Event::CancelRejected {
        client_order_id: id.to_string(),
        reason: reason.to_string(),
    }])
}

fn decide_replace_ack(
    state: &PartitionState,
    id: &str,
    broker_id: &str,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if order.state != OrderState::ReplacePending {
        return Ok(halt(format!("replace ack for {id} in {}", order.state)));
    }
    let (new_qty, new_price) = match (order.pending_replace_qty, order.pending_replace_price) {
        (Some(q), Some(p)) => (q, p),
        _ => return Ok(halt(format!("replace ack without pending terms on {id}"))),
    };
    if new_qty <= order.filled_qty {
        return Ok(halt("replace qty does not exceed filled qty"));
    }
    let successor = successor_order(order, new_qty, new_price, broker_id, wall_ms, mono_ns)?;
    Ok(vec![Event::ReplaceConfirmed {
        client_order_id: id.to_string(),
        successor,
    }])
}

fn decide_replace_reject(
    state: &PartitionState,
    id: &str,
    reason: &str,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if order.state != OrderState::ReplacePending {
        return Ok(halt(format!("replace reject for {id} in {}", order.state)));
    }
    Ok(vec![Event::ReplaceRejected {
        client_order_id: id.to_string(),
        reason: reason.to_string(),
    }])
}

fn decide_expire(state: &PartitionState, id: &str) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if next_state(order.state, Trigger::Expire).is_err() {
        return Ok(halt(format!("cannot expire {id} from {}", order.state)));
    }
    Ok(vec![Event::OrderExpired {
        client_order_id: id.to_string(),
    }])
}

fn decide_request_cancel(
    state: &PartitionState,
    id: &str,
    correlation_id: &str,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if next_state(order.state, Trigger::CancelRequest).is_err() {
        return Err(CoreError::new(format!(
            "cannot cancel {id} from {}",
            order.state
        )));
    }
    Ok(vec![Event::CancelRequested {
        client_order_id: id.to_string(),
        request: CancelRequest {
            client_order_id: id.to_string(),
            broker_order_id: order.broker_order_id.clone(),
            algo_id: order.algo_id.clone(),
            correlation_id: correlation_id.to_string(),
        },
    }])
}

fn decide_request_replace(
    state: &PartitionState,
    id: &str,
    new_qty: i64,
    new_price: i64,
    correlation_id: &str,
    wall_ms: i64,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    if next_state(order.state, Trigger::ReplaceRequest).is_err() {
        return Err(CoreError::new(format!(
            "cannot replace {id} from {}",
            order.state
        )));
    }
    if new_qty <= order.filled_qty || new_price <= 0 {
        return Ok(vec![Event::ReplaceRiskRejected {
            client_order_id: id.to_string(),
            rule: RiskRule::InvalidPrice,
            detail: "replace qty/price invalid".into(),
        }]);
    }
    let mut probe = order_as_intent(order, new_qty, new_price, wall_ms);
    // A replace of an existing working order must not trip the self-trade check
    // against itself. assess() sees the resting order. Drop it from a clone.
    let mut shadow = state.clone();
    shadow.orders.remove(id);
    probe.qty = new_qty - order.filled_qty;
    probe.intent_id = format!("{}r{}", order.client_order_id, order.replace_seq + 1);
    match rms::assess(&shadow, &probe) {
        RiskOutcome::Reject { rule, detail } => Ok(vec![Event::ReplaceRiskRejected {
            client_order_id: id.to_string(),
            rule,
            detail,
        }]),
        RiskOutcome::Approve { .. } => {
            let cfg = state
                .risk
                .as_ref()
                .ok_or_else(|| CoreError::new("no config"))?;
            let reg = cfg
                .strategy(&order.strategy_id)
                .ok_or_else(|| CoreError::new("strategy missing"))?;
            let seg = order.segment.as_str();
            let seg_stamps = state
                .rates
                .by_segment
                .get(seg)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let strat_stamps = state
                .rates
                .by_strategy
                .get(&order.strategy_id)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            if !rms::can_reserve(&state.rates.account, wall_ms, cfg.account_ops, 1)
                || !rms::can_reserve(seg_stamps, wall_ms, cfg.segment_ops, 1)
                || !rms::can_reserve(strat_stamps, wall_ms, reg.max_ops, 1)
            {
                return Ok(vec![Event::ReplaceRiskRejected {
                    client_order_id: id.to_string(),
                    rule: RiskRule::OrderRate,
                    detail: "replace would exceed order rate".into(),
                }]);
            }
            let successor_id = format!("{}r{}", order.client_order_id, order.replace_seq + 1);
            if !valid_clord(&successor_id) {
                return Err(CoreError::new("successor client order id is too long"));
            }
            Ok(vec![Event::ReplaceRequested {
                client_order_id: id.to_string(),
                new_qty,
                new_price,
                wall_ms,
                request: ReplaceRequest {
                    client_order_id: id.to_string(),
                    broker_order_id: order.broker_order_id.clone(),
                    new_client_order_id: successor_id,
                    new_qty,
                    new_price,
                    correlation_id: correlation_id.to_string(),
                },
            }])
        }
    }
}

fn decide_timer(state: &PartitionState, wall_ms: i64) -> Result<Vec<Event>, CoreError> {
    let Some(cfg) = &state.risk else {
        return Ok(vec![]);
    };
    let mut events = Vec::new();
    for order in state.orders.values() {
        let age = wall_ms.saturating_sub(order.last_wall_ms);
        match order.state {
            OrderState::Sent | OrderState::CancelPending | OrderState::ReplacePending
                if age >= cfg.unknown_timeout_ms =>
            {
                events.push(Event::OrderUnknown {
                    client_order_id: order.client_order_id.clone(),
                    reason: "ack timeout".into(),
                    wall_ms,
                });
            }
            OrderState::Unknown if order.pending_resend && order.filled_qty == 0 => {
                events.extend(resend_or_defer(state, order, wall_ms)?);
            }
            OrderState::Unknown if order.needs_query && age >= cfg.unknown_timeout_ms => {
                events.push(Event::QueryRequested {
                    client_order_id: order.client_order_id.clone(),
                    wall_ms,
                });
            }
            _ => {}
        }
    }
    Ok(events)
}

fn resend_or_defer(
    state: &PartitionState,
    order: &Order,
    wall_ms: i64,
) -> Result<Vec<Event>, CoreError> {
    let Some(cfg) = &state.risk else {
        return Ok(halt("resend without config"));
    };
    if state.halt.is_some()
        || state.is_killed(&order.strategy_id, &order.instrument_id, &order.account_id)
        || order.send_attempts > cfg.max_resends
    {
        return Ok(vec![Event::PartitionHalted {
            reason: format!(
                "not resending {}: halt, kill, or resend budget",
                order.client_order_id
            ),
        }]);
    }
    let reg = cfg
        .strategy(&order.strategy_id)
        .ok_or_else(|| CoreError::new("strategy missing"))?;
    let seg = order.segment.as_str();
    let seg_stamps = state
        .rates
        .by_segment
        .get(seg)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let strat_stamps = state
        .rates
        .by_strategy
        .get(&order.strategy_id)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if !rms::can_reserve(&state.rates.account, wall_ms, cfg.account_ops, 1)
        || !rms::can_reserve(seg_stamps, wall_ms, cfg.segment_ops, 1)
        || !rms::can_reserve(strat_stamps, wall_ms, reg.max_ops, 1)
    {
        return Ok(vec![Event::ResendDeferred {
            client_order_id: order.client_order_id.clone(),
        }]);
    }
    let outbound = outbound_from(order, state)?;
    Ok(vec![Event::ResendAuthorized {
        client_order_id: order.client_order_id.clone(),
        outbound,
        wall_ms,
    }])
}

fn decide_kill(state: &PartitionState, scope: &KillScope, reason: &str) -> Vec<Event> {
    let mut events = vec![Event::KillApplied {
        scope: scope.clone(),
        reason: reason.to_string(),
    }];
    for order in state.orders.values() {
        if !scope_matches(scope, order) {
            continue;
        }
        match order.state {
            OrderState::RiskApproved => events.push(Event::LocalWithdraw {
                client_order_id: order.client_order_id.clone(),
                reason: reason.to_string(),
            }),
            OrderState::Sent
            | OrderState::Acked
            | OrderState::PartiallyFilled
            | OrderState::Unknown => {
                events.push(Event::CancelRequested {
                    client_order_id: order.client_order_id.clone(),
                    request: CancelRequest {
                        client_order_id: order.client_order_id.clone(),
                        broker_order_id: order.broker_order_id.clone(),
                        algo_id: order.algo_id.clone(),
                        correlation_id: order.correlation_id.clone(),
                    },
                });
            }
            _ => {}
        }
    }
    events
}

fn decide_disconnect(state: &PartitionState, wall_ms: i64) -> Vec<Event> {
    let mut events = Vec::new();
    for order in state.orders.values() {
        if matches!(
            order.state,
            OrderState::Sent
                | OrderState::Acked
                | OrderState::PartiallyFilled
                | OrderState::CancelPending
                | OrderState::ReplacePending
        ) {
            events.push(Event::OrderUnknown {
                client_order_id: order.client_order_id.clone(),
                reason: "broker disconnect".into(),
                wall_ms,
            });
        }
    }
    events
}

fn decide_session(state: &PartitionState, day: i32) -> Vec<Event> {
    let mut events = vec![Event::SessionRolled { day }];
    for order in state.orders.values() {
        if order.validity != Validity::Day {
            continue;
        }
        match order.state {
            OrderState::Acked | OrderState::PartiallyFilled => {
                events.push(Event::OrderExpired {
                    client_order_id: order.client_order_id.clone(),
                });
            }
            OrderState::RiskApproved => events.push(Event::LocalWithdraw {
                client_order_id: order.client_order_id.clone(),
                reason: "session roll".into(),
            }),
            OrderState::Sent | OrderState::CancelPending | OrderState::ReplacePending => {
                events.push(Event::OrderUnknown {
                    client_order_id: order.client_order_id.clone(),
                    reason: "session roll".into(),
                    wall_ms: order.last_wall_ms,
                });
            }
            _ => {}
        }
    }
    events
}

fn decide_recon(state: &PartitionState, report: &crate::types::ReconReport) -> Vec<Event> {
    let mut problems = Vec::new();
    let mut broker_orders = BTreeMap::new();
    for row in &report.orders {
        if broker_orders
            .insert(row.client_order_id.clone(), row)
            .is_some()
        {
            problems.push(format!("duplicate broker row {}", row.client_order_id));
        }
    }
    for order in state.orders.values() {
        let Some(row) = broker_orders.get(&order.client_order_id) else {
            if is_working(order.state) && order.state != OrderState::RiskApproved {
                problems.push(format!("broker missing {}", order.client_order_id));
            }
            continue;
        };
        if row.filled_qty != order.filled_qty || row.leaves_qty != order.leaves_qty() {
            problems.push(format!(
                "qty mismatch {} internal {}/{} broker {}/{}",
                order.client_order_id,
                order.filled_qty,
                order.leaves_qty(),
                row.filled_qty,
                row.leaves_qty
            ));
        }
        if !status_compatible(order.state, &row.status) {
            problems.push(format!(
                "status mismatch {} internal {} broker {}",
                order.client_order_id, order.state, row.status
            ));
        }
    }
    for (id, row) in &broker_orders {
        if !state.orders.contains_key(id) {
            problems.push(format!(
                "unsolicited broker order {id} status {}",
                row.status
            ));
        }
    }
    let mut ours: BTreeMap<String, i64> = BTreeMap::new();
    for pos in state.positions.values() {
        *ours.entry(pos.instrument_id.clone()).or_default() += pos.net_qty;
    }
    let mut theirs: BTreeMap<String, i64> = BTreeMap::new();
    for pos in &report.positions {
        *theirs.entry(pos.instrument_id.clone()).or_default() += pos.net_qty;
    }
    let mut instruments: BTreeSet<String> = ours.keys().cloned().collect();
    instruments.extend(theirs.keys().cloned());
    for id in instruments {
        let a = ours.get(&id).copied().unwrap_or(0);
        let b = theirs.get(&id).copied().unwrap_or(0);
        if a != b {
            problems.push(format!("position mismatch {id}: internal {a} broker {b}"));
        }
    }
    let internal_trades: BTreeSet<&str> = state.seen_trade_ids.iter().map(String::as_str).collect();
    let broker_trades: BTreeSet<&str> = report.trades.iter().map(|t| t.trade_id.as_str()).collect();
    if internal_trades != broker_trades {
        problems.push(format!(
            "trade book mismatch internal {} broker {}",
            internal_trades.len(),
            broker_trades.len()
        ));
    }
    if problems.is_empty() {
        vec![Event::ReconOk {
            detail: "books match".into(),
        }]
    } else {
        halt(problems.join("; "))
    }
}

fn decide_resolve(
    state: &PartitionState,
    id: &str,
    view: &BrokerView,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Vec<Event>, CoreError> {
    let order = require_order(state, id)?;
    match view {
        BrokerView::NotFound => {
            if order.filled_qty > 0 {
                return Ok(halt(format!(
                    "broker has no trace of partially filled {id}"
                )));
            }
            match order.state {
                OrderState::Unknown => {
                    if state.halt.is_some()
                        || state.is_killed(
                            &order.strategy_id,
                            &order.instrument_id,
                            &order.account_id,
                        )
                    {
                        return Ok(vec![Event::UnknownResolved {
                            client_order_id: id.to_string(),
                            resolved_to: OrderState::Cancelled,
                            broker_order_id: None,
                            detail: "not found while kill or halt is active".into(),
                        }]);
                    }
                    let max = state.risk.as_ref().map(|c| c.max_resends).unwrap_or(0);
                    if order.send_attempts > max {
                        return Ok(halt(format!("resend budget exhausted for {id}")));
                    }
                    resend_or_defer(state, order, wall_ms)
                }
                OrderState::RiskApproved => Ok(vec![]),
                _ if is_working(order.state) => Ok(halt(format!(
                    "broker does not have live order {id} in {}",
                    order.state
                ))),
                _ => Ok(vec![]),
            }
        }
        BrokerView::Working {
            broker_order_id,
            filled_qty,
            leaves_qty,
        } => {
            if *filled_qty != order.filled_qty || *leaves_qty != order.leaves_qty() {
                return Ok(halt(format!("working view qty mismatch on {id}")));
            }
            let resolved_to = if *filled_qty == 0 {
                OrderState::Acked
            } else {
                OrderState::PartiallyFilled
            };
            adopt_view(order, id, resolved_to, broker_order_id, wall_ms, mono_ns)
        }
        BrokerView::Filled {
            broker_order_id,
            filled_qty,
        } => {
            if *filled_qty != order.qty || order.filled_qty != order.qty {
                return Ok(halt(format!(
                    "filled view on {id} does not match applied fills; refusing to invent trades"
                )));
            }
            adopt_view(
                order,
                id,
                OrderState::Filled,
                broker_order_id,
                wall_ms,
                mono_ns,
            )
        }
        BrokerView::Cancelled {
            broker_order_id,
            filled_qty,
        } => {
            if *filled_qty != order.filled_qty {
                return Ok(halt(format!("cancelled view qty mismatch on {id}")));
            }
            match order.state {
                OrderState::CancelPending => Ok(vec![Event::CancelConfirmed {
                    client_order_id: id.to_string(),
                }]),
                OrderState::Cancelled => Ok(vec![]),
                OrderState::Unknown => Ok(vec![Event::UnknownResolved {
                    client_order_id: id.to_string(),
                    resolved_to: OrderState::Cancelled,
                    broker_order_id: Some(broker_order_id.clone()),
                    detail: "broker cancelled".into(),
                }]),
                _ => Ok(halt(format!("cancelled view for {id} in {}", order.state))),
            }
        }
        BrokerView::Rejected {
            broker_order_id,
            reason,
        } => {
            if order.filled_qty != 0 {
                return Ok(halt(format!("rejected view after fills on {id}")));
            }
            match order.state {
                OrderState::Sent | OrderState::Unknown => Ok(vec![Event::BrokerReject {
                    client_order_id: id.to_string(),
                    reason: format!("{reason} ({broker_order_id})"),
                }]),
                OrderState::Rejected | OrderState::RiskRejected => Ok(vec![]),
                _ => Ok(halt(format!("rejected view for {id} in {}", order.state))),
            }
        }
        BrokerView::Expired { .. } => match order.state {
            OrderState::Expired => Ok(vec![]),
            _ if next_state(order.state, Trigger::Expire).is_ok() => {
                Ok(vec![Event::OrderExpired {
                    client_order_id: id.to_string(),
                }])
            }
            _ => Ok(halt(format!("expired view for {id} in {}", order.state))),
        },
    }
}

fn adopt_view(
    order: &Order,
    id: &str,
    resolved_to: OrderState,
    broker_order_id: &str,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Vec<Event>, CoreError> {
    if order.state == resolved_to && order.broker_order_id.as_deref() == Some(broker_order_id) {
        return Ok(vec![]);
    }
    if order.state == OrderState::Unknown || order.state == OrderState::RiskApproved {
        if order.state == OrderState::RiskApproved && resolved_to == OrderState::Acked {
            return Ok(vec![
                Event::OrderSent {
                    client_order_id: id.to_string(),
                    wall_ms,
                    mono_ns,
                },
                Event::BrokerAck {
                    client_order_id: id.to_string(),
                    broker_order_id: broker_order_id.to_string(),
                    wall_ms,
                    mono_ns,
                },
            ]);
        }
        if order.state == OrderState::Unknown {
            return Ok(vec![Event::UnknownResolved {
                client_order_id: id.to_string(),
                resolved_to,
                broker_order_id: Some(broker_order_id.to_string()),
                detail: "status query".into(),
            }]);
        }
    }
    if order.broker_order_id.is_none()
        && matches!(
            order.state,
            OrderState::Sent | OrderState::Acked | OrderState::PartiallyFilled | OrderState::Filled
        )
    {
        return Ok(vec![Event::BrokerAck {
            client_order_id: id.to_string(),
            broker_order_id: broker_order_id.to_string(),
            wall_ms,
            mono_ns,
        }]);
    }
    if order.broker_order_id.as_deref() != Some(broker_order_id) {
        return Ok(halt(format!("broker id mismatch on {id}")));
    }
    if order.state != resolved_to {
        return Ok(halt(format!(
            "state mismatch on {id}: {} vs {resolved_to}",
            order.state
        )));
    }
    Ok(vec![])
}

fn apply_ack(
    state: &mut PartitionState,
    id: &str,
    broker_order_id: &str,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<(), CoreError> {
    let order = order_mut(state, id)?;
    if let Some(existing) = &order.broker_order_id {
        if existing != broker_order_id {
            return Err(CoreError::new("broker id conflict"));
        }
    }
    match order.state {
        OrderState::Sent => {
            order.state = next_state(order.state, Trigger::Ack)?;
            order.broker_order_id = Some(broker_order_id.to_string());
            order.needs_query = false;
            order.push_hop(HopStage::Ack, wall_ms, mono_ns);
        }
        OrderState::Unknown => {
            let trigger = if order.filled_qty == 0 {
                Trigger::Ack
            } else if order.filled_qty < order.qty {
                Trigger::ReconToPartial
            } else {
                Trigger::ReconToFilled
            };
            order.state = next_state(order.state, trigger)?;
            order.broker_order_id = Some(broker_order_id.to_string());
            order.needs_query = false;
            order.push_hop(HopStage::Ack, wall_ms, mono_ns);
        }
        OrderState::Acked
        | OrderState::PartiallyFilled
        | OrderState::Filled
        | OrderState::CancelPending
        | OrderState::ReplacePending => {
            order.broker_order_id = Some(broker_order_id.to_string());
            if order.state != OrderState::Filled {
                order.push_hop(HopStage::Ack, wall_ms, mono_ns);
            } else {
                order.hops.push(Hop {
                    stage: HopStage::Ack,
                    wall_ms,
                    mono_ns,
                });
            }
        }
        _ => return Err(CoreError::new("ack applied in a refusing state")),
    }
    Ok(())
}

fn apply_fill(
    state: &mut PartitionState,
    id: &str,
    trade_id: &str,
    qty: i64,
    price: i64,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<(), CoreError> {
    if state.seen_trade_ids.contains(trade_id) {
        return Err(CoreError::new("fill trade id already applied"));
    }
    let (strategy_id, instrument_id, side, prev, filled, order_qty) = {
        let order = require_order(state, id)?;
        (
            order.strategy_id.clone(),
            order.instrument_id.clone(),
            order.side,
            order.state,
            order.filled_qty,
            order.qty,
        )
    };
    if qty <= 0 || price <= 0 || filled + qty > order_qty {
        return Err(CoreError::new("fill breaks quantity bounds"));
    }
    let complete = filled + qty == order_qty;
    let new_state = if complete {
        next_state(prev, Trigger::FillComplete)?
    } else if matches!(prev, OrderState::CancelPending | OrderState::ReplacePending) {
        prev
    } else {
        next_state(prev, Trigger::FillPartial)?
    };
    {
        let order = order_mut(state, id)?;
        order.state = new_state;
        order.filled_qty += qty;
        order.filled_notional += i128::from(price) * i128::from(qty);
        order.avg_fill_price = (order.filled_notional / i128::from(order.filled_qty)) as i64;
        order.fill_trade_ids.insert(trade_id.to_string());
        order.needs_query = false;
        order.push_hop(HopStage::Fill, wall_ms, mono_ns);
    }
    state.seen_trade_ids.insert(trade_id.to_string());
    let book_side = match side {
        Side::Buy => pnl::BookSide::Buy,
        Side::Sell => pnl::BookSide::Sell,
    };
    let key = pos_key(&strategy_id, &instrument_id);
    let pos = state
        .positions
        .entry(key)
        .or_insert_with(|| Position::new(&strategy_id, &instrument_id));
    pnl::apply_fill(pos, book_side, qty, price);
    Ok(())
}

fn consume_rate(
    state: &mut PartitionState,
    segment: &str,
    strategy: &str,
    now: i64,
) -> Result<(), CoreError> {
    let (account_max, segment_max, strategy_max) = {
        let cfg = state
            .risk
            .as_ref()
            .ok_or_else(|| CoreError::new("no risk config"))?;
        let reg = cfg
            .strategy(strategy)
            .ok_or_else(|| CoreError::new("strategy not registered at apply"))?;
        (cfg.account_ops, cfg.segment_ops, reg.max_ops)
    };
    let mut account = state.rates.account.clone();
    let mut segment_v = state
        .rates
        .by_segment
        .get(segment)
        .cloned()
        .unwrap_or_default();
    let mut strategy_v = state
        .rates
        .by_strategy
        .get(strategy)
        .cloned()
        .unwrap_or_default();
    if !reserve_all(
        &mut account,
        &mut segment_v,
        &mut strategy_v,
        now,
        account_max,
        segment_max,
        strategy_max,
        1,
    ) {
        return Err(CoreError::new("rate reservation failed during apply"));
    }
    state.rates.account = account;
    state
        .rates
        .by_segment
        .insert(segment.to_string(), segment_v);
    state
        .rates
        .by_strategy
        .insert(strategy.to_string(), strategy_v);
    Ok(())
}

fn new_order(
    intent: &OrderIntent,
    clord: &str,
    slice_index: u32,
    slice_count: u32,
    qty: i64,
    segment: crate::types::Segment,
) -> Order {
    let mut order = Order {
        client_order_id: clord.to_string(),
        broker_order_id: None,
        intent_id: intent.intent_id.clone(),
        account_id: intent.account_id.clone(),
        strategy_id: intent.strategy_id.clone(),
        instrument_id: intent.instrument_id.clone(),
        segment,
        side: intent.side,
        order_type: intent.order_type,
        product: intent.product,
        validity: intent.validity,
        qty,
        price: intent.price,
        filled_qty: 0,
        filled_notional: 0,
        avg_fill_price: 0,
        state: OrderState::New,
        state_before_pending: None,
        algo_id: intent.algo_id.clone(),
        parent_intent_id: intent.intent_id.clone(),
        slice_index,
        slice_count,
        risk_approved: false,
        send_attempts: 0,
        replace_seq: 0,
        pending_resend: false,
        needs_query: false,
        correlation_id: intent.correlation_id.clone(),
        created_wall_ms: intent.wall_ms,
        created_mono_ns: intent.mono_ns,
        last_wall_ms: intent.wall_ms,
        fill_trade_ids: BTreeSet::new(),
        hops: Vec::new(),
        replaces: None,
        replaced_by: None,
        pending_replace_qty: None,
        pending_replace_price: None,
        reject_rule: None,
        reject_detail: None,
    };
    // Segment is filled by the caller via outbound_from / a second pass.
    order.push_hop(HopStage::Signal, intent.wall_ms, intent.mono_ns);
    order
}

fn outbound_from(order: &Order, state: &PartitionState) -> Result<OutboundOrder, CoreError> {
    let inst = state
        .instruments
        .get(&order.instrument_id)
        .ok_or_else(|| CoreError::new("instrument missing for outbound"))?;
    Ok(OutboundOrder {
        client_order_id: order.client_order_id.clone(),
        account_id: order.account_id.clone(),
        strategy_id: order.strategy_id.clone(),
        instrument_id: order.instrument_id.clone(),
        exchange: inst.exchange,
        segment: inst.segment,
        symbol: inst.symbol.clone(),
        side: order.side,
        order_type: order.order_type,
        product: order.product,
        validity: order.validity,
        qty: order.qty,
        price: order.price,
        algo_id: order.algo_id.clone(),
        correlation_id: order.correlation_id.clone(),
    })
}

fn successor_order(
    old: &Order,
    new_total_qty: i64,
    new_price: i64,
    broker_id: &str,
    wall_ms: i64,
    mono_ns: u64,
) -> Result<Order, CoreError> {
    let clord = format!("{}r{}", old.client_order_id, old.replace_seq + 1);
    if !valid_clord(&clord) {
        return Err(CoreError::new("successor id invalid"));
    }
    let mut order = old.clone();
    order.client_order_id = clord;
    order.broker_order_id = Some(broker_id.to_string());
    order.qty = new_total_qty - old.filled_qty;
    order.price = new_price;
    order.filled_qty = 0;
    order.filled_notional = 0;
    order.avg_fill_price = 0;
    order.state = OrderState::Acked;
    order.state_before_pending = None;
    order.risk_approved = true;
    order.send_attempts = 1;
    order.replace_seq = old.replace_seq + 1;
    order.pending_resend = false;
    order.needs_query = false;
    order.created_wall_ms = wall_ms;
    order.created_mono_ns = mono_ns;
    order.last_wall_ms = wall_ms;
    order.fill_trade_ids.clear();
    order.hops = vec![Hop {
        stage: HopStage::Ack,
        wall_ms,
        mono_ns,
    }];
    order.replaces = Some(old.client_order_id.clone());
    order.replaced_by = None;
    order.pending_replace_qty = None;
    order.pending_replace_price = None;
    order.reject_rule = None;
    order.reject_detail = None;
    Ok(order)
}

fn order_as_intent(order: &Order, new_qty: i64, new_price: i64, wall_ms: i64) -> OrderIntent {
    OrderIntent {
        intent_id: format!("probe-{}", order.client_order_id),
        account_id: order.account_id.clone(),
        strategy_id: order.strategy_id.clone(),
        instrument_id: order.instrument_id.clone(),
        side: order.side,
        order_type: if new_price > 0 {
            OrderType::Limit
        } else {
            order.order_type
        },
        product: order.product,
        validity: order.validity,
        qty: new_qty,
        price: new_price,
        algo_id: order.algo_id.clone(),
        correlation_id: order.correlation_id.clone(),
        wall_ms,
        mono_ns: order.created_mono_ns,
    }
}

fn valid_clord(id: &str) -> bool {
    !id.is_empty() && id.len() <= 20 && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
}

fn require_order<'a>(state: &'a PartitionState, id: &str) -> Result<&'a Order, CoreError> {
    state
        .orders
        .get(id)
        .ok_or_else(|| CoreError::new(format!("unknown order {id}")))
}

fn order_mut<'a>(state: &'a mut PartitionState, id: &str) -> Result<&'a mut Order, CoreError> {
    state
        .orders
        .get_mut(id)
        .ok_or_else(|| CoreError::new(format!("unknown order {id}")))
}

fn halt(reason: impl Into<String>) -> Vec<Event> {
    vec![Event::PartitionHalted {
        reason: reason.into(),
    }]
}

fn status_compatible(state: OrderState, status: &str) -> bool {
    let status = status.to_ascii_lowercase();
    match status.as_str() {
        "working" | "open" | "pending" | "trigger pending" => matches!(
            state,
            OrderState::Sent
                | OrderState::Acked
                | OrderState::PartiallyFilled
                | OrderState::CancelPending
                | OrderState::ReplacePending
                | OrderState::Unknown
                | OrderState::RiskApproved
        ),
        "filled" | "complete" => state == OrderState::Filled,
        "cancelled" | "canceled" => state == OrderState::Cancelled,
        "rejected" => matches!(state, OrderState::Rejected | OrderState::RiskRejected),
        "expired" => state == OrderState::Expired,
        "replaced" => state == OrderState::Replaced,
        _ => false,
    }
}

fn recon_trigger(state: OrderState) -> Option<Trigger> {
    Some(match state {
        OrderState::Sent => Trigger::ReconToSent,
        OrderState::Acked => Trigger::ReconToAcked,
        OrderState::Rejected => Trigger::ReconToRejected,
        OrderState::PartiallyFilled => Trigger::ReconToPartial,
        OrderState::Filled => Trigger::ReconToFilled,
        OrderState::Cancelled => Trigger::ReconToCancelled,
        OrderState::Replaced => Trigger::ReconToReplaced,
        OrderState::Expired => Trigger::ReconToExpired,
        _ => return None,
    })
}

fn scope_matches(scope: &KillScope, order: &Order) -> bool {
    match scope {
        KillScope::Global => true,
        KillScope::Account { account_id } => order.account_id == *account_id,
        KillScope::Strategy { strategy_id } => order.strategy_id == *strategy_id,
        KillScope::Instrument { instrument_id } => order.instrument_id == *instrument_id,
    }
}

fn apply_kill_flag(flags: &mut crate::state::KillFlags, scope: &KillScope, on: bool) {
    match scope {
        KillScope::Global => flags.global = on,
        KillScope::Account { account_id } => {
            if on {
                flags.accounts.insert(account_id.clone());
            } else {
                flags.accounts.remove(account_id);
            }
        }
        KillScope::Strategy { strategy_id } => {
            if on {
                flags.strategies.insert(strategy_id.clone());
            } else {
                flags.strategies.remove(strategy_id);
            }
        }
        KillScope::Instrument { instrument_id } => {
            if on {
                flags.instruments.insert(instrument_id.clone());
            } else {
                flags.instruments.remove(instrument_id);
            }
        }
    }
}
