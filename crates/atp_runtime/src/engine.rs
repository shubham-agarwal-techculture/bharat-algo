//! Single-writer runtime for one account partition.
//!
//! The core decides. This type performs the effects: broker I/O, the event
//! clock, metrics, and traces. A dropped session becomes UNKNOWN and is
//! resolved by a status query. A resend reuses the client order id and is
//! journaled only after that query says the broker does not have the order.

use crate::broker::{Broker, BrokerError, Inbound};
use crate::metrics::Metrics;
use crate::simulator::Simulator;
use crate::trace::{span_from, TraceLog};
use atp_core::catalog::{sample_catalog, sample_strategies};
use atp_core::config::{demo_risk_config, RiskConfig};
use atp_core::decide::CoreError;
use atp_core::instrument::Instrument;
use atp_core::strategy::{Strategy, StrategyAction, StrategyCtx};
use atp_core::{
    pos_key, BrokerView, CancelRequest, Command, Effect, HopStage, JournalMeta, KillScope,
    OrderIntent, OrderState, OutboundOrder, PartitionWriter, ReplaceRequest,
};
use std::path::Path;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    Paper,
    Live,
}

impl RunMode {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "paper" => Ok(Self::Paper),
            "live" => Ok(Self::Live),
            other => Err(format!("unknown mode {other}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Live => "live",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Broker(#[from] BrokerError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Other(String),
}

/// Paper never selects the kite gateway. Live refuses kite because that
/// adapter is an uncertified skeleton.
pub fn preflight(mode: RunMode, gateway: &str) -> Result<(), EngineError> {
    match (mode, gateway) {
        (RunMode::Live, "kite") => Err(EngineError::Other(crate::kite::NOT_YET_LIVE.into())),
        (RunMode::Paper, "kite") => Err(EngineError::Other(
            "paper mode sends orders only to the simulator; refusing the kite gateway".into(),
        )),
        _ => Ok(()),
    }
}

#[derive(Debug)]
pub enum ReportKind {
    Health,
    Metrics,
    Latency,
    State,
    Traces,
}

pub enum Control {
    Kill {
        scope: KillScope,
        reason: String,
        reply: Sender<Result<(), String>>,
    },
    Report {
        kind: ReportKind,
        reply: Sender<String>,
    },
}

pub struct Engine<B: Broker> {
    pub writer: PartitionWriter,
    pub broker: B,
    pub strategies: Vec<Box<dyn Strategy>>,
    pub mode: RunMode,
    clock_wall: i64,
    clock_mono: u64,
    metrics: Metrics,
    traces: TraceLog,
    control: Option<Receiver<Control>>,
}

impl<B: Broker> Engine<B> {
    pub fn boot(
        broker: B,
        mode: RunMode,
        journal: Option<&Path>,
        account_id: &str,
        now_ms: i64,
    ) -> Result<Self, EngineError> {
        preflight(mode, broker.name())?;
        let writer = match journal {
            Some(path) => PartitionWriter::open(path, account_id)?,
            None => PartitionWriter::memory(account_id),
        };
        let metrics = Metrics::install().map_err(EngineError::Other)?;
        let mut engine = Self {
            writer,
            broker,
            strategies: Vec::new(),
            mode,
            clock_wall: now_ms,
            clock_mono: 0,
            metrics,
            traces: TraceLog::new(None),
            control: None,
        };
        engine.broker.set_now(now_ms);
        engine.metrics.refresh(&engine.writer.state, now_ms);
        Ok(engine)
    }

    pub fn set_otel(&mut self, endpoint: Option<String>) {
        self.traces.set_endpoint(endpoint);
    }

    pub fn attach_control(&mut self) -> Sender<Control> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.control = Some(rx);
        tx
    }

    pub fn now(&self) -> i64 {
        self.clock_wall
    }

    pub fn set_time(&mut self, wall_ms: i64) {
        if wall_ms >= self.clock_wall {
            self.clock_wall = wall_ms;
            self.broker.set_now(wall_ms);
        }
    }

    pub fn advance(&mut self, ms: i64) {
        if ms > 0 {
            self.set_time(self.clock_wall.saturating_add(ms));
        }
    }

    pub fn load_instruments(&mut self, instruments: Vec<Instrument>) -> Result<(), EngineError> {
        let meta = self.meta("instruments");
        self.commit(Command::LoadInstruments { instruments, meta })?;
        Ok(())
    }

    pub fn apply_config(&mut self, config: RiskConfig) -> Result<(), EngineError> {
        let version = config.version;
        let meta = self.meta(&format!("config-{version}"));
        self.commit(Command::ApplyConfig { config, meta })?;
        Ok(())
    }

    pub fn note_compliance(
        &mut self,
        static_ip: &str,
        audit_retention_days: u32,
        algo_ids: Vec<String>,
    ) -> Result<(), EngineError> {
        let meta = self.meta("compliance");
        self.commit(Command::Compliance {
            static_ip: static_ip.to_string(),
            audit_retention_days,
            algo_ids,
            meta,
        })?;
        Ok(())
    }

    pub fn publish_market(
        &mut self,
        instrument_id: &str,
        ltp: i64,
        bid: i64,
        ask: i64,
        source_seq: u64,
        correlation_id: &str,
    ) -> Result<(), EngineError> {
        let exchange_wall_ms = self.clock_wall;
        let meta = self.meta(correlation_id);
        self.commit(Command::MarketData {
            instrument_id: instrument_id.to_string(),
            ltp,
            bid,
            ask,
            source_seq,
            exchange_wall_ms,
            resync: false,
            meta,
        })?;
        Ok(())
    }

    /// Normalized tick plus strategy evaluation. Strategies return intents;
    /// they never receive the broker.
    pub fn on_tick(
        &mut self,
        instrument_id: &str,
        ltp: i64,
        source_seq: u64,
    ) -> Result<(), EngineError> {
        let correlation_id = format!("md-{instrument_id}-{source_seq}");
        self.publish_market(instrument_id, ltp, ltp, ltp, source_seq, &correlation_id)?;
        self.eval_strategies(instrument_id, &correlation_id)
    }

    pub fn submit_intent(&mut self, intent: OrderIntent) -> Result<(), EngineError> {
        let started = Instant::now();
        let effects = self.commit(Command::OrderIntent(intent))?;
        self.drain(effects)?;
        self.metrics.observe_e2e(started.elapsed());
        Ok(())
    }

    pub fn timer(&mut self) -> Result<(), EngineError> {
        let meta = self.meta("timer");
        let effects = self.commit(Command::Timer { meta })?;
        self.drain(effects)
    }

    pub fn session_lost(&mut self) -> Result<(), EngineError> {
        let meta = self.meta("disconnect");
        let effects = self.commit(Command::BrokerDisconnect { meta })?;
        self.drain(effects)
    }

    pub fn poll_broker(&mut self) -> Result<(), EngineError> {
        let effects = self.ingest_poll()?;
        self.drain(effects)
    }

    pub fn reconcile(&mut self) -> Result<bool, EngineError> {
        self.broker.set_now(self.clock_wall);
        let report = self.broker.books()?;
        let before = self.writer.state.halt.clone();
        let meta = self.meta("recon");
        let effects = self.commit(Command::Recon { report, meta })?;
        self.drain(effects)?;
        let ok = self.writer.state.halt.is_none();
        if !ok && before.is_none() {
            self.metrics.inc_recon_mismatch();
        }
        Ok(ok)
    }

    pub fn kill(&mut self, scope: KillScope, reason: &str) -> Result<(), EngineError> {
        let meta = self.meta("kill");
        let effects = self.commit(Command::Kill {
            scope,
            reason: reason.to_string(),
            meta,
        })?;
        self.drain(effects)
    }

    /// Rebuild in-flight uncertainty from the broker book. RISK_APPROVED and
    /// not-found is the one case the runtime may still send, because that
    /// order never left. Every other in-flight order is queried first.
    pub fn recover(&mut self) -> Result<(), EngineError> {
        let meta = self.meta("recover");
        let effects = self.commit(Command::BrokerDisconnect { meta })?;
        self.drain(effects)?;
        let pending: Vec<String> = self
            .writer
            .state
            .orders
            .iter()
            .filter(|(_, order)| order.state == OrderState::RiskApproved)
            .map(|(id, _)| id.clone())
            .collect();
        for id in pending {
            self.broker.set_now(self.clock_wall);
            let view = self.broker.query(&id)?;
            match view {
                BrokerView::NotFound => {
                    let outbound = outbound_of(&self.writer.state, &id)?;
                    self.drain(vec![Effect::Send(outbound)])?;
                }
                other => {
                    let meta = self.meta("recover");
                    let effects = self.commit(Command::ResolveUnknown {
                        client_order_id: id,
                        view: other,
                        meta,
                    })?;
                    self.drain(effects)?;
                }
            }
        }
        Ok(())
    }

    pub fn poll_control(&mut self) {
        loop {
            let msg = {
                let Some(rx) = self.control.as_ref() else {
                    break;
                };
                match rx.try_recv() {
                    Ok(msg) => msg,
                    Err(_) => break,
                }
            };
            match msg {
                Control::Kill {
                    scope,
                    reason,
                    reply,
                } => {
                    let result = self
                        .kill(scope, &reason)
                        .map(|_| ())
                        .map_err(|e| e.to_string());
                    let _ = reply.send(result);
                }
                Control::Report { kind, reply } => {
                    let _ = reply.send(self.report(&kind));
                }
            }
        }
    }

    pub fn report(&self, kind: &ReportKind) -> String {
        match kind {
            ReportKind::Health => {
                let status = if self.writer.state.halt.is_none() {
                    "ok"
                } else {
                    "halted"
                };
                format!(
                    "{{\"status\":\"{status}\",\"mode\":\"{}\",\"seq\":{},\"gateway\":\"{}\"}}",
                    self.mode.as_str(),
                    self.writer.state.last_seq,
                    self.broker.name()
                )
            }
            ReportKind::Metrics => self.metrics.encode(),
            ReportKind::Latency => self.metrics.latency_json(),
            ReportKind::State => state_json(&self.writer),
            ReportKind::Traces => self.traces.json(),
        }
    }

    pub fn latency(&self) -> &crate::metrics::Reservoir {
        &self.metrics.hot
    }

    pub fn e2e(&self) -> &crate::metrics::Reservoir {
        &self.metrics.e2e_samples
    }

    fn eval_strategies(
        &mut self,
        instrument_id: &str,
        correlation_id: &str,
    ) -> Result<(), EngineError> {
        let account = self.writer.state.account_id.clone();
        let ltp = self
            .writer
            .state
            .market
            .get(instrument_id)
            .map(|m| m.ltp)
            .unwrap_or(0);
        let wall = self.clock_wall;
        let mono = self.clock_mono;
        let mut planned = Vec::new();
        for strategy in &mut self.strategies {
            if strategy.instrument_id() != instrument_id {
                continue;
            }
            let key = pos_key(strategy.id(), instrument_id);
            let position_qty = self
                .writer
                .state
                .positions
                .get(&key)
                .map(|p| p.net_qty)
                .unwrap_or(0);
            let killed = self
                .writer
                .state
                .is_killed(strategy.id(), instrument_id, &account);
            let ctx = StrategyCtx {
                account_id: &account,
                instrument_id,
                ltp,
                position_qty,
                correlation_id,
                wall_ms: wall,
                mono_ns: mono,
                killed,
            };
            let id = strategy.id().to_string();
            for action in strategy.on_market(&ctx) {
                planned.push((id.clone(), action));
            }
        }
        for (strategy_id, action) in planned {
            match action {
                StrategyAction::Signal { name, note } => {
                    let meta = self.meta(correlation_id);
                    self.commit(Command::Signal {
                        strategy_id,
                        name,
                        instrument_id: instrument_id.to_string(),
                        note,
                        meta,
                    })?;
                }
                StrategyAction::Intent(intent) => self.submit_intent(intent)?,
            }
        }
        Ok(())
    }

    fn commit(&mut self, cmd: Command) -> Result<Vec<Effect>, EngineError> {
        let seq_before = self.writer.state.last_seq;
        let started = Instant::now();
        let effects = self.writer.handle(cmd)?;
        self.metrics.observe_hot(started.elapsed());
        self.observe_since(seq_before);
        Ok(effects)
    }

    fn observe_since(&mut self, seq_before: u64) {
        let fresh: Vec<_> = self
            .writer
            .records()
            .iter()
            .filter(|record| record.seq > seq_before)
            .cloned()
            .collect();
        for record in &fresh {
            tracing::info!(
                seq = record.seq,
                correlation_id = %record.correlation_id,
                wall_ms = record.wall_ms,
                mono_ns = record.mono_ns,
                "committed"
            );
            for event in &record.events {
                self.metrics.on_event(event);
            }
            self.traces.push(span_from(record));
        }
        self.metrics.refresh(&self.writer.state, self.clock_wall);
    }

    fn drain(&mut self, mut effects: Vec<Effect>) -> Result<(), EngineError> {
        for _ in 0..16 {
            if effects.is_empty() {
                return Ok(());
            }
            let batch = std::mem::take(&mut effects);
            for effect in batch {
                effects.extend(self.exec(effect)?);
            }
        }
        Err(EngineError::Other("effect drain exceeded its guard".into()))
    }

    fn exec(&mut self, effect: Effect) -> Result<Vec<Effect>, EngineError> {
        match effect {
            Effect::Send(order) => self.exec_send(order),
            Effect::Cancel(req) => self.exec_cancel(req),
            Effect::Replace(req) => self.exec_replace(req),
            Effect::Query(id) => self.exec_query(id),
        }
    }

    fn exec_send(&mut self, order: OutboundOrder) -> Result<Vec<Effect>, EngineError> {
        if order.algo_id.is_empty() {
            return Err(EngineError::Other(
                "refusing to send an order without an algo id".into(),
            ));
        }
        let mut more = Vec::new();
        let phase = self
            .writer
            .state
            .orders
            .get(&order.client_order_id)
            .map(|o| o.state);
        if phase == Some(OrderState::RiskApproved) {
            let id = order.client_order_id.clone();
            let meta = self.order_meta(&id);
            more.extend(self.commit(Command::MarkDispatched {
                client_order_id: id,
                meta,
            })?);
        }
        self.broker.set_now(self.clock_wall);
        match self.broker.submit(&order) {
            Ok(()) => {
                let id = order.client_order_id.clone();
                let meta = self.order_meta(&id);
                more.extend(self.commit(Command::RecordHop {
                    client_order_id: id,
                    stage: HopStage::Gateway,
                    meta,
                })?);
                more.extend(self.ingest_poll()?);
            }
            Err(err) => {
                let id = order.client_order_id.clone();
                let detail = err.to_string();
                let meta = self.order_meta(&id);
                more.extend(self.commit(Command::DispatchFailed {
                    client_order_id: id,
                    detail,
                    meta,
                })?);
            }
        }
        Ok(more)
    }

    fn exec_cancel(&mut self, req: CancelRequest) -> Result<Vec<Effect>, EngineError> {
        self.broker.set_now(self.clock_wall);
        if let Err(err) = self.broker.cancel(&req) {
            let detail = err.to_string();
            let meta = self.meta("cancel-fail");
            return self.commit(Command::DispatchFailed {
                client_order_id: req.client_order_id,
                detail,
                meta,
            });
        }
        self.ingest_poll()
    }

    fn exec_replace(&mut self, req: ReplaceRequest) -> Result<Vec<Effect>, EngineError> {
        self.broker.set_now(self.clock_wall);
        if let Err(err) = self.broker.replace(&req) {
            let detail = err.to_string();
            let meta = self.meta("replace-fail");
            return self.commit(Command::DispatchFailed {
                client_order_id: req.client_order_id,
                detail,
                meta,
            });
        }
        self.ingest_poll()
    }

    fn exec_query(&mut self, id: String) -> Result<Vec<Effect>, EngineError> {
        self.broker.set_now(self.clock_wall);
        match self.broker.query(&id) {
            Ok(view) => {
                let meta = self.order_meta(&id);
                self.commit(Command::ResolveUnknown {
                    client_order_id: id,
                    view,
                    meta,
                })
            }
            Err(err) => {
                tracing::warn!(client_order_id = %id, error = %err, "status query failed");
                let meta = self.meta("query-failed");
                self.commit(Command::QueryFailed {
                    client_order_id: id,
                    meta,
                })
            }
        }
    }

    fn ingest_poll(&mut self) -> Result<Vec<Effect>, EngineError> {
        self.broker.set_now(self.clock_wall);
        let inbound = self.broker.poll();
        let mut more = Vec::new();
        for msg in inbound {
            let cmd = self.inbound_cmd(msg);
            more.extend(self.commit(cmd)?);
        }
        Ok(more)
    }

    fn inbound_cmd(&mut self, msg: Inbound) -> Command {
        let meta_for = |this: &mut Self, id: &str| this.order_meta(id);
        match msg {
            Inbound::Ack {
                client_order_id,
                broker_order_id,
            } => {
                let meta = meta_for(self, &client_order_id);
                Command::BrokerAck {
                    client_order_id,
                    broker_order_id,
                    meta,
                }
            }
            Inbound::Reject {
                client_order_id,
                reason,
            } => {
                let meta = meta_for(self, &client_order_id);
                Command::BrokerReject {
                    client_order_id,
                    reason,
                    meta,
                }
            }
            Inbound::Fill {
                client_order_id,
                trade_id,
                qty,
                price,
            } => {
                let meta = meta_for(self, &client_order_id);
                Command::Fill {
                    client_order_id,
                    trade_id,
                    qty,
                    price,
                    meta,
                }
            }
            Inbound::CancelAck { client_order_id } => {
                let meta = meta_for(self, &client_order_id);
                Command::CancelAck {
                    client_order_id,
                    meta,
                }
            }
            Inbound::CancelReject {
                client_order_id,
                reason,
            } => {
                let meta = meta_for(self, &client_order_id);
                Command::CancelReject {
                    client_order_id,
                    reason,
                    meta,
                }
            }
            Inbound::ReplaceAck {
                client_order_id,
                new_broker_order_id,
            } => {
                let meta = meta_for(self, &client_order_id);
                Command::ReplaceAck {
                    client_order_id,
                    new_broker_order_id,
                    meta,
                }
            }
            Inbound::ReplaceReject {
                client_order_id,
                reason,
            } => {
                let meta = meta_for(self, &client_order_id);
                Command::ReplaceReject {
                    client_order_id,
                    reason,
                    meta,
                }
            }
            Inbound::Expired { client_order_id } => {
                let meta = meta_for(self, &client_order_id);
                Command::Expired {
                    client_order_id,
                    meta,
                }
            }
        }
    }

    fn meta(&mut self, correlation_id: &str) -> JournalMeta {
        self.clock_mono = self.clock_mono.saturating_add(1_000_000);
        JournalMeta {
            correlation_id: correlation_id.to_string(),
            wall_ms: self.clock_wall,
            mono_ns: self.clock_mono,
        }
    }

    fn order_meta(&mut self, client_order_id: &str) -> JournalMeta {
        let correlation_id = self
            .writer
            .state
            .orders
            .get(client_order_id)
            .map(|o| o.correlation_id.clone())
            .unwrap_or_else(|| "engine".into());
        self.meta(&correlation_id)
    }
}

impl Engine<Simulator> {
    pub fn paper_memory(now_ms: i64) -> Result<Self, EngineError> {
        let mut engine = Engine::boot(Simulator::new(0, 0), RunMode::Paper, None, "ACC1", now_ms)?;
        engine.strategies = sample_strategies();
        engine.load_instruments(sample_catalog())?;
        engine.apply_config(demo_risk_config())?;
        let algos = engine
            .writer
            .state
            .risk
            .as_ref()
            .map(|cfg| cfg.strategies.iter().map(|s| s.algo_id.clone()).collect())
            .unwrap_or_default();
        engine.note_compliance("10.0.0.8", 1825, algos)?;
        Ok(engine)
    }
}

fn outbound_of(state: &atp_core::PartitionState, id: &str) -> Result<OutboundOrder, EngineError> {
    let order = state
        .orders
        .get(id)
        .ok_or_else(|| EngineError::Other(format!("unknown order {id}")))?;
    let inst = state
        .instruments
        .get(&order.instrument_id)
        .ok_or_else(|| EngineError::Other(format!("unknown instrument {}", order.instrument_id)))?;
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

fn state_json(writer: &PartitionWriter) -> String {
    let orders: Vec<_> = writer
        .state
        .orders
        .values()
        .map(|order| {
            serde_json::json!({
                "client_order_id": order.client_order_id,
                "state": order.state.as_str(),
                "algo_id": order.algo_id,
                "correlation_id": order.correlation_id,
                "instrument_id": order.instrument_id,
                "qty": order.qty,
                "filled_qty": order.filled_qty,
                "send_attempts": order.send_attempts,
                "hops": order.hops.iter().map(|h| format!("{:?}", h.stage)).collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::json!({
        "account_id": writer.state.account_id,
        "seq": writer.state.last_seq,
        "halt": writer.state.halt,
        "orders": orders,
    })
    .to_string()
}
