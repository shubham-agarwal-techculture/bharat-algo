//! Commands are inputs. Events are the only facts that change state.
//! Effects are the outbox implied by a committed event; replay ignores them.

use crate::config::RiskConfig;
use crate::instrument::Instrument;
use crate::types::{
    BrokerView, CancelRequest, HopStage, KillScope, Order, OrderIntent, OrderState, OutboundOrder,
    ReconReport, ReplaceRequest, RiskRule,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalMeta {
    pub correlation_id: String,
    pub wall_ms: i64,
    pub mono_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Command {
    LoadInstruments {
        instruments: Vec<Instrument>,
        meta: JournalMeta,
    },
    MarketData {
        instrument_id: String,
        ltp: i64,
        bid: i64,
        ask: i64,
        source_seq: u64,
        exchange_wall_ms: i64,
        resync: bool,
        meta: JournalMeta,
    },
    Signal {
        strategy_id: String,
        name: String,
        instrument_id: String,
        note: String,
        meta: JournalMeta,
    },
    OrderIntent(OrderIntent),
    MarkDispatched {
        client_order_id: String,
        meta: JournalMeta,
    },
    DispatchFailed {
        client_order_id: String,
        detail: String,
        meta: JournalMeta,
    },
    BrokerAck {
        client_order_id: String,
        broker_order_id: String,
        meta: JournalMeta,
    },
    BrokerReject {
        client_order_id: String,
        reason: String,
        meta: JournalMeta,
    },
    Fill {
        client_order_id: String,
        trade_id: String,
        qty: i64,
        price: i64,
        meta: JournalMeta,
    },
    CancelAck {
        client_order_id: String,
        meta: JournalMeta,
    },
    CancelReject {
        client_order_id: String,
        reason: String,
        meta: JournalMeta,
    },
    ReplaceAck {
        client_order_id: String,
        new_broker_order_id: String,
        meta: JournalMeta,
    },
    ReplaceReject {
        client_order_id: String,
        reason: String,
        meta: JournalMeta,
    },
    Expired {
        client_order_id: String,
        meta: JournalMeta,
    },
    RequestCancel {
        client_order_id: String,
        meta: JournalMeta,
    },
    RequestReplace {
        client_order_id: String,
        new_qty: i64,
        new_price: i64,
        meta: JournalMeta,
    },
    Timer {
        meta: JournalMeta,
    },
    ApplyConfig {
        config: RiskConfig,
        meta: JournalMeta,
    },
    Kill {
        scope: KillScope,
        reason: String,
        meta: JournalMeta,
    },
    ClearKill {
        scope: KillScope,
        reason: String,
        meta: JournalMeta,
    },
    Recon {
        report: ReconReport,
        meta: JournalMeta,
    },
    SessionRoll {
        day: i32,
        meta: JournalMeta,
    },
    RecordHop {
        client_order_id: String,
        stage: HopStage,
        meta: JournalMeta,
    },
    ResolveUnknown {
        client_order_id: String,
        view: BrokerView,
        meta: JournalMeta,
    },
    BrokerDisconnect {
        meta: JournalMeta,
    },
    Compliance {
        static_ip: String,
        audit_retention_days: u32,
        algo_ids: Vec<String>,
        meta: JournalMeta,
    },
    Resume {
        reason: String,
        meta: JournalMeta,
    },
    QueryFailed {
        client_order_id: String,
        meta: JournalMeta,
    },
}

impl Command {
    pub fn journal_meta(&self) -> JournalMeta {
        match self {
            Self::OrderIntent(intent) => JournalMeta {
                correlation_id: intent.correlation_id.clone(),
                wall_ms: intent.wall_ms,
                mono_ns: intent.mono_ns,
            },
            Self::LoadInstruments { meta, .. }
            | Self::MarketData { meta, .. }
            | Self::Signal { meta, .. }
            | Self::MarkDispatched { meta, .. }
            | Self::DispatchFailed { meta, .. }
            | Self::BrokerAck { meta, .. }
            | Self::BrokerReject { meta, .. }
            | Self::Fill { meta, .. }
            | Self::CancelAck { meta, .. }
            | Self::CancelReject { meta, .. }
            | Self::ReplaceAck { meta, .. }
            | Self::ReplaceReject { meta, .. }
            | Self::Expired { meta, .. }
            | Self::RequestCancel { meta, .. }
            | Self::RequestReplace { meta, .. }
            | Self::Timer { meta, .. }
            | Self::ApplyConfig { meta, .. }
            | Self::Kill { meta, .. }
            | Self::ClearKill { meta, .. }
            | Self::Recon { meta, .. }
            | Self::SessionRoll { meta, .. }
            | Self::RecordHop { meta, .. }
            | Self::ResolveUnknown { meta, .. }
            | Self::BrokerDisconnect { meta, .. }
            | Self::Compliance { meta, .. }
            | Self::Resume { meta, .. }
            | Self::QueryFailed { meta, .. } => meta.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    InstrumentsLoaded {
        instruments: Vec<Instrument>,
    },
    MarketDataUpdated {
        instrument_id: String,
        ltp: i64,
        bid: i64,
        ask: i64,
        source_seq: u64,
        exchange_wall_ms: i64,
        gap: bool,
    },
    SignalRecorded {
        strategy_id: String,
        name: String,
        instrument_id: String,
        note: String,
        correlation_id: String,
    },
    OrderRegistered {
        order: Order,
    },
    RiskRejected {
        client_order_id: String,
        rule: RiskRule,
        detail: String,
    },
    RiskApproved {
        client_order_id: String,
        outbound: OutboundOrder,
    },
    OrderSent {
        client_order_id: String,
        wall_ms: i64,
        mono_ns: u64,
    },
    OrderUnknown {
        client_order_id: String,
        reason: String,
        wall_ms: i64,
    },
    BrokerAck {
        client_order_id: String,
        broker_order_id: String,
        wall_ms: i64,
        mono_ns: u64,
    },
    BrokerReject {
        client_order_id: String,
        reason: String,
    },
    FillApplied {
        client_order_id: String,
        trade_id: String,
        qty: i64,
        price: i64,
        wall_ms: i64,
        mono_ns: u64,
    },
    DuplicateFillIgnored {
        client_order_id: String,
        trade_id: String,
    },
    CancelRequested {
        client_order_id: String,
        request: CancelRequest,
    },
    CancelConfirmed {
        client_order_id: String,
    },
    CancelRejected {
        client_order_id: String,
        reason: String,
    },
    ReplaceRequested {
        client_order_id: String,
        new_qty: i64,
        new_price: i64,
        wall_ms: i64,
        request: ReplaceRequest,
    },
    ReplaceConfirmed {
        client_order_id: String,
        successor: Order,
    },
    ReplaceRejected {
        client_order_id: String,
        reason: String,
    },
    ReplaceRiskRejected {
        client_order_id: String,
        rule: RiskRule,
        detail: String,
    },
    QueryFailedNoted {
        client_order_id: String,
        wall_ms: i64,
    },
    OrderExpired {
        client_order_id: String,
    },
    LocalWithdraw {
        client_order_id: String,
        reason: String,
    },
    UnknownResolved {
        client_order_id: String,
        resolved_to: OrderState,
        broker_order_id: Option<String>,
        detail: String,
    },
    ResendAuthorized {
        client_order_id: String,
        outbound: OutboundOrder,
        wall_ms: i64,
    },
    ResendDeferred {
        client_order_id: String,
    },
    QueryRequested {
        client_order_id: String,
        wall_ms: i64,
    },
    ConfigApplied {
        config: RiskConfig,
    },
    KillApplied {
        scope: KillScope,
        reason: String,
    },
    KillCleared {
        scope: KillScope,
        reason: String,
    },
    PartitionHalted {
        reason: String,
    },
    PartitionResumed {
        reason: String,
    },
    ReconOk {
        detail: String,
    },
    SessionRolled {
        day: i32,
    },
    HopRecorded {
        client_order_id: String,
        stage: HopStage,
        wall_ms: i64,
        mono_ns: u64,
    },
    ComplianceNoted {
        static_ip: String,
        audit_retention_days: u32,
        algo_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Effect {
    Send(OutboundOrder),
    Cancel(CancelRequest),
    Replace(ReplaceRequest),
    Query(String),
}

pub fn effects(event: &Event) -> Vec<Effect> {
    match event {
        Event::RiskApproved { outbound, .. } | Event::ResendAuthorized { outbound, .. } => {
            vec![Effect::Send(outbound.clone())]
        }
        Event::CancelRequested { request, .. } => vec![Effect::Cancel(request.clone())],
        Event::ReplaceRequested { request, .. } => vec![Effect::Replace(request.clone())],
        Event::OrderUnknown {
            client_order_id, ..
        }
        | Event::QueryRequested {
            client_order_id, ..
        } => {
            vec![Effect::Query(client_order_id.clone())]
        }
        _ => Vec::new(),
    }
}
