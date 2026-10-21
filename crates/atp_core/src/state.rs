//! Partition state. One account, one writer. Maps are ordered so a state hash
//! does not depend on insertion order.

use crate::config::RiskConfig;
use crate::instrument::Instrument;
use crate::pnl::Position;
use crate::types::{Order, OrderState};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub instrument_id: String,
    pub ltp: i64,
    pub bid: i64,
    pub ask: i64,
    pub source_seq: u64,
    pub exchange_wall_ms: i64,
    pub gap: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct KillFlags {
    pub global: bool,
    pub accounts: BTreeSet<String>,
    pub strategies: BTreeSet<String>,
    pub instruments: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalRecord {
    pub strategy_id: String,
    pub name: String,
    pub instrument_id: String,
    pub note: String,
    pub correlation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComplianceRecord {
    pub static_ip: String,
    pub audit_retention_days: u32,
    pub algo_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RateState {
    pub account: Vec<i64>,
    /// Segment code → approval timestamps (event time, ms).
    pub by_segment: BTreeMap<String, Vec<i64>>,
    pub by_strategy: BTreeMap<String, Vec<i64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionState {
    pub account_id: String,
    pub last_seq: u64,
    pub orders: BTreeMap<String, Order>,
    pub intent_ids: BTreeSet<String>,
    pub positions: BTreeMap<String, Position>,
    pub market: BTreeMap<String, MarketSnapshot>,
    pub instruments: BTreeMap<String, Instrument>,
    pub risk: Option<RiskConfig>,
    pub kills: KillFlags,
    pub halt: Option<String>,
    pub rates: RateState,
    pub session_day: i32,
    pub last_signals: BTreeMap<String, SignalRecord>,
    pub compliance: Option<ComplianceRecord>,
    pub seen_trade_ids: BTreeSet<String>,
}

impl PartitionState {
    pub fn new(account_id: impl Into<String>) -> Self {
        Self {
            account_id: account_id.into(),
            last_seq: 0,
            orders: BTreeMap::new(),
            intent_ids: BTreeSet::new(),
            positions: BTreeMap::new(),
            market: BTreeMap::new(),
            instruments: BTreeMap::new(),
            risk: None,
            kills: KillFlags::default(),
            halt: None,
            rates: RateState::default(),
            session_day: 0,
            last_signals: BTreeMap::new(),
            compliance: None,
            seen_trade_ids: BTreeSet::new(),
        }
    }

    pub fn is_killed(&self, strategy_id: &str, instrument_id: &str, account_id: &str) -> bool {
        self.kills.global
            || self.kills.accounts.contains(account_id)
            || self.kills.strategies.contains(strategy_id)
            || self.kills.instruments.contains(instrument_id)
    }

    pub fn has_unknown_order(&self) -> bool {
        self.orders.values().any(|o| o.state == OrderState::Unknown)
    }
}

pub fn is_working(state: OrderState) -> bool {
    matches!(
        state,
        OrderState::RiskApproved
            | OrderState::Sent
            | OrderState::Acked
            | OrderState::PartiallyFilled
            | OrderState::CancelPending
            | OrderState::ReplacePending
            | OrderState::Unknown
    )
}
