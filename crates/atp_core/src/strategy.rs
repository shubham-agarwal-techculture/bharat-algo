//! Strategy plugins see market and position snapshots and return intents.
//! They have no broker handle. The runtime journals every action they return
//! and does not call them again during replay.

use crate::types::{OrderIntent, OrderType, Product, Side};

#[derive(Debug, Clone)]
pub struct StrategyCtx<'a> {
    pub account_id: &'a str,
    pub instrument_id: &'a str,
    pub ltp: i64,
    pub position_qty: i64,
    pub correlation_id: &'a str,
    pub wall_ms: i64,
    pub mono_ns: u64,
    pub killed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrategyAction {
    Signal { name: String, note: String },
    Intent(OrderIntent),
}

pub trait Strategy: Send {
    fn id(&self) -> &str;
    fn algo_id(&self) -> &str;
    fn instrument_id(&self) -> &str;
    fn on_market(&mut self, ctx: &StrategyCtx<'_>) -> Vec<StrategyAction>;
}

/// One-shot threshold: buy one order when LTP is at or below `buy_below` and
/// the strategy is flat; sell when LTP is at or above `sell_above` and long.
/// Intent ids are stable so a retried evaluation is idempotent.
#[derive(Debug, Clone)]
pub struct ThresholdStrategy {
    pub id: String,
    pub algo_id: String,
    pub account_id: String,
    pub instrument_id: String,
    pub buy_below: i64,
    pub sell_above: i64,
    pub qty: i64,
    pub product: Product,
    pub order_type_limit: bool,
    pub entered: bool,
    pub exited: bool,
}

impl Strategy for ThresholdStrategy {
    fn id(&self) -> &str {
        &self.id
    }
    fn algo_id(&self) -> &str {
        &self.algo_id
    }
    fn instrument_id(&self) -> &str {
        &self.instrument_id
    }

    fn on_market(&mut self, ctx: &StrategyCtx<'_>) -> Vec<StrategyAction> {
        if ctx.killed || ctx.instrument_id != self.instrument_id {
            return Vec::new();
        }
        if !self.entered && ctx.position_qty == 0 && ctx.ltp <= self.buy_below {
            self.entered = true;
            let intent_id = format!("{}-e", self.id);
            return vec![
                StrategyAction::Signal {
                    name: "entry".into(),
                    note: format!("ltp {} <= {}", ctx.ltp, self.buy_below),
                },
                StrategyAction::Intent(OrderIntent {
                    intent_id,
                    account_id: self.account_id.clone(),
                    strategy_id: self.id.clone(),
                    instrument_id: self.instrument_id.clone(),
                    side: Side::Buy,
                    order_type: if self.order_type_limit {
                        OrderType::Limit
                    } else {
                        OrderType::Market
                    },
                    product: self.product,
                    validity: crate::types::Validity::Day,
                    qty: self.qty,
                    price: ctx.ltp,
                    algo_id: self.algo_id.clone(),
                    correlation_id: ctx.correlation_id.to_string(),
                    wall_ms: ctx.wall_ms,
                    mono_ns: ctx.mono_ns,
                }),
            ];
        }
        if !self.exited && ctx.position_qty > 0 && ctx.ltp >= self.sell_above {
            self.exited = true;
            let intent_id = format!("{}-x", self.id);
            return vec![
                StrategyAction::Signal {
                    name: "exit".into(),
                    note: format!("ltp {} >= {}", ctx.ltp, self.sell_above),
                },
                StrategyAction::Intent(OrderIntent {
                    intent_id,
                    account_id: self.account_id.clone(),
                    strategy_id: self.id.clone(),
                    instrument_id: self.instrument_id.clone(),
                    side: Side::Sell,
                    order_type: OrderType::Limit,
                    product: self.product,
                    validity: crate::types::Validity::Day,
                    qty: ctx.position_qty.min(self.qty),
                    price: ctx.ltp,
                    algo_id: self.algo_id.clone(),
                    correlation_id: ctx.correlation_id.to_string(),
                    wall_ms: ctx.wall_ms,
                    mono_ns: ctx.mono_ns,
                }),
            ];
        }
        Vec::new()
    }
}
