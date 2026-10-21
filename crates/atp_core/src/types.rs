//! Domain types shared by the pure core.
//!
//! Prices are integers in INR × [`PRICE_SCALE`] (10_000). One price unit is
//! 0.0001 INR, so equity ticks of 0.05 and currency ticks of 0.0025 are exact.
//! Notional for a limit order is `price * qty` in the same scaled-INR unit.

use serde::{Deserialize, Serialize};
use std::fmt;

pub const PRICE_SCALE: i64 = 10_000;
pub const SCHEMA_VERSION: u32 = 1;
pub const RATE_WINDOW_MS: i64 = 1_000;
pub const MAX_SLICES: usize = 20;
pub const GENESIS_HASH: &str = "GENESIS";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Exchange {
    Nse,
    Bse,
    Mcx,
}

impl Exchange {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Nse => "NSE",
            Self::Bse => "BSE",
            Self::Mcx => "MCX",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "NSE" => Some(Self::Nse),
            "BSE" => Some(Self::Bse),
            "MCX" => Some(Self::Mcx),
            _ => None,
        }
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Segment {
    /// Equity cash.
    Cm,
    /// Equity futures and options.
    Fo,
    /// Currency derivatives.
    Cd,
    /// Commodity derivatives (MCX).
    Co,
}

impl Segment {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cm => "CM",
            Self::Fo => "FO",
            Self::Cd => "CD",
            Self::Co => "CO",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "CM" => Some(Self::Cm),
            "FO" => Some(Self::Fo),
            "CD" => Some(Self::Cd),
            "CO" => Some(Self::Co),
            _ => None,
        }
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OptionType {
    #[serde(rename = "CE")]
    Call,
    #[serde(rename = "PE")]
    Put,
}

impl OptionType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Call => "CE",
            Self::Put => "PE",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "CE" => Some(Self::Call),
            "PE" => Some(Self::Put),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }

    pub fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Product {
    /// Delivery / cash.
    Cnc,
    /// Intraday.
    Mis,
    /// Carry-forward derivatives.
    Nrml,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Validity {
    Day,
    Ioc,
}

/// Explicit order lifecycle. Terminal states have no outbound transitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderState {
    New,
    RiskApproved,
    RiskRejected,
    Sent,
    Acked,
    Rejected,
    PartiallyFilled,
    Filled,
    CancelPending,
    Cancelled,
    ReplacePending,
    Replaced,
    Expired,
    Unknown,
}

impl OrderState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::RiskRejected
                | Self::Rejected
                | Self::Filled
                | Self::Cancelled
                | Self::Replaced
                | Self::Expired
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::RiskApproved => "RISK_APPROVED",
            Self::RiskRejected => "RISK_REJECTED",
            Self::Sent => "SENT",
            Self::Acked => "ACKED",
            Self::Rejected => "REJECTED",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::CancelPending => "CANCEL_PENDING",
            Self::Cancelled => "CANCELLED",
            Self::ReplacePending => "REPLACE_PENDING",
            Self::Replaced => "REPLACED",
            Self::Expired => "EXPIRED",
            Self::Unknown => "UNKNOWN",
        }
    }

    pub fn all() -> &'static [OrderState] {
        &ALL_STATES
    }
}

const ALL_STATES: [OrderState; 14] = [
    OrderState::New,
    OrderState::RiskApproved,
    OrderState::RiskRejected,
    OrderState::Sent,
    OrderState::Acked,
    OrderState::Rejected,
    OrderState::PartiallyFilled,
    OrderState::Filled,
    OrderState::CancelPending,
    OrderState::Cancelled,
    OrderState::ReplacePending,
    OrderState::Replaced,
    OrderState::Expired,
    OrderState::Unknown,
];

impl fmt::Display for OrderState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskRule {
    MaxQty,
    MaxOrderValue,
    PositionLimit,
    ExposureLimit,
    Margin,
    MarginUnavailable,
    StrategyDailyLoss,
    AccountDailyLoss,
    OrderRate,
    FatFingerPrice,
    FatFingerQty,
    FatFingerNotional,
    SelfTrade,
    TradingHours,
    StaleFeed,
    FeedGap,
    PriceBand,
    KillSwitch,
    AlgoId,
    LotSize,
    TickSize,
    InvalidPrice,
    StrategyNotRegistered,
    AccountMismatch,
    ConfigNotLoaded,
    UncertainState,
    PartitionHalted,
    MarketDataMissing,
    SliceOverflow,
    Overflow,
    Instrument,
}

impl RiskRule {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MaxQty => "MAX_QTY",
            Self::MaxOrderValue => "MAX_ORDER_VALUE",
            Self::PositionLimit => "POSITION_LIMIT",
            Self::ExposureLimit => "EXPOSURE_LIMIT",
            Self::Margin => "MARGIN",
            Self::MarginUnavailable => "MARGIN_UNAVAILABLE",
            Self::StrategyDailyLoss => "STRATEGY_DAILY_LOSS",
            Self::AccountDailyLoss => "ACCOUNT_DAILY_LOSS",
            Self::OrderRate => "ORDER_RATE",
            Self::FatFingerPrice => "FAT_FINGER_PRICE",
            Self::FatFingerQty => "FAT_FINGER_QTY",
            Self::FatFingerNotional => "FAT_FINGER_NOTIONAL",
            Self::SelfTrade => "SELF_TRADE",
            Self::TradingHours => "TRADING_HOURS",
            Self::StaleFeed => "STALE_FEED",
            Self::FeedGap => "FEED_GAP",
            Self::PriceBand => "PRICE_BAND",
            Self::KillSwitch => "KILL_SWITCH",
            Self::AlgoId => "ALGO_ID",
            Self::LotSize => "LOT_SIZE",
            Self::TickSize => "TICK_SIZE",
            Self::InvalidPrice => "INVALID_PRICE",
            Self::StrategyNotRegistered => "STRATEGY_NOT_REGISTERED",
            Self::AccountMismatch => "ACCOUNT_MISMATCH",
            Self::ConfigNotLoaded => "CONFIG_NOT_LOADED",
            Self::UncertainState => "UNCERTAIN_STATE",
            Self::PartitionHalted => "PARTITION_HALTED",
            Self::MarketDataMissing => "MARKET_DATA_MISSING",
            Self::SliceOverflow => "SLICE_OVERFLOW",
            Self::Overflow => "OVERFLOW",
            Self::Instrument => "INSTRUMENT",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HopStage {
    Signal,
    Risk,
    Oms,
    Gateway,
    Ack,
    Fill,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hop {
    pub stage: HopStage,
    pub wall_ms: i64,
    pub mono_ns: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTime {
    pub wall_ms: i64,
    pub mono_ns: u64,
}

/// Margin model hook. The flat model is deterministic and uses only state.
/// A portfolio SPAN implementation would use the same signature; it must not
/// perform I/O inside `decide`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "model", rename_all = "snake_case")]
pub enum MarginModel {
    /// `required = price * qty * bps / 10_000` per segment. No netting.
    FlatBps { cm: u32, fo: u32, cd: u32, co: u32 },
    /// Explicit fail-closed hook used when the margin source is unavailable.
    Unavailable,
}

impl MarginModel {
    pub fn segment_bps(&self, segment: Segment) -> Option<u32> {
        match self {
            Self::Unavailable => None,
            Self::FlatBps { cm, fo, cd, co } => Some(match segment {
                Segment::Cm => *cm,
                Segment::Fo => *fo,
                Segment::Cd => *cd,
                Segment::Co => *co,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyRisk {
    pub strategy_id: String,
    pub algo_id: String,
    /// Scaled INR. See [`PRICE_SCALE`].
    pub daily_loss: i64,
    pub max_order_qty: i64,
    pub max_ops: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KillScope {
    Global,
    Account { account_id: String },
    Strategy { strategy_id: String },
    Instrument { instrument_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboundOrder {
    pub client_order_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub instrument_id: String,
    pub exchange: Exchange,
    pub segment: Segment,
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub product: Product,
    pub validity: Validity,
    pub qty: i64,
    pub price: i64,
    pub algo_id: String,
    pub correlation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRequest {
    pub client_order_id: String,
    pub broker_order_id: Option<String>,
    pub algo_id: String,
    pub correlation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaceRequest {
    pub client_order_id: String,
    pub broker_order_id: Option<String>,
    pub new_client_order_id: String,
    pub new_qty: i64,
    pub new_price: i64,
    pub correlation_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    pub client_order_id: String,
    pub broker_order_id: Option<String>,
    pub intent_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub instrument_id: String,
    pub segment: Segment,
    pub side: Side,
    pub order_type: OrderType,
    pub product: Product,
    pub validity: Validity,
    pub qty: i64,
    pub price: i64,
    pub filled_qty: i64,
    #[serde(with = "crate::types::i128_str")]
    pub filled_notional: i128,
    pub avg_fill_price: i64,
    pub state: OrderState,
    pub state_before_pending: Option<OrderState>,
    pub algo_id: String,
    pub parent_intent_id: String,
    pub slice_index: u32,
    pub slice_count: u32,
    pub risk_approved: bool,
    pub send_attempts: u32,
    pub replace_seq: u32,
    pub pending_resend: bool,
    pub needs_query: bool,
    pub correlation_id: String,
    pub created_wall_ms: i64,
    pub created_mono_ns: u64,
    pub last_wall_ms: i64,
    pub fill_trade_ids: std::collections::BTreeSet<String>,
    pub hops: Vec<Hop>,
    pub replaces: Option<String>,
    pub replaced_by: Option<String>,
    pub pending_replace_qty: Option<i64>,
    pub pending_replace_price: Option<i64>,
    pub reject_rule: Option<String>,
    pub reject_detail: Option<String>,
}

impl Order {
    pub fn leaves_qty(&self) -> i64 {
        (self.qty - self.filled_qty).max(0)
    }

    pub fn push_hop(&mut self, stage: HopStage, wall_ms: i64, mono_ns: u64) {
        self.hops.push(Hop {
            stage,
            wall_ms,
            mono_ns,
        });
        self.last_wall_ms = wall_ms;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderIntent {
    pub intent_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub instrument_id: String,
    pub side: Side,
    pub order_type: OrderType,
    pub product: Product,
    pub validity: Validity,
    pub qty: i64,
    pub price: i64,
    pub algo_id: String,
    pub correlation_id: String,
    pub wall_ms: i64,
    pub mono_ns: u64,
}

/// Broker's view of an order, supplied as a reconciliation fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BrokerView {
    NotFound,
    Working {
        broker_order_id: String,
        filled_qty: i64,
        leaves_qty: i64,
    },
    Filled {
        broker_order_id: String,
        filled_qty: i64,
    },
    Cancelled {
        broker_order_id: String,
        filled_qty: i64,
    },
    Rejected {
        broker_order_id: String,
        reason: String,
    },
    Expired {
        broker_order_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconOrder {
    pub client_order_id: String,
    pub broker_order_id: String,
    pub filled_qty: i64,
    pub leaves_qty: i64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconPosition {
    pub instrument_id: String,
    pub net_qty: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconTrade {
    pub trade_id: String,
    pub client_order_id: String,
    pub qty: i64,
    pub price: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconReport {
    pub orders: Vec<ReconOrder>,
    pub positions: Vec<ReconPosition>,
    pub trades: Vec<ReconTrade>,
}

pub fn pos_key(strategy_id: &str, instrument_id: &str) -> String {
    format!("{strategy_id}\u{1f}{instrument_id}")
}

pub fn notional(price: i64, qty: i64) -> Option<i128> {
    if price < 0 || qty < 0 {
        return None;
    }
    Some(price as i128 * qty as i128)
}

/// Parse a decimal string into scaled price units without binary floating point.
pub fn parse_scaled(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty decimal".into());
    }
    let neg = s.starts_with('-');
    let s = s
        .strip_prefix('-')
        .or_else(|| s.strip_prefix('+'))
        .unwrap_or(s);
    if s.is_empty() || s.starts_with('.') && s.len() == 1 {
        return Err(format!("bad decimal {s}"));
    }
    let (whole, frac) = match s.split_once('.') {
        Some((w, f)) => (w, f),
        None => (s, ""),
    };
    if whole.is_empty() && frac.is_empty() {
        return Err("bad decimal".into());
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) || !frac.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("bad decimal {s}"));
    }
    if frac.len() > 4 {
        return Err(format!("more than 4 decimal places in {s}"));
    }
    let w: i64 = if whole.is_empty() {
        0
    } else {
        whole
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?
    };
    let mut fbuf = frac.to_string();
    while fbuf.len() < 4 {
        fbuf.push('0');
    }
    let f: i64 = fbuf
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    let mut v = w
        .checked_mul(PRICE_SCALE)
        .and_then(|p| p.checked_add(f))
        .ok_or_else(|| "decimal overflow".to_string())?;
    if neg {
        v = -v;
    }
    Ok(v)
}

pub mod i128_str {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse::<i128>().map_err(serde::de::Error::custom)
    }
}

pub fn format_scaled(v: i64) -> String {
    let neg = v < 0;
    let v = v.unsigned_abs();
    let scale = PRICE_SCALE as u64;
    let whole = v / scale;
    let frac = v % scale;
    let body = format!("{whole}.{frac:04}");
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaled_prices_are_exact() {
        assert_eq!(parse_scaled("0.05").unwrap(), 500);
        assert_eq!(parse_scaled("0.0025").unwrap(), 25);
        assert_eq!(parse_scaled("2500").unwrap(), 25_000_000);
        assert_eq!(parse_scaled("84.5").unwrap(), 845_000);
        assert_eq!(format_scaled(25), "0.0025");
        assert_eq!(format_scaled(25_000_000), "2500.0000");
    }
}
