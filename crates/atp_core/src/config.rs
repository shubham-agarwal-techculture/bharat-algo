//! Versioned risk limits. The journal stores the parsed integer form.
//! TOML at the edge uses decimal INR strings so limits are exact.

use crate::types::{parse_scaled, MarginModel, StrategyRisk};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskConfig {
    pub version: u64,
    pub account_id: String,
    pub max_order_qty: i64,
    pub max_order_value: i64,
    pub max_position_qty: i64,
    pub max_exposure: i64,
    pub max_margin: i64,
    pub account_daily_loss: i64,
    pub account_ops: u32,
    pub segment_ops: u32,
    pub fat_finger_price_bps: i64,
    pub fat_finger_qty: i64,
    pub fat_finger_notional: i64,
    pub stale_feed_ms: i64,
    pub unknown_timeout_ms: i64,
    pub max_resends: u32,
    pub margin: MarginModel,
    pub holidays: Vec<i32>,
    pub strategies: Vec<StrategyRisk>,
}

impl RiskConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.version == 0 {
            return Err("config version must be >= 1".into());
        }
        if self.account_id.is_empty() {
            return Err("account id required".into());
        }
        if self.max_order_qty <= 0
            || self.max_order_value <= 0
            || self.max_position_qty <= 0
            || self.max_exposure <= 0
            || self.max_margin <= 0
            || self.account_daily_loss <= 0
        {
            return Err("limits must be positive".into());
        }
        if self.account_ops == 0 || self.segment_ops == 0 {
            return Err("ops limits must be positive".into());
        }
        if self.fat_finger_price_bps <= 0
            || self.fat_finger_qty <= 0
            || self.fat_finger_notional <= 0
            || self.stale_feed_ms <= 0
            || self.unknown_timeout_ms <= 0
        {
            return Err("thresholds must be positive".into());
        }
        if self.strategies.is_empty() {
            return Err("at least one strategy registration is required".into());
        }
        let mut ids = std::collections::BTreeSet::new();
        for s in &self.strategies {
            if s.strategy_id.is_empty() || s.algo_id.is_empty() {
                return Err("strategy id and algo id are required".into());
            }
            if s.algo_id.len() > 20 {
                return Err(format!("algo id {} exceeds 20 chars", s.algo_id));
            }
            if !ids.insert(s.strategy_id.clone()) {
                return Err(format!("duplicate strategy {}", s.strategy_id));
            }
            if s.daily_loss <= 0 || s.max_order_qty <= 0 || s.max_ops == 0 {
                return Err(format!("bad limits for {}", s.strategy_id));
            }
        }
        match &self.margin {
            MarginModel::Unavailable => {}
            MarginModel::FlatBps { cm, fo, cd, co } => {
                if *cm == 0 || *fo == 0 || *cd == 0 || *co == 0 {
                    return Err(
                        "margin factors must be non-zero or the model must be unavailable".into(),
                    );
                }
            }
        }
        Ok(())
    }

    pub fn strategy(&self, id: &str) -> Option<&StrategyRisk> {
        self.strategies.iter().find(|s| s.strategy_id == id)
    }
}

#[derive(Debug, Deserialize)]
struct RiskToml {
    version: u64,
    account_id: String,
    max_order_qty: i64,
    max_order_value: String,
    max_position_qty: i64,
    max_exposure: String,
    max_margin: String,
    account_daily_loss: String,
    account_ops: u32,
    segment_ops: u32,
    fat_finger_price_bps: i64,
    fat_finger_qty: i64,
    fat_finger_notional: String,
    stale_feed_ms: i64,
    unknown_timeout_ms: i64,
    max_resends: u32,
    margin_cm_bps: u32,
    margin_fo_bps: u32,
    margin_cd_bps: u32,
    margin_co_bps: u32,
    #[serde(default)]
    holidays: Vec<i32>,
    strategy: Vec<StrategyToml>,
}

#[derive(Debug, Deserialize)]
struct StrategyToml {
    strategy_id: String,
    algo_id: String,
    daily_loss: String,
    max_order_qty: i64,
    max_ops: u32,
}

pub fn load_risk_toml(text: &str) -> Result<RiskConfig, String> {
    let raw: RiskToml = toml::from_str(text).map_err(|e| e.to_string())?;
    let mut holidays = raw.holidays;
    holidays.sort_unstable();
    holidays.dedup();
    let mut strategies: Vec<StrategyRisk> = raw
        .strategy
        .into_iter()
        .map(|s| {
            Ok(StrategyRisk {
                strategy_id: s.strategy_id,
                algo_id: s.algo_id,
                daily_loss: parse_scaled(&s.daily_loss)?,
                max_order_qty: s.max_order_qty,
                max_ops: s.max_ops,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    strategies.sort_by(|a, b| a.strategy_id.cmp(&b.strategy_id));
    let cfg = RiskConfig {
        version: raw.version,
        account_id: raw.account_id,
        max_order_qty: raw.max_order_qty,
        max_order_value: parse_scaled(&raw.max_order_value)?,
        max_position_qty: raw.max_position_qty,
        max_exposure: parse_scaled(&raw.max_exposure)?,
        max_margin: parse_scaled(&raw.max_margin)?,
        account_daily_loss: parse_scaled(&raw.account_daily_loss)?,
        account_ops: raw.account_ops,
        segment_ops: raw.segment_ops,
        fat_finger_price_bps: raw.fat_finger_price_bps,
        fat_finger_qty: raw.fat_finger_qty,
        fat_finger_notional: parse_scaled(&raw.fat_finger_notional)?,
        stale_feed_ms: raw.stale_feed_ms,
        unknown_timeout_ms: raw.unknown_timeout_ms,
        max_resends: raw.max_resends,
        margin: MarginModel::FlatBps {
            cm: raw.margin_cm_bps,
            fo: raw.margin_fo_bps,
            cd: raw.margin_cd_bps,
            co: raw.margin_co_bps,
        },
        holidays,
        strategies,
    };
    cfg.validate()?;
    Ok(cfg)
}

/// Wide limits for the simulator demo. Not a production limit set.
pub fn demo_risk_config() -> RiskConfig {
    let strat = |id: &str, algo: &str| StrategyRisk {
        strategy_id: id.into(),
        algo_id: algo.into(),
        daily_loss: parse_scaled("100000").unwrap(),
        max_order_qty: 100_000,
        max_ops: 20,
    };
    RiskConfig {
        version: 1,
        account_id: "ACC1".into(),
        max_order_qty: 100_000,
        max_order_value: parse_scaled("50000000").unwrap(),
        max_position_qty: 100_000,
        max_exposure: parse_scaled("50000000").unwrap(),
        max_margin: parse_scaled("10000000").unwrap(),
        account_daily_loss: parse_scaled("100000").unwrap(),
        account_ops: 40,
        segment_ops: 20,
        fat_finger_price_bps: 500,
        fat_finger_qty: 50_000,
        fat_finger_notional: parse_scaled("5000000").unwrap(),
        stale_feed_ms: 5_000,
        unknown_timeout_ms: 500,
        max_resends: 1,
        margin: MarginModel::FlatBps {
            cm: 10_000,
            fo: 1_500,
            cd: 500,
            co: 1_000,
        },
        holidays: vec![20260126],
        strategies: vec![
            strat("nse-cm", "ALGONSCM001"),
            strat("nse-fo", "ALGONSFO001"),
            strat("bse-cm", "ALGOBSCM001"),
            strat("bse-fo", "ALGOBSFO001"),
            strat("mcx-co", "ALGOMCX0001"),
            strat("nse-cd", "ALGONSCD001"),
            strat("bse-cd", "ALGOBSCD001"),
            strat("manual", "ALGOMANUAL1"),
        ],
    }
}
