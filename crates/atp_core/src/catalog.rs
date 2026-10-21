//! Built-in sample instrument master used by tests and the simulator demo.
//! Production loads the same schema from `config/instruments.toml`.

use crate::instrument::{cash_sessions, currency_sessions, mcx_sessions, Instrument};
use crate::strategy::{Strategy, ThresholdStrategy};
use crate::types::{parse_scaled, Exchange, OptionType, Product, Segment};

fn px(s: &str) -> i64 {
    parse_scaled(s).expect("sample price")
}

fn cash(exchange: Exchange, symbol: &str, low: &str, high: &str) -> Instrument {
    Instrument {
        id: format!("{}:CM:{symbol}", exchange.as_str()),
        exchange,
        segment: Segment::Cm,
        symbol: symbol.into(),
        lot_size: 1,
        tick_size: px("0.05"),
        expiry: None,
        strike: None,
        option_type: None,
        freeze_qty: 0,
        band_low: px(low),
        band_high: px(high),
        circuit_bps: 1_000,
        sessions: cash_sessions(),
    }
}

#[allow(clippy::too_many_arguments)]
fn future(
    exchange: Exchange,
    segment: Segment,
    symbol: &str,
    lot: i64,
    tick: &str,
    freeze: i64,
    expiry: i32,
    low: &str,
    high: &str,
    sessions: Vec<crate::instrument::Session>,
) -> Instrument {
    Instrument {
        id: format!("{}:{}:{symbol}", exchange.as_str(), segment.as_str()),
        exchange,
        segment,
        symbol: symbol.into(),
        lot_size: lot,
        tick_size: px(tick),
        expiry: Some(expiry),
        strike: None,
        option_type: None,
        freeze_qty: freeze,
        band_low: px(low),
        band_high: px(high),
        circuit_bps: 1_000,
        sessions,
    }
}

#[allow(clippy::too_many_arguments)]
fn option(
    exchange: Exchange,
    segment: Segment,
    symbol: &str,
    lot: i64,
    tick: &str,
    freeze: i64,
    expiry: i32,
    strike: &str,
    kind: OptionType,
    low: &str,
    high: &str,
    sessions: Vec<crate::instrument::Session>,
) -> Instrument {
    Instrument {
        id: format!("{}:{}:{symbol}", exchange.as_str(), segment.as_str()),
        exchange,
        segment,
        symbol: symbol.into(),
        lot_size: lot,
        tick_size: px(tick),
        expiry: Some(expiry),
        strike: Some(px(strike)),
        option_type: Some(kind),
        freeze_qty: freeze,
        band_low: px(low),
        band_high: px(high),
        circuit_bps: 2_000,
        sessions,
    }
}

pub fn sample_catalog() -> Vec<Instrument> {
    let mut v = vec![
        cash(Exchange::Nse, "RELIANCE", "2000", "4000"),
        cash(Exchange::Nse, "INFY", "1000", "2500"),
        cash(Exchange::Nse, "TCS", "2500", "5000"),
        cash(Exchange::Nse, "HDFCBANK", "1200", "2200"),
        future(
            Exchange::Nse,
            Segment::Fo,
            "NIFTY25OCTFUT",
            75,
            "0.05",
            1800,
            20261027,
            "15000",
            "40000",
            cash_sessions(),
        ),
        option(
            Exchange::Nse,
            Segment::Fo,
            "NIFTY25OCT24000CE",
            75,
            "0.05",
            1800,
            20261027,
            "24000",
            OptionType::Call,
            "0.05",
            "5000",
            cash_sessions(),
        ),
        option(
            Exchange::Nse,
            Segment::Fo,
            "NIFTY25OCT24000PE",
            75,
            "0.05",
            1800,
            20261027,
            "24000",
            OptionType::Put,
            "0.05",
            "5000",
            cash_sessions(),
        ),
        future(
            Exchange::Nse,
            Segment::Fo,
            "BANKNIFTY25OCTFUT",
            15,
            "0.05",
            900,
            20261027,
            "30000",
            "80000",
            cash_sessions(),
        ),
        future(
            Exchange::Nse,
            Segment::Fo,
            "RELIANCE25OCTFUT",
            250,
            "0.05",
            5000,
            20261027,
            "2000",
            "4000",
            cash_sessions(),
        ),
        cash(Exchange::Bse, "RELIANCE", "2000", "4000"),
        cash(Exchange::Bse, "TCS", "2500", "5000"),
        future(
            Exchange::Bse,
            Segment::Fo,
            "SENSEX25OCTFUT",
            20,
            "0.05",
            500,
            20261027,
            "50000",
            "120000",
            cash_sessions(),
        ),
        option(
            Exchange::Bse,
            Segment::Fo,
            "SENSEX25OCT80000CE",
            20,
            "0.05",
            500,
            20261027,
            "80000",
            OptionType::Call,
            "0.05",
            "15000",
            cash_sessions(),
        ),
        future(
            Exchange::Mcx,
            Segment::Co,
            "CRUDEOIL25NOVFUT",
            1,
            "1",
            100,
            20261118,
            "1000",
            "20000",
            mcx_sessions(),
        ),
        future(
            Exchange::Mcx,
            Segment::Co,
            "GOLD25DECFUT",
            1,
            "1",
            100,
            20261204,
            "50000",
            "150000",
            mcx_sessions(),
        ),
        future(
            Exchange::Mcx,
            Segment::Co,
            "SILVER25DECFUT",
            1,
            "1",
            100,
            20261204,
            "60000",
            "200000",
            mcx_sessions(),
        ),
        future(
            Exchange::Mcx,
            Segment::Co,
            "NATURALGAS25NOVFUT",
            1,
            "0.1",
            200,
            20261124,
            "50",
            "1000",
            mcx_sessions(),
        ),
        future(
            Exchange::Nse,
            Segment::Cd,
            "USDINR25OCTFUT",
            1,
            "0.0025",
            1000,
            20261027,
            "70",
            "110",
            currency_sessions(),
        ),
        option(
            Exchange::Nse,
            Segment::Cd,
            "USDINR25OCT85CE",
            1,
            "0.0025",
            1000,
            20261027,
            "85",
            OptionType::Call,
            "0.0025",
            "10",
            currency_sessions(),
        ),
        future(
            Exchange::Nse,
            Segment::Cd,
            "EURINR25OCTFUT",
            1,
            "0.0025",
            1000,
            20261027,
            "80",
            "130",
            currency_sessions(),
        ),
        future(
            Exchange::Bse,
            Segment::Cd,
            "USDINR25OCTFUT",
            1,
            "0.0025",
            1000,
            20261027,
            "70",
            "110",
            currency_sessions(),
        ),
    ];
    v.sort_by(|a, b| a.id.cmp(&b.id));
    for inst in &v {
        inst.validate().expect("sample instrument");
    }
    v
}

#[derive(Debug, Clone, Copy)]
pub struct SampleLeg {
    pub strategy_id: &'static str,
    pub algo_id: &'static str,
    pub instrument_id: &'static str,
    pub buy: &'static str,
    pub sell: &'static str,
    pub qty: i64,
    pub product: Product,
}

pub fn sample_legs() -> &'static [SampleLeg] {
    &[
        SampleLeg {
            strategy_id: "nse-cm",
            algo_id: "ALGONSCM001",
            instrument_id: "NSE:CM:RELIANCE",
            buy: "2500",
            sell: "2600",
            qty: 1,
            product: Product::Cnc,
        },
        SampleLeg {
            strategy_id: "nse-fo",
            algo_id: "ALGONSFO001",
            instrument_id: "NSE:FO:NIFTY25OCTFUT",
            buy: "25000",
            sell: "25500",
            qty: 75,
            product: Product::Nrml,
        },
        SampleLeg {
            strategy_id: "bse-cm",
            algo_id: "ALGOBSCM001",
            instrument_id: "BSE:CM:RELIANCE",
            buy: "2500",
            sell: "2600",
            qty: 1,
            product: Product::Cnc,
        },
        SampleLeg {
            strategy_id: "bse-fo",
            algo_id: "ALGOBSFO001",
            instrument_id: "BSE:FO:SENSEX25OCTFUT",
            buy: "80000",
            sell: "82000",
            qty: 20,
            product: Product::Nrml,
        },
        SampleLeg {
            strategy_id: "mcx-co",
            algo_id: "ALGOMCX0001",
            instrument_id: "MCX:CO:CRUDEOIL25NOVFUT",
            buy: "6500",
            sell: "6800",
            qty: 1,
            product: Product::Nrml,
        },
        SampleLeg {
            strategy_id: "nse-cd",
            algo_id: "ALGONSCD001",
            instrument_id: "NSE:CD:USDINR25OCTFUT",
            buy: "84",
            sell: "85",
            qty: 1,
            product: Product::Nrml,
        },
        SampleLeg {
            strategy_id: "bse-cd",
            algo_id: "ALGOBSCD001",
            instrument_id: "BSE:CD:USDINR25OCTFUT",
            buy: "84",
            sell: "85",
            qty: 1,
            product: Product::Nrml,
        },
    ]
}

pub fn sample_strategies() -> Vec<Box<dyn Strategy>> {
    sample_legs()
        .iter()
        .map(|leg| {
            Box::new(ThresholdStrategy {
                id: leg.strategy_id.into(),
                algo_id: leg.algo_id.into(),
                account_id: "ACC1".into(),
                instrument_id: leg.instrument_id.into(),
                buy_below: px(leg.buy),
                sell_above: px(leg.sell),
                qty: leg.qty,
                product: leg.product,
                order_type_limit: true,
                entered: false,
                exited: false,
            }) as Box<dyn Strategy>
        })
        .collect()
}

#[cfg(test)]
mod file_roundtrip {
    use super::sample_catalog;
    use crate::config::{demo_risk_config, load_risk_toml};
    use crate::instrument::load_instruments_toml;

    #[test]
    fn instruments_toml_matches_the_sample_book() {
        let text = include_str!("../../../config/instruments.toml");
        let mut loaded = load_instruments_toml(text).expect("instruments.toml");
        loaded.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(loaded, sample_catalog());
    }

    #[test]
    fn risk_toml_matches_the_demo_limits() {
        let mut demo = demo_risk_config();
        demo.strategies
            .sort_by(|a, b| a.strategy_id.cmp(&b.strategy_id));
        demo.holidays.sort_unstable();
        let loaded = load_risk_toml(include_str!("../../../config/risk.toml")).expect("risk.toml");
        assert_eq!(loaded, demo);
    }
}
