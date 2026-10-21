//! Zerodha Kite Connect skeleton.
//!
//! NOT YET LIVE. This mapping is pure and unit-tested so a later certification
//! pass has a frozen request shape. Every method that would touch the network
//! returns [`NOT_YET_LIVE`]. Do not point real capital at this adapter.

use crate::broker::{Broker, BrokerError, Inbound};
use atp_core::{
    BrokerView, CancelRequest, Exchange, KillScope, OrderType, OutboundOrder, Product, ReconReport,
    ReplaceRequest, Segment, Side, Validity,
};
use serde::Serialize;

pub const NOT_YET_LIVE: &str =
    "Zerodha Kite Connect adapter is a skeleton and is not certified for live capital";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KiteParams {
    pub exchange: &'static str,
    pub tradingsymbol: String,
    pub transaction_type: &'static str,
    pub order_type: &'static str,
    pub product: &'static str,
    pub validity: &'static str,
    pub quantity: i64,
    pub price: String,
    /// Kite tag, used as the idempotent client order id. Maximum 20 characters.
    pub tag: String,
    pub variety: &'static str,
    pub algo_id: String,
}

pub fn to_kite(order: &OutboundOrder) -> Result<KiteParams, String> {
    if order.algo_id.is_empty() {
        return Err("algo id is required on every outbound order".into());
    }
    if order.client_order_id.is_empty() || order.client_order_id.len() > 20 {
        return Err("kite tag must be 1-20 characters".into());
    }
    Ok(KiteParams {
        exchange: kite_exchange(order.exchange, order.segment)?,
        tradingsymbol: order.symbol.clone(),
        transaction_type: match order.side {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        },
        order_type: match order.order_type {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
        },
        product: match order.product {
            Product::Cnc => "CNC",
            Product::Mis => "MIS",
            Product::Nrml => "NRML",
        },
        validity: match order.validity {
            Validity::Day => "DAY",
            Validity::Ioc => "IOC",
        },
        quantity: order.qty,
        price: atp_core::format_scaled(order.price),
        tag: order.client_order_id.clone(),
        variety: "regular",
        algo_id: order.algo_id.clone(),
    })
}

fn kite_exchange(exchange: Exchange, segment: Segment) -> Result<&'static str, String> {
    Ok(match (exchange, segment) {
        (Exchange::Nse, Segment::Cm) => "NSE",
        (Exchange::Nse, Segment::Fo) => "NFO",
        (Exchange::Nse, Segment::Cd) => "CDS",
        (Exchange::Bse, Segment::Cm) => "BSE",
        (Exchange::Bse, Segment::Fo) => "BFO",
        (Exchange::Bse, Segment::Cd) => "BCD",
        (Exchange::Mcx, Segment::Co) => "MCX",
        _ => {
            return Err(format!(
                "no Kite exchange for {} {}",
                exchange.as_str(),
                segment.as_str()
            ))
        }
    })
}

#[derive(Debug, Clone)]
pub struct KiteAdapter {
    pub api_key: String,
    pub access_token: String,
    pub user_id: String,
    now: i64,
}

impl KiteAdapter {
    pub fn skeleton(
        api_key: impl Into<String>,
        access_token: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Self {
        Self {
            api_key: api_key.into(),
            access_token: access_token.into(),
            user_id: user_id.into(),
            now: 0,
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "kite skeleton user {} key-len {} token-len {} ({NOT_YET_LIVE})",
            self.user_id,
            self.api_key.len(),
            self.access_token.len()
        )
    }
}

impl Broker for KiteAdapter {
    fn submit(&mut self, order: &OutboundOrder) -> Result<(), BrokerError> {
        let _ = to_kite(order).map_err(BrokerError::Other)?;
        Err(BrokerError::NotYetLive(NOT_YET_LIVE))
    }
    fn cancel(&mut self, _req: &CancelRequest) -> Result<(), BrokerError> {
        Err(BrokerError::NotYetLive(NOT_YET_LIVE))
    }
    fn replace(&mut self, _req: &ReplaceRequest) -> Result<(), BrokerError> {
        Err(BrokerError::NotYetLive(NOT_YET_LIVE))
    }
    fn query(&mut self, _client_order_id: &str) -> Result<BrokerView, BrokerError> {
        Err(BrokerError::NotYetLive(NOT_YET_LIVE))
    }
    fn poll(&mut self) -> Vec<Inbound> {
        Vec::new()
    }
    fn books(&mut self) -> Result<ReconReport, BrokerError> {
        Err(BrokerError::NotYetLive(NOT_YET_LIVE))
    }
    fn cancel_all(&mut self, _scope: &KillScope) -> Result<Vec<String>, BrokerError> {
        Err(BrokerError::NotYetLive(NOT_YET_LIVE))
    }
    fn set_now(&mut self, wall_ms: i64) {
        self.now = wall_ms;
    }
    fn name(&self) -> &str {
        "kite"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atp_core::{OrderType, Product, Side, Validity};

    fn sample(exchange: Exchange, segment: Segment) -> OutboundOrder {
        OutboundOrder {
            client_order_id: "nse-cm-e".into(),
            account_id: "ACC1".into(),
            strategy_id: "nse-cm".into(),
            instrument_id: "NSE:CM:RELIANCE".into(),
            exchange,
            segment,
            symbol: "RELIANCE".into(),
            side: Side::Buy,
            order_type: OrderType::Limit,
            product: Product::Cnc,
            validity: Validity::Day,
            qty: 1,
            price: 25_000_000,
            algo_id: "ALGONSCM001".into(),
            correlation_id: "c1".into(),
        }
    }

    #[test]
    fn maps_every_supported_segment() {
        let cases = [
            (Exchange::Nse, Segment::Cm, "NSE"),
            (Exchange::Nse, Segment::Fo, "NFO"),
            (Exchange::Nse, Segment::Cd, "CDS"),
            (Exchange::Bse, Segment::Cm, "BSE"),
            (Exchange::Bse, Segment::Fo, "BFO"),
            (Exchange::Bse, Segment::Cd, "BCD"),
            (Exchange::Mcx, Segment::Co, "MCX"),
        ];
        for (ex, seg, kite) in cases {
            let params = to_kite(&sample(ex, seg)).unwrap();
            assert_eq!(params.exchange, kite);
            assert_eq!(params.tag, "nse-cm-e");
            assert_eq!(params.transaction_type, "BUY");
            assert_eq!(params.price, "2500.0000");
            assert_eq!(params.algo_id, "ALGONSCM001");
        }
    }

    #[test]
    fn submit_is_not_yet_live() {
        let mut kite = KiteAdapter::skeleton("key", "token", "AB1234");
        let err = kite
            .submit(&sample(Exchange::Nse, Segment::Cm))
            .unwrap_err();
        assert!(matches!(err, BrokerError::NotYetLive(_)));
        assert!(kite.describe().contains("NOT") || kite.describe().contains("not certified"));
        assert!(kite.poll().is_empty());
    }
}
