//! Instrument master covering NSE/BSE cash, F&O, currency, and MCX.

use crate::types::{parse_scaled, Exchange, OptionType, Segment};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// Inclusive minute-of-day IST.
    pub start_min: u16,
    /// Exclusive minute-of-day IST.
    pub end_min: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instrument {
    pub id: String,
    pub exchange: Exchange,
    pub segment: Segment,
    pub symbol: String,
    pub lot_size: i64,
    pub tick_size: i64,
    /// YYYYMMDD.
    pub expiry: Option<i32>,
    pub strike: Option<i64>,
    pub option_type: Option<OptionType>,
    /// 0 means the exchange does not impose a freeze quantity.
    pub freeze_qty: i64,
    pub band_low: i64,
    pub band_high: i64,
    /// Additional collar around LTP, in basis points. 0 disables the collar.
    pub circuit_bps: i64,
    pub sessions: Vec<Session>,
}

impl Instrument {
    pub fn validate(&self) -> Result<(), String> {
        let expected = format!(
            "{}:{}:{}",
            self.exchange.as_str(),
            self.segment.as_str(),
            self.symbol
        );
        if self.id != expected {
            return Err(format!("id {} does not match {expected}", self.id));
        }
        if self.symbol.is_empty() {
            return Err("empty symbol".into());
        }
        if self.lot_size <= 0 {
            return Err("lot size must be positive".into());
        }
        if self.tick_size <= 0 {
            return Err("tick size must be positive".into());
        }
        if self.band_low <= 0 || self.band_high < self.band_low {
            return Err("price band invalid".into());
        }
        if self.circuit_bps < 0 {
            return Err("circuit bps negative".into());
        }
        if self.freeze_qty < 0 {
            return Err("freeze qty negative".into());
        }
        if self.freeze_qty > 0
            && (self.freeze_qty < self.lot_size || self.freeze_qty % self.lot_size != 0)
        {
            return Err("freeze qty must be a positive multiple of lot size".into());
        }
        if self.sessions.is_empty() {
            return Err("no trading sessions".into());
        }
        for s in &self.sessions {
            if s.start_min >= s.end_min || s.end_min > 24 * 60 {
                return Err("session window invalid".into());
            }
        }
        match (self.option_type, self.strike, self.expiry) {
            (Some(_), Some(strike), Some(_)) if strike > 0 => {}
            (Some(_), _, _) => return Err("option requires strike and expiry".into()),
            (None, Some(_), _) => return Err("strike set without option type".into()),
            (None, None, None) if self.segment == Segment::Cm => {}
            (None, None, Some(_)) if self.segment != Segment::Cm => {}
            (None, None, None) => return Err("derivative requires expiry".into()),
            (None, None, Some(_)) => return Err("cash instrument cannot have expiry".into()),
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct InstrumentFile {
    instrument: Vec<InstrumentToml>,
}

#[derive(Debug, Deserialize)]
struct InstrumentToml {
    id: String,
    exchange: String,
    segment: String,
    symbol: String,
    lot_size: i64,
    tick_size: String,
    #[serde(default)]
    expiry: Option<i32>,
    #[serde(default)]
    strike: Option<String>,
    #[serde(default)]
    option_type: Option<String>,
    freeze_qty: i64,
    band_low: String,
    band_high: String,
    circuit_bps: i64,
    sessions: Vec<String>,
}

pub fn parse_session(s: &str) -> Result<Session, String> {
    let (a, b) = s
        .split_once('-')
        .ok_or_else(|| format!("bad session {s}"))?;
    Ok(Session {
        start_min: parse_hhmm(a)?,
        end_min: parse_hhmm(b)?,
    })
}

fn parse_hhmm(s: &str) -> Result<u16, String> {
    let (h, m) = s.split_once(':').ok_or_else(|| format!("bad time {s}"))?;
    let h: u16 = h.parse().map_err(|_| format!("bad time {s}"))?;
    let m: u16 = m.parse().map_err(|_| format!("bad time {s}"))?;
    if h > 24 || m > 59 {
        return Err(format!("bad time {s}"));
    }
    Ok(h * 60 + m)
}

pub fn load_instruments_toml(text: &str) -> Result<Vec<Instrument>, String> {
    let file: InstrumentFile = toml::from_str(text).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(file.instrument.len());
    for row in file.instrument {
        let exchange = Exchange::parse(&row.exchange)
            .ok_or_else(|| format!("bad exchange {}", row.exchange))?;
        let segment =
            Segment::parse(&row.segment).ok_or_else(|| format!("bad segment {}", row.segment))?;
        let option_type = match row.option_type.as_deref() {
            None | Some("") => None,
            Some(v) => Some(OptionType::parse(v).ok_or_else(|| format!("bad option type {v}"))?),
        };
        let strike = match row.strike.as_deref() {
            None | Some("") => None,
            Some(v) => Some(parse_scaled(v)?),
        };
        let mut sessions = Vec::new();
        for s in &row.sessions {
            sessions.push(parse_session(s)?);
        }
        let inst = Instrument {
            id: row.id,
            exchange,
            segment,
            symbol: row.symbol,
            lot_size: row.lot_size,
            tick_size: parse_scaled(&row.tick_size)?,
            expiry: row.expiry,
            strike,
            option_type,
            freeze_qty: row.freeze_qty,
            band_low: parse_scaled(&row.band_low)?,
            band_high: parse_scaled(&row.band_high)?,
            circuit_bps: row.circuit_bps,
            sessions,
        };
        inst.validate()?;
        out.push(inst);
    }
    Ok(out)
}

pub fn cash_sessions() -> Vec<Session> {
    vec![Session {
        start_min: 9 * 60 + 15,
        end_min: 15 * 60 + 30,
    }]
}

pub fn currency_sessions() -> Vec<Session> {
    vec![Session {
        start_min: 9 * 60,
        end_min: 17 * 60,
    }]
}

pub fn mcx_sessions() -> Vec<Session> {
    vec![
        Session {
            start_min: 9 * 60,
            end_min: 17 * 60,
        },
        Session {
            start_min: 17 * 60,
            end_min: 23 * 60 + 30,
        },
    ]
}
