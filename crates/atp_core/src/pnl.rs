//! Integer position and PnL accounting. Average price is truncated toward zero
//! and the open notional is rebuilt from that average so the invariant
//! `avg_notional == avg_price * abs(net_qty)` holds after every fill.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub strategy_id: String,
    pub instrument_id: String,
    pub net_qty: i64,
    /// `avg_price * abs(net_qty)` in scaled INR.
    #[serde(with = "crate::types::i128_str")]
    pub avg_notional: i128,
    #[serde(with = "crate::types::i128_str")]
    pub lifetime_realized: i128,
    #[serde(with = "crate::types::i128_str")]
    pub daily_realized: i128,
}

impl Position {
    pub fn new(strategy_id: &str, instrument_id: &str) -> Self {
        Self {
            strategy_id: strategy_id.to_string(),
            instrument_id: instrument_id.to_string(),
            net_qty: 0,
            avg_notional: 0,
            lifetime_realized: 0,
            daily_realized: 0,
        }
    }

    pub fn avg_price(&self) -> i64 {
        let q = self.net_qty.unsigned_abs() as i128;
        if q == 0 {
            0
        } else {
            (self.avg_notional / q) as i64
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookSide {
    Buy,
    Sell,
}

/// Apply one fill. `price` is scaled INR per unit.
pub fn apply_fill(pos: &mut Position, side: BookSide, qty: i64, price: i64) {
    assert!(qty > 0, "fill qty");
    assert!(price > 0, "fill price");
    let signed: i64 = match side {
        BookSide::Buy => qty,
        BookSide::Sell => -qty,
    };
    if pos.net_qty == 0 || pos.net_qty.signum() == signed.signum() {
        pos.avg_notional += price as i128 * qty as i128;
        pos.net_qty += signed;
        pos.avg_notional = pos.avg_price() as i128 * pos.net_qty.unsigned_abs() as i128;
        return;
    }
    let close_qty = qty.min(pos.net_qty.abs());
    let avg = pos.avg_price() as i128;
    let pnl_per = if pos.net_qty > 0 {
        price as i128 - avg
    } else {
        avg - price as i128
    };
    let realized = pnl_per * close_qty as i128;
    pos.lifetime_realized += realized;
    pos.daily_realized += realized;
    pos.net_qty += if pos.net_qty > 0 {
        -close_qty
    } else {
        close_qty
    };
    if pos.net_qty == 0 {
        pos.avg_notional = 0;
    } else {
        pos.avg_notional = avg * pos.net_qty.unsigned_abs() as i128;
    }
    let remaining = qty - close_qty;
    if remaining > 0 {
        pos.net_qty = match side {
            BookSide::Buy => remaining,
            BookSide::Sell => -remaining,
        };
        pos.avg_notional = price as i128 * remaining as i128;
    }
}

pub fn unrealized(pos: &Position, ltp: i64) -> Option<i128> {
    if pos.net_qty == 0 {
        return Some(0);
    }
    if ltp <= 0 {
        return None;
    }
    let avg = pos.avg_price() as i128;
    let diff = if pos.net_qty > 0 {
        ltp as i128 - avg
    } else {
        avg - ltp as i128
    };
    Some(diff * pos.net_qty.unsigned_abs() as i128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_partial_reduce_and_flip() {
        let mut p = Position::new("s", "i");
        apply_fill(&mut p, BookSide::Buy, 10, 100);
        assert_eq!(p.net_qty, 10);
        assert_eq!(p.avg_price(), 100);
        assert_eq!(unrealized(&p, 110).unwrap(), 100);
        apply_fill(&mut p, BookSide::Sell, 4, 120);
        assert_eq!(p.net_qty, 6);
        assert_eq!(p.avg_price(), 100);
        assert_eq!(p.lifetime_realized, 80);
        apply_fill(&mut p, BookSide::Sell, 10, 90);
        assert_eq!(p.net_qty, -4);
        assert_eq!(p.avg_price(), 90);
        assert_eq!(p.lifetime_realized, 80 - 60);
        apply_fill(&mut p, BookSide::Buy, 4, 80);
        assert_eq!(p.net_qty, 0);
        assert_eq!(p.lifetime_realized, 20 + 40);
        assert_eq!(p.avg_notional, 0);
    }
}
