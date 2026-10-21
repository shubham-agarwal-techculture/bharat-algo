//! IST civil calendar. India has no daylight saving; the offset is fixed.
//! The core never reads the host clock — callers pass event time.

use crate::instrument::{Instrument, Session};
use serde::{Deserialize, Serialize};

pub const IST_OFFSET_MS: i64 = 5 * 3_600_000 + 30 * 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CivilTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub weekday: u8,
    pub minute_of_day: u16,
}

/// Days since Unix epoch for a civil date (Howard Hinnant).
pub fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let y = year as i64 - if month <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = if month > 2 {
        month as i64 - 3
    } else {
        month as i64 + 9
    };
    let doy = ((153 * m + 2) / 5 + day as i64 - 1) as u64;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe as i64 - 719468
}

pub fn civil_from_days(mut z: i64) -> (i32, u32, u32) {
    z += 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

pub fn ymd_hms_ist_to_unix_ms(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> i64 {
    let days = days_from_civil(y, m, d);
    let local_ms = days * 86_400_000 + ((h as i64 * 60 + min as i64) * 60 + s as i64) * 1_000;
    local_ms - IST_OFFSET_MS
}

pub fn unix_ms_to_ist(wall_ms: i64) -> CivilTime {
    let ist = wall_ms + IST_OFFSET_MS;
    let days = ist.div_euclid(86_400_000);
    let ms_of_day = ist.rem_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    // Unix day 0 is Thursday. 0 = Sunday … 6 = Saturday.
    let weekday = (days + 4).rem_euclid(7) as u8;
    CivilTime {
        year,
        month,
        day,
        weekday,
        minute_of_day: (ms_of_day / 60_000) as u16,
    }
}

pub fn yyyymmdd(t: CivilTime) -> i32 {
    t.year * 10_000 + t.month as i32 * 100 + t.day as i32
}

pub fn is_weekend(weekday: u8) -> bool {
    weekday == 0 || weekday == 6
}

pub fn session_contains(session: Session, minute: u16) -> bool {
    minute >= session.start_min && minute < session.end_min
}

/// Continuous-session check. Weekends and configured holidays are closed for
/// every segment, including MCX evening and currency.
pub fn is_instrument_open(inst: &Instrument, wall_ms: i64, holidays: &[i32]) -> bool {
    let t = unix_ms_to_ist(wall_ms);
    if is_weekend(t.weekday) {
        return false;
    }
    if holidays.contains(&yyyymmdd(t)) {
        return false;
    }
    inst.sessions
        .iter()
        .any(|s| session_contains(*s, t.minute_of_day))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::instrument::Instrument;
    use crate::types::*;

    fn inst(sessions: Vec<Session>) -> Instrument {
        Instrument {
            id: "NSE:CM:RELIANCE".into(),
            exchange: Exchange::Nse,
            segment: Segment::Cm,
            symbol: "RELIANCE".into(),
            lot_size: 1,
            tick_size: 500,
            expiry: None,
            strike: None,
            option_type: None,
            freeze_qty: 0,
            band_low: 1,
            band_high: 100,
            circuit_bps: 0,
            sessions,
        }
    }

    #[test]
    fn civil_roundtrip() {
        for (y, m, d) in [
            (1970, 1, 1),
            (2026, 1, 7),
            (2026, 1, 26),
            (2024, 2, 29),
            (1999, 12, 31),
        ] {
            let days = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(days), (y, m, d));
        }
    }

    #[test]
    fn nse_cash_hours_and_mcx_evening() {
        let cash = inst(vec![Session {
            start_min: 9 * 60 + 15,
            end_min: 15 * 60 + 30,
        }]);
        let mcx = inst(vec![
            Session {
                start_min: 9 * 60,
                end_min: 17 * 60,
            },
            Session {
                start_min: 17 * 60,
                end_min: 23 * 60 + 30,
            },
        ]);
        let holidays = [20260126];
        // 2026-01-07 is a Wednesday.
        let wed_open = ymd_hms_ist_to_unix_ms(2026, 1, 7, 9, 15, 0);
        assert_eq!(unix_ms_to_ist(wed_open).weekday, 3);
        assert!(is_instrument_open(&cash, wed_open, &holidays));
        let before = ymd_hms_ist_to_unix_ms(2026, 1, 7, 9, 14, 0);
        assert!(!is_instrument_open(&cash, before, &holidays));
        let close = ymd_hms_ist_to_unix_ms(2026, 1, 7, 15, 30, 0);
        assert!(!is_instrument_open(&cash, close, &holidays));
        let evening = ymd_hms_ist_to_unix_ms(2026, 1, 7, 18, 0, 0);
        assert!(!is_instrument_open(&cash, evening, &holidays));
        assert!(is_instrument_open(&mcx, evening, &holidays));
        let late = ymd_hms_ist_to_unix_ms(2026, 1, 7, 23, 30, 0);
        assert!(!is_instrument_open(&mcx, late, &holidays));
        // Saturday
        let sat = ymd_hms_ist_to_unix_ms(2026, 1, 10, 10, 0, 0);
        assert_eq!(unix_ms_to_ist(sat).weekday, 6);
        assert!(!is_instrument_open(&mcx, sat, &holidays));
        // Republic Day
        let holiday = ymd_hms_ist_to_unix_ms(2026, 1, 26, 10, 0, 0);
        assert!(!is_instrument_open(&cash, holiday, &holidays));
    }
}
