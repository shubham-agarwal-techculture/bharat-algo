//! Seven sample strategies, one per segment, against the in-process simulator.
//! The clock is the Wednesday cash-session instant used by the core tests, so
//! the run does not depend on the wall clock of the machine.

use crate::broker::Broker;
use crate::engine::{Engine, EngineError, RunMode};
use crate::simulator::Simulator;
use atp_core::calendar::ymd_hms_ist_to_unix_ms;
use atp_core::catalog::sample_legs;
use atp_core::config::load_risk_toml;
use atp_core::instrument::load_instruments_toml;
use atp_core::replay::{diff_states, replay_records, state_hash};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct OrderLine {
    pub client_order_id: String,
    pub instrument_id: String,
    pub state: String,
    pub algo_id: String,
    pub correlation_id: String,
    pub filled_qty: i64,
    pub send_attempts: u32,
}

#[derive(Debug, Clone)]
pub struct DemoReport {
    pub mode: String,
    pub gateway: String,
    pub orders: Vec<OrderLine>,
    pub replay_match: bool,
    pub recon_ok: bool,
    pub halt: Option<String>,
    pub journal_seq: u64,
    pub state_hash: String,
    pub hot_p50_ms: f64,
    pub hot_p99_ms: f64,
    pub hot_p999_ms: f64,
    pub e2e_p50_ms: f64,
    pub e2e_p99_ms: f64,
    pub e2e_p999_ms: f64,
}

impl DemoReport {
    pub fn ok(&self) -> bool {
        self.replay_match
            && self.recon_ok
            && self.halt.is_none()
            && self.orders.len() == sample_legs().len() * 2
            && self
                .orders
                .iter()
                .all(|o| o.state == "FILLED" && !o.algo_id.is_empty())
    }
}

pub fn demo_clock() -> i64 {
    ymd_hms_ist_to_unix_ms(2026, 1, 7, 10, 0, 0)
}

/// Deletes `journal` if it exists. The demo is a clean scenario; production
/// recovery opens the file and replays it.
pub fn run_demo(
    journal: &Path,
    instruments_path: &Path,
    risk_path: &Path,
) -> Result<(Engine<Simulator>, DemoReport), EngineError> {
    if journal.exists() {
        fs::remove_file(journal)?;
    }
    if let Some(parent) = journal.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut engine = Engine::boot(
        Simulator::new(0, 0),
        RunMode::Paper,
        Some(journal),
        "ACC1",
        demo_clock(),
    )?;
    engine.strategies = atp_core::catalog::sample_strategies();
    let instruments = load_instruments_toml(&fs::read_to_string(instruments_path)?)
        .map_err(EngineError::Other)?;
    let risk = load_risk_toml(&fs::read_to_string(risk_path)?).map_err(EngineError::Other)?;
    engine.load_instruments(instruments)?;
    engine.apply_config(risk)?;
    let algos = engine
        .writer
        .state
        .risk
        .as_ref()
        .map(|cfg| cfg.strategies.iter().map(|s| s.algo_id.clone()).collect())
        .unwrap_or_default();
    engine.note_compliance("10.0.0.8", 1825, algos)?;

    for (seq, leg) in sample_legs().iter().enumerate() {
        let buy = atp_core::parse_scaled(leg.buy).map_err(EngineError::Other)?;
        let sell = atp_core::parse_scaled(leg.sell).map_err(EngineError::Other)?;
        let base = (seq as u64).saturating_mul(2);
        engine.on_tick(leg.instrument_id, buy, base + 1)?;
        engine.on_tick(leg.instrument_id, sell, base + 2)?;
    }

    let recon_ok = engine.reconcile()?;
    let replayed = replay_records("ACC1", engine.writer.records(), None)?;
    let live_hash = state_hash(&engine.writer.state);
    let replay_hash = state_hash(&replayed);
    let replay_match =
        live_hash == replay_hash && diff_states(&engine.writer.state, &replayed).is_empty();
    let orders = engine
        .writer
        .state
        .orders
        .values()
        .map(|order| OrderLine {
            client_order_id: order.client_order_id.clone(),
            instrument_id: order.instrument_id.clone(),
            state: order.state.as_str().to_string(),
            algo_id: order.algo_id.clone(),
            correlation_id: order.correlation_id.clone(),
            filled_qty: order.filled_qty,
            send_attempts: order.send_attempts,
        })
        .collect();
    let report = DemoReport {
        mode: engine.mode.as_str().to_string(),
        gateway: engine.broker.name().to_string(),
        orders,
        replay_match,
        recon_ok,
        halt: engine.writer.state.halt.clone(),
        journal_seq: engine.writer.state.last_seq,
        state_hash: live_hash,
        hot_p50_ms: engine.latency().percentile(0.50),
        hot_p99_ms: engine.latency().percentile(0.99),
        hot_p999_ms: engine.latency().percentile(0.999),
        e2e_p50_ms: engine.e2e().percentile(0.50),
        e2e_p99_ms: engine.e2e().percentile(0.99),
        e2e_p999_ms: engine.e2e().percentile(0.999),
    };
    Ok((engine, report))
}

pub fn format_report(report: &DemoReport) -> String {
    let mut out = format!(
        "demo account ACC1 mode {} gateway {}\n",
        report.mode, report.gateway
    );
    let mut orders = report.orders.clone();
    orders.sort_by(|a, b| a.client_order_id.cmp(&b.client_order_id));
    for order in &orders {
        out.push_str(&format!(
            "{} {} {} algo={} corr={} filled={} sends={}\n",
            order.instrument_id,
            order.client_order_id,
            order.state,
            order.algo_id,
            order.correlation_id,
            order.filled_qty,
            order.send_attempts
        ));
    }
    out.push_str(&format!(
        "replay match={} seq={} hash={}\nrecon ok={}\nhalt={}\nhot_ms p50={} p99={} p99.9={}\ne2e_ms p50={} p99={} p99.9={}\n",
        report.replay_match,
        report.journal_seq,
        report.state_hash,
        report.recon_ok,
        report.halt.as_deref().unwrap_or("none"),
        report.hot_p50_ms,
        report.hot_p99_ms,
        report.hot_p999_ms,
        report.e2e_p50_ms,
        report.e2e_p99_ms,
        report.e2e_p999_ms,
    ));
    out
}
