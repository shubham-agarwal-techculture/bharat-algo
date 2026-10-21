//! Prometheus metrics owned by a local registry. Nothing here touches the
//! global default registry, so tests can construct more than one engine.

use atp_core::pnl::unrealized;
use atp_core::rms::margin_required;
use atp_core::{Event, OrderState, PartitionState, PRICE_SCALE};
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Opts, Registry, TextEncoder,
};
use std::collections::VecDeque;
use std::time::Duration;

const BUCKETS_MS: [f64; 10] = [0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 1000.0];

pub struct Reservoir {
    samples: VecDeque<f64>,
    cap: usize,
}

impl Reservoir {
    fn new(cap: usize) -> Self {
        Self {
            samples: VecDeque::with_capacity(cap),
            cap,
        }
    }

    pub fn push(&mut self, ms: f64) {
        if self.samples.len() == self.cap {
            self.samples.pop_front();
        }
        self.samples.push_back(ms);
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Nearest-rank percentile. `p` is in 0..=1. Empty reservoir returns 0.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }
        let mut v: Vec<f64> = self.samples.iter().copied().collect();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let rank = ((p * v.len() as f64).ceil() as usize)
            .saturating_sub(1)
            .min(v.len() - 1);
        v[rank]
    }
}

pub struct Metrics {
    registry: Registry,
    orders: IntCounterVec,
    risk_rejects: IntCounterVec,
    broker_rejects: IntCounter,
    feed_lag_ms: Gauge,
    open_orders: IntGauge,
    orders_by_state: IntGaugeVec,
    pnl_inr: Gauge,
    margin_use_inr: Gauge,
    kill_switch: IntGauge,
    recon_mismatches: IntCounter,
    halted: IntGauge,
    journal_seq: IntGauge,
    hot_path: Histogram,
    e2e: Histogram,
    pub hot: Reservoir,
    pub e2e_samples: Reservoir,
}

impl Metrics {
    pub fn install() -> Result<Self, String> {
        let registry = Registry::new();
        let orders = IntCounterVec::new(
            Opts::new("atp_orders_total", "Order lifecycle events"),
            &["event"],
        )
        .map_err(|e| e.to_string())?;
        let risk_rejects = IntCounterVec::new(
            Opts::new("atp_risk_rejects_total", "Pre-trade risk rejects by rule"),
            &["rule"],
        )
        .map_err(|e| e.to_string())?;
        let broker_rejects = IntCounter::with_opts(Opts::new(
            "atp_broker_rejects_total",
            "Broker rejects of working orders",
        ))
        .map_err(|e| e.to_string())?;
        let feed_lag_ms = Gauge::with_opts(Opts::new(
            "atp_feed_lag_ms",
            "Oldest market-data age versus the event clock, in milliseconds",
        ))
        .map_err(|e| e.to_string())?;
        let open_orders = IntGauge::with_opts(Opts::new(
            "atp_open_orders",
            "Orders that are not in a terminal state",
        ))
        .map_err(|e| e.to_string())?;
        let orders_by_state = IntGaugeVec::new(
            Opts::new("atp_orders_by_state", "Current orders by lifecycle state"),
            &["state"],
        )
        .map_err(|e| e.to_string())?;
        let pnl_inr = Gauge::with_opts(Opts::new(
            "atp_pnl_inr",
            "Realized plus marked unrealized PnL in INR. Dashboard only; the journal is the book.",
        ))
        .map_err(|e| e.to_string())?;
        let margin_use_inr = Gauge::with_opts(Opts::new(
            "atp_margin_use_inr",
            "Sum of the configured margin model across open positions, in INR",
        ))
        .map_err(|e| e.to_string())?;
        let kill_switch = IntGauge::with_opts(Opts::new(
            "atp_kill_switch",
            "1 when any kill scope is latched",
        ))
        .map_err(|e| e.to_string())?;
        let recon_mismatches = IntCounter::with_opts(Opts::new(
            "atp_recon_mismatches",
            "Reconciliation runs that halted the partition",
        ))
        .map_err(|e| e.to_string())?;
        let halted = IntGauge::with_opts(Opts::new("atp_halted", "1 when the partition is halted"))
            .map_err(|e| e.to_string())?;
        let journal_seq = IntGauge::with_opts(Opts::new(
            "atp_journal_seq",
            "Last committed journal sequence",
        ))
        .map_err(|e| e.to_string())?;
        let hot_path = Histogram::with_opts(
            HistogramOpts::new(
                "atp_hot_path_ms",
                "decide + apply + journal commit, milliseconds",
            )
            .buckets(BUCKETS_MS.to_vec()),
        )
        .map_err(|e| e.to_string())?;
        let e2e = Histogram::with_opts(
            HistogramOpts::new(
                "atp_e2e_ms",
                "Intent commit through gateway submit, milliseconds",
            )
            .buckets(BUCKETS_MS.to_vec()),
        )
        .map_err(|e| e.to_string())?;

        for metric in [
            Box::new(orders.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(risk_rejects.clone()),
            Box::new(broker_rejects.clone()),
            Box::new(feed_lag_ms.clone()),
            Box::new(open_orders.clone()),
            Box::new(orders_by_state.clone()),
            Box::new(pnl_inr.clone()),
            Box::new(margin_use_inr.clone()),
            Box::new(kill_switch.clone()),
            Box::new(recon_mismatches.clone()),
            Box::new(halted.clone()),
            Box::new(journal_seq.clone()),
            Box::new(hot_path.clone()),
            Box::new(e2e.clone()),
        ] {
            registry.register(metric).map_err(|e| e.to_string())?;
        }

        Ok(Self {
            registry,
            orders,
            risk_rejects,
            broker_rejects,
            feed_lag_ms,
            open_orders,
            orders_by_state,
            pnl_inr,
            margin_use_inr,
            kill_switch,
            recon_mismatches,
            halted,
            journal_seq,
            hot_path,
            e2e,
            hot: Reservoir::new(4096),
            e2e_samples: Reservoir::new(4096),
        })
    }

    pub fn observe_hot(&mut self, elapsed: Duration) {
        let ms = duration_ms(elapsed);
        self.hot.push(ms);
        self.hot_path.observe(ms);
    }

    pub fn observe_e2e(&mut self, elapsed: Duration) {
        let ms = duration_ms(elapsed);
        self.e2e_samples.push(ms);
        self.e2e.observe(ms);
    }

    pub fn inc_recon_mismatch(&self) {
        self.recon_mismatches.inc();
    }

    pub fn on_event(&self, event: &Event) {
        match event {
            Event::OrderSent { .. } | Event::ResendAuthorized { .. } => {
                self.orders.with_label_values(&["sent"]).inc();
            }
            Event::RiskRejected { rule, .. } => {
                self.orders.with_label_values(&["risk_rejected"]).inc();
                self.risk_rejects.with_label_values(&[rule.as_str()]).inc();
            }
            Event::BrokerReject { .. } => {
                self.orders.with_label_values(&["broker_rejected"]).inc();
                self.broker_rejects.inc();
            }
            Event::FillApplied { .. } => {
                self.orders.with_label_values(&["fill"]).inc();
            }
            Event::DuplicateFillIgnored { .. } => {
                self.orders.with_label_values(&["duplicate_fill"]).inc();
            }
            Event::CancelConfirmed { .. } => {
                self.orders.with_label_values(&["cancelled"]).inc();
            }
            Event::OrderUnknown { .. } => {
                self.orders.with_label_values(&["unknown"]).inc();
            }
            Event::PartitionHalted { .. } => {
                self.orders.with_label_values(&["halted"]).inc();
            }
            _ => {}
        }
    }

    pub fn refresh(&self, state: &PartitionState, now_ms: i64) {
        let mut open = 0i64;
        for state_name in OrderState::all() {
            let count = state
                .orders
                .values()
                .filter(|o| o.state == *state_name)
                .count() as i64;
            self.orders_by_state
                .with_label_values(&[state_name.as_str()])
                .set(count);
            if !state_name.is_terminal() {
                open += count;
            }
        }
        self.open_orders.set(open);
        self.halted.set(i64::from(state.halt.is_some()));
        let killed = state.kills.global
            || !state.kills.accounts.is_empty()
            || !state.kills.strategies.is_empty()
            || !state.kills.instruments.is_empty();
        self.kill_switch.set(i64::from(killed));
        self.journal_seq.set(state.last_seq as i64);

        let lag = state
            .market
            .values()
            .map(|m| now_ms.saturating_sub(m.exchange_wall_ms))
            .max()
            .unwrap_or(0)
            .max(0) as f64;
        self.feed_lag_ms.set(lag);

        let mut pnl = 0i128;
        let mut margin = 0i128;
        for pos in state.positions.values() {
            pnl += pos.lifetime_realized;
            if let Some(md) = state.market.get(&pos.instrument_id) {
                if let Some(u) = unrealized(pos, md.ltp) {
                    pnl += u;
                }
            }
            if let Some(cfg) = &state.risk {
                if let Some(inst) = state.instruments.get(&pos.instrument_id) {
                    let qty = pos.net_qty.unsigned_abs() as i64;
                    if let Some(req) = margin_required(
                        &cfg.margin,
                        cfg.margin.segment_bps(inst.segment),
                        pos.avg_price(),
                        qty,
                    ) {
                        margin += req;
                    }
                }
            }
        }
        self.pnl_inr.set(inr(pnl));
        self.margin_use_inr.set(inr(margin));
    }

    pub fn encode(&self) -> String {
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        let _ = TextEncoder::new().encode(&metric_families, &mut buf);
        String::from_utf8(buf).unwrap_or_default()
    }

    pub fn latency_json(&self) -> String {
        format!(
            concat!(
                "{{\"hot_ms\":{{\"p50\":{hot50},\"p99\":{hot99},\"p99_9\":{hot999},\"n\":{hotn}}},",
                "\"e2e_ms\":{{\"p50\":{e50},\"p99\":{e99},\"p99_9\":{e999},\"n\":{en}}}}}"
            ),
            hot50 = self.hot.percentile(0.50),
            hot99 = self.hot.percentile(0.99),
            hot999 = self.hot.percentile(0.999),
            hotn = self.hot.len(),
            e50 = self.e2e_samples.percentile(0.50),
            e99 = self.e2e_samples.percentile(0.99),
            e999 = self.e2e_samples.percentile(0.999),
            en = self.e2e_samples.len(),
        )
    }
}

fn duration_ms(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1000.0
}

fn inr(scaled: i128) -> f64 {
    (scaled as f64) / (PRICE_SCALE as f64)
}
