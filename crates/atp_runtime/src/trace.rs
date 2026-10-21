//! One correlation id, one trace. The trace id is the first 16 bytes of
//! SHA-256(correlation id), so a replay of the same journal rebuilds the same
//! identifiers without a random source.

use atp_core::JournalRecord;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[derive(Debug, Clone, Serialize)]
pub struct Span {
    pub trace_id: String,
    pub span_id: String,
    pub correlation_id: String,
    pub name: String,
    pub seq: u64,
    pub wall_ms: i64,
    pub mono_ns: u64,
}

pub fn trace_id(correlation_id: &str) -> String {
    let digest = Sha256::digest(correlation_id.as_bytes());
    hex(&digest[..16])
}

pub fn span_from(record: &JournalRecord) -> Span {
    let name = record
        .events
        .iter()
        .map(event_name)
        .collect::<Vec<_>>()
        .join(",");
    let material = format!("{}:{}", record.correlation_id, record.seq);
    let digest = Sha256::digest(material.as_bytes());
    Span {
        trace_id: trace_id(&record.correlation_id),
        span_id: hex(&digest[..8]),
        correlation_id: record.correlation_id.clone(),
        name,
        seq: record.seq,
        wall_ms: record.wall_ms,
        mono_ns: record.mono_ns,
    }
}

fn event_name(event: &atp_core::Event) -> &'static str {
    match event {
        atp_core::Event::InstrumentsLoaded { .. } => "instruments",
        atp_core::Event::MarketDataUpdated { .. } => "market_data",
        atp_core::Event::SignalRecorded { .. } => "signal",
        atp_core::Event::OrderRegistered { .. } => "registered",
        atp_core::Event::RiskRejected { .. } => "risk_rejected",
        atp_core::Event::RiskApproved { .. } => "risk",
        atp_core::Event::OrderSent { .. } => "oms",
        atp_core::Event::OrderUnknown { .. } => "unknown",
        atp_core::Event::BrokerAck { .. } => "ack",
        atp_core::Event::BrokerReject { .. } => "broker_reject",
        atp_core::Event::FillApplied { .. } => "fill",
        atp_core::Event::DuplicateFillIgnored { .. } => "duplicate_fill",
        atp_core::Event::CancelRequested { .. } => "cancel",
        atp_core::Event::CancelConfirmed { .. } => "cancel_ack",
        atp_core::Event::CancelRejected { .. } => "cancel_reject",
        atp_core::Event::ReplaceRequested { .. } => "replace",
        atp_core::Event::ReplaceConfirmed { .. } => "replace_ack",
        atp_core::Event::ReplaceRejected { .. } => "replace_reject",
        atp_core::Event::ReplaceRiskRejected { .. } => "replace_risk_reject",
        atp_core::Event::QueryFailedNoted { .. } => "query_failed",
        atp_core::Event::OrderExpired { .. } => "expired",
        atp_core::Event::LocalWithdraw { .. } => "withdraw",
        atp_core::Event::UnknownResolved { .. } => "resolved",
        atp_core::Event::ResendAuthorized { .. } => "resend",
        atp_core::Event::ResendDeferred { .. } => "resend_deferred",
        atp_core::Event::QueryRequested { .. } => "query",
        atp_core::Event::ConfigApplied { .. } => "config",
        atp_core::Event::KillApplied { .. } => "kill",
        atp_core::Event::KillCleared { .. } => "kill_cleared",
        atp_core::Event::PartitionHalted { .. } => "halt",
        atp_core::Event::PartitionResumed { .. } => "resume",
        atp_core::Event::ReconOk { .. } => "recon_ok",
        atp_core::Event::SessionRolled { .. } => "session",
        atp_core::Event::HopRecorded { .. } => "hop",
        atp_core::Event::ComplianceNoted { .. } => "compliance",
    }
}

pub struct TraceLog {
    spans: Vec<Span>,
    cap: usize,
    endpoint: Option<String>,
}

impl TraceLog {
    pub fn new(endpoint: Option<String>) -> Self {
        Self {
            spans: Vec::new(),
            cap: 500,
            endpoint,
        }
    }

    pub fn set_endpoint(&mut self, endpoint: Option<String>) {
        self.endpoint = endpoint.filter(|value| !value.is_empty());
    }

    pub fn push(&mut self, span: Span) {
        if let Some(endpoint) = &self.endpoint {
            export_otlp(endpoint, &span);
        }
        if self.spans.len() == self.cap {
            self.spans.remove(0);
        }
        self.spans.push(span);
    }

    pub fn json(&self) -> String {
        serde_json::to_string(&self.spans).unwrap_or_else(|_| "[]".into())
    }
}

fn export_otlp(endpoint: &str, span: &Span) {
    let Some(rest) = endpoint.strip_prefix("http://") else {
        return;
    };
    let (host, path_prefix) = match rest.split_once('/') {
        Some((host, path)) => (host, format!("/{path}")),
        None => (rest, String::new()),
    };
    if host.is_empty() {
        return;
    }
    let path = format!("{path_prefix}/v1/traces");
    let start = (span.wall_ms.max(0) as u64).saturating_mul(1_000_000);
    let end = start.saturating_add(span.mono_ns.min(1_000_000_000));
    let body = serde_json::json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "atp-engine"}}
            ]},
            "scopeSpans": [{
                "scope": {"name": "atp"},
                "spans": [{
                    "traceId": span.trace_id,
                    "spanId": span.span_id,
                    "name": span.name,
                    "startTimeUnixNano": start.to_string(),
                    "endTimeUnixNano": end.to_string(),
                    "attributes": [
                        {"key": "correlation_id", "value": {"stringValue": span.correlation_id}},
                        {"key": "journal.seq", "value": {"intValue": span.seq.to_string()}}
                    ]
                }]
            }]
        }]
    });
    let payload = body.to_string();
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
        payload.len()
    );
    let Ok(mut addrs) = host.to_socket_addrs() else {
        return;
    };
    let Some(addr) = addrs.next() else {
        return;
    };
    let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(30)) else {
        return;
    };
    let _ = stream.set_write_timeout(Some(Duration::from_millis(30)));
    let _ = stream.write_all(req.as_bytes());
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_is_stable_and_32_hex() {
        let a = trace_id("corr-nse-cm-e");
        let b = trace_id("corr-nse-cm-e");
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, trace_id("other"));
    }
}
