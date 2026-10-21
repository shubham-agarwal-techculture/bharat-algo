//! Control plane. A wedged core that does not drain the control channel
//! within 200 ms yields 503, which is the signal the kill-switch process uses
//! to talk to the broker directly.

use crate::broker::Broker;
use crate::engine::{Control, ReportKind};
use atp_core::KillScope;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;
use tiny_http::{Header, Method, Response, Server, StatusCode};

pub fn serve(addr: &str, tx: Sender<Control>) -> Result<(), String> {
    let server = Server::http(addr).map_err(|e| e.to_string())?;
    for request in server.incoming_requests() {
        let method = request.method().clone();
        let url = request.url().to_string();
        let path = url.split('?').next().unwrap_or("/");
        let mut body = String::new();
        let mut request = request;
        if method == Method::Post {
            let _ = request.as_reader().read_to_string(&mut body);
        }
        let (status, payload, content_type) = match (method, path) {
            (Method::Get, "/health") => reply(&tx, ReportKind::Health, "application/json"),
            (Method::Get, "/metrics") => {
                reply(&tx, ReportKind::Metrics, "text/plain; version=0.0.4")
            }
            (Method::Get, "/latency") => reply(&tx, ReportKind::Latency, "application/json"),
            (Method::Get, "/state") => reply(&tx, ReportKind::State, "application/json"),
            (Method::Get, "/traces") => reply(&tx, ReportKind::Traces, "application/json"),
            (Method::Post, "/kill") => kill(&tx, &body),
            _ => (
                StatusCode(404),
                "not found\n".to_string(),
                "text/plain".to_string(),
            ),
        };
        let header = Header::from_bytes(b"Content-Type", content_type.as_bytes())
            .unwrap_or_else(|_| Header::from_bytes(b"Content-Type", b"text/plain").unwrap());
        let response = Response::from_string(payload)
            .with_status_code(status)
            .with_header(header);
        let _ = request.respond(response);
    }
    Ok(())
}

fn reply(
    tx: &Sender<Control>,
    kind: ReportKind,
    content_type: &str,
) -> (StatusCode, String, String) {
    match ask(tx, kind) {
        Some(body) => (StatusCode(200), body, content_type.to_string()),
        None => (
            StatusCode(503),
            "engine did not answer\n".into(),
            "text/plain".into(),
        ),
    }
}

fn ask(tx: &Sender<Control>, kind: ReportKind) -> Option<String> {
    let (reply_tx, reply_rx): (Sender<String>, Receiver<String>) = std::sync::mpsc::channel();
    tx.send(Control::Report {
        kind,
        reply: reply_tx,
    })
    .ok()?;
    reply_rx.recv_timeout(Duration::from_millis(200)).ok()
}

fn kill(tx: &Sender<Control>, body: &str) -> (StatusCode, String, String) {
    let scope = match parse_kill(body) {
        Ok(scope) => scope,
        Err(err) => return (StatusCode(400), err, "text/plain".into()),
    };
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    if tx
        .send(Control::Kill {
            scope: scope.0,
            reason: scope.1,
            reply: reply_tx,
        })
        .is_err()
    {
        return (
            StatusCode(503),
            "engine is gone\n".into(),
            "text/plain".into(),
        );
    }
    match reply_rx.recv_timeout(Duration::from_millis(200)) {
        Ok(Ok(())) => (StatusCode(200), "killed\n".into(), "text/plain".into()),
        Ok(Err(err)) => (StatusCode(409), err, "text/plain".into()),
        Err(_) => (
            StatusCode(503),
            "engine did not answer\n".into(),
            "text/plain".into(),
        ),
    }
}

fn parse_kill(body: &str) -> Result<(KillScope, String), String> {
    let value: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("bad kill body: {e}"))?;
    let reason = value
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("operator")
        .to_string();
    let scope = value
        .get("scope")
        .and_then(|v| v.as_str())
        .unwrap_or("global");
    let scope = match scope {
        "global" => KillScope::Global,
        "account" => KillScope::Account {
            account_id: value
                .get("account_id")
                .and_then(|v| v.as_str())
                .unwrap_or("ACC1")
                .to_string(),
        },
        "strategy" => KillScope::Strategy {
            strategy_id: value
                .get("strategy_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "strategy_id required".to_string())?
                .to_string(),
        },
        "instrument" => KillScope::Instrument {
            instrument_id: value
                .get("instrument_id")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "instrument_id required".to_string())?
                .to_string(),
        },
        other => return Err(format!("unknown scope {other}")),
    };
    Ok((scope, reason))
}

/// POST a kill. Returns the HTTP status, or an error if the engine cannot be
/// reached. The kill-switch process treats any error as "core is down".
pub fn post_kill(url: &str, body: &str) -> Result<u16, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "only http:// kill urls are supported".to_string())?;
    let (host, path) = match rest.split_once('/') {
        Some((host, path)) => (host.to_string(), format!("/{path}")),
        None => (rest.to_string(), "/kill".to_string()),
    };
    let addr = host
        .to_socket_addrs()
        .map_err(|e| e.to_string())?
        .next()
        .ok_or_else(|| format!("cannot resolve {host}"))?;
    let mut stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200))
        .map_err(|e| e.to_string())?;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    std::io::Write::write_all(&mut stream, req.as_bytes()).map_err(|e| e.to_string())?;
    let mut buf = String::new();
    let _ = std::io::Read::read_to_string(&mut stream, &mut buf);
    let status = buf
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if status == 0 {
        Err(format!("empty response from engine: {buf}"))
    } else {
        Ok(status)
    }
}

use std::net::ToSocketAddrs;

pub fn issue_kill(
    engine_url: &str,
    sim_addr: &str,
    scope: KillScope,
    reason: &str,
) -> Result<String, String> {
    let body = kill_body(&scope, reason);
    match post_kill(engine_url, &body) {
        Ok(status) if (200..300).contains(&status) => Ok("engine".into()),
        other => {
            tracing::warn!(?other, "engine kill unavailable; cancelling at the broker");
            let mut broker =
                crate::broker::TcpBroker::connect(sim_addr).map_err(|e| e.to_string())?;
            broker.cancel_all(&scope).map_err(|e| e.to_string())?;
            Ok("broker".into())
        }
    }
}

fn kill_body(scope: &KillScope, reason: &str) -> String {
    match scope {
        KillScope::Global => serde_json::json!({"scope": "global", "reason": reason}),
        KillScope::Account { account_id } => {
            serde_json::json!({"scope": "account", "account_id": account_id, "reason": reason})
        }
        KillScope::Strategy { strategy_id } => {
            serde_json::json!({"scope": "strategy", "strategy_id": strategy_id, "reason": reason})
        }
        KillScope::Instrument { instrument_id } => {
            serde_json::json!({"scope": "instrument", "instrument_id": instrument_id, "reason": reason})
        }
    }
    .to_string()
}
