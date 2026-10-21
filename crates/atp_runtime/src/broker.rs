//! Broker gateway. Strategies never see this trait.

use atp_core::{BrokerView, CancelRequest, KillScope, OutboundOrder, ReconReport, ReplaceRequest};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;

#[derive(Debug, thiserror::Error)]
pub enum BrokerError {
    #[error("broker session is down")]
    Disconnected,
    #[error("not yet live: {0}")]
    NotYetLive(&'static str),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Inbound {
    Ack {
        client_order_id: String,
        broker_order_id: String,
    },
    Reject {
        client_order_id: String,
        reason: String,
    },
    Fill {
        client_order_id: String,
        trade_id: String,
        qty: i64,
        price: i64,
    },
    CancelAck {
        client_order_id: String,
    },
    CancelReject {
        client_order_id: String,
        reason: String,
    },
    ReplaceAck {
        client_order_id: String,
        new_broker_order_id: String,
    },
    ReplaceReject {
        client_order_id: String,
        reason: String,
    },
    Expired {
        client_order_id: String,
    },
}

pub trait Broker {
    fn submit(&mut self, order: &OutboundOrder) -> Result<(), BrokerError>;
    fn cancel(&mut self, req: &CancelRequest) -> Result<(), BrokerError>;
    fn replace(&mut self, req: &ReplaceRequest) -> Result<(), BrokerError>;
    fn query(&mut self, client_order_id: &str) -> Result<BrokerView, BrokerError>;
    fn poll(&mut self) -> Vec<Inbound>;
    fn books(&mut self) -> Result<ReconReport, BrokerError>;
    fn cancel_all(&mut self, scope: &KillScope) -> Result<Vec<String>, BrokerError>;
    fn set_now(&mut self, wall_ms: i64);
    fn name(&self) -> &str;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum WireReq {
    Submit { order: OutboundOrder, now: i64 },
    Cancel { req: CancelRequest, now: i64 },
    Replace { req: ReplaceRequest, now: i64 },
    Query { id: String, now: i64 },
    Books { now: i64 },
    CancelAll { scope: KillScope, now: i64 },
    Poll { now: i64 },
    Ping,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WireResp {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub view: Option<BrokerView>,
    #[serde(default)]
    pub inbound: Vec<Inbound>,
    #[serde(default)]
    pub books: Option<ReconReport>,
    #[serde(default)]
    pub cancelled: Vec<String>,
}

pub struct TcpBroker {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    now: i64,
}

impl TcpBroker {
    pub fn connect(addr: &str) -> Result<Self, BrokerError> {
        let stream = TcpStream::connect(addr).map_err(|e| BrokerError::Other(e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| BrokerError::Other(e.to_string()))?;
        let writer = stream
            .try_clone()
            .map_err(|e| BrokerError::Other(e.to_string()))?;
        Ok(Self {
            reader: BufReader::new(stream),
            writer,
            now: 0,
        })
    }

    fn roundtrip(&mut self, req: &WireReq) -> Result<WireResp, BrokerError> {
        let line = serde_json::to_string(req).map_err(|e| BrokerError::Other(e.to_string()))?;
        self.writer
            .write_all(line.as_bytes())
            .and_then(|_| self.writer.write_all(b"\n"))
            .and_then(|_| self.writer.flush())
            .map_err(|_| BrokerError::Disconnected)?;
        let mut buf = String::new();
        let n = self
            .reader
            .read_line(&mut buf)
            .map_err(|_| BrokerError::Disconnected)?;
        if n == 0 {
            return Err(BrokerError::Disconnected);
        }
        let resp: WireResp =
            serde_json::from_str(&buf).map_err(|e| BrokerError::Other(e.to_string()))?;
        if resp.ok {
            Ok(resp)
        } else {
            Err(BrokerError::Other(
                resp.error.unwrap_or_else(|| "broker error".into()),
            ))
        }
    }
}

impl Broker for TcpBroker {
    fn submit(&mut self, order: &OutboundOrder) -> Result<(), BrokerError> {
        self.roundtrip(&WireReq::Submit {
            order: order.clone(),
            now: self.now,
        })
        .map(|_| ())
    }
    fn cancel(&mut self, req: &CancelRequest) -> Result<(), BrokerError> {
        self.roundtrip(&WireReq::Cancel {
            req: req.clone(),
            now: self.now,
        })
        .map(|_| ())
    }
    fn replace(&mut self, req: &ReplaceRequest) -> Result<(), BrokerError> {
        self.roundtrip(&WireReq::Replace {
            req: req.clone(),
            now: self.now,
        })
        .map(|_| ())
    }
    fn query(&mut self, client_order_id: &str) -> Result<BrokerView, BrokerError> {
        let resp = self.roundtrip(&WireReq::Query {
            id: client_order_id.to_string(),
            now: self.now,
        })?;
        resp.view
            .ok_or_else(|| BrokerError::Other("query without a view".into()))
    }
    fn poll(&mut self) -> Vec<Inbound> {
        match self.roundtrip(&WireReq::Poll { now: self.now }) {
            Ok(resp) => resp.inbound,
            Err(_) => Vec::new(),
        }
    }
    fn books(&mut self) -> Result<ReconReport, BrokerError> {
        let resp = self.roundtrip(&WireReq::Books { now: self.now })?;
        resp.books
            .ok_or_else(|| BrokerError::Other("books missing".into()))
    }
    fn cancel_all(&mut self, scope: &KillScope) -> Result<Vec<String>, BrokerError> {
        let resp = self.roundtrip(&WireReq::CancelAll {
            scope: scope.clone(),
            now: self.now,
        })?;
        Ok(resp.cancelled)
    }
    fn set_now(&mut self, wall_ms: i64) {
        self.now = wall_ms;
    }
    fn name(&self) -> &str {
        "tcp-simulator"
    }
}
