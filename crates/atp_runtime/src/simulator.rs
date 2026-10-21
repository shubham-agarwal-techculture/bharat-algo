//! Exchange/broker simulator.
//!
//! Latency, rejects, partial fills, duplicate fills, excess fills, disconnects,
//! and out-of-order acks are scripted per client order id. A dropped session
//! does not erase the order: status queries still work, which is what the OMS
//! uses to resolve UNKNOWN without a blind resend.

use crate::broker::{Broker, BrokerError, Inbound};
use atp_core::{
    BrokerView, CancelRequest, KillScope, OutboundOrder, ReconOrder, ReconPosition, ReconReport,
    ReconTrade, ReplaceRequest, Side,
};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// Order is stored, but no ack is emitted.
    DisconnectAfterAccept,
    /// Submit fails and the order is not stored.
    DisconnectBeforeAccept,
    Reject,
    DuplicateFill,
    Partial {
        first: i64,
    },
    FillBeforeAck,
    ExcessFill,
    DelayAck {
        ready_at: i64,
    },
}

#[derive(Clone)]
struct SimOrder {
    broker_order_id: String,
    instrument_id: String,
    side: Side,
    qty: i64,
    #[allow(dead_code)]
    price: i64,
    filled: i64,
    status: &'static str,
}

struct Due {
    ready: i64,
    msg: Inbound,
}

struct PendingFill {
    client_order_id: String,
    qty: i64,
    price: i64,
    trade_id: String,
}

pub struct Simulator {
    pub ack_delay_ms: i64,
    pub fill_delay_ms: i64,
    now: i64,
    orders: BTreeMap<String, SimOrder>,
    trades: Vec<ReconTrade>,
    positions: BTreeMap<String, i64>,
    due: VecDeque<Due>,
    pending_fills: BTreeMap<String, PendingFill>,
    faults: BTreeMap<String, Fault>,
    pub attempt_count: BTreeMap<String, u32>,
    pub accept_count: BTreeMap<String, u32>,
    /// Every submit the gateway accepted into `submit`, including rejects.
    /// Used to prove a retry did not create a second live order.
    pub submitted: Vec<OutboundOrder>,
    next_id: u64,
    next_trade: u64,
}

impl Simulator {
    pub fn new(ack_delay_ms: i64, fill_delay_ms: i64) -> Self {
        Self {
            ack_delay_ms,
            fill_delay_ms,
            now: 0,
            orders: BTreeMap::new(),
            trades: Vec::new(),
            positions: BTreeMap::new(),
            due: VecDeque::new(),
            pending_fills: BTreeMap::new(),
            faults: BTreeMap::new(),
            attempt_count: BTreeMap::new(),
            accept_count: BTreeMap::new(),
            submitted: Vec::new(),
            next_id: 1,
            next_trade: 1,
        }
    }

    pub fn script(&mut self, client_order_id: impl Into<String>, fault: Fault) {
        self.faults.insert(client_order_id.into(), fault);
    }

    fn bump(map: &mut BTreeMap<String, u32>, id: &str) {
        *map.entry(id.to_string()).or_insert(0) += 1;
    }

    fn alloc_broker(&mut self) -> String {
        let id = format!("SIM{:06}", self.next_id);
        self.next_id += 1;
        id
    }

    fn alloc_trade(&mut self) -> String {
        let id = format!("TR{:06}", self.next_trade);
        self.next_trade += 1;
        id
    }

    fn enqueue(&mut self, ready: i64, msg: Inbound) {
        self.due.push_back(Due { ready, msg });
    }

    fn schedule_fill(
        &mut self,
        client_order_id: String,
        qty: i64,
        price: i64,
        ready: i64,
        trade_id: String,
    ) {
        let msg = Inbound::Fill {
            client_order_id: client_order_id.clone(),
            trade_id: trade_id.clone(),
            qty,
            price,
        };
        let key = fill_key(&client_order_id, &trade_id, qty);
        self.pending_fills.insert(
            key,
            PendingFill {
                client_order_id,
                qty,
                price,
                trade_id,
            },
        );
        self.enqueue(ready, msg);
    }

    fn commit_fill(&mut self, fill: PendingFill) {
        if self.trades.iter().any(|t| t.trade_id == fill.trade_id) {
            return;
        }
        let Some(order) = self.orders.get_mut(&fill.client_order_id) else {
            return;
        };
        let room = (order.qty - order.filled).max(0);
        let qty = fill.qty.min(room);
        if qty <= 0 {
            return;
        }
        order.filled += qty;
        if order.filled >= order.qty {
            order.status = "filled";
        }
        let instrument = order.instrument_id.clone();
        let side = order.side;
        let signed = match side {
            Side::Buy => qty,
            Side::Sell => -qty,
        };
        *self.positions.entry(instrument).or_insert(0) += signed;
        self.trades.push(ReconTrade {
            trade_id: fill.trade_id,
            client_order_id: fill.client_order_id,
            qty,
            price: fill.price,
        });
    }

    /// A cancel that wins drops fills that have not yet been handed to the
    /// engine. Delivering them afterwards would fill an order the book has
    /// already cancelled.
    fn forget_unsent_fills(&mut self, id: &str) {
        self.pending_fills
            .retain(|_, fill| fill.client_order_id != id);
        let mut kept = VecDeque::new();
        while let Some(item) = self.due.pop_front() {
            let drop =
                matches!(&item.msg, Inbound::Fill { client_order_id, .. } if client_order_id == id);
            if !drop {
                kept.push_back(item);
            }
        }
        self.due = kept;
    }

    fn view(order: &SimOrder) -> BrokerView {
        let leaves = (order.qty - order.filled).max(0);
        match order.status {
            "filled" => BrokerView::Filled {
                broker_order_id: order.broker_order_id.clone(),
                filled_qty: order.filled,
            },
            "cancelled" => BrokerView::Cancelled {
                broker_order_id: order.broker_order_id.clone(),
                filled_qty: order.filled,
            },
            "rejected" => BrokerView::Rejected {
                broker_order_id: order.broker_order_id.clone(),
                reason: "simulated reject".into(),
            },
            "expired" => BrokerView::Expired {
                broker_order_id: order.broker_order_id.clone(),
            },
            _ => BrokerView::Working {
                broker_order_id: order.broker_order_id.clone(),
                filled_qty: order.filled,
                leaves_qty: leaves,
            },
        }
    }
}

impl Broker for Simulator {
    fn submit(&mut self, order: &OutboundOrder) -> Result<(), BrokerError> {
        let id = order.client_order_id.clone();
        self.submitted.push(order.clone());
        Self::bump(&mut self.attempt_count, &id);
        if self.orders.contains_key(&id) {
            Self::bump(&mut self.accept_count, &id);
            return Ok(());
        }
        let fault = self.faults.get(&id).cloned();
        if matches!(fault, Some(Fault::DisconnectBeforeAccept)) {
            self.faults.remove(&id);
            return Err(BrokerError::Disconnected);
        }
        let broker_order_id = self.alloc_broker();
        let mut stored = SimOrder {
            broker_order_id: broker_order_id.clone(),
            instrument_id: order.instrument_id.clone(),
            side: order.side,
            qty: order.qty,
            price: order.price,
            filled: 0,
            status: "working",
        };
        Self::bump(&mut self.accept_count, &id);
        if matches!(fault, Some(Fault::Reject)) {
            stored.status = "rejected";
            self.orders.insert(id.clone(), stored);
            self.enqueue(
                self.now + self.ack_delay_ms,
                Inbound::Reject {
                    client_order_id: id,
                    reason: "simulated reject".into(),
                },
            );
            return Ok(());
        }
        self.orders.insert(id.clone(), stored);
        if matches!(fault, Some(Fault::DisconnectAfterAccept)) {
            return Ok(());
        }
        let fill_at = self.now + self.fill_delay_ms.max(self.ack_delay_ms);
        let ack_at = match fault {
            Some(Fault::DelayAck { ready_at }) => ready_at,
            Some(Fault::FillBeforeAck) => fill_at + 5,
            _ => self.now + self.ack_delay_ms,
        };
        if !matches!(fault, Some(Fault::FillBeforeAck)) {
            self.enqueue(
                ack_at,
                Inbound::Ack {
                    client_order_id: id.clone(),
                    broker_order_id: broker_order_id.clone(),
                },
            );
        }
        match fault {
            Some(Fault::Partial { first }) => {
                let trade = self.alloc_trade();
                self.schedule_fill(id, first.min(order.qty).max(1), order.price, fill_at, trade);
            }
            Some(Fault::DuplicateFill) => {
                let trade = self.alloc_trade();
                self.schedule_fill(id.clone(), order.qty, order.price, fill_at, trade.clone());
                self.schedule_fill(id, order.qty, order.price, fill_at, trade);
            }
            Some(Fault::ExcessFill) => {
                let trade = self.alloc_trade();
                self.schedule_fill(id, order.qty + 5, order.price, fill_at, trade);
            }
            Some(Fault::FillBeforeAck) => {
                let trade = self.alloc_trade();
                self.schedule_fill(
                    id.clone(),
                    order.qty,
                    order.price,
                    self.now + self.ack_delay_ms,
                    trade,
                );
                self.enqueue(
                    ack_at,
                    Inbound::Ack {
                        client_order_id: id,
                        broker_order_id,
                    },
                );
            }
            _ => {
                let trade = self.alloc_trade();
                self.schedule_fill(id, order.qty, order.price, fill_at, trade);
            }
        }
        Ok(())
    }

    fn cancel(&mut self, req: &CancelRequest) -> Result<(), BrokerError> {
        let Some(order) = self.orders.get_mut(&req.client_order_id) else {
            self.enqueue(
                self.now,
                Inbound::CancelReject {
                    client_order_id: req.client_order_id.clone(),
                    reason: "unknown order".into(),
                },
            );
            return Ok(());
        };
        if order.status != "working" {
            let id = req.client_order_id.clone();
            let status = order.status;
            self.enqueue(
                self.now,
                Inbound::CancelReject {
                    client_order_id: id,
                    reason: format!("status {status}"),
                },
            );
            return Ok(());
        }
        order.status = "cancelled";
        let id = req.client_order_id.clone();
        self.forget_unsent_fills(&id);
        self.enqueue(
            self.now + self.ack_delay_ms,
            Inbound::CancelAck {
                client_order_id: id,
            },
        );
        Ok(())
    }

    fn replace(&mut self, req: &ReplaceRequest) -> Result<(), BrokerError> {
        let Some(old) = self.orders.remove(&req.client_order_id) else {
            self.enqueue(
                self.now,
                Inbound::ReplaceReject {
                    client_order_id: req.client_order_id.clone(),
                    reason: "unknown order".into(),
                },
            );
            return Ok(());
        };
        if old.status != "working" || req.new_qty <= old.filled {
            self.orders.insert(req.client_order_id.clone(), old);
            self.enqueue(
                self.now,
                Inbound::ReplaceReject {
                    client_order_id: req.client_order_id.clone(),
                    reason: "not replaceable".into(),
                },
            );
            return Ok(());
        }
        let broker_order_id = self.alloc_broker();
        self.orders.insert(
            req.new_client_order_id.clone(),
            SimOrder {
                broker_order_id: broker_order_id.clone(),
                instrument_id: old.instrument_id,
                side: old.side,
                qty: req.new_qty - old.filled,
                price: req.new_price,
                filled: 0,
                status: "working",
            },
        );
        self.enqueue(
            self.now + self.ack_delay_ms,
            Inbound::ReplaceAck {
                client_order_id: req.client_order_id.clone(),
                new_broker_order_id: broker_order_id,
            },
        );
        Ok(())
    }

    fn query(&mut self, client_order_id: &str) -> Result<BrokerView, BrokerError> {
        Ok(self
            .orders
            .get(client_order_id)
            .map(Self::view)
            .unwrap_or(BrokerView::NotFound))
    }

    fn poll(&mut self) -> Vec<Inbound> {
        let mut later = VecDeque::new();
        let mut ready = Vec::new();
        while let Some(item) = self.due.pop_front() {
            if item.ready <= self.now {
                ready.push(item.msg);
            } else {
                later.push_back(item);
            }
        }
        self.due = later;
        for msg in &ready {
            if let Inbound::Fill {
                client_order_id,
                trade_id,
                qty,
                ..
            } = msg
            {
                let key = fill_key(client_order_id, trade_id, *qty);
                if let Some(fill) = self.pending_fills.remove(&key) {
                    self.commit_fill(fill);
                }
            }
        }
        ready
    }

    fn books(&mut self) -> Result<ReconReport, BrokerError> {
        let orders = self
            .orders
            .iter()
            .map(|(id, order)| ReconOrder {
                client_order_id: id.clone(),
                broker_order_id: order.broker_order_id.clone(),
                filled_qty: order.filled,
                leaves_qty: (order.qty - order.filled).max(0),
                status: order.status.to_string(),
            })
            .collect();
        let positions = self
            .positions
            .iter()
            .filter(|(_, qty)| **qty != 0)
            .map(|(instrument_id, net_qty)| ReconPosition {
                instrument_id: instrument_id.clone(),
                net_qty: *net_qty,
            })
            .collect();
        Ok(ReconReport {
            orders,
            positions,
            trades: self.trades.clone(),
        })
    }

    fn cancel_all(&mut self, scope: &KillScope) -> Result<Vec<String>, BrokerError> {
        let ids: Vec<String> = self
            .orders
            .iter()
            .filter(|(_, order)| order.status == "working" && scope_hit(scope, order))
            .map(|(id, _)| id.clone())
            .collect();
        for id in &ids {
            if let Some(order) = self.orders.get_mut(id) {
                order.status = "cancelled";
            }
            self.forget_unsent_fills(id);
            self.enqueue(
                self.now,
                Inbound::CancelAck {
                    client_order_id: id.clone(),
                },
            );
        }
        Ok(ids)
    }

    fn set_now(&mut self, wall_ms: i64) {
        if wall_ms > self.now {
            self.now = wall_ms;
        }
    }

    fn name(&self) -> &str {
        "simulator"
    }
}

fn fill_key(client_order_id: &str, trade_id: &str, qty: i64) -> String {
    format!("{client_order_id}|{trade_id}|{qty}")
}

fn scope_hit(scope: &KillScope, order: &SimOrder) -> bool {
    match scope {
        KillScope::Global | KillScope::Account { .. } | KillScope::Strategy { .. } => true,
        KillScope::Instrument { instrument_id } => order.instrument_id == *instrument_id,
    }
}

/// Line-oriented session used by `simbroker`. One process can serve the engine
/// and the kill switch at the same time.
pub fn serve(addr: &str, sim: std::sync::Arc<std::sync::Mutex<Simulator>>) -> Result<(), String> {
    use crate::broker::{WireReq, WireResp};
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind(addr).map_err(|e| e.to_string())?;
    for conn in listener.incoming() {
        let Ok(stream) = conn else { continue };
        let sim = sim.clone();
        let _ = stream.set_nodelay(true);
        std::thread::spawn(move || {
            let mut writer = match stream.try_clone() {
                Ok(s) => s,
                Err(_) => return,
            };
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            loop {
                line.clear();
                if reader
                    .read_line(&mut line)
                    .ok()
                    .filter(|n| *n > 0)
                    .is_none()
                {
                    break;
                }
                let req: WireReq = match serde_json::from_str(&line) {
                    Ok(req) => req,
                    Err(err) => {
                        let _ = writeln!(
                            writer,
                            "{}",
                            serde_json::to_string(&WireResp {
                                ok: false,
                                error: Some(err.to_string()),
                                view: None,
                                inbound: Vec::new(),
                                books: None,
                                cancelled: Vec::new(),
                            })
                            .unwrap_or_default()
                        );
                        continue;
                    }
                };
                let resp = {
                    let Ok(mut sim) = sim.lock() else { break };
                    dispatch(&mut sim, req)
                };
                if writeln!(
                    writer,
                    "{}",
                    serde_json::to_string(&resp).unwrap_or_default()
                )
                .is_err()
                {
                    break;
                }
            }
        });
    }
    Ok(())
}

fn dispatch(sim: &mut Simulator, req: crate::broker::WireReq) -> crate::broker::WireResp {
    use crate::broker::{WireReq, WireResp};
    let mut resp = WireResp {
        ok: true,
        error: None,
        view: None,
        inbound: Vec::new(),
        books: None,
        cancelled: Vec::new(),
    };
    let result = match req {
        WireReq::Submit { order, now } => {
            sim.set_now(now);
            sim.submit(&order).map(|_| ())
        }
        WireReq::Cancel { req, now } => {
            sim.set_now(now);
            sim.cancel(&req).map(|_| ())
        }
        WireReq::Replace { req, now } => {
            sim.set_now(now);
            sim.replace(&req).map(|_| ())
        }
        WireReq::Query { id, now } => {
            sim.set_now(now);
            sim.query(&id).map(|view| {
                resp.view = Some(view);
            })
        }
        WireReq::Books { now } => {
            sim.set_now(now);
            sim.books().map(|books| resp.books = Some(books))
        }
        WireReq::CancelAll { scope, now } => {
            sim.set_now(now);
            sim.cancel_all(&scope).map(|ids| resp.cancelled = ids)
        }
        WireReq::Poll { now } => {
            sim.set_now(now);
            resp.inbound = sim.poll();
            Ok(())
        }
        WireReq::Ping => Ok(()),
    };
    if let Err(err) = result {
        resp.ok = false;
        resp.error = Some(err.to_string());
    }
    resp
}
