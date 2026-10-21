//! Runtime around the deterministic core.
//!
//! This crate is the only place that reads a clock, opens a socket, or talks
//! to a broker. Replay of a journal never calls into it: effects in the core
//! are an outbox, and this process is the only consumer.

pub mod broker;
pub mod demo;
pub mod engine;
pub mod http;
pub mod kite;
pub mod metrics;
pub mod replay_tool;
pub mod simulator;
pub mod trace;

pub use demo::{format_report, run_demo, DemoReport};
pub use engine::{preflight, Control, Engine, EngineError, ReportKind, RunMode};
pub use kite::{to_kite, KiteAdapter, NOT_YET_LIVE};
