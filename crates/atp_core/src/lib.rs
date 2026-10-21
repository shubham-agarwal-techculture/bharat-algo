//! Deterministic trading core.
//!
//! OMS and RMS are pure state machines. Time, market data, broker facts, and
//! configuration enter only as commands. `decide` proposes events; `apply`
//! folds events into state. Nothing in this crate reads a clock, opens a
//! socket, or draws a random number. Replaying a journal reproduces state.

pub mod calendar;
pub mod catalog;
pub mod config;
pub mod decide;
pub mod events;
pub mod instrument;
pub mod journal;
pub mod oms;
pub mod pnl;
pub mod replay;
pub mod rms;
pub mod state;
pub mod strategy;
pub mod types;

pub use config::RiskConfig;
pub use decide::{apply, check_invariants, decide};
pub use events::{effects, Command, Effect, Event, JournalMeta};
pub use journal::{AppendOnlyJournal, JournalRecord, PartitionWriter};
pub use replay::{diff_states, replay_from_snapshot, replay_records, state_hash, Snapshot};
pub use state::PartitionState;
pub use types::*;
