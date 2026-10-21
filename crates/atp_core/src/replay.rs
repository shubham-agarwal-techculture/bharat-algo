//! Rebuild partition state from a journal, optionally starting at a snapshot,
//! and diff two states field by field.

use crate::decide::{apply, check_invariants, CoreError};
use crate::journal::{content_hash, JournalRecord};
use crate::state::PartitionState;
use crate::types::{GENESIS_HASH, SCHEMA_VERSION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema: u32,
    pub seq: u64,
    pub journal_head: String,
    pub state_hash: String,
    pub state: PartitionState,
}

pub fn state_hash(state: &PartitionState) -> String {
    let bytes = serde_json::to_vec(state).expect("state serializes");
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn verify_chain(records: &[JournalRecord]) -> Result<(), CoreError> {
    let mut prev = GENESIS_HASH.to_string();
    for (idx, record) in records.iter().enumerate() {
        let expected_seq = (idx as u64) + 1;
        if record.schema != SCHEMA_VERSION {
            return Err(CoreError::new(format!(
                "unsupported journal schema {} at seq {}",
                record.schema, record.seq
            )));
        }
        if record.seq != expected_seq {
            return Err(CoreError::new(format!(
                "journal sequence gap: expected {expected_seq} found {}",
                record.seq
            )));
        }
        if record.prev_hash != prev {
            return Err(CoreError::new(format!(
                "journal hash mismatch at seq {}",
                record.seq
            )));
        }
        prev = content_hash(record);
    }
    Ok(())
}

pub fn replay_records(
    account_id: &str,
    records: &[JournalRecord],
    up_to_seq: Option<u64>,
) -> Result<PartitionState, CoreError> {
    verify_chain(records)?;
    let mut state = PartitionState::new(account_id);
    for record in records {
        if let Some(limit) = up_to_seq {
            if record.seq > limit {
                break;
            }
        }
        if record.partition != account_id {
            return Err(CoreError::new(format!(
                "record {} belongs to partition {}",
                record.seq, record.partition
            )));
        }
        for event in &record.events {
            apply(&mut state, event)?;
        }
        state.last_seq = record.seq;
        check_invariants(&state)?;
    }
    Ok(state)
}

pub fn replay_from_snapshot(
    snapshot: &Snapshot,
    records: &[JournalRecord],
    up_to_seq: Option<u64>,
) -> Result<PartitionState, CoreError> {
    if snapshot.schema != SCHEMA_VERSION {
        return Err(CoreError::new("unsupported snapshot schema"));
    }
    let actual = state_hash(&snapshot.state);
    if actual != snapshot.state_hash {
        return Err(CoreError::new("snapshot hash does not match its state"));
    }
    if snapshot.state.last_seq != snapshot.seq {
        return Err(CoreError::new("snapshot seq does not match state.last_seq"));
    }
    verify_chain(records)?;
    if snapshot.seq > 0 {
        let head = records
            .get(snapshot.seq as usize - 1)
            .ok_or_else(|| CoreError::new("snapshot seq is past the journal"))?;
        if content_hash(head) != snapshot.journal_head {
            return Err(CoreError::new("snapshot journal head does not match"));
        }
    }
    let mut state = snapshot.state.clone();
    for record in records {
        if record.seq <= snapshot.seq {
            continue;
        }
        if let Some(limit) = up_to_seq {
            if record.seq > limit {
                break;
            }
        }
        for event in &record.events {
            apply(&mut state, event)?;
        }
        state.last_seq = record.seq;
        check_invariants(&state)?;
    }
    Ok(state)
}

pub fn diff_states(left: &PartitionState, right: &PartitionState) -> Vec<String> {
    let a = serde_json::to_value(left).unwrap_or(serde_json::Value::Null);
    let b = serde_json::to_value(right).unwrap_or(serde_json::Value::Null);
    let mut out = Vec::new();
    diff_value(&a, &b, "$", &mut out);
    out
}

fn diff_value(
    left: &serde_json::Value,
    right: &serde_json::Value,
    path: &str,
    out: &mut Vec<String>,
) {
    use serde_json::Value;
    match (left, right) {
        (Value::Object(a), Value::Object(b)) => {
            let mut keys: Vec<&String> = a.keys().collect();
            for key in b.keys() {
                if !a.contains_key(key) {
                    keys.push(key);
                }
            }
            keys.sort();
            keys.dedup();
            for key in keys {
                let child = format!("{path}.{key}");
                match (a.get(key), b.get(key)) {
                    (Some(l), Some(r)) => diff_value(l, r, &child, out),
                    (Some(l), None) => out.push(format!("{child} only on left: {l}")),
                    (None, Some(r)) => out.push(format!("{child} only on right: {r}")),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            let n = a.len().max(b.len());
            for idx in 0..n {
                let child = format!("{path}[{idx}]");
                match (a.get(idx), b.get(idx)) {
                    (Some(l), Some(r)) => diff_value(l, r, &child, out),
                    (Some(l), None) => out.push(format!("{child} only on left: {l}")),
                    (None, Some(r)) => out.push(format!("{child} only on right: {r}")),
                    (None, None) => {}
                }
            }
        }
        (l, r) if l != r => out.push(format!("{path}: {l} != {r}")),
        _ => {}
    }
}

pub fn snapshot_from(state: &PartitionState, journal_head: &str) -> Snapshot {
    Snapshot {
        schema: SCHEMA_VERSION,
        seq: state.last_seq,
        journal_head: journal_head.to_string(),
        state_hash: state_hash(state),
        state: state.clone(),
    }
}
