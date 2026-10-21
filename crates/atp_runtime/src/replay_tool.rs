//! Rebuild partition state at a sequence number and, when a snapshot is
//! supplied, diff the two. This is a read-only tool: it does not append and
//! it does not talk to a broker.

use atp_core::journal::load_journal;
use atp_core::replay::{diff_states, replay_from_snapshot, replay_records, state_hash, Snapshot};
use std::fs;
use std::path::Path;

pub fn explain(
    journal: &Path,
    account: &str,
    up_to_seq: Option<u64>,
    snapshot_path: Option<&Path>,
) -> Result<String, String> {
    let records = load_journal(journal).map_err(|e| e.to_string())?;
    let rebuilt = match snapshot_path {
        Some(path) => {
            let text = fs::read_to_string(path).map_err(|e| e.to_string())?;
            let snapshot: Snapshot = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            replay_from_snapshot(&snapshot, &records, up_to_seq).map_err(|e| e.to_string())?
        }
        None => replay_records(account, &records, up_to_seq).map_err(|e| e.to_string())?,
    };
    let mut out = format!(
        "seq={} orders={} halt={} hash={}\n",
        rebuilt.last_seq,
        rebuilt.orders.len(),
        rebuilt.halt.as_deref().unwrap_or("none"),
        state_hash(&rebuilt)
    );
    if let Some(path) = snapshot_path {
        let text = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let snapshot: Snapshot = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        if up_to_seq.unwrap_or(u64::MAX) == snapshot.seq
            || up_to_seq.is_none() && rebuilt.last_seq == snapshot.seq
        {
            let diffs = diff_states(&snapshot.state, &rebuilt);
            if diffs.is_empty() {
                out.push_str("snapshot diff: identical\n");
            } else {
                out.push_str(&format!("snapshot diff: {} field(s)\n", diffs.len()));
                for line in diffs.iter().take(50) {
                    out.push_str(line);
                    out.push('\n');
                }
            }
        } else {
            let diffs = diff_states(&snapshot.state, &rebuilt);
            out.push_str(&format!(
                "snapshot at seq {} vs rebuilt seq {}: {} field difference(s)\n",
                snapshot.seq,
                rebuilt.last_seq,
                diffs.len()
            ));
        }
    }
    Ok(out)
}
