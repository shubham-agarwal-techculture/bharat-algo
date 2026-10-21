//! Append-only hash-chained journal. One record is one atomic command.

use crate::decide::{apply, check_invariants, decide, CoreError};
use crate::events::{effects, Command, Effect, Event};
use crate::replay::{replay_records, verify_chain};
use crate::state::PartitionState;
use crate::types::{GENESIS_HASH, SCHEMA_VERSION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalRecord {
    pub schema: u32,
    pub seq: u64,
    pub partition: String,
    pub correlation_id: String,
    pub wall_ms: i64,
    pub mono_ns: u64,
    pub prev_hash: String,
    pub events: Vec<Event>,
}

#[derive(Serialize)]
struct HashView<'a> {
    seq: u64,
    partition: &'a str,
    correlation_id: &'a str,
    wall_ms: i64,
    mono_ns: u64,
    prev_hash: &'a str,
    events: &'a [Event],
}

pub fn content_hash(record: &JournalRecord) -> String {
    let view = HashView {
        seq: record.seq,
        partition: &record.partition,
        correlation_id: &record.correlation_id,
        wall_ms: record.wall_ms,
        mono_ns: record.mono_ns,
        prev_hash: &record.prev_hash,
        events: &record.events,
    };
    let bytes = serde_json::to_vec(&view).expect("journal record serializes");
    hex_encode(Sha256::digest(bytes))
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

enum Store {
    Memory {
        records: Vec<JournalRecord>,
    },
    File {
        #[allow(dead_code)]
        path: PathBuf,
        writer: BufWriter<File>,
        records: Vec<JournalRecord>,
    },
}

pub struct AppendOnlyJournal {
    next_seq: u64,
    prev_hash: String,
    store: Store,
}

impl AppendOnlyJournal {
    pub fn memory() -> Self {
        Self {
            next_seq: 1,
            prev_hash: GENESIS_HASH.to_string(),
            store: Store::Memory {
                records: Vec::new(),
            },
        }
    }

    pub fn open(path: &Path) -> Result<Self, CoreError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| CoreError::new(e.to_string()))?;
            }
        }
        let records = load_journal(path)?;
        verify_chain(&records)?;
        let (next_seq, prev_hash) = if let Some(last) = records.last() {
            (last.seq + 1, content_hash(last))
        } else {
            (1, GENESIS_HASH.to_string())
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| CoreError::new(e.to_string()))?;
        Ok(Self {
            next_seq,
            prev_hash,
            store: Store::File {
                path: path.to_path_buf(),
                writer: BufWriter::new(file),
                records,
            },
        })
    }

    pub fn records(&self) -> &[JournalRecord] {
        match &self.store {
            Store::Memory { records } | Store::File { records, .. } => records,
        }
    }

    pub fn head_hash(&self) -> &str {
        &self.prev_hash
    }

    fn append(
        &mut self,
        partition: &str,
        correlation_id: &str,
        wall_ms: i64,
        mono_ns: u64,
        events: &[Event],
    ) -> Result<u64, CoreError> {
        let seq = self.next_seq;
        let record = JournalRecord {
            schema: SCHEMA_VERSION,
            seq,
            partition: partition.to_string(),
            correlation_id: correlation_id.to_string(),
            wall_ms,
            mono_ns,
            prev_hash: self.prev_hash.clone(),
            events: events.to_vec(),
        };
        let line = serde_json::to_string(&record).map_err(|e| CoreError::new(e.to_string()))?;
        match &mut self.store {
            Store::Memory { records } => records.push(record.clone()),
            Store::File {
                writer, records, ..
            } => {
                writer
                    .write_all(line.as_bytes())
                    .and_then(|_| writer.write_all(b"\n"))
                    .and_then(|_| writer.flush())
                    .map_err(|e| CoreError::new(e.to_string()))?;
                writer
                    .get_ref()
                    .sync_data()
                    .map_err(|e| CoreError::new(e.to_string()))?;
                records.push(record.clone());
            }
        }
        self.prev_hash = content_hash(&record);
        self.next_seq += 1;
        Ok(seq)
    }
}

pub fn load_journal(path: &Path) -> Result<Vec<JournalRecord>, CoreError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    repair_torn_tail(path)?;
    let file = File::open(path).map_err(|e| CoreError::new(e.to_string()))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| CoreError::new(e.to_string()))?;
        if line.trim().is_empty() {
            continue;
        }
        let record: JournalRecord = serde_json::from_str(&line)
            .map_err(|e| CoreError::new(format!("corrupt journal line {}: {e}", idx + 1)))?;
        records.push(record);
    }
    Ok(records)
}

fn repair_torn_tail(path: &Path) -> Result<(), CoreError> {
    let bytes = std::fs::read(path).map_err(|e| CoreError::new(e.to_string()))?;
    if bytes.is_empty() || bytes.last() == Some(&b'\n') {
        return Ok(());
    }
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1] != b'\n' {
        end -= 1;
    }
    std::fs::write(path, &bytes[..end]).map_err(|e| CoreError::new(e.to_string()))?;
    Ok(())
}

/// Single writer for one account partition. Not cloneable on purpose.
pub struct PartitionWriter {
    pub state: PartitionState,
    journal: AppendOnlyJournal,
}

impl PartitionWriter {
    pub fn memory(account_id: impl Into<String>) -> Self {
        Self {
            state: PartitionState::new(account_id),
            journal: AppendOnlyJournal::memory(),
        }
    }

    pub fn open(path: &Path, account_id: &str) -> Result<Self, CoreError> {
        let journal = AppendOnlyJournal::open(path)?;
        let state = replay_records(account_id, journal.records(), None)?;
        Ok(Self { state, journal })
    }

    pub fn records(&self) -> &[JournalRecord] {
        self.journal.records()
    }

    pub fn head_hash(&self) -> &str {
        self.journal.head_hash()
    }

    pub fn handle(&mut self, cmd: Command) -> Result<Vec<Effect>, CoreError> {
        let events = decide(&self.state, &cmd)?;
        if events.is_empty() {
            return Ok(Vec::new());
        }
        let mut trial = self.state.clone();
        for event in &events {
            apply(&mut trial, event)?;
        }
        check_invariants(&trial)?;
        let meta = cmd.journal_meta();
        let seq = self.journal.append(
            &self.state.account_id,
            &meta.correlation_id,
            meta.wall_ms,
            meta.mono_ns,
            &events,
        )?;
        trial.last_seq = seq;
        self.state = trial;
        let mut out = Vec::new();
        for event in &events {
            out.extend(effects(event));
        }
        Ok(out)
    }
}
