//! Replay a journal up to a sequence and diff it against a snapshot.

use atp_runtime::replay_tool::explain;
use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "replay")]
struct Args {
    #[arg(long)]
    journal: PathBuf,
    #[arg(long, default_value = "ACC1")]
    account: String,
    /// Inclusive sequence. Omit to replay the whole journal.
    #[arg(long)]
    seq: Option<u64>,
    #[arg(long)]
    snapshot: Option<PathBuf>,
}

fn main() -> ExitCode {
    let args = Args::parse();
    match explain(
        &args.journal,
        &args.account,
        args.seq,
        args.snapshot.as_deref(),
    ) {
        Ok(text) => {
            print!("{text}");
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("replay failed: {err}");
            ExitCode::from(1)
        }
    }
}
