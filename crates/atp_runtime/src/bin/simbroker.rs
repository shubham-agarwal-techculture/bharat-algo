//! Standalone simulator. The engine and the kill switch can both connect.
//! A dropped TCP session does not erase the book.

use atp_runtime::simulator::{serve, Simulator};
use clap::Parser;
use std::sync::{Arc, Mutex};

#[derive(Parser)]
#[command(name = "simbroker")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18423")]
    addr: String,
    #[arg(long, default_value_t = 0)]
    ack_delay_ms: i64,
    #[arg(long, default_value_t = 0)]
    fill_delay_ms: i64,
}

fn main() {
    let args = Args::parse();
    let sim = Arc::new(Mutex::new(Simulator::new(
        args.ack_delay_ms,
        args.fill_delay_ms,
    )));
    println!("simbroker listening on {}", args.addr);
    if let Err(err) = serve(&args.addr, sim) {
        eprintln!("simbroker failed: {err}");
        std::process::exit(1);
    }
}
