//! Paper/live engine process. `--demo` runs the seven-segment simulator
//! scenario on a fixed session clock and optionally keeps the metrics port open.

use atp_runtime::demo::{format_report, run_demo};
use atp_runtime::engine::RunMode;
use clap::Parser;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(name = "engine")]
struct Args {
    /// Run the seven sample strategies against the simulator.
    #[arg(long)]
    demo: bool,
    /// `paper` routes orders to the simulator. `live` refuses the Kite skeleton.
    #[arg(long, default_value = "paper")]
    mode: String,
    #[arg(long, default_value = "data/demo.jsonl")]
    journal: PathBuf,
    #[arg(long, default_value = "config/instruments.toml")]
    instruments: PathBuf,
    #[arg(long, default_value = "config/risk.toml")]
    risk: PathBuf,
    #[arg(long, default_value = "127.0.0.1:18420")]
    http: String,
    /// OTLP HTTP endpoint, for example `http://127.0.0.1:14318`.
    #[arg(long)]
    otel: Option<String>,
    /// Keep `/metrics` up for this many seconds after the demo.
    #[arg(long, default_value_t = 0)]
    hold_secs: u64,
}

fn main() {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let mode = match RunMode::parse(&args.mode) {
        Ok(mode) => mode,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    if mode == RunMode::Live {
        eprintln!("{}", atp_runtime::NOT_YET_LIVE);
        std::process::exit(2);
    }
    if !args.demo {
        eprintln!("pass --demo to run the simulator scenario");
        std::process::exit(2);
    }

    let (mut engine, report) = match run_demo(&args.journal, &args.instruments, &args.risk) {
        Ok(pair) => pair,
        Err(err) => {
            eprintln!("demo failed: {err}");
            std::process::exit(1);
        }
    };
    print!("{}", format_report(&report));
    if !report.ok() {
        eprintln!("demo checks failed");
        std::process::exit(1);
    }
    if args.hold_secs == 0 {
        return;
    }
    engine.set_otel(args.otel);
    let tx = engine.attach_control();
    let addr = args.http.clone();
    thread::spawn(move || {
        if let Err(err) = atp_runtime::http::serve(&addr, tx) {
            eprintln!("http server stopped: {err}");
        }
    });
    println!("metrics http://{}/metrics", args.http);
    println!("state   http://{}/state", args.http);
    println!("traces  http://{}/traces", args.http);
    let until = Instant::now() + Duration::from_secs(args.hold_secs);
    while Instant::now() < until {
        engine.poll_control();
        thread::sleep(Duration::from_millis(20));
    }
}
