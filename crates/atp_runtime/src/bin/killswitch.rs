//! Separate kill-switch process.
//!
//! It asks the engine to cancel and halt. If the engine does not answer, it
//! cancels working orders on the simulator directly. A certified live adapter
//! would be called the same way; the Kite skeleton refuses that call.

use atp_core::KillScope;
use atp_runtime::http::issue_kill;
use clap::Parser;

#[derive(Parser)]
#[command(name = "killswitch")]
struct Args {
    /// Engine kill URL, for example `http://127.0.0.1:18420/kill`.
    #[arg(long, default_value = "http://127.0.0.1:18420/kill")]
    engine: String,
    /// Simulator address used when the engine is down.
    #[arg(long, default_value = "127.0.0.1:18423")]
    sim: String,
    /// `global`, `account`, `strategy`, or `instrument`.
    #[arg(long, default_value = "global")]
    scope: String,
    #[arg(long, default_value = "ACC1")]
    account: String,
    #[arg(long)]
    strategy: Option<String>,
    #[arg(long)]
    instrument: Option<String>,
    #[arg(long, default_value = "operator kill")]
    reason: String,
}

fn main() {
    let args = Args::parse();
    let scope = match args.scope.as_str() {
        "global" => KillScope::Global,
        "account" => KillScope::Account {
            account_id: args.account,
        },
        "strategy" => KillScope::Strategy {
            strategy_id: args.strategy.unwrap_or_else(|| {
                eprintln!("--strategy is required");
                std::process::exit(2);
            }),
        },
        "instrument" => KillScope::Instrument {
            instrument_id: args.instrument.unwrap_or_else(|| {
                eprintln!("--instrument is required");
                std::process::exit(2);
            }),
        },
        other => {
            eprintln!("unknown scope {other}");
            std::process::exit(2);
        }
    };
    match issue_kill(&args.engine, &args.sim, scope, &args.reason) {
        Ok(via) => println!("kill applied via {via}"),
        Err(err) => {
            eprintln!("kill failed: {err}");
            std::process::exit(1);
        }
    }
}
