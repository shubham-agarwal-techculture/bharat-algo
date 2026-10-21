#!/usr/bin/env bash
# One-command paper demo against the in-process exchange simulator.
# HOLD_SECS keeps the metrics port open after the scenario (default 0).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
mkdir -p data
args=(
  --demo
  --mode paper
  --journal data/demo.jsonl
  --instruments config/instruments.toml
  --risk config/risk.toml
  --http "${HTTP:-127.0.0.1:18420}"
  --hold-secs "${HOLD_SECS:-0}"
)
if [[ -n "${OTEL:-}" ]]; then
  args+=(--otel "$OTEL")
fi
exec cargo run -q -p atp_runtime --bin engine -- "${args[@]}"
