# Runbook

The journal is the book. Do not edit it by hand. Do not delete it to “clear” an error. A halted partition stays halted until a person resumes it, and the next reconciliation will halt it again if the books still disagree.

## Startup

1. Confirm `config/instruments.toml` and `config/risk.toml`. Money fields are decimal INR strings. The risk `version` must be higher than the version already in the journal or the reload is rejected.
2. Paper mode is the only mode this build will run. `engine --mode live` exits before it sends anything. The Zerodha Kite adapter is a skeleton and returns `not certified for live capital`.
3. One process is the single writer for one account. Start a second account as a second process with its own journal file. Do not point two writers at one file.
4. Local paper demo:

```bash
./scripts/demo.sh
```

Leave the metrics port up:

```bash
HOLD_SECS=3600 ./scripts/demo.sh
```

Then open `http://127.0.0.1:18420/metrics`, `/state`, `/traces`, and `/latency`.

5. Full local stack, when Docker is available:

```bash
docker compose up --build
```

Grafana is on port 13300, Prometheus on 19090, the engine on 18420, and the OTLP HTTP receiver on 14318. Dashboards are provisioned under the ATP folder.

6. A durable simulator that the kill switch can reach on its own:

```bash
cargo run -p atp_runtime --bin simbroker -- --addr 127.0.0.1:18423
```

The in-process demo does not need this. Use it when the kill switch must cancel even if the engine process is gone.

## Kill switch

Scopes are global, account, strategy, and instrument. A kill is journaled. Orders that have not been sent are withdrawn locally. Orders the broker may have seen get a cancel with the same client order id. New intents in that scope are risk-rejected.

From the engine, while it is answering:

```bash
curl -s -X POST http://127.0.0.1:18420/kill \
  -H 'content-type: application/json' \
  -d '{"scope":"global","reason":"operator"}'
```

From the separate process, which falls back to the simulator if the engine does not answer within the HTTP timeout:

```bash
cargo run -p atp_runtime --bin killswitch -- \
  --engine http://127.0.0.1:18420/kill \
  --sim 127.0.0.1:18423 \
  --scope global \
  --reason "core not answering"
```

Strategy and instrument scopes need `--strategy` or `--instrument`.

Clearing a kill (`ClearKill` in the core) does not by itself resume a halt, and it does not rest cancelled orders. Put them back only with a new intent after the scope is clear and the feed is fresh.

## Recovery from the journal

1. Stop the writer. A second process must not append while you inspect.
2. Replay to the end, or to a sequence:

```bash
cargo run -p atp_runtime --bin replay -- --journal data/demo.jsonl --account ACC1
cargo run -p atp_runtime --bin replay -- --journal data/demo.jsonl --account ACC1 --seq 12
```

3. To diff a snapshot, write one with the library `snapshot_from` (the demo test does this) and pass `--snapshot`. A hash mismatch or a broken `prev_hash` chain fails closed. A torn final line with no newline is truncated on load. A complete corrupt line is not truncated; fix the file from a copy or stop.
4. Start the engine on the same file. It replays, then `recover` queries the broker before any resend. `RISK_APPROVED` and broker `NotFound` is the only case that may still be sent. Anything already sent is marked `UNKNOWN` and adopted from the query. A query that says not-found may authorize one resend of the same client order id, up to `max_resends`. A second miss halts.
5. Do not mint a new client order id for a retry. The simulator treats a repeated id as the same order and does not schedule a second fill.

## Reconciliation failures

`reconcile` compares client order id, filled quantity, leaves quantity, status, net position by instrument, and the set of trade ids. It does not invent fills to force a match.

On mismatch the partition halts. New risk is rejected with `PARTITION_HALTED`. Alerts `ReconMismatch` and the `atp_halted` gauge fire.

What to do:

1. Leave the kill latched if you are unsure whether further cancels are required. Cancels of already-terminal orders are ignored by the core.
2. Pull the broker order book, trade book, and positions. Diff them against `replay` output. The interesting fields are filled quantity and trade ids.
3. If the broker has a fill we do not, do not type a fill into the journal. Find the missing execution report and append it through the engine as a `Fill` command so the state machine checks quantity bounds. An excess fill halts and does not change quantity.
4. `Resume` clears the halt flag only. The next reconciliation halts again while the books differ. Resume only after they match, then reconcile once more before allowing strategies to intend.

## Stale feeds, gaps, and unknown orders

A sequence gap sticks on that instrument until a resync event. A tick older than `stale_feed_ms` (5 seconds in the demo config) rejects new risk. Any order in `UNKNOWN` rejects new risk for the whole account until a query or a broker report resolves it.

## What this build will not do

- It will not send to Kite Connect.
- It will not delete or compact the journal. `audit_retention_days` is recorded (demo: 1825) so a later archive job has a number. This process is not that job.
- It will not compute SPAN margin. The margin hook is a flat basis-point model, or `Unavailable`, which rejects every order.
