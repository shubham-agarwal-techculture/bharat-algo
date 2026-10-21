# Roadmap to live capital

This repository is the paper foundation. Nothing in it is certified to send an order to NSE, BSE, or MCX.

## Phase 1 — Paper, now

What runs today:

- Event-sourced OMS and RMS, replay, and a hash-chained journal.
- Pre-trade checks, freeze-qty slicing, and an explicit order state table.
- Seven sample strategies, one per segment, filling against the simulator.
- Fault tests for disconnect, duplicate fills, delayed acks, rejects, and excess fills.
- A kill-switch process that cancels at the broker when the engine does not answer.
- Metrics, traces, and a Compose stack. Dashboards show order state and latency once Prometheus is scraping the engine.

Exit criteria for this phase, already met by `./scripts/demo.sh` and `cargo test`: the same journal replays to the same state, no order is sent twice when the broker already has it, and live mode refuses to start.

## Phase 2 — Small capital, one account

Do this before any real order:

1. Register algo ids with the broker and the exchange. The ids already travel on every outbound order (`ALGONSCM001` and the rest are samples, not registrations).
2. Pin the production process to the static IP that was declared. The compliance record in the journal is the hook; the host and the broker allow-list are operational work.
3. Replace the flat margin hook with the broker’s margin result, still as an input event. If that source is down, keep `MarginModel::Unavailable` so the core rejects.
4. Certify one adapter. The Kite file is the request-shape skeleton (`NSE`/`NFO`/`CDS`/`BSE`/`BFO`/`BCD`/`MCX`, tag = client order id). Certification means: contract tests against the broker’s sandbox, duplicate-client-id behavior proven, cancel and status-query proven, and a written decision that `NotYetLive` can be removed. Until that decision, the process must keep refusing `--mode live`.
5. Trade one account, one strategy, size capped by the versioned risk file at a notional you can lose. Reconcile on a timer of seconds, not minutes. Halt on the first mismatch.
6. Run the engine and the kill switch as separate processes, both able to reach the broker. Practice the runbook on a session before the open.

## Phase 3 — Scale

Only after phase 2 has a boring week:

- Shard a hot instrument into its own account partition and process. The writer stays single-threaded per partition. Do not share a journal.
- Move the margin model to portfolio SPAN, still pure: the span parameters arrive as a journaled config, the core does not call out.
- Co-locate the writer. The latency budget that remains after this process is the broker and the exchange. Keep the hot-path histogram under 10 ms p99 and the signal-to-gateway histogram under 100 ms p99, and page on the alerts already in `deploy/prometheus/alerts.yml`.
- Archive journals older than the retention window to immutable storage. Do not add deletion to the writer.
- Add drop-copy and a second reconciler that does not share the writer’s address space.

Out of scope until those are done: multi-broker smart routing, a research notebook on the order path, and any change that lets a strategy open a socket.
