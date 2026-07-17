# MMR Audit — Remaining-Work Roadmap

Scoping for the architectural-tier items left after the confirmed-bug remediation
(passes 1–4, all merged to `master`). These are **design changes**, not bug fixes:
each links IB/broker truth to internal state, or hardens a subsystem. Ordered by
value-to-capital-safety, with effort (S ≈ half-day, M ≈ 1–2 days, L ≈ 3–5 days),
risk, and a paper-validation plan for each.

Legend: **effort** S/M/L · **risk** = blast radius if the change is wrong.

---

## Cluster A — Order lifecycle & broker-truth reconciliation  ★ highest value

The single biggest gap. Today internal state (proposals in DuckDB, the in-memory
book) and broker reality can silently diverge. Three related pieces that share
one new component — an **OrderLifecycleTracker** that subscribes to
`orderStatusEvent` + `execDetailsEvent`, and is the one place order truth flows in.

### A1 — Order success from IB ack, not the local echo  ✅ DONE (paper-validated)
- **Problem** (`trading_runtime.py:956`): `place_expressive_order` treats the
  `placeOrder` echo (a `Trade` object comes back) as success. IB-side rejection
  (`orderStatus.status ∈ {Cancelled, Inactive, ApiCancelled}` with a reject
  reason) is never observed, so a bracket can transmit with a rejected stop-loss
  and still be marked EXECUTED.
- **Approach**: after placing, await the first *decisive* `orderStatus` for the
  order id — accepted (`PreSubmitted`/`Submitted`/`Filled`) vs rejected — with a
  bounded timeout. On timeout, return the same UNKNOWN/reconcile result the
  approve() path already uses (don't guess). Reuse `_place_and_wait` which
  already has the Trade; extend it to watch status, not just the ack.
- **Risk**: touches the live order path; a too-tight timeout could report
  UNKNOWN on a slow-but-fine order (safe direction, but noisy).
- **Validate (paper)**: place a deliberately-rejectable order (e.g. absurd limit
  far outside NBBO, or an unsupported combo) and confirm it returns failure with
  the IB reason instead of success.

### A2 — Record fill / cancel / reject events  ✅ DONE (paper-validated)
- **Problem**: `EventType.ORDER_FILLED/CANCELLED/REJECTED` are defined but only
  `ORDER_SUBMITTED` is ever written. The risk-gate lookback and the audit trail
  are blind to outcomes.
- **Approach**: in the OrderLifecycleTracker, on terminal `orderStatus` /
  `execDetails`, write the corresponding event (with order id, fill qty/price,
  reject reason). Pure append; no control-flow change.
- **Risk**: low — additive, off the order-placement hot path.
- **Validate (paper)**: fill a market order, confirm an `ORDER_FILLED` row lands
  in the event store with the right qty/price.

### A3 — Startup reconciliation pass  ✅ DONE (paper-validated, report-only)
- **Problem** (`sdk.py:1419`, design): proposals are marked EXECUTED at
  *submission*, never confirmed against fills; after a trader_service restart the
  book is repopulated from `reqAllOpenOrders` but nothing re-syncs proposal state
  or verifies protective orders still exist. A crash between bracket legs leaves
  staged `transmit=False` orders at the gateway that no one reconciles.
- **Approach**: a reconciliation routine run once after connect + account-pin:
  pull `reqAllOpenOrdersAsync` + `reqExecutionsAsync` + positions, then
  (a) for each non-terminal proposal, match its `order_ids` against IB status and
  advance/repair (APPROVED-but-unknown → EXECUTED if filled, or FAILED if truly
  absent); (b) flag positions whose protective orders are missing; (c) surface a
  reconciliation report (log + optional dashboard panel). Read-only by default —
  it *reports* divergence and only auto-repairs proposal status (never places or
  cancels orders without a flag).
- **Risk**: medium — must be careful not to auto-act on transient
  reconnect-window states; gate any order action behind an explicit flag.
- **Validate (paper)**: approve a proposal, `docker restart mmr-mmr-1`
  mid-flight, confirm the reconciliation pass reports the correct
  filled/unfilled state instead of a stale EXECUTED.

**Status:** ✅ **Cluster A complete.** A1+A2 via `OrderLifecycleTracker`; A3 via
`reconciliation.py` + `mmr reconcile` (report-only). All paper-validated: real
fills recorded ORDER_FILLED/CANCELLED, and reconcile flagged a real
executed-still-working proposal and an unprotected position. Auto-repair of
unambiguous proposal-status divergences remains a deliberate future opt-in.

---

## Cluster B — IB historical-data timezone keying  ✅ DONE (paper-validated)

- **Problem** (`ib_history_worker.py:287`, `:329`): IB daily bars are timestamped
  at UTC midnight and null-row markers use the host machine's local tz, while
  Massive/TwelveData daily bars key at ET midnight. Cross-source merges shift a
  day, and `data summary` coverage is subtly wrong.
- **Approach**: normalise IB daily bars to the exchange/session date (ET for US,
  the contract's `timeZoneId` for intl) at ingest, matching the Massive
  convention; localise null-row markers to the requested tz, not the host's.
- **Risk**: medium — changes stored bar keys; needs a one-time check that
  existing data doesn't double-key. Backtest results on daily bars could shift by
  a day where it was previously wrong.
- **Validate (paper)**: download the same daily series via IB and via Massive for
  one US symbol, confirm the dates align 1:1 after the fix.

---

## Cluster C — Contract / universe resolution integrity

### C1 — idea-scanner location→exchange resolution  ✅ DONE (paper-validated: ASX/US)
- **Problem** (`idea_scanner.py:1229`): location-code parsing can resolve symbols
  on the wrong exchange (SMART fallback, TSE-vs-TSEJ, region tokens like `MAJOR`
  taken as an exchange). Feeds an LLM confidently-wrong instruments — a direct
  precision-principle violation.
- **Approach**: a vetted location→(exchange, primaryExchange) map for the
  documented codes; after resolve, assert the returned contract's
  primaryExchange matches intent and reject/skip mismatches (fail loud).
- **Risk**: medium — an over-strict map could reject legitimate resolutions;
  must be validated against live IB for each supported market (ASX/TSE/SEHK/…).
- **Validate (paper)**: resolve BHP/CBA (ASX), 0700 (SEHK), a TSE name; confirm
  each lands on the local listing, not a US ADR.

### C2 — universe resolver-cache invalidation  (S, low risk)
- **Problem** (`universe.py:145`): the resolver cache is never invalidated and
  its hit path bypasses exchange/sec_type filters, so a stale/loose entry can win.
- **Approach**: key the cache on (conId/symbol, exchange, sec_type); invalidate
  on universe edits; make the hit path honour the same filters as a miss.
- **Risk**: low — localized; add a cache-key test.
- **Validate**: unit test — same symbol on two exchanges resolves distinctly.

---

## Cluster D — Service robustness

### D1 — strategy_runtime run() EADDRINUSE restart-safety  ✅ DONE
- **Problem** (`strategy_runtime.py:655`): a crash-restart re-binds the RPC port;
  if the old socket lingers, EADDRINUSE turns any transient startup failure into
  permanent service death.
- **Approach**: set `SO_REUSEADDR` (and retry-with-backoff on bind), or detect
  and cleanly close a stale bind before re-binding.
- **Risk**: low.
- **Validate (paper)**: kill -9 the strategy_service, confirm it restarts and
  re-binds instead of crash-looping.

### D2 — strategy enabled-state persistence  ✅ DONE (paper-validated)
- **Problem** (`strategy_runtime.py:217`): enabled/disabled state isn't
  persisted and `WAITING_HISTORICAL_DATA` never transitions, so a restart can
  lose a strategy's enabled state.
- **Approach**: persist per-strategy state to DuckDB; restore on load; drive the
  WAITING→RUNNING transition off the history-ready signal.
- **Risk**: low.
- **Validate (paper)**: enable a strategy, restart strategy_service, confirm it
  comes back enabled.

---

## Cluster E — ibreactive resource leaks  ⏭️ SKIPPED (see note)

- **Problem**: `ibreactive.py:520` leaks two Rx subscriptions per snapshot on the
  hot ticker path; `:822` `get_shortable_shares` starts a streaming `reqMktData`
  that's never cancelled (IB market-data line leak); `:541` any IB message
  carrying the md reqId (incl. informational warnings) is escalated to an error.
- **Approach**: ensure snapshot paths dispose both subscriptions; add
  `cancelMktData` for the shortable-shares stream; filter `:541` to genuine
  error codes (not IB info codes).
- **Risk**: medium — IB-runtime lifecycle; over-cancelling could kill a live
  feed. Validate carefully in paper (watch md-line count over many snapshots).
- **Validate (paper)**: loop 100 snapshots, confirm IB market-data line usage
  returns to baseline (no monotonic growth).
- **DECISION (skipped):** on inspection, 520/541 live in `__subscribe_contract` —
  the *live* long-lived ticker-subscription path (just fixed for reconnect); a
  "fix" there risks regressing the strategy feed. 822 is a minor infrequent leak
  whose only safe fix needs subscriber ref-counting (cancelling a shared
  contract stream would kill a strategy). Poor risk/reward on the hot path.

---

## Cluster F — narrow CLI / polish  (S each, low risk)

- **Options `--json` stdout** (`mmr_cli.py:8718`): options buy/sell emit Rich
  console lines that corrupt `--json`; guard rendering behind `_json_mode`.
- **Sweep exception safety** (`mmr_cli.py:6001`): one unexpected job exception
  (or parent crash) leaves the sweep half-finalised; wrap per-job + finalize.
- **Sweep SIGINT-restore** (`mmr_cli.py:5841`): save/restore the handler
  (deferred earlier as a re-indent; do it with a small context manager).
- **Data-download trader_service probe leak** (`mmr_cli.py:7323`): dead cleanup
  branch leaks the RPC client on a failed probe.

---

## Cluster G — Discovered in live paper operation (2026-07-03)

Found while running the ASX/US ORB+VWAP strategies live (paper). The live-trading
path for bar-based strategies was rebuilt this session — see commits e2db6f3
(ORB on_prices + exchange-aware session), e911fee (tick→bar resampling layer),
e2b2fd1 (VwapReclaim on_prices). These are the residual robustness items.

### G1 — mass-enable of strategies can time out the enable RPC under DB contention  (S, low risk)

- **Symptom:** enabling many strategies in quick succession (observed: 11 at once
  after a restart) produced one `enable_strategy: RPC call ... timed out after
  6000ms` plus a burst of asyncio slow-callback warnings, all at startup.
- **Mechanism:** each `StrategyRuntime.enable_strategy` calls
  `_persist_enabled()` (D2), which does a DuckDB DELETE+INSERT to `strategy_state`.
  N simultaneous enables → N writes to a DB also serving event_store, priming
  reads, and a concurrent reconcile. Under `DuckDBConnection.execute_atomic`'s
  IOException backoff (up to ~45s), one enable's write can exceed the client's
  6s RPC timeout. The `strategies enable` CLI then reports the cosmetic
  "Enable failed: None" even though the server completes the write and the
  strategy ends up RUNNING.
- **Impact:** none observed — self-resolving; every strategy ended up enabled.
  Latent edge: under heavier contention a write *could* fail after retries and
  leave a strategy not-enabled while the client already gave up.
- **NOT caused by** the new priming/resampling (priming is lazy/on-first-tick,
  not on the enable path).
- **Fix options (offline, pick one):** (a) stagger enables in the CLI loop with a
  small delay; (b) raise the enable RPC timeout; (c) make `_persist_enabled`
  fire-and-forget / off the RPC-reply path; (d) batch multiple enables into one
  RPC + one DB write. (a) or (d) preferred.
- **Validate:** enable 10+ strategies in a tight loop under concurrent load,
  confirm no enable RPC timeout and all reach RUNNING.

### G2 — IB market-data-farm status codes logged at ERROR  (XS, cosmetic)

- **Symptom:** during the nightly IB Gateway restart/reconnect, farm-status
  messages (`errorCode 2119` "Market data farm is connecting", and the related
  2103/2105 "farm disconnected") are logged at ERROR by `ibreactivex`
  (`__handle_error`). Observed 3× on one reconnect.
- **Mechanism:** `__handle_error` early-returns (suppresses) only the "farm OK"
  codes (2104/2106/2107/2158); the transient "connecting/disconnected" farm
  codes fall through to the ERROR log path.
- **Impact:** none functional — pure log noise, but it can mask a real error in a
  `grep -i error` scan (as it briefly did in this review).
- **Fix:** add 2103/2105/2119 (and 2158 if not covered) to the suppressed/INFO
  set in `__handle_error`, or log all `reqId == -1` farm-status codes at INFO.
- **NOTE:** the reconnect itself recovered cleanly (reentrancy guard fired,
  subscriptions republished, all strategies stayed RUNNING) — this is only about
  the log level of the status messages, not the reconnect behaviour.

### G3 — `get_status` reports connected/upstream-OK while the IB socket is dead  (S, medium value)

- **Symptom (2026-07-05):** IB Gateway's 23:59 auto-restart hung (stuck login
  dialog, API port never opened). trader_service retried reconnection every
  ~130s for **10.5 hours** (289 attempts) — yet `mmr status` reported
  `connected: true, ib_upstream_connected: true` the whole time. The only
  honest tell was the (stale) `account_values_warning`.
- **Mechanism:** `ib_upstream_connected` is driven by IB error codes
  (1100/1102/2103/…). When the *socket itself* dies, no error code ever
  arrives, so the flag freezes at its last value. `connected` likewise doesn't
  reflect the live socket state during a reconnect loop.
- **Impact:** a dead gateway is invisible to `status` and to any monitoring
  built on it. The 2026-07-04 outage would have silently killed Monday's
  open had it not been caught manually.
- **Fix:** `get_status` should surface socket truth: `ib.isConnected()`,
  reconnect-loop state (attempt count, time since last success), and flip
  `ib_upstream_connected` to false/unknown while disconnected. The CLI
  pre-flight check should treat "reconnecting" as down.
- **Validate (paper):** stop the gateway container, confirm `mmr status`
  reports disconnected within one reconnect cycle.

### G4 — SDK status `open_orders` counts order *updates*, not open orders  (XS, cosmetic)

- **Symptom:** `mmr status` showed `open_orders` climbing 6 → 8 → 10 across
  gateway reconnects while `mmr orders` (broker truth) showed 1 (the PLS stop).
- **Mechanism:** `sdk.py:2578` computes `sum(len(v) for v in orders.values())`
  over the book's per-orderId *update-history lists*; every reconnect re-report
  appends entries. The risk gate is unaffected — it uses
  `book.get_open_order_count()` (deduped by orderId, active statuses only).
- **Fix:** count distinct orderIds with an active latest status (reuse
  `get_open_order_count` via RPC instead of recomputing in the SDK).

### G6 — Strategy signals never execute: the auto-execute path does not exist  ✅ DONE (paper-validated live 2026-07-05)

> **Built** as `trader/strategy/auto_executor.py` + strategy_runtime hooks, per
> the design constraints below. 42 unit tests (`tests/test_auto_executor.py`).
> **Live paper validation** (Sunday evening, ASX closing auction): a test
> strategy's BUY signal flowed ticks → bar → on_prices → Signal →
> AutoExecutor → propose (auto-sized 55 sh FMG) → approve → IB ACK
> (PreSubmitted MKT order, proposal #14 EXECUTED); a second bar's BUY was
> correctly skipped (no pyramiding); a *separate process* was also refused by
> the shared attribution DB; after cancelling the unfilled order, reconcile
> correctly marked the attribution CLOSED_EXTERNALLY. Time exits + SELL-close
> + broker-clamping are unit-tested but did not run live (market closed by
> validation time) — watch the first real session. Kill switch:
> `MMR_AUTO_EXECUTE_DISABLED=1`.

- **Symptom (2026-07-05 ASX session):** orb_wds emitted a BUY at 17:39 PDT
  (correct ORB timing), it was logged, written to the event store, and
  published — and nothing happened. No proposal, no order, no trade. Same for
  orb_bhp on 2026-07-02.
- **Mechanism — the pipeline dead-ends at the MessageBus:**
  `strategy_runtime.py:403-420` (signal → log → event store →
  `zmq_messagebus_client.write('signal', signal)`) is the *entire* signal
  handling. Nothing anywhere subscribes to the `'signal'` topic. The
  `auto_execute` flag is loaded from YAML, surfaced in `strategies` output
  (commit dabca4b), carried in StrategyConfig/StrategyContext — and never read
  by any execution logic. `git log -S auto_execute` confirms no consumer was
  ever written; the strategy-side execution half was simply never built.
- **Also missing live:** `Signal.max_hold_bars` / `close_by_time` time-exits
  are backtest-only by documented admission (`strategy.py:23-30`) — VwapReclaim
  sets `close_by_time` on every entry signal, so even with an executor its
  positions would never flatten EOD without this.
- **Design constraints for the fix** (match the backtest semantics that
  validated these strategies):
  - Long-only: BUY opens (one position per conid per strategy, no pyramiding),
    SELL closes the held quantity, SELL-when-flat is a no-op
    (`backtester.py:372-424`).
  - Sizing via `PositionSizer` (confidence=probability, risk from signal) —
    same knobs the propose pipeline uses.
  - Route through the existing machinery, not around it: proposal record for
    audit + executioner path (trading filter + risk gate already live there).
  - Honor `paper_only` and account-mismatch guards.
  - Time-exits: a small scheduler in strategy_runtime that synthesizes SELLs
    for `close_by_time`/`max_hold_bars`, mirroring `backtester.py:469-515`.
  - Idempotency across reconnects/restarts (signal dedup by (strategy, conid,
    bar-timestamp) — the event store already has the data).
- **Validate (paper):** replay a session where a strategy signals; confirm
  proposal → order → fill → position, EOD flatten, and risk-gate rejection
  paths all land in the event store.

### G5 — ib_async decoder KeyError traceback noise after reconnect  (XS, cosmetic)

- **Symptom:** `KeyError: 81` traceback from `ib_async/wrapper.py:874`
  (`contractDetails` for a reqId with no `_results` slot) logged after a
  reconnect, with the full PLS contract dump (~40 lines of noise).
- **Mechanism:** a contract-details response from the pre-disconnect session
  arrives after reconnect; the wrapper's request table was reset.
- **Fix:** harmless upstream race — either suppress via logging filter on
  `ib_async.decoder` for this signature, or ignore.

---

## Recommended sequence

1. **Cluster A (A1+A2, then A3)** — the capital-safety core; do first. ~1 week.
2. **Cluster B** — data correctness that silently poisons daily backtests. ~1–2 days.
3. **Clusters D + F** — cheap robustness/polish wins; good to batch. ~1–2 days.
4. **Cluster C** — precision fixes; C2 cheap, C1 needs live-IB validation. ~1–2 days.
5. **Cluster E** — IB-runtime leak fixes; validate hardest. ~1–2 days.

Rough total: **~2.5–3 weeks** of focused work. Everything is paper-validatable
against the running `DUM422056` stack before it ever touches live. A1–A3 and E
are the ones that genuinely need the live-IB paper environment; the rest are
mostly unit-testable.

## Design note — shared infrastructure

Cluster A's **OrderLifecycleTracker** (subscribe once to
`orderStatusEvent`/`execDetailsEvent`; write fill/cancel/reject events; expose
order-id → status) is the keystone: A1 (ack), A2 (events), and A3
(reconciliation) all read from it, and it's the natural home for future
order-related features. Build it first, thin, and layer the three behaviours on top.
