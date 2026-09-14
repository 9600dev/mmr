# MMR architecture and execution review — 2026-09-08

**Historical review.** Subsequent implementation, pinned regressions, measured
improvements and final verification status are tracked in the
[remediation report](REMEDIATION_2026-09-08.md). Findings below describe the
reviewed commit; they are preserved as the original evidence.

Reviewed commit: `e6918526d4915c44473996278322146188a8ce6a`.

**MMR's pure safety kernel is substantially better verified than the protocol
around it. The current system cannot reliably promise that a strategy trades
only its own instrument/inventory, that an accepted close remains managed until
executions and surviving orders reconcile, or that restarts and ambiguous
responses cannot create duplicate exposure.** These are
reproduced behavior gaps, not hypothetical concerns about code style.

The recommended next investment is an exact-identity, durable-intent and
fill-driven execution protocol, accompanied by one shared strategy construction
and bar-delivery contract. Moving arithmetic to another language or adding more
in-sample backtests would not address the failures found here.

Deliverables:

- [Proposed strategy execution contract](STRATEGY_EXECUTION_CONTRACT.md), clearly
  distinguished from the guarantees currently implemented.
- [Executable counterexamples and controls](../tests/review/README.md).
- [Runtime benchmark](../scripts/review_runtime_benchmark.py) and
  [storage benchmark](../scripts/review_storage_benchmark.py), using synthetic
  input, temporary storage and no broker.
- [Runtime measurements](evidence/review_runtime_performance_2026-09-08.json)
  and [storage measurements](evidence/review_storage_performance_2026-09-08.json).

Production code, deployed strategy files and trading configuration were not
intentionally changed. No broker orders or service restarts were performed.
The existing backup test was corrected to synchronize with its child writer;
new review tests are separate from the unchanged human-owned invariant suite.
The local operational-state file was absent. This is a source/behavior review,
not certification of any running account or paper exchange session.

## Scope and method

Four parallel review tracks covered strategy loading/dispatch/auto-execution,
order authorization/broker lifecycle/SDK resizing, messaging/storage, and market
data/performance/deployment operation. Reviewers read the existing safety and
audit roadmaps, traced callers through real production methods, built bounded
counterexamples, and independently challenged one another's fixtures and claims.

The critical paths inspected include `StrategyRuntime`, `AutoExecutor`,
`TraderServiceApi`, `Trader`, `TradeExecutioner`, `OrderLifecycleTracker`,
`RiskGate`, SDK approval/resize, the IB order adapter, RPC/PubSub/MessageBus,
DuckDB/event/proposal/auto-exec stores, tick normalization/resampling and history
priming. Service startup, health checks and deployment/config behavior were also
reviewed. Existing tests cover the pure arithmetic and research kernels; this
review did not independently re-derive every statistical algorithm, audit every
web endpoint, or test every vendor/venue integration.

Counterexamples use the real runtime/gate/storage methods, with broker/network
effects replaced by controlled adapters or ephemeral local sockets. Acceptance,
partial fill, cancellation and unknown outcomes are independent in the new
fixtures. No real market data or operational positions are included in artifacts.

Priority means consequence, not implementation effort: **P1** can create
unintended exposure, abandon management, lose execution truth or indefinitely
stop a critical path; **P2** materially corrupts data/availability/performance.
Known design limitations are labeled separately from newly confirmed defects.

## What the system currently guarantees

| Boundary | Existing useful control | Limit found in this review |
|---|---|---|
| Pure order arithmetic | Contracted split, size, exit and protective-stop functions; extensive properties | Correct math can be supplied another instrument's position or a quantity already reserved by another order. |
| Proposal approval | Atomic proposal compare-and-set and immutable terminal statuses | Deduplicates one proposal; an ambiguous retry can create a different proposal and another order. |
| Order placement | Structural checks and capability tokens | Passing some gate evidence does not prove every required check ran; protective-child exemptions can authorize new exposure. |
| Broker lifecycle | Acceptance/status observation and terminal event recording | Acceptance is not fill; partial fills followed by cancellation are missing. |
| Position ownership | Strategy/conId attribution and aggregate broker clamp | Requested quantity is attributed before fill; aggregate holdings do not identify manual/other-strategy inventory. |
| Protection | Broker-side stops, startup reconciliation and repair attempts | Unknown reads/cancels can duplicate stops or clear attribution; live stop presence does not prove correct coverage. |
| Deployment | Source gauntlet, config loader, enable/disarm controls | Runtime omits class from PASS lookup; effective params and applied configuration can differ from what was reviewed. |
| Data integrity | Bar-quality spec, quarantine, historical/live normalization | Missing-column input bypasses validation; historical priming and completed/session-filtered live bars differ. |
| Storage concurrency | Short-lived connections, process-local locks and retry | Not a transaction around every callback; read/merge/write can lose concurrent updates; lock waits block the broker loop. |
| Transport/recovery | Request-ID matching, socket replacement, heartbeat/health probes | Default send can block forever; a timed-out request can still execute; publisher startup can hang after bind failure. |

The passing controls matter: a true exact-contract exit still works with an
unavailable gate; an explicit-quantity open is refused by proposal policy;
fully filled strategy-owned positions close; ordinary invalid bars quarantine;
sequential history merges retain updates; concurrent processes cannot both
claim the same proposal. The problems emerge from composition and failure states.

## 1. Authorization can describe the wrong exposure

| ID | Priority | Reproduced trigger and consequence | Code and required correction |
|---|---|---|---|
| O01 | P1 | Hold 100 stock shares; SELL 100 unheld option contracts with the same symbol and a different conId. All are submitted with `risk_gate=None` as an exit. | [position/split resolution](../trader/trading/trading_runtime.py#L1087): remove same-symbol authorization fallback; use exact account/conId identity. |
| O02 | P1 | `require_proposal_approval=True` rejects explicit BUY 10 but permits an amount-based BUY that later sizes to the same 10 shares. | [RPC gate](../trader/messaging/trader_service_api.py#L119), [size resolution](../trader/trading/trading_runtime.py#L2184): authorize the fully resolved opening quantity. |
| O03 | P1 | SELL 100 closes a long but retains STOP_LOSS exit specification, creating a BUY 100 child exempted as `PROTECTIVE_CHILD`. That child can reopen long. | [child construction](../trader/trading/trading_runtime.py#L1937): prove the parent opens the exposure the child reduces; gate explicit reentry. |
| O05 | P1, known | A direct open still submits when its margin provider is unavailable because the direct path never calls it. | [direct execution](../trader/trading/executioner.py#L404): unify mandatory gate coverage. Already deferred in [Safety Roadmap tranche 2](SAFETY_ROADMAP.md#tranche-2--designed-not-built). |
| O06 | P1 | `exit_type='BRACKETT'` with TP/SL prices silently becomes a naked transmitting market entry. | [ExecutionSpec.validate](../trader/trading/proposal.py#L57), [placement fallback](../trader/trading/trading_runtime.py#L1996): validate enums/field combinations before any placement. |
| O07 | P1 | NaN margin, equity and NetLiquidation return approved with both leverage/cushion marked pass. | [check_leverage](../trader/trading/risk_gate.py#L482): finite/domain validation before truthiness or comparisons. |
| O09 | Design extension | A $10 add to a $1,000 holding in a $10,000 account passes the nominal 10% cap. | [RiskLimits](../trader/trading/risk_gate.py#L32) explicitly describes per-order behavior. Aggregate position concentration needs a separately named contract, including working-order reservations. This is not a regression in the documented per-order algorithm. |

These are server-side problems. A careful strategy, source gauntlet or client
proposal workflow cannot establish the missing authority at the final order
boundary. A normalized server-owned intent and complete, typed gate evidence
should feed the single broker adapter.

## 2. Acceptance, execution and inventory are conflated

| ID | Priority | Reproduced trigger and consequence | Code and required correction |
|---|---|---|---|
| S01 | P1 | Manual long 100 plus strategy BUY 140 filling only 40 leads to attributed 140; strategy exit sells all 140, including the manual shares. | [executed quantity](../trader/strategy/auto_executor.py#L1250), [close clamp](../trader/strategy/auto_executor.py#L1034): attribute execution fills, not proposal/order quantities. |
| S02 | P1 | An accepted close with zero fills immediately marks attribution CLOSED and removes management, despite the broker still holding the position. | [close result](../trader/strategy/auto_executor.py#L1063): retain CLOSING and remaining owned inventory until fills reconcile. |
| S03 | P1 | Same-bar ambiguous submission, crash after broker fill/before attribution, and UNKNOWN quantity followed by restart each permit another open. | [open path](../trader/strategy/auto_executor.py#L949), [OPEN-only queries](../trader/strategy/auto_executor.py#L452): durable intent/idempotency before submission; reserve and reconcile UNKNOWN instead of treating it as flat. Three separate counterexamples. |
| O08 | P1 | Submitted with 40/100 filled, then Cancelled, records no ORDER_FILLED. Real inventory and cash movement disappear from the fill ledger. | [lifecycle](../trader/trading/order_lifecycle.py#L83), [event wiring](../trader/trading/trading_runtime.py#L481): ingest executions with persistent execution-ID dedup and replay; keep order status separate. |
| M04 | P1 | First fill-event append fails; repeating the same Filled callback never retries and stores zero events. | [terminal marker](../trader/trading/order_lifecycle.py#L87), [append](../trader/trading/order_lifecycle.py#L168): durable dedup and retry/reconciliation, without marking persistence before it succeeds. |

The existing proposal state machine is valuable and passed its concurrent
claim control. It does not provide broker idempotency: a fresh proposal is a new
claim. Nor can terminal `EXECUTED` represent all future partial fills, cancels,
busts and protection transitions. Introduce an order/execution projection rather
than weakening proposal terminal-state invariants.

## 3. Protective orders and exits need one coordinated protocol

| ID | Priority | Reproduced trigger and consequence | Code and required correction |
|---|---|---|---|
| O04 | P1 | Two accepted, unfilled SELL 100 closes against 100 held leave 200 independently executable shares. No concurrency is required. | [split_for_order](../trader/trading/trading_runtime.py#L1148): reserve reducible capacity and coordinate all remaining executable exits. |
| S04 | P1 | Stop cancellation explicitly fails, but the close proceeds and fills; a live SELL stop remains against a flat account. | [cancel_protective](../trader/strategy/auto_executor.py#L1220): cancellation is a tracked transition, not a successful API call; retain unknown state and coordinate competing reductions. |
| S05 | P1 | An orders-read exception becomes an empty list, interpreted as a dead stop; repair creates a second stop. | [protective lookup](../trader/strategy/auto_executor.py#L1197): distinguish unreadable/partial/complete-empty snapshots before replacement. |
| S06 | P1 | First 40 of a 140-share entry receive stop 40; later 100 fill, but repair sees the live ID and leaves coverage at 40. | [protective repair](../trader/strategy/auto_executor.py#L1135): reconcile desired quantity/price against actual order state, not presence alone. |
| S07 | P1 | Reconcile recognizes an empty startup read as inconclusive, then the worker continues into SELL processing, cancels protection and marks CLOSED_EXTERNALLY from that empty read. | [worker](../trader/strategy/auto_executor.py#L678), [grace](../trader/strategy/auto_executor.py#L713): propagate snapshot/readiness state to every dependent decision. |
| O11 | P1 | Resize sees successful cancel request with `PendingCancel` and submits a replacement stop while the old one can still execute. | [resize cancellation](../trader/sdk.py#L2117): terminal cancellation/reconciliation or validated broker modification/OCA protocol. |
| O12 | P1 | SELL trim 50 is only Submitted against holding 100, but resize replaces protection with stop 50. | [resize trim](../trader/sdk.py#L2044), [replacement](../trader/sdk.py#L2137): protect actual inventory through partial/absent fills using the same reduction coordinator. |
| S12 | P1 | A strategy callback raises, becomes ERROR, and future bars stop its time-exit/protective-management checks. DISABLED has the same coupling. | [enabled filter](../trader/strategy/strategy_runtime.py#L566), [dispatch](../trader/strategy/strategy_runtime.py#L744): position management must outlive signal generation. |

Do not fix these one line at a time by blocking exits or waiting for a trim while
leaving another full-size sell executable. Preserve the unrefusable exit-intent
policy and explicitly coordinate broker orders, remaining quantities and
uncertainty. Broker disconnection makes an immediate confirmed fill impossible;
the API must report that honestly while retaining the exit request and management.

## 4. The deployed rule can differ from the tested rule

| ID | Priority | Reproduced trigger and consequence | Code and required correction |
|---|---|---|---|
| S08 | P1 | YAML declares uppercase THRESHOLD=5; the live instance still reads class default 20. Backtesting applies the override. ORB works around some keys but not all. | [load/install](../trader/strategy/strategy_runtime.py#L1173), [backtest overrides](../trader/simulation/backtester.py#L835): share typed construction and report effective params. |
| S09 | P1 | PASS for class V1 in a module permits an untested sibling class to arm even with `MMR_GAUNTLET_ENFORCE=1`. | [runtime lookup](../trader/strategy/strategy_runtime.py#L1018): include class in the authoritative PASS identity, as CLI already does. |
| S10 | P1 | Replacing existing config with `auto_execute:false`, or removing the entry entirely, leaves the old RUNNING instance armed. CLI undeploy only writes YAML. | [existing-name return](../trader/strategy/strategy_runtime.py#L1057), [config load](../trader/strategy/strategy_runtime.py#L1227), [undeploy](../trader/mmr_cli.py#L5345): apply generation-aware config diffs and revoke opening authority before acknowledgment. Two counterexamples. |
| S13 | P2, known | An `on_bar`-only class can pass/load/arm yet live dispatch only calls the base `on_prices` no-op. | [dispatch](../trader/strategy/strategy_runtime.py#L773), [documented backtest-only API](../trader/trading/strategy.py#L226): reject unsupported live capabilities or provide a parity adapter. Test accepts either remedy. |

Add-only reload is documented for existing configuration changes. The dangerous
user-facing consequence is that removal/disarm can appear successful without
revoking execution authority. An applied-generation acknowledgment is necessary.
Queued SignalWork also snapshots arming/state before execution; source review
shows why generation checks must occur again at the side-effect boundary.

**Retired S11:** a direct-constructor paper-only test initially suggested paper
deployments were refused. Independent caller review found
`Container` synchronizes `paper_trading` and `trading_mode`; the normal service
path avoids it. The test and production claim were removed. The constructor's
inconsistent defaults alone do not establish a deployed failure.

## 5. Bar and storage contracts are not uniform

| ID | Priority | Reproduced trigger and consequence | Code and required correction |
|---|---|---|---|
| D01 | P2 | A transient first history read fails; an empty priming marker prevents subsequent frames from ever retrying. | [priming](../trader/strategy/strategy_runtime.py#L613): explicit readiness/retry and invalidate after history refresh; do not cache failure as success. |
| D02 | P1 | Current still-forming historical bar remains at frame tail even though live resampling excludes its forming bar. | [frame merge](../trader/strategy/strategy_runtime.py#L648): one completeness rule for history and live. |
| D03 | P2 | A Sunday quote is suppressed as the current bar but remains in retained history and enters a later valid strategy window. | [tail-only gate](../trader/strategy/strategy_runtime.py#L757): filter/admit every bar by the declared session/price-basis policy; retain legitimate extended-hours data. |
| D04 | P2 | First observed minute has cumulative volume 1,000,000→1,000,005 and is emitted with volume 1,000,005, though only 5 shares were observed. | [volume derivation](../trader/data/market_data.py#L118): mark/omit incomplete first interval and establish a baseline. Current behavior is documented, but incompatible with a complete per-minute-volume contract. |
| D05 | P2 | A valid daily bar stamped midnight ET is rejected by the intraday session gate and never reaches a daily strategy. | [session check](../trader/strategy/strategy_runtime.py#L681): distinguish daily session labels from intraday timestamps. |
| D06 | P2 | HKEX 12:30 lunch break is considered in-session. | [session_window](../trader/data/market_session.py#L168): represent break intervals, not just outer open/close endpoints. |
| M02 | P1 | Starting minutes [0,10], two successful concurrent merges of [5] and [6] leave only one addition. | [overlap merge](../trader/data/data_access.py#L581): atomic keyed upsert/merge; separate locked read and write do not protect the whole operation. |
| M03 | P2 | Missing `high` or `volume` bypasses bar validation and persists rows that the pure quality spec calls errors. | [mask schema handling](../trader/data/bar_quality.py#L378), [write fallback](../trader/data/data_access.py#L442): validate required schema before mask; do not persist unvalidated data after validation exceptions. |
| M05 | P2 | One instant in UTC/Pacific fails dedup; distinct historical DST-fold instants collide. | [AutoExecState._naive](../trader/strategy/auto_executor.py#L520): normalize UTC before dropping tzinfo, or store timezone-aware instants. |
| M06 | P2 | An `execute_atomic` callback DELETE commits even when the next statement fails. Persisted enabled/disabled state uses this pattern. | [connection callback](../trader/data/duckdb_store.py#L177), [persist_enabled](../trader/strategy/strategy_runtime.py#L507): real BEGIN/COMMIT/ROLLBACK for transactional callbacks; audit callers before changing helper semantics. |

The startup source path also subscribes before historical download completes,
and successful downloads do not invalidate priming caches. The D01 case pins
the permanently-empty-cache behavior; the scheduling race is source-confirmed,
not a separately reproduced IB session.

## 6. Performance and bounded failure

| ID | Priority | Finding | Evidence |
|---|---|---|---|
| D07 | P2 | Every strategy resamples the entire tick stream before the same-bar dedup check. Four subscribers cause four full resamples for one tick. | [dispatch](../trader/strategy/strategy_runtime.py#L744), [benchmark](../scripts/review_runtime_benchmark.py). Incremental bars and shared per-instrument/interval frames remove the repeated work. |
| O10 | P2 | Completed order calls retain hot per-contract subscriptions on both expressive and simple paths. Five calls leave five listeners, retaining closures and increasing future fan-out. | [IB adapter](../trader/listeners/ibreactive.py#L523), [simple subscription](../trader/messaging/trader_service_api.py#L183). Dispose the actual temporary subscription; preserve shared lifecycle observers and use exact order identity. |
| M01 | P1 | Default RPC receive budget is 10 seconds, but `timeout=None` leaves send unbounded. An absent peer is still blocked after 11.5 seconds. | [socket options](../trader/messaging/clientserver.py#L917), [default runtime client](../trader/strategy/strategy_runtime.py#L472). Bound lock/send/receive under one deadline. |
| M07 | P1 | Publisher worker dies on bind failure while start waits forever for readiness; supervisor never sees a failed process. | [publisher](../trader/messaging/clientserver.py#L1350), [startup wait](../trader/messaging/clientserver.py#L1380). Communicate startup failure and bound waits. MessageBus has the same source pattern. |
| M08 | P1 | Synchronous event-store writes on the IB callback path wait on another process's DuckDB lock. | [callback append](../trader/trading/order_lifecycle.py#L168), [backoff](../trader/data/duckdb_store.py#L148), [benchmark](../scripts/review_storage_benchmark.py). Separate journal ingestion from bulk-storage locks and preserve durable replay. |

Local measurements: macOS arm64, Python 3.12.13, pandas 3.0.1, NumPy 2.4.2,
ib_async 2.1.0, DuckDB 1.4.4. They are synthetic microbenchmarks on this machine,
not broker latency or production capacity claims. Exact samples/versions are
preserved in the linked JSON files.

| Measurement | Result |
|---|---|
| Same-bar tick dispatch, 200,000 retained ticks, one strategy | 9.725 ms median per tick |
| Same input, four strategies sharing one instrument/interval | 34.797 ms median per tick, before strategy computation, broker, DB or calendar work |
| Retained frame for 200,000 ticks | 19.2 MB, before additional frames/copies/caches |
| Another process holds operational DB for 1 second | `on_trade` blocks 1.218 s; 10 ms heartbeat pauses 1.230 s |
| SELECT 1 with connection open/close, 50 samples | 4.189 ms median; 4.567 ms p95 |
| Update 10 bars against 100,000 existing bars | 3.155 s median across three runs; entire history read/rewritten |

The database retry comment understates the implemented budget. Its 32 sleeps
can sum to approximately **55.15–110.3 seconds**, excluding connection overhead
and the unbounded process-local lock wait. Raising RPC timeouts does not solve
event-loop starvation. Likewise, two days of raw ticks is a time-retention limit,
not a small bound on work per tick.

The runtime/executor and publisher also use unbounded queues or spawn tasks
without an explicit execution budget. This source-confirmed overload risk needs
queue age/depth and event-loop lag metrics, bounded flow control, and independent
exit priority. There is no benchmark here of a full-session queue saturation.

## 7. Trust, operations and evidence limitations

Several high-impact issues are already acknowledged in the roadmaps: strategy
code is arbitrary in-process Python; the gauntlet's AST import scan is not a
sandbox; restricted pickle module/type allowances are not a general safe data
format; direct-path leverage is deferred; runtime gauntlet is warn-only by
default. This review does not count those documented facts as new discoveries.
For model-deployed code, process isolation and constrained execution capabilities
remain more valuable than describing an import allowlist as isolation.

Source-only followups, **not included in the reproduced counterexample count**:

- Public expressive RPC accepts raw contract/action/quantity rather than a
  server-validated approved proposal identity. Clarify/authenticate the proposer
  trust boundary; fencing the convenience simple path alone is not authority.
- Several notional/tier paths compare instrument-price-derived values with
  account-base or USD thresholds; SDK conversion includes a rate-1 fallback.
  Audit FX, multipliers and valuation freshness end-to-end with non-USD contracts.
- Direct split results can expose only the reduction observable, while
  expressive failure can follow a successfully submitted reduction. Return
  per-leg partial outcomes so callers do not retry an already-effective close.
- Broker snapshots from different feeds are not one atomic snapshot. External
  clients/manual trading and corporate actions require explicit reconciliation
  and ownership-adjustment semantics.
- In-process strategy exceptions are contained, but hangs/CPU/memory exhaustion
  are not isolated. Runtime callback budgets need subprocess enforcement.

The local Docker/health inspection was read-only. No paper-mode broker session
was used: deterministic adapters made the relevant failure interleavings
repeatable. Actual exchange order-type, OCA, partial-fill, reconnect, lot/tick
and account-mode behavior remains a required paper-validation stage before
claiming the proposed contract.

## 8. Why existing checks did not find these cases

The safety kernel's contracts, properties, static gate and mutation scope are
useful and should stay. They cannot validate facts supplied by an I/O resolver
or prove that two separately correct functions compose into a safe protocol.

- Existing auto-executor FakeSDK makes every successful approve fill
  synchronously, erasing accepted/unfilled/partial states.
- `tests/test_strategy_runtime.py` includes a copied stub labeled an exact copy
  that still dispatches raw ticks; it omits current bar/session/dedup logic.
- Tests often stop at a component boundary: the startup-empty reconciliation
  test misses the worker immediately processing a close afterward.
- A nonempty passing gate record cannot establish complete gate coverage.
- All normal tests can pass while the new explicitly unresolved review cases
  remain xfailed. That is bookkeeping for findings, not proof of safety.

The first full run recorded **2,708 passes, 3 failures, 11 errors**. Twelve
mutation-gate problems were missing dev-only `mutmut`; installing the prescribed
3.6.0 in the local environment resolved them. The documented vectorbt/numba
ordering failure passed in isolation. The backup test's fixed 200 ms sleep raced
child-process startup; it now waits for an explicit lock-acquired acknowledgment,
with bounded child cleanup. Its assertion was preserved. Focused backup and
mutation-gate verification then passed **20 tests**.

The final full run passed **2,728 tests with 44 expected failures** in 184.39
seconds. Those 44 are the new unresolved review counterexamples; their markers
do not constitute remediation. Six additional controls passed. The review
contains 39 numbered items: 38 represented by counterexample tests (some have
multiple cases), plus the measured M08 event-loop contention finding. This count
includes clearly labeled existing limitations and proposed stronger contracts.

Final aggregate verification is recorded in
[review_validation_2026-09-08.json](evidence/review_validation_2026-09-08.json).
The initial type gate passed: **0 kernel diagnostics, 94 advisory diagnostics,
none beyond their baselines**. No baseline or invariant was weakened. No full
mutation/CrossHair rerun was required for this review because no production
kernel implementation changed; existing scores are not claimed as a new run.

## 9. Recommended implementation sequence

1. **Close authority holes in small verified changes.** Exact account/conId
   positions; amount sizing before proposal gates; strict execution specs;
   finite margin data; class-aware gauntlet; shared parameter construction;
   complete direct-path gate coverage. Keep each regression and the existing
   exit/structural invariants. Re-run kernel verification for actual code changes.
2. **Build durable intent and execution truth.** One transactional operational
   owner, stable intent/broker/execution IDs, pending/unknown reservations,
   execution-driven attribution, late-result reconciliation, and a separately
   defined degraded exit path when journal storage is unavailable.
3. **Unify position and protection management.** Coordinate closes, partial
   entries, stops, cancellations and resize; protect actual owned inventory;
   continue managing positions through strategy failure/disarm/removal. Define
   supported external/manual activity and broker-native coordination limits.
4. **Make deployment a versioned applied state.** Validate capabilities and
   effective params; revoke old generations including queued work; report
   readiness/arming truth. Add strategy process isolation and callback budgets.
5. **Unify and accelerate market data.** Completeness and session policy shared
   across priming/live; reliable history refresh; atomic bounded-range upserts;
   incremental bars and shared fan-out. Avoid copying full histories per tick.
6. **Exercise the protocol under faults.** Run a deterministic broker simulator
   through crash/partial-fill/cancel/reconnect/storage scenarios, then repeat
   relevant transitions in verified paper mode. Observe unknown-intent age,
   ownership differences, protection coverage, queue age and event-loop lag.

Completion should be measured by named contracts and failure scenarios becoming
true, with expected-failure markers retired, rather than by code volume or a
larger count of passing happy-path tests.
