# Execution architecture remediation — September 2026

The working-tree implementation addresses the **39 numbered findings** in the
[architecture review](ARCHITECTURE_REVIEW_2026-09-08.md), plus the additional
execution, recovery, arithmetic and catalogue defects found while verifying it.
The original 44 expected-failure markers were removed; the counterexamples
remain ordinary regression tests. The current API and operating guarantees are
specified in the [strategy execution contract](STRATEGY_EXECUTION_CONTRACT.md).

**Implementation and final verification are complete for this remediation.**
The source fingerprint is `1fd8240d18afb6d22bbeadba21f30a4993b9ff96fdde050ee69b279233268ad2`.
The final suite passed **4,566 tests**, with one platform skip, plus **21**
isolated async tests. Type checks, the verified Docker image, native callback
measurement, paper restart/replay and normal paper-service availability checks
passed. The [final verification record](evidence/remediation_final_verification_2026-09-11.json)
binds each result to its source and states the remaining limits.

Normal paper services are running on that image. A subsequent explicit roster
check found missing contract metadata in the existing empty catalogue. Exact
broker definitions were recovered into a separate catalogue and MMR alone was
restarted. Every configured explicit identifier now resolves, and all configured
strategies are **INSTALLED**, with zero **ERROR** states; auto-execution remains
off. This does not establish running strategy callbacks or fresh market data:
the CLI roster and tick-flow checks remain skipped. Historical data and the
previous ledger have **not** been restored.

The completed frozen-source mutation run covered **12,763 variants across 18
modules**, and all existing score floors passed. Later corrected-function
checks and exact-body diagnostics remain separate evidence; no combined
final-system mutation score is claimed.

The final native-callback correction prevents an individual fill price from
being mistaken for a cumulative average. Two one-share fills at $10 and $20
can have a known $30 audit total while the cumulative owned basis still needs
aligned price evidence. The correction also rejects native unset-price
sentinels and preserves valid sub-dollar averages. These scenarios use locally
modeled native broker callbacks, not newly observed paper-broker fills.

## What changed

Order submission now carries a durable intent identity through the SDK, RPC
server and broker order reference. Operational intent and execution records use
a SQLite WAL sidecar beside the trading DuckDB file. Unknown submissions retain
reservations; acceptance does not fabricate a fill or release management.
Broker executions and cumulative status progress drive attributed inventory,
with an idempotent outbox delivering audit events to DuckDB.

The server resolves exposure using the pinned account and exact conId. Its
order-decision lock covers authorization, competing reductions and submission.
Pending cancellation retains executable capacity; a replacement is reconciled
against actual remaining inventory. Opening orders share margin policy, explicit
valuation units and approved-proposal verification. A restored or degraded
journal suspends new exposure. Reductions require enough confirmed ownership
and competing-order evidence to avoid submitting a second executable exit.

Live strategy imports, construction and callbacks run in separate, bounded
worker processes. Deployment generations revoke stale opening authority;
effective parameters use the same coercion as backtests. Position management
runs independently of strategy code and recovers persisted exit policies.
Historical and live bars share completeness/session filtering and incremental
buffers. Storage writes use actual transactions and keyed upserts.


## Final repair batch

The completed original-source mutation review covered 12,239 variants across
all 18 configured modules. Both the canonical run and its unchanged score gate
passed, but survivor review and stronger native-producer tests exposed further
contract gaps. Passing the score floor did not establish that those gaps were
absent. The original run is retained under its own source fingerprint.

The first repair batch adds 273 test cases and strengthens existing assertions. Before
production replacement, the affected-case run observed 250 passes, 51 expected
assertion failures and four bounded timeout failures. A separate compatibility
run passed all 670 selected cases. These are original-production observations,
not results for the repaired build.

| Boundary | Corrected behavior | Regressions |
|---|---|---|
| Extreme finite protective-stop cost | Bound price rounding instead of decrementing one cent potentially billions of times; preserve the strict below-entry stop rule. | [protective plans](../tests/test_executor_protective_plan_contract_gaps.py) |
| Excessive PBO split request | Limit the split count to usable data without an input-sized decrement loop. | [selection and walk-forward contracts](../tests/test_selection_walkforward_contract_gaps.py) |
| Rolling strategy opening limits | Count distinct submitted/receipt-observed intent identities across durable receipts and successful bar logs; retain separate unkeyed legacy records and persist receipt-observed time. | [manifest state](../tests/test_state_manifest_contract_gaps.py) |
| Unreadable freshness inputs | Refuse new exposure when age, interval or the effective threshold cannot be evaluated; use the documented default for invalid environment configuration. | [freshness gates](../tests/test_pnl_and_gates.py), [worker admission](../tests/test_executor_admission_observability.py) |
| Bookkeeping failure after broker submission | Preserve the receipt, broker IDs and split outcome; report uncertainty without converting a potentially submitted attempt into a safe-to-retry failure. | [SDK receipt persistence](../tests/test_sdk_submission_receipt_persistence.py) |
| Terminal protective order before fill reconciliation | Retain its checkpoint until scoped execution history has durably updated ownership; a terminal-status probe alone cannot authorize a replacement. | [native executor controls](../tests/test_auto_executor.py) |
| Strategy-owned stop cost | Persist matched opening fill costs with ownership checkpoints; use the strategy’s surviving entry basis, independent of other holdings in the account. Missing cost evidence defers a new price plan; replacement rechecks cost after cancellation and reports why a new plan is unavailable. | Strategy-owned cost and native mixed-holding regressions |
| Missing cumulative fill average | Distinguish an execution slice price from an average aligned to total filled quantity. A known audit slice can coexist with unavailable owned cost; later aligned evidence restores the basis without duplicate quantity or fills. | [Lifecycle endpoint](../tests/test_order_lifecycle_contract_gaps.py) and [native owned-cost recovery](../tests/test_auto_executor.py) regressions; the counterexample uses locally modeled native callbacks, not a naturally observed paper-broker event. |
| Fresh adds to unresolved legacy ownership | Refuse a new automated add before intent/proposal creation when the existing attributed holding has no ownership epoch. Continue reconciling fills from earlier submissions. | Native worker/migrated-holding regression in [executor controls](../tests/test_auto_executor.py) |
| Cost warning after protective cancellation | Report the unavailable owned cost and deferred repair without claiming that a cancelled stop still protects the holding. | Native cancellation/price-recovery regression in [executor controls](../tests/test_auto_executor.py) |
| Exact conId discovery | Discover the security type without imposing the SDK’s stock default, then require its symbol and venue hints to resolve back to the same conId. | Native cash-contract regression in [executor controls](../tests/test_auto_executor.py) |
| Native derivative definitions | Preserve the broker contract multiplier through catalogue storage and amount sizing; missing derivative multipliers remain unavailable. | [SDK catalogue and sizing](../tests/test_sdk.py) |
| Typed and cached catalogue resolution | Keep integer conIds distinct from numeric tickers; validate consistency within the requested query scope before venue deduplication, first_only truncation or caching; invalidate cached queries on supported writes. | [SDK resolution](../tests/test_sdk.py), [universe resolver](../tests/test_universe_resolver.py) |

The exact-integer sizing regressions also pin the promised `ValueError` when
rounding makes an unaffordable share appear affordable. The already repaired
production path behaved correctly; these two cases close an oracle gap.

The targeted archived-body diagnostic run exercised 512 selected variants after
137 passing baseline batches. It observed 463 matching assertion failures,
47 other call failures and two initially undistinguished variants. Independent
review retained the actual exception origins: native errors, missing required
outputs and one forbidden-I/O boundary. The two undistinguished cases exposed
a log-correlation oracle gap; two focused followups passed their ordinary
baselines and caught both variants with exact log-argument assertions. These
are targeted diagnostics, not a replacement for the canonical mutation run.

## Finding-to-regression map

“Implemented” in these tables means the corresponding behavior is present in
the working tree and its counterexample is retained without an expected-failure
marker. It does not substitute for the final checks or paper scenarios below.
S11 was retired during review and is intentionally absent: normal Container
construction synchronizes paper-mode settings, so that suspected defect was not
established.

### Order authorization, coordination and lifecycle — O01–O12

The original regressions and subsequent order scenarios are in
[test_review_order_contract.py](../tests/review/test_review_order_contract.py).

| ID | Implemented behavior | Pinned regression |
|---|---|---|
| O01 | [`Trader._signed_position`, `get_positions`, `split_for_order`](../trader/trading/trading_runtime.py) use exact account/conId identity. Same-symbol stock holdings cannot authorize an option exit. | `test_stock_position_does_not_authorize_unheld_option_short`; `test_account_filter_applies_to_broker_and_fallback_position_sources` |
| O02 | [`TraderServiceApi.place_order_simple`](../trader/messaging/trader_service_api.py) establishes opening-policy permission independently of a missing quantity; the final sized order cannot bypass proposal policy. | `test_amount_sized_open_requires_proposal` |
| O03 | [`Trader._place_expressive_order`](../trader/trading/trading_runtime.py) strips protective-child construction from a pure reduction, as well as the reduction half of a flip. | `test_closing_long_does_not_attach_ungated_reopening_buy` |
| O04 | [`serialized_orders`](../trader/trading/trading_runtime.py) and [`TradeExecutioner._coordinate_reduction`](../trader/trading/executioner.py) coordinate working capacity, wait for terminal cancellation and re-clamp after racing fills. | `test_working_exit_orders_cannot_exceed_held_quantity_ungated`; `test_concurrent_reductions_share_one_account_reservation`; `test_pending_reduction_cancel_retains_capacity_until_terminal`; `test_fill_racing_cancel_clamps_replacement_to_remaining_inventory` |
| O05 | [`Trader.margin_checks`](../trader/trading/trading_runtime.py) supplies the same opening-order margin policy to direct and expressive execution. Unreadable applicable margin inputs refuse an open. | `test_direct_open_fails_closed_when_margin_cannot_be_read` |
| O06 | [`ExecutionSpec.from_dict/validate`](../trader/trading/proposal.py) reject unknown fields/enums, invalid prices and incompatible protection inputs before submission. | `test_unknown_exit_type_does_not_submit_naked_entry`; additional cases in [test_proposal.py](../tests/test_proposal.py) |
| O07 | [`RiskGate.check_leverage`](../trader/trading/risk_gate.py) checks finite margin/equity/account values before comparisons. NaN cannot produce passing checks. | `test_nan_margin_inputs_are_unevaluable` |
| O08 | [`OrderLifecycleTracker`](../trader/trading/order_lifecycle.py) records partial execution progress independently of terminal order status. Persistent execution IDs and cumulative checkpoints prevent replay duplication. | `test_partial_fill_is_recorded_when_remainder_is_cancelled`; [test_execution_journal.py](../tests/test_execution_journal.py) |
| O09 | [`Trader.aggregate_position_value`](../trader/trading/trading_runtime.py) supplies the concentration check with post-order exposure including same-direction working orders. This intentionally strengthens the old per-order interpretation. | `test_adding_to_position_at_concentration_cap_is_refused` |
| O10 | [`IBAIORx.subscribe_place_order`](../trader/listeners/ibreactive.py), the executioner and RPC paths consume bounded placement receipts and dispose temporary subscriptions. Shared lifecycle observation remains separate. | `test_order_placement_disposes_its_temporary_subscription`, parameterized for both paths |
| O11 | [`MMR.execute_resize_plan`](../trader/sdk.py) delegates to the server's coordinated resize path. It does not interpret `PendingCancel` as permission to place independent replacement protection. | `test_resize_waits_for_protective_cancel_before_replacement`; `test_unconfirmed_oca_modification_never_publishes_resize_trim` |
| O12 | [`Trader.resize_position`](../trader/trading/trading_runtime.py) preserves actual inventory coverage using an existing matching protective tranche and reduce-with-block OCA. Unsupported handoffs defer before mutation. | `test_resize_keeps_full_protection_until_trim_fills`; `test_matching_resize_tranche_retains_partial_fill_protection_and_capacity`; `test_resize_defers_single_stop_partial_handoff_before_any_mutation` |

### Strategy ownership, recovery and deployment — S01–S10, S12–S13

Original regressions are in
[test_review_strategy_contract.py](../tests/review/test_review_strategy_contract.py).
The [execution recovery scenarios](../tests/test_execution_recovery.py) exercise
the same protocol through restart and failure boundaries.

| ID | Implemented behavior | Pinned regression |
|---|---|---|
| S01 | [`AutoExecutor`](../trader/strategy/auto_executor.py) attributes actual matched execution progress, never the requested order quantity or an unrelated aggregate holding. | `test_partial_fill_never_closes_manual_shares`; recovery tests for partial progress, replayed order IDs and attribution-commit crashes |
| S02 | An accepted close remains managed with its remaining owned quantity until executions and orders reconcile. A terminal partial close retains an exit intent for the residual. | `test_accepted_but_unfilled_close_retains_managed_position`; `test_terminal_partial_close_retries_only_unfilled_owned_residual` |
| S03 | [`IntentStore`](../trader/strategy/execution_intents.py) persists intent before propose/approve; [`ServerOrderJournal`](../trader/data/server_order_journal.py) claims the same identity before broker submission. Unknowns reserve across restart. | `test_ambiguous_open_prevents_duplicate_attempt_for_same_bar`; `test_crash_after_broker_fill_does_not_allow_another_open`; `test_unknown_execution_quantity_blocks_reentry`; recovery tests for durable pre-approve reservation and unknown replies |
| S04 | Protective cancellation remains a tracked transition. Failed/pending cancellation leaves the close pending; management retries without requiring another signal. | `test_failed_stop_cancel_cannot_leave_live_sell_after_close`; `test_pending_cancel_exit_retries_without_another_signal` |
| S05 | Unreadable working-order snapshots are distinct from confirmed absence. Ambiguous stop placement retains a recoverable intent rather than authorizing duplicate repair. | `test_failed_order_read_does_not_duplicate_protective`; `test_ambiguous_protective_placement_is_recovered_without_duplicate` |
| S06 | Protective repair compares required owned coverage against working quantity and observed fills, rather than treating a stored order ID as sufficient protection. | `test_protective_grows_when_more_of_entry_fills`; recovery of replacement-stop fills |
| S07 | Startup snapshot readiness propagates through worker processing. An inconclusive empty broker read cannot clear attribution or cancel protection downstream. | `test_first_empty_broker_read_preserves_attribution_through_worker` |
| S08 | [`parameters.apply_param_overrides`](../trader/strategy/parameters.py) is shared by live workers and the [backtester](../trader/simulation/backtester.py). Effective typed values are reported; unknown uppercase tunables are refused. | `test_live_runtime_applies_declared_uppercase_tunables`; [dispatch parameter tests](../tests/test_strategy_dispatch_contract.py); [isolated-worker parameter test](../tests/test_callback_worker.py) |
| S09 | [`StrategyRuntime._gauntlet_allows_arming`](../trader/strategy/strategy_runtime.py) queries PASS by exact source hash **and class**. The existing `MMR_GAUNTLET_ENFORCE` policy switch still governs runtime refusal. | `test_runtime_gauntlet_refuses_untested_sibling_class` |
| S10 | Config replacement/removal revokes the old generation before further loading; enable rotates it. Queued opens recheck current authority after slow proposal work. Config application reports applied state through the [service API](../trader/messaging/strategy_service_api.py). | `test_config_disarm_applies_to_existing_runtime_instance`; `test_config_removal_revokes_opening_authority`; dispatch tests for source edits/failed replacement/re-enable; `test_authority_is_rechecked_after_slow_proposal_creation` |
| S12 | [`StrategyRuntime._management_loop`](../trader/strategy/strategy_runtime.py) and executor management work run independently of callbacks. Disabled, failed and removed deployments retain management of attributed positions and persisted exit policies. | `test_strategy_error_does_not_stop_future_time_exit_checks`; recovery tests for pending exits and storage outages; [runtime worker integration](../tests/test_runtime_worker_integration.py) |
| S13 | [`StrategyCallbackWorker`](../trader/strategy/callback_worker.py) rejects classes that only implement the documented backtest-only `on_bar`/`on_panel` APIs. Live deployment requires `on_prices`. | `test_live_runtime_rejects_or_dispatches_on_bar_only_strategy`; worker initialization-failure tests |

### Market data — D01–D07

Original examples are in
[test_review_market_data_contract.py](../tests/review/test_review_market_data_contract.py).
Additional incremental and session-identity cases are in
[test_strategy_dispatch_contract.py](../tests/test_strategy_dispatch_contract.py).

| ID | Implemented behavior | Pinned regression |
|---|---|---|
| D01 | [`StrategyRuntime._prime_hist_bars`](../trader/strategy/strategy_runtime.py) caches successful reads only; history jobs retry off-loop and invalidate on completed downloads/config changes. | `test_history_becomes_visible_after_a_transient_priming_read_failure`; `test_history_invalidation_recovers_successfully_cached_empty_read` |
| D02 | `_strategy_frame` applies one completed-bar rule after historical/live merge. Historical rows cannot reintroduce the current forming interval. | `test_forming_history_does_not_override_completed_live_frame` |
| D03 | `_filter_session_frame` filters every input row, so a rejected off-session quote cannot survive inside a later valid window. Legitimate extended-hours rows remain admitted. | `test_out_of_session_quotes_do_not_enter_later_strategy_windows`; daily off-session quote regression |
| D04 | [`resample_ticks_to_bars`](../trader/data/market_data.py) measures the first interval from observed cumulative-volume change; [`LiveBarBuffer`](../trader/strategy/live_bars.py) establishes an initial baseline. | `test_mid_session_subscription_does_not_invent_a_million_share_minute`; batch/incremental equivalence across volume reset |
| D05 | Daily bars use venue session identities/completion rather than intraday midnight checks. Historical and live labels deduplicate to one session, including ASX sessions crossing UTC midnight. | `test_valid_daily_bar_reaches_daily_strategy`; `test_daily_history_and_live_share_one_session_identity`; `test_asx_summer_daily_session_crosses_midnight_utc_without_splitting` |
| D06 | [`session_intervals`](../trader/data/market_session.py) represents lunch breaks separately from the outer session envelope. Internal break boundaries exclude the pause; closing-auction semantics are retained. | `test_hong_kong_lunch_break_is_out_of_session`; [market-session tests](../tests/test_market_session.py) |
| D07 | One incremental bar buffer and completed-frame cache serve each conId/interval. Same-bar ticks do not resample retained history per strategy; raw health/sample ticks are bounded. | `test_shared_bar_subscription_reuses_one_resample_per_tick`; `test_same_bar_ticks_do_not_repeat_history_resampling`; saved runtime benchmark below |

### Messaging and storage — M01–M08

The original tests are in
[test_review_transport_storage.py](../tests/review/test_review_transport_storage.py).
Broader lifecycle tests use ephemeral ports in
[test_transport_lifecycle.py](../tests/test_transport_lifecycle.py).

| ID | Implemented behavior | Pinned regression |
|---|---|---|
| M01 | [`RPCClient`](../trader/messaging/clientserver.py) has a finite default; one monotonic deadline covers client-lock wait, send and receive. A post-send timeout reports UNKNOWN and replaces the socket without claiming server-side cancellation. | `test_default_rpc_deadline_also_bounds_sending`; `test_rpc_lock_wait_uses_call_deadline` |
| M02 | [`TickData.write_resolve_overlap`](../trader/data/data_access.py) submits incoming rows to an atomic keyed upsert in [`DuckDBDataStore`](../trader/data/duckdb_store.py). It no longer reads and rewrites the entire existing range outside one transaction. | `test_concurrent_history_merges_preserve_both_updates`; sequential control; history-merge benchmark |
| M03 | The shared data write boundary requires OHLCV columns and successful validation before persistence. Validator exceptions cannot fall through to writing the original frame. | `test_incomplete_bar_cannot_enter_queryable_history`, parameterized for missing high/volume; quarantine controls |
| M04 | [`OrderLifecycleTracker`](../trader/trading/order_lifecycle.py) persists execution progress and outbox identity before considering audit delivery acknowledged; [`EventStore.append_once`](../trader/data/event_store.py) makes retries idempotent. | `test_terminal_fill_is_retried_after_transient_persistence_failure`; [journal acknowledgment-crash/replay tests](../tests/test_execution_journal.py) |
| M05 | [`AutoExecState`](../trader/strategy/auto_executor.py) converts instants to UTC before storing naive database keys. Equivalent zones deduplicate; distinct DST-fold instants remain distinct. | `test_bar_dedup_matches_the_same_instant_in_different_zones`; `test_bar_dedup_distinguishes_both_occurrences_of_dst_hour` |
| M06 | [`DuckDBConnection.execute_atomic`](../trader/data/duckdb_store.py) wraps callbacks in BEGIN/COMMIT/ROLLBACK. Existing callers no longer start nested transactions. | `test_failed_atomic_callback_preserves_previous_disabled_state`; DuckDB/proposal concurrency tests |
| M07 | Publisher and message-bus startup communicate worker failures within bounded waits. Queue capacity and shutdown are explicit; RPC async close drains canceled tasks and supports object restart. | `test_publisher_bind_failure_returns_to_the_caller`; message-bus bind-failure, publisher saturation, context release and restart tests |
| M08 | Broker callbacks snapshot/enqueue data; independent workers ingest the SQLite journal and deliver the DuckDB outbox. Submission audit writes also run off-loop while preserving account-order accounting sequence. | `test_audit_store_block_does_not_block_callback_or_durable_ingestion`; `test_submission_audit_does_not_block_broker_loop_or_let_next_order_overtake`; saved storage benchmark |

## Recovery guarantees added during verification

Broker observations retain physical identities across numeric/permanent-ID
promotion and restarts. Startup first loads durable checkpoints, then merges
buffered observations; a smaller or incomplete replay cannot erase a known
fill. Partial execution remains owned even when the remainder is cancelled,
and missing final quantities retain reservations.

Exit requests and physical reduction orders have separate lifetimes. A durable
request can cover the current holding and exact already-pending openings;
subsequent residual orders keep that scope. Each physical reduction remains
bound to the holding against which it was sent. A late fill from an old stop
cannot reduce an unrelated later holding. Unknown legacy provenance stays
visible and does not authorize automated cancellation or replacement.

Protective repair reconciles fills after cancellation, reloads ownership and
broker inventory, and recomputes the replacement. Manual inventory in the same
instrument cannot conceal an oversized replacement. Disabling or removing a
strategy revokes opening authority while management of already owned inventory
continues outside its callback process.

Account loss gating uses a fresh callback from the exact account-level IB PnL
subscription. Connection replacement and loss of upstream readiness invalidate
it. A finite but incomplete per-position PnL cache cannot authorize a new open.

Backup manifests cover engine-consistent DuckDB and SQLite snapshots, including
committed WAL. Partial backups do not replace complete recovery points; restore
verifies every file and blocks opening until broker reconciliation is complete.

## Measured performance

These are saved synthetic measurements on macOS arm64, Python 3.12.13,
pandas 3.0.1, NumPy 2.4.2, ib_async 2.1.0 and DuckDB 1.4.4. They measure specific
paths, not broker latency, market capacity or a full trading session. Baseline
dispatch used nine samples per case; remediation dispatch used 25. No
statistical confidence claim follows from these small local samples.
Each evidence file records the source used for its measurement. The comparison
table and earlier samples below retain their original source bindings and
precede later repairs. A separate frozen-source native callback probe follows;
it is not a before/after comparison or a full performance rerun.

| Measurement | Review baseline | Remediation measurement |
|---|---:|---:|
| Same-bar dispatch seeded with 200,000 ticks, one strategy: median | 9.725 ms | 0.290 ms |
| Same input, four strategies sharing conId/interval: median | 34.797 ms | 0.382 ms |
| Four-strategy case: p95 | 36.392 ms | 0.811 ms |
| Retained raw tick frame after dispatch | 200,000 rows / 19.2 MB | 2,048 rows / 196,608 bytes |
| `on_trade` during another process's one-second DuckDB hold | 1.218 s | 0.160 ms median over three samples after the startup-recovery repair |
| Maximum heartbeat gap in that contention probe (10 ms requested) | 1.230 s | 12.212 ms maximum over those three samples |
| Replace ten bars in 100,000-row history: median of three | 3.155 s | 24.506 ms |
| SELECT 1, open/close connection: median of 50 | 4.189 ms | 4.477 ms |

The four-strategy same-bar path was about 91 times faster in this measurement;
the ten-bar update was about 129 times faster. Generic connection open/close
did not improve. Raw-tick memory excludes historical bars, incremental buffers,
worker processes and copies. The same-bar probe excludes strategy computation,
calendar work, broker calls, persistence and network I/O.

The [final-source native callback probe](evidence/remediation_native_lifecycle_callback_performance_2026-09-11_20260911T034713.json)
ran after the final suite and Docker checks. Across three samples with another
process holding the temporary DuckDB file, median callback time was **0.139 ms**,
median exact-Trade waiter completion was **0.234 ms**, and the maximum heartbeat
gap was **12.206 ms** for a requested 10 ms interval. All waiters completed
before the holder released the database, and every journal drained afterwards.
These are descriptive local samples, not a latency guarantee or a broker test.

The [previous frozen-source native callback probe](evidence/remediation_native_lifecycle_callback_performance_2026-09-10_20260910T161911.json)
ran three samples after the clean full suite and before paper/mutation work.
With another process holding the temporary DuckDB file, median callback time
was **0.139 ms** and the exact native Trade waiter's median completion time was
**0.207 ms**. The largest observed heartbeat gap was **12.221 ms** for a
requested 10 ms interval. All samples completed the waiter while the database
was held and drained the journal after release. Earlier measurements above
retain their original source identity.

Historical benchmark artifacts and the repeatable harnesses retain their
original source bindings. These measurements are descriptive synthetic samples,
not broker latency, throughput capacity or an execution SLA.

## Operating limits retained explicitly

1. **Submission is not execution.** RPC timeout cannot cancel a handler already
   running. Unknown intents can remain unresolved until authoritative broker
   evidence is available; no “exactly once fill” promise is made. Execution
   ingestion is asynchronous, so a crash before its journal commit relies on
   broker-history replay. Broker retention limits and unavailable history still
   require operator reconciliation.
2. **Broker coordination has a scope.** The account lock coordinates this
   trader process; it cannot serialize an independent TWS/manual client. The
   supported resize protocol needs an already matching protective tranche and
   broker-confirmed reduce-with-block OCA. An arbitrary single larger stop
   cannot be split atomically by this API and is deferred without mutation.
   Growing a position remains an opening-order proposal workflow. Actual
   venue/OCA behavior must be paper-validated.
3. **Ownership is execution-derived, not universal custody accounting.** Manual
   holdings are not assigned to a strategy merely because they share a conId.
   External trading, transfers, corporate actions, busts and corrections can
   still require reviewed ownership adjustment. A broker position and separate
   orders/executions queries are not one atomic snapshot.
   One strategy executor must own each attribution database; concurrent
   strategy-service writers are not coordinated across the separate intent and
   inventory journals. Management still reads lifetime intent history, so the
   dispatch microbenchmark does not establish long-history management capacity.
   Legacy records without immutable ownership evidence remain unresolved;
   automatic close/replacement coordination defers and retains working stops.
   Fresh automated adds are refused before proposal creation; confirmed fills
   from earlier submissions still reconcile. Direct account reductions remain available. The existing `mmr reconcile`
   command reports divergence; it does not attest or reset ownership, and this
   change introduces no automatic legacy ownership-reset workflow.
4. **Isolation is a liveness boundary.** Worker code retains the service user's
   OS permissions; it is not a security sandbox. A Linux memory limit is
   optional and unsupported platforms reject an explicit limit request. There
   is no default portable heap cap or general checkpoint/replay of arbitrary
   Python strategy state. Failed workers require explicit enable/redeployment.
   Only the declared read-only data facade and live `on_prices` API are supported.
5. **Runtime gauntlet policy remains configurable.** Hash/class matching is
   fixed. Runtime refusal still requires `MMR_GAUNTLET_ENFORCE=1`; without it,
   the existing warn-only path remains. Deployment/enable CLI gauntlet checks
   and the live double-arm are separate controls.
6. **Some data/audit failure preferences remain deliberate.** Unknown calendar
   mappings/errors remain permissive and logged; normal extended-hours data is
   included. A partial initial interval only contains observed volume, not
   reconstructed pre-subscription trades. Impossible bars never enter queryable
   history, but quarantine recording remains best-effort if its own write
   fails, preserving good rows. The bounded signal-audit stream is also
   best-effort; execution intent/outbox durability is a separate mechanism.
7. **Restore is a reconciliation event.** Individual snapshot files are
   transactionally consistent, but the DuckDB/SQLite set spans an interval and
   may precede later broker activity. Restore does not automatically clear its
   opening block. Both the snapshot interval and subsequent activity require
   reconciliation; an old backup cannot establish current broker truth alone.

8. **Protective cost needs owned execution evidence.** Stops use quoted fill
   prices for the current ownership epoch; account-average `avgCost` is not an
   owned entry price. Legacy holdings and incomplete intermediate fill-price
   evidence can leave the basis unknown. If detected before cancellation,
   management retains working protection. If a new unpriced fill arrives while
   cancellation completes, the old stop can already be terminal; replacement
   waits for adequate price evidence and logs the deferred repair. Protection
   is not uninterrupted across this handoff. Positive cost checkpoints
   written by the earlier faulty fallback still require reconciliation;
   their values alone cannot identify the error. Settled priced fills
   are not automatically reopened for broker corrections or busts.
9. **Catalogue cache invalidation has a writer scope.** Supported accessor writes
   invalidate that accessor's cache. Independent accessors and direct database
   writers require an explicit invalidation or process restart; this is not a
   distributed cache-coherence guarantee.

## Verification record

| Final check | Observed result |
|---|---|
| Full suite | **4,566 passed**, one Linux-only platform skip, 17 existing pandas warnings; 326.48 s test time |
| Isolated async suite | **21 passed**, 1.04 s test time |
| Type gate | **PASS**, zero kernel errors; 86 existing advisory diagnostics, none beyond baseline |
| Symbolic checks | **38 checked, zero not clean**; reused on the identical 14 checked modules and identified dependencies, within the original bounded budgets |
| Docker build and network-disabled smoke | **PASS**; all 387 captured source inputs and 500 context files verified unchanged |
| Native callback contention probe | **PASS**, three samples; exact-Trade waiters completed while the temporary database was held |
| Paper restart and replay | **PASS**, 13 historical physical identities and eight fills preserved; no new fills or claims; existing schema unchanged |
| Normal paper service availability | **PASS**, trader/strategy/data readiness and container/host CLI verification; CLI roster and tick-flow checks **SKIPPED**, so continuous market-data delivery is not certified |
| Post-catalogue restart | **PASS**, exact identifier lookups and explicit runtime roster checked; all configured strategies INSTALLED, zero ERROR states, auto-execution off; gateway uptime unchanged |

The verified image is `mmr-remediation:final-1fd8240d`, manifest
`sha256:ce0f3ab1e2e5de9ed68bff229252564a4be403d16e62cc0c3e1112907784f35c`.
Result-only documentation updates after the smoke check are outside that
captured image context. All final ordinary and type runs retained exact source
and their recorded environment subset. The
[final evidence](evidence/remediation_final_verification_2026-09-11.json)
contains commands, timings, hashes and scope limits.

The previous frozen source passed 4,527 ordinary tests with one platform skip,
plus 21 isolated async tests. Its full mutation result is **11,492 killed,
1,240 survived, 21 timed out and 10 without tests** across 12,763 variants.
All 18 unchanged score floors passed. The
[frozen mutation evidence](evidence/remediation_mutation_frozen_6eaadf73_2026-09-11.json)
retains every family and the survivor review. The corrected callback scope
separately completed 596 variants: 544 killed and 52 survived, with no timeout
or no-test outcomes. Four later native boundary controls passed their ordinary
baselines and caught their four exact archived faulty bodies. Those diagnostics
do not rewrite the completed scope's raw counts. The corrected executor's two
methods separately completed 143 variants: 128 killed and 15 survived, again
with no timeout or no-test outcomes. All 67 scoped survivors were reviewed:
63 have bounded or conditional explanations, and four callback gaps were
detected by the separate later controls, as recorded in the
[scoped evidence](evidence/remediation_corrected_scoped_mutation_2026-09-11.json).
It also preserves the 75-body diagnostic result (66 matching assertions,
eight other call failures and one undistinguished rounding variant), the
earlier 16 warning controls and the four later callback controls separately.
The undistinguished rounding variant has a bounded equivalence argument;
neither that argument nor an exception-origin review rewrites a test outcome.

The source fingerprint covers Python files under `trader/`, `tests/` and
`scripts/`, plus `pyproject.toml`. Captured environment records include `uv.lock`
and a defined subset of installed package versions; they are not a complete
machine attestation. Docker build and smoke evidence has its own context.

The human-owned invariant assertions and recorded type/mutation score floors
were not weakened. Three invariant fixture repairs expose the simulated fills
and zero outstanding reservations required by the new collaborators; the exact
[manifest fixture patch](evidence/manifest_fixture_repair.patch) and
[reservation fixture patch](evidence/reservation_fixture_repair.patch) preserve
the assertions. This is not a claim that invariant files are byte-identical.

One ordinary warning-message assertion was updated to match the factual
deferred-repair message. Its stop, quantity, cancellation and deduplication
assertions remain intact; this was not a change to the human-owned invariant
specification.

The full-suite command retains the repository's explicit exclusion of
`tests/test_ibrx_async.py`, whose long-lived mocked tasks are documented as flaky
in combination with the full suite. The macOS skip covers the Linux-only
address-space limit. Neither is counted as a passing test.

Mutation coverage is the configured 18-module scope, including the executor,
order lifecycle and contracted safety kernel. The standalone queue, intent,
journal, callback-worker and bar-buffer modules have ordinary/integration tests
but are not directly mutated here. Account-PnL readiness, SDK and catalogue
changes likewise rely on their ordinary regressions and applicable broker
observations. No-test variants, timeouts and survivors retain separate counts;
meeting a score floor is not proof that every possible fault is caught.

Historical paper execution exercised four rounds, eight priced fills and 13
physical identities. The final [replay evidence](evidence/remediation_replay_2026-09-09.json)
checks those preserved observations across both service generations and guarded
cleanup; no new fill or server claim was produced. The
[final cost-schema evidence](evidence/remediation_owned_cost_schema_2026-09-11.json)
records preservation; the [earlier creation record](evidence/remediation_owned_cost_schema_2026-09-10.json)
remains separate. The
eight-column bar log and already-existing empty cost-history table were
unchanged; the final replay did not create or migrate them again.

Nonempty cost recovery is covered by deterministic tests, not by the flat paper
replay. Partial fills, triggered stops and OCA reductions were not observed in
the historical paper scenarios. Tests cover those paths but do not certify
every venue topology. Normal service restoration and the current local data
inventory are recorded separately in the gitignored operational handoff.

The original review baseline (2,728 passed and 44 expected failures) remains in
[review evidence](evidence/review_validation_2026-09-08.json). Historical
measurements and interrupted mutation runs retain their original source
identities; none substitutes for the final-build checks above.

## Post-review fixes — 2026-09-11

A code review of the uncommitted remediation tree (ten verified findings)
produced the following changes. Each has a pinned regression; none touches
`tests/invariants/`.

| # | Defect | Fix | Regression |
|---|--------|-----|------------|
| 1 | The reservation migration writes a NULL-scoped claim for every leg of every pre-existing intent, and `unobserved_reduction_quantity` raised `UNKNOWN` on it before the per-contract filter, so one orphan legacy leg blocked every non-protective exit on every instrument. | A legacy identity under this trader's own client id that no observation matches after a complete open-order/execution replay is settled durably; before replay, or under another client id, it keeps deferring. | `tests/test_server_reduction_reservations.py` (three legacy cases replace the prior single one) |
| 2 | `fx_rates_to_base` matched only the bare `ExchangeRate` tag; a live ledger account delivers `$LEDGER-ExchangeRate`, so every non-base open was refused as `unevaluable:position-value`. | Both tags are read, ledger form wins, base currency is 1.0 by definition. | `tests/test_fx_rates_ledger_tags.py` |
| 3 | The server re-valued an amount-sized approved quantity at the marketable side with zero tolerance; any positive spread failed the match and moved the proposal APPROVED→FAILED. | Budget match values at the client's anchor (last first, no client limit pushing up) with a one-percent drift allowance; the approver tier is unchanged. | `tests/test_proposal_budget_match.py` |
| 4 | A position-classified exit cancelled every competing same-direction order, including another owner's protective stop, and could then refuse the exit, leaving the position naked. | Only the caller's own owner class is displaced; a shortfall against foreign orders is refused `DEFERRED` before any cancel, and DEFERRED/UNKNOWN verdicts pass through to the caller unwrapped. | `tests/test_reduction_ownership_rule.py` |
| 5 | Rows attributed by the previous build have `ownership_epoch NULL` forever, so the executor could never close, emergency-close or re-protect a position held at deploy time. | `mmr strategies adopt NAME CONID --attest` (RPC `adopt_legacy_holding`): broker-corroborated, single-shot, seeds the cost basis with the attestation recorded on the cost event. | `tests/test_legacy_holding_adoption.py` |
| 6 | Independent management defaulted a missing exit policy to one-minute bars in UTC and executed real reductions at the wrong time. | Missing interval/session fall back to the loaded strategy's context; with no strategy loaded the time exits are deferred with one error line. | `tests/test_management_policy_fallback.py` |
| 7 | `_journal_degraded` was set at seven sites and cleared nowhere; one transient SQLite lock disabled new exposure until restart. | Re-probed with a real transaction before an open is refused; cleared on any successful durable operation. Opens still fail closed at the reservation and audit writes. | `tests/test_journal_degraded_recovery.py` |
| 8 | `_advance_close` deferred on an incomplete positions snapshot but escalated an incomplete orders snapshot to the emergency path while the CLOSE intent stayed WAITING. | Both incompleteness cases defer identically. | `tests/test_advance_close_incomplete_orders.py` |
| 9 | `enable_strategy` flipped RUNNING on a worker thread before the callback worker was registered; a bar in that window set ERROR and revoked opening authority while the RPC reported success. | The dispatcher defers such bars without advancing the watermark; the worker is registered before the state flip; `runtime_status` snapshots the worker table. | `tests/test_dispatch_worker_pending.py` |
| 10 | `emergency_close_position` placed the raw position record (`exchange=''`), which IB rejects with error 321, so the last-resort exit failed on every retry. | `Trader.routable_contract` supplies the routing venue only when the record has none. | `tests/test_routable_contract.py` |

Mutation evidence for the auto-executor changes (the only touched module in
the mutation scope): a targeted pass over the four changed functions against
their new tests scored 598 killed / 59 survived; every survivor in the new code
is a documented equivalent (SQL keyword case, log text, pandas orient case,
conId-default arithmetic, falsy defaults) recorded in the ledger in
`scripts/run_mutation.sh`. The interim full pass held the module at 88.6%
against its 77.7% floor with the remaining modules all at or above theirs.

Smaller items from the same review: the strategy callback worker encodes
numpy/pandas scalars in signal metadata instead of failing permanently
(`tests/test_callback_worker_json.py`); stored proposals written by a newer
build load with unknown execution fields dropped and logged
(`tests/test_execution_spec_forward_compat.py`); `CLAUDE.md` now lists
`strategies deploy/undeploy` as requiring strategy_service acknowledgment.
The review also noted that the executor's idle `queue.Empty` branch can run a
management cycle in addition to the runtime's one-hertz pulse; that behaviour
is retained deliberately, because `tests/test_executor_idle_management_contract.py`
pins worker liveness on every idle wake independent of wall-clock scheduling,
and the extra cycle only occurs when a pulse and an idle wake fall in the same
second.
