# September 2026 execution regressions

These tests pin the repaired behavior identified by
[the historical architecture review](../../docs/ARCHITECTURE_REVIEW_2026-09-08.md).
The original **44 expected-failure markers have been removed**; all review cases
now run as ordinary assertions alongside their passing controls and additional
recovery/race regressions. A failure is a regression, not an expected review
finding.

The [remediation report](../../docs/REMEDIATION_2026-09-08.md) maps all 39 review
IDs to implementations and tests, records performance evidence and states the
remaining operating limits. The [execution contract](../../docs/STRATEGY_EXECUTION_CONTRACT.md)
describes the supported interface. Final full-suite, mutation and paper results
belong in the remediation report, not in this historical baseline.

```bash
.venv/bin/python -m pytest tests/review/ -q --timeout=60
```

There are no `xfail` markers in this directory. At the original review's
completion, there were **6 passing controls and 44 expected failures**, and the
full suite reported 2,728 passed plus those failures. Those numbers describe the
pre-remediation commit; the original assertions remain as pinned regressions.

| File | IDs | Scope |
|---|---|---|
| `test_review_order_contract.py` | O01–O12 | Exact contract authorization, order sizing/gates, partial fills, working exits, protection/resize, subscription lifetime |
| `test_review_strategy_contract.py` | S01–S10, S12–S13 | Accepted/partial/unknown outcomes, crash recovery, ownership, stop repair, deployment/parameter/dispatch parity |
| `test_review_market_data_contract.py` | D01–D07 | Priming, completed bars, session windows, volume and repeated resampling |
| `test_review_transport_storage.py` | M01–M07 | Bounded transport/startup, transactions, concurrent updates, required schema, terminal-event durability, timestamp identity |

M08 has before/after evidence in `scripts/review_storage_benchmark.py` and
durable-ingestion tests in `tests/test_execution_journal.py`. S11 was retired
after caller review showed the normal Container path avoids the suspected
paper-mode problem; the ID is intentionally not reused.

Some cases intentionally strengthen the old behavior: O09 tests aggregate
concentration beyond the former per-order interpretation; S13 accepts explicit
rejection of a documented backtest-only API; D07 prevents repeated full-history
resampling. The user authorized implementing the review. The human-owned
invariant assertions and baselines were preserved; a narrowly scoped simulated
fill fixture repair is recorded separately in the remediation report.

Fixtures isolate broker acceptance, fills and cancellation. Passing controls
preserve ordinary true exits, explicit proposal refusal, owned-position closure,
normal quarantine, sequential merge correctness and cross-process proposal CAS.
All databases are temporary and socket tests use ephemeral loopback ports.
Subprocess probes have readiness handshakes, deadlines and termination cleanup.

The examples do not establish every broker behavior. Broader scenarios are in
`tests/test_execution_recovery.py`, `tests/test_execution_queue.py`,
`tests/test_strategy_dispatch_contract.py`, `tests/test_callback_worker.py` and
`tests/test_runtime_worker_integration.py`. Cancellation/protection tests retain
exit liveness and competing-order checks; OCA, external trading, broker replay
coverage and actual paper execution still need the explicit operating scope and
verification record in the remediation report.
