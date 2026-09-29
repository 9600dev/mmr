# Low-frequency live path: monthly runbook (C02)

The sales-to-price sleeve (`equity/s2p_top_quintile`) is paper-traded through
MMR's `propose -> approve` pipeline with weights computed by **exactly the
research code path** (`trader.research.lowfreq`: same loader, same strategy
class, same `precompute_panel -> on_panel -> normalise_weights` sequence
`Backtester.run_panel` runs). The paper ledger values the sleeve and its
virtual decisive benchmark (`eq_ew_covered`, D05) from the same stored bars,
and the benchmark is executed by `Backtester.run_panel` itself, so the sleeve
accumulates honest out-of-sample evidence at zero capital risk.

Every command below is complete and runs from the host. `EXEC` carries the
persistent environment: the harness reads only the databases named by
`LOWFREQ_RESEARCH_CONFIG` (D03), which in the container is the live config
(both DuckDB paths), opened read-only.

```
EXEC="docker exec -u trader -w /home/trader/mmr \
  -e LOWFREQ_RESEARCH_CONFIG=/home/trader/.config/mmr/trader.yaml \
  -e SEC_API_KEY=<your sec-api.io key> mmr-mmr-1"
```

## Once a month: the FIRST NYSE session of the month, after the close

The strategy rebalances on the first session of each calendar month
(`CoveredTilt.REBALANCE = 'month'`). Its decision uses that session's close and
the backtest fills at the NEXT session's open, so the cycle is: refresh after
the close, generate targets, propose, review, approve before the next open.
`D` below is the decision session, e.g. `2026-10-01`.

### 1. Refresh data through the decision session (incremental, append-only)

```
$EXEC python3 scripts/build_pit_universe.py --incremental --end D
$EXEC python3 scripts/fetch_fundamentals.py --incremental
```

`--end D` is required: without it the refresh stops at yesterday and `targets`
refuses with STALE BARS. `build_pit_universe.py --incremental` resumes from
the day after the last stored session; stored sessions are never re-fetched or
rewritten (a session whose bars are not yet published comes back empty and is
fetched on the next run). `fetch_fundamentals.py --incremental` asks sec-api,
for every current point-in-time top-500 member, only for filings accepted
after the newest acceptance instant already stored for that name (newest per
period wins, decided by the instant), and appends them with `INSERT OR IGNORE`.
Both are idempotent: a second run fetches and writes nothing.

### 2. Generate targets (the D08 file)

```
$EXEC python3 -m trader.lowfreq_live targets equity/s2p_top_quintile \
    --sleeve paper_s2p --capital 25000 --as-of D
```

Writes `~/.local/share/mmr/data/targets/paper_s2p/D.json` (`mmr.targets.v1`,
carrying the strategy source sha256) and, published first as an atomic pair,
`D.benchmark.json` with the decisive benchmark's weights on the identical panel
and coverage. It **refuses** (exit 2, nothing written) when:

- the latest stored session is not `--as-of` (STALE BARS: repeat step 1 with `--end D`);
- the newest stored filing acceptance is more than 7 days before `--as-of`
  (STALE FUNDAMENTALS; `--max-fundamentals-lag-days` adjusts);
- `--as-of` is not a rebalance session for the strategy (it holds: nothing to trade);
- `--params` names a cell outside the registered grid (D02);
- the result would violate D08 (weights outside (0, 1], sum above 1.0);
- the file exists (`--overwrite` replaces a decision record deliberately).

### 3. Plan, propose, review, approve (rebalancer, C01)

```
$EXEC python3 -m trader.rebalance plan    --sleeve paper_s2p    # dry run: orders, cash after, refusals
$EXEC python3 -m trader.rebalance propose --sleeve paper_s2p    # PENDING proposals in group sleeve_paper_s2p
$EXEC mmr proposals                                             # review sizing and risk
$EXEC mmr approve <id>                                          # one by one, SELLs first
```

The rebalancer only creates PENDING proposals (D12); approval is the operator's
act. See `docs/REBALANCE.md`.

### 4. Reconcile the fills into the journal (C01)

```
$EXEC python3 -m trader.rebalance reconcile --sleeve paper_s2p
$EXEC python3 -m trader.lowfreq_live fills --sleeve paper_s2p
```

`sleeve_fills` (D13) is the sleeve's book of record: one row per IB execution
of the sleeve's EXECUTED proposals. The ledger reads it and never writes it;
submission is not a fill (broker acceptance is not inventory, `CLAUDE.md`).

## Every session, after the close

```
$EXEC python3 scripts/build_pit_universe.py --incremental --end <today>
$EXEC python3 -m trader.lowfreq_live track  --sleeve paper_s2p
$EXEC python3 -m trader.lowfreq_live report --sleeve paper_s2p
```

`track` first reads IB positions through `trader_service` and **refuses**
when the journaled holdings exceed what IB shows, or when IB cannot be read
(D13, fail closed; `--no-reconcile` records anyway and says so). It then
appends one row per stored session to `lowfreq_paper_ledger` (in the trading
DuckDB named by `trader.yaml`): sleeve holdings x closes, cash, sleeve value,
and the virtual benchmark's value. Both sides are replayed from inception on
every run from durable inputs only (`sleeve_fills`, the target files and
sidecars, `pit_daily_bars`); stored rows are never rewritten, and a replay that
differs from a stored row by more than 1 bp of capital is printed as a
DISAGREEMENT (exit 2) for the operator to investigate, e.g. a fill reconciled
late.

Benchmark rules: `Backtester.run_panel` executes the sidecar weights with the
sleeve's capital and the equity base costs (D01) - whole shares truncated off
the decision close, fills at the next session's open, buys scaled pro rata to
cash and floored (D06), a name silent for 5 sessions exits at its last print
(D07), missing prints carried at the last close and counted. Cash earns zero
(D03). A held name absent from `pit_daily_bars` on a session (below the
top-800 storage cutoff that day) is carried at its last close and counted in
`n_unpriced` / `benchmark_n_unpriced`.

`report` prints since-inception sleeve and benchmark return, active return,
its annualised mean, and tracking error (annualised standard deviation of the
daily active return). `--json` on any command gives machine-readable output.

## Where things live

| What | Where |
|---|---|
| Point-in-time bars | `pit_daily_bars` in `history_duckdb_path` |
| As-of fundamentals | `fundamentals` in `duckdb_path` |
| Target files + benchmark sidecars | `~/.local/share/mmr/data/targets/<sleeve>/` |
| Fills journal (C01 writes, D13) | `sleeve_fills` in `duckdb_path` |
| Paper ledger | `lowfreq_paper_ledger` in `duckdb_path` |

## Tests

`tests/test_lowfreq_live_incremental.py` (fake Massive / sec-api / Polygon
clients on a temporary DuckDB, including same-period multi-filing reruns),
`tests/test_lowfreq_live_targets.py` (D08 validity, refusals, parity with
`run_panel` on a fake research DB), `tests/test_lowfreq_live_parity.py`
(parity on the research dev DB for 2023-10-02, 2023-11-01, 2023-12-01 against
the instructions `run_panel` consumed and the book it then held),
`tests/test_lowfreq_live_ledger.py` (harness-derived known answers: whole
shares, D06 floor, an opening gap, a stopped name, config parity with
`engine._config`, reconciliation refusals, append-only, CLI round trip).
