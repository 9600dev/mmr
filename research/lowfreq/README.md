# Low-frequency research harness (`trader/research/lowfreq`)

Evaluates a **registered** daily-bar panel strategy honestly and emits a
machine-readable `PASS / PROMISING / FAIL` verdict. It searches for nothing
itself: the families `xasset/` (C02, D04) and `equity/` (C03, D05) register
candidates here. This file says how decisions D01–D06 are implemented.

## Run

```bash
# walk-forward protocol on the dev period declared by the family
uv run python -m trader.research.lowfreq evaluate <family>/<id> [--json] [--quiet]
# fixed-cell mode: ONE registered grid cell, no selection, warm-up before --start
uv run python -m trader.research.lowfreq evaluate <family>/<id> \
    --fixed-params '{"LOOKBACK": 126}' --start 2024-07-01 --end 2025-06-30
```

A fixed cell must be a registered grid cell (else refused and ledgered).
Exit 0 = completed (any verdict); 1 = error or unregistered id; 2 = bad
arguments (no ledger row). Python API:
`trader.research.lowfreq.evaluate(target, fixed_params=None, start=None,
end=None)`, `load_etf_panel(tickers=None, start, end)`,
`load_equity_panel(start, end, top_n=500)`.

## Isolation (D03)

Everything reads the two DuckDB files named by `LOWFREQ_RESEARCH_CONFIG`
(default `~/.local/share/mmr/research_dev/trader_research.yaml`), opened
**read-only**; the PIT and fundamentals loaders in `scripts/` are wrapped
(`config.bound_container`, `config.read_only_duckdb_store`). Nothing writes
`backtest_runs`; no network. Cash earns zero — a cash yield means holding
BIL explicitly, benchmarks included. `LOWFREQ_RESEARCH_ROOT` relocates the
family directories.

## Registry (`<family>/candidates.yaml`, D02)

```yaml
family: xasset
universe: etf                 # etf | equity  -> loader and base costs
dev_period: {start: 2007-07-01, end: 2024-06-28}
walk_forward: {train_days: 1826, test_days: 365}   # calendar days, rolling
warmup_days: 400              # history loaded before every window
candidates:
  - id: tsmom_12m             # ^[a-z0-9_]+$
    role: candidate           # candidate | benchmark
    hypothesis: one sentence, written before the run
    rationale: why the market should pay for this
    strategy: {module: strategies.lowfreq_xasset_tsmom, class: TsMom}   # or a .py path
    params: {LOOKBACK: [126, 252], TOP_N: [3, 5]}     # grid; {} = no grid
    default_params: {LOOKBACK: 252, TOP_N: 5}         # the pre-registered cell
    benchmarks: [etf_ew, etf_6040]                    # benchmark-role ids
    registered_at: 2026-09-28
```

Entries may override `universe`, `dev_period`, `walk_forward`, `warmup_days`,
`tickers`, `top_n`.

**Equity universe rule (D05).** `load_equity_panel` reads 45 days before
the requested start so the first month has members and keeps a name's prices
after it leaves the top-N membership and exposes membership as
`fields['member']`. Weight > 0 only for a member; a BUY fill decided outside
membership is refused (`universe violation`, ledgered as an error).
**Stopped names (D07):** no price is ever fabricated; a holding whose bar is
its last stored print is sold by the executor at that close
(`panel_exit_at_last_print`); count and value are reported under
`stopped_names`. Without both rules every leaver was unsellable (511 frozen
holdings, cash −$3.2M in covered-EW; pre-D06). `pit_daily_bars` starts
2016-08-01 and membership needs the prior month's ranks, so a window from
2016-08-01 begins with September's rebalance. `evaluate`
refuses an id that is not in the file. The **registry-entry
sha256** hashes the entry's canonical JSON: reordering the YAML is not a new
trial, changing any field is. Strategy classes subclass
`trader.trading.strategy.Strategy` and implement `precompute_panel` /
`on_panel` (weights keyed by ticker; `None` = hold, `{}` = flatten); upper-case
class attributes are the tunables. A strategy defining
`set_research_data(dict)` receives the equity panel's as-of fundamentals
(`fields`, `signals`) before the run; `reference_strategies.LowFreqStrategy` is a ready base whose
`due(index, panel)` is True on the first NYSE session of each calendar month
(`REBALANCE = 'week'` for weeks), judged against the exchange calendar's
previous session, so a warm-up origin or a holiday never shifts the
schedule.

## Protocol (D01), in execution order

1. **Lookahead probes** on **every executable cell**, and on every
   benchmark **under each coverage frame a cell may hand it**, before any
   metric exists, with no bypass (short panels included):
   `assert_no_panel_lookahead`; a reference run replaying the executor's exact
   sequence (one state, `on_panel` in order — honest stateful strategies are
   fine); and, at **sampled prefixes** (first/last three indices plus three
   runs of 23 consecutive indices, each longer than a month of sessions), a
   FRESH instance on bars ≤ *i*: its precomputed values over those bars and
   every instruction replayed from 0 to *i* must equal the reference run's
   under exact key semantics (`None` ≠ `{}`; an omitted key ≠ explicit 0).
   Failure raises, is ledgered as `error`, writes no results.
2. **Rolling walk-forward** via `trader/simulation/walk_forward.py`: folds of
   `train_days`/`test_days`; the cell with the best train Sharpe runs on the
   test window; every benchmark runs on the identical window through
   `Backtester.run_panel` (fills at t+1 open, same costs, gross ≤ 1), owning
   the coverage the **chosen cell published on that window** (derived from
   the window's own bars; a candidate BUY outside its published coverage is a
   `coverage violation`). With one cell no fit is run. A fold in
   which no cell scores, or whose window fails, errors the evaluation —
   folds are never skipped. OOS series are stitched across folds.
3. **Decisive benchmark**: per fold, the benchmark with the higher OOS
   Sharpe (D04: beat BOTH). Active return = strategy − decisive, daily, OOS.
   Gate (a) uses the max stitched benchmark Sharpe.
4. **3× cost stress**: chosen cells and the decisive benchmark re-run at 3×
   slippage, commission and borrow (no re-selection).
5. **Full period**: the default cell, all benchmarks, and — for grids of ≥ 4
   cells — every cell, feeding `pbo_cscv` (CSCV, 16 splits). Stability =
   fraction of consecutive folds keeping the same cell.
6. **Statistics**: annualised Sharpe (√252), annualised active mean,
   Newey-West t (Bartlett bandwidth 4(n/100)^(2/9)), per-year active sums
   (years with < 60 OOS days shown with `*`, not counted), max drawdown of
   the stitched OOS equity, and DSR = PSR of the active series against
   `expected_max_sharpe(N, V)`, **N = distinct candidate hashes in the family
   ledger including this run**, V from their recorded active Sharpes (N > 1
   with fewer than two Sharpes gives no DSR, failing gate (c)).
7. **Results JSON, then ledger line, then output.**

Base costs: ETFs 3 bps + $0.005/share; equities 5 bps + $0.005/share; shorts
50 bps/yr. Capital $1M, whole shares. Capacity (D05): turnover/yr,
names held, median 21-day $ADV.
**Cash is never overdrawn (D06).** In `run_panel` a rebalance's sells settle
first, then its buys are scaled pro rata to the cash available, in whole
shares, so cash after every fill is ≥ 0. Every window reports
`scaled_buy_fills` / `scaled_rebalances`, `min_cash_fraction` (rebuilt from
the fill log; ≥ 0 long-only), `forced_exits` / `forced_exit_value_fraction`
(D07) and `frozen_value_fraction_end` (no print in the last 21 bars). Read
many forced exits or scaled fills as unresolved, not return. Any
`run_panel` result written before 2026-09-29 predates the guard: pre-D06.

### Gates

| gate | PASS | PROMISING |
|---|---|---|
| (a) OOS Sharpe ≥ every benchmark's | required | required |
| (b) active annualised return > 0, t(NW) | ≥ 2.0 | ≥ 1.0 |
| (c) DSR of active return, deflated by family trial count | ≥ 0.90 | — |
| (d) OOS max drawdown within 5 pp of the decisive benchmark's | required | required |
| (e) active return > 0 in ≥ 60 % of counted OOS years | required | — |
| (f) active return > 0 at 3× base cost | required | required |
| (g) grid ≥ 4 cells: PBO < 0.35 and stability ≥ 0.5 | required | — |

Anything else is FAIL; an uncomputable gate input fails (fail closed).
Fixed-cell mode has one window and one cell, so (g) is n/a.

## Ledger (`<family>/trials.jsonl`)

One JSON line per `evaluate` call, appended **before** anything is printed:
a missing family or unregistered id (`status: refused`; a target without a
`/` goes to the root-level `research/lowfreq/trials.jsonl`), any failure
including a results-write failure (`error`), or `ok` (appended only after
the results file exists). A target that is not two `^[a-z0-9_]+$` segments
is refused at the root ledger before any family file is touched.
`harness_version` 2 rows share one key set (`ledger.LEDGER_FIELDS`, None
when not applicable; the first nineteen `reference/` rows are version 1,
lacking `error` and `results_path`, and stay): `timestamp`, `git_sha`,
`harness_version`, `family`, `id`, `role`, `entry_sha256`, `strategy_sha256`,
`mode`, `grid`, `fixed_params`, `start`, `end`, `n_trials`, `metrics`
(fixed `METRIC_FIELDS`), `verdict`, `status`, `error`, `elapsed_s`,
`results_path`. The DSR trial count is the number of distinct `entry_sha256`
values with `role: candidate`; benchmark rows never count. Never edit or
delete lines.

## Results (`<family>/results/<id>.json`)

Entry, window, costs, lookahead report, fold table, OOS and benchmark
summaries, active statistics per year, DSR with its trial list, PBO,
stability, full-period metrics per cell, `stopped_names`, gates, verdict,
ledger row, OOS/decisive daily series. Fixed-cell runs go to
`results/<id>__fixed_<start>_<end>.json`.

## Reference family (`reference/`)

Benchmark-role entries only: `etf_ew` (monthly equal weight of listed ETFs),
`etf_6040` (60 % SPY / 40 % IEF monthly), `eq_ew_covered` (monthly equal
weight of the covered PIT universe). A candidate narrows "covered" with a
boolean `coverage` frame (time × ticker) in its `precompute_panel` state,
forwarded per window to every benchmark defining `set_coverage`.
`reference/results/` holds their post-D06/D07 dev-period numbers (each ETF
benchmark vs the other, `eq_ew_covered` vs cash).

## Tests

`tests/research/` runs on a temporary DuckDB pair and family root: loader
isolation, as-of fundamentals, ledger schema on every path, known answers
(identical → FAIL; drift → PASS; t+1 readers refused; sub-unit weights leave
cash at zero), stateful strategies accepted, per-window coverage, D07 exits,
safe paths, fixed-cell mode, CLI exit codes. D06 is pinned in
`tests/test_panel_cash.py`.
