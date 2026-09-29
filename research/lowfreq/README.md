# Low-frequency research harness (`trader/research/lowfreq`)

Evaluates a **registered** daily-bar panel strategy honestly and emits a
machine-readable `PASS / PROMISING / FAIL` verdict. It searches for nothing
itself: the families `xasset/` (C02, D04) and `equity/` (C03, D05) register
candidates here and read the verdicts. This file says how design decisions
D01–D06 are implemented.

## Run

```bash
# walk-forward protocol on the dev period declared by the family
uv run python -m trader.research.lowfreq evaluate <family>/<id> [--json] [--quiet]
# fixed-cell mode: ONE registered grid cell, no selection, warm-up before --start
uv run python -m trader.research.lowfreq evaluate <family>/<id> \
    --fixed-params '{"LOOKBACK": 126}' --start 2024-07-01 --end 2025-06-30
```

A fixed cell must be a registered grid cell (otherwise refused and ledgered
as an error); `grid.cells` is then 1, `grid.registered_cells` the grid size.

Exit 0 = completed (whatever the verdict); 1 = error or unregistered id;
2 = bad arguments (never reaches the ledger). Python API:
`trader.research.lowfreq.evaluate(target, fixed_params=None, start=None,
end=None)`; loaders `load_etf_panel(tickers=None, start, end)` and
`load_equity_panel(start, end, top_n=500)`.

## Isolation (D03)

Everything reads the two DuckDB files named by `LOWFREQ_RESEARCH_CONFIG`
(default `~/.local/share/mmr/research_dev/trader_research.yaml`), opened
**read-only**; the PIT and fundamentals loaders in `scripts/` are wrapped
(`config.bound_container`, `config.read_only_duckdb_store`), not copied.
Nothing writes `backtest_runs`; no network is touched. Cash earns zero — a
book that wants a cash yield holds BIL explicitly, benchmarks included.
`LOWFREQ_RESEARCH_ROOT` relocates the family directories.

## Registry (`<family>/candidates.yaml`, D02)

```yaml
family: xasset
universe: etf                 # etf | equity  -> loader and base costs
dev_period: {start: 2007-07-01, end: 2024-06-28}
walk_forward: {train_days: 1826, test_days: 365}   # calendar days, rolling
warmup_days: 400              # history loaded before every window
candidates:
  - id: tsmom_12m
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

**Equity universe rule (D05).** `load_equity_panel` keeps a name's prices
after it leaves the top-N membership (as long as `pit_daily_bars` stores
them) and exposes membership as `fields['member']`. Weight > 0 is allowed
only for a member; a BUY fill decided outside membership is refused
(`universe violation`, ledgered as an error). A name that stops printing
keeps its last close for 30 sessions as mark and fill price, so the exit at
the next rebalance fills. Without both rules every leaver was unsellable (511
frozen holdings, cash −$3.2M in the covered-EW benchmark; pre-D06 figures).
`evaluate` refuses an id that is not in the file. The **registry-entry
sha256** hashes the entry's canonical JSON: reordering the YAML is not a new
trial, changing any field is. Strategy classes subclass
`trader.trading.strategy.Strategy` and implement `precompute_panel` /
`on_panel` (weights keyed by ticker; `None` = hold, `{}` = flatten). Upper-case
class attributes are the tunables the grid overrides. A strategy that defines
`set_research_data(dict)` receives the equity panel's as-of fundamentals
(`fields`: shares_diluted, revenue_ttm, …; `signals`: sales_to_price, …)
before the run; `reference_strategies.LowFreqStrategy` is a ready base whose
`due(index, panel)` is True on the first trading day of each calendar month
(`REBALANCE = 'week'` for weeks) — calendar-based, so a warm-up origin never
shifts the schedule.

## Protocol (D01), in execution order

1. **Lookahead probes** on **every executable cell and every benchmark**
   before any metric exists, with no bypass: `assert_no_panel_lookahead`
   (hiding the last bars must not change a past precomputed value) plus
   `on_panel` probes at 8 indices (weights at *i* must not depend on bars
   after *i*). Failure raises, is ledgered as `error`, writes no results.
2. **Rolling walk-forward** via `trader/simulation/walk_forward.py`: folds of
   `train_days`/`test_days`; the cell with the best train Sharpe is run on the
   test window; every benchmark is run on the identical window through
   `Backtester.run_panel` (fills at t+1 open, same costs, gross ≤ 1), owning
   the **chosen cell's** coverage. With one cell no fit is run. A fold in
   which no cell scores, or whose window fails, errors the whole evaluation —
   folds are never skipped. OOS series are stitched across folds.
3. **Decisive benchmark**: per fold, the benchmark with the higher OOS Sharpe
   (D04: a candidate must beat BOTH). Active return = strategy − decisive,
   daily, OOS only. Gate (a) compares against the max stitched benchmark
   Sharpe.
4. **3× cost stress**: chosen cells and the decisive benchmark re-run on the
   test windows at 3× slippage, commission and borrow (no re-selection).
5. **Full period**: the default cell, all benchmarks, and — for grids of ≥ 4
   cells — every cell, whose daily returns feed `pbo_cscv` (CSCV, 16 splits).
   Selection stability = fraction of consecutive folds keeping the same cell.
6. **Statistics**: annualised Sharpe (√252), arithmetic annualised active
   mean, Newey-West t (Bartlett bandwidth 4(n/100)^(2/9)), per-calendar-year
   active sums (years with < 60 OOS days shown with `*`, not counted), max
   drawdown of the stitched OOS equity, and DSR = PSR of the active series
   against `expected_max_sharpe(N, V)`, **N = distinct candidate hashes in
   the family ledger including this run**, V from their recorded active
   Sharpes; with N > 1 and fewer than two recorded Sharpes there is no DSR and
   gate (c) fails.
7. **Results JSON, then ledger line, then output.**

Base costs: ETFs 3 bps + $0.005/share; equities 5 bps + $0.005/share; shorts
50 bps/yr. Capital $1M; whole shares. Capacity (D05) per window: turnover/yr,
median names held, median 21-day $ADV of held names.
**Cash is never overdrawn (D06).** In `run_panel` a rebalance's sells are
executed and logged first, then its buys are scaled pro rata to the cash then
available, in whole shares, so cash after every fill is ≥ 0 and a book sized
off a holding it could not sell cannot lever itself.
Every window reports `scaled_buy_fills` / `scaled_rebalances` (how often the
guard acted), `min_cash_fraction` (rebuilt from the fill log; ≥ 0 for a
long-only book) and `frozen_value_fraction_end` (holdings with no print in
the last 21 bars, marked at a stale carry-forward, over equity). Read a large
frozen fraction or many scaled fills as unresolved, not as return. Results
written before 2026-09-29 predate the guard and are labelled pre-D06 wherever
quoted.

### Gates

| gate | PASS | PROMISING |
|---|---|---|
| (a) OOS Sharpe ≥ every benchmark's OOS Sharpe | required | required |
| (b) active annualised return > 0, t(NW) | ≥ 2.0 | ≥ 1.0 |
| (c) DSR of active return, deflated by family trial count | ≥ 0.90 | — |
| (d) OOS max drawdown no worse than decisive benchmark's by > 5 pp | required | required |
| (e) active return positive in ≥ 60 % of counted OOS calendar years | required | — |
| (f) active return > 0 at 3× base cost | required | required |
| (g) grid ≥ 4 cells: PBO < 0.35 and stability ≥ 0.5 | required | — |

Anything else is FAIL. A gate whose input could not be computed fails
(fail closed). Fixed-cell mode has one window and one cell, so (g) is n/a.

## Ledger (`<family>/trials.jsonl`)

One JSON line per `evaluate` call, appended **before** anything is printed:
a missing family or unregistered id (`status: refused`), any failure
including a results-write failure (`error`, with the message), or `ok`
(appended only after the results file exists). Every row has the same keys
(`ledger.LEDGER_FIELDS`, None when not applicable): `timestamp`, `git_sha`,
`harness_version`, `family`, `id`, `role`, `entry_sha256`, `strategy_sha256`,
`mode`, `grid`, `fixed_params`, `start`, `end`, `n_trials`, `metrics`
(fixed `METRIC_FIELDS`: OOS Sharpe/CAGR/DD, active return, t(NW), active
Sharpe, DSR, 3×-cost active return, PBO, stability, full-period Sharpe, min
cash, scaled fills), `verdict`, `status`, `error`, `elapsed_s`,
`results_path`. The trial count for DSR is the number of distinct
`entry_sha256` values with `role: candidate`; benchmark rows never count.
Never edit or delete lines.

## Results (`<family>/results/<id>.json`)

Full record: entry, window, costs, lookahead report, fold table (chosen
cell, coverage forwarded, OOS and benchmark metrics, decisive id), stitched
OOS and benchmark summaries, active statistics with the per-year table, DSR
with its trial list, PBO, stability, full-period metrics for the report cell
and every grid cell, gates, verdict, the ledger row, and the OOS/decisive
daily return series. Fixed-cell runs go to
`results/<id>__fixed_<start>_<end>.json`.

## Reference family (`reference/`)

Benchmark-role entries only: `etf_ew` (monthly equal weight of listed ETFs),
`etf_6040` (60 % SPY / 40 % IEF monthly), `eq_ew_covered` (monthly equal
weight of the covered PIT universe). A candidate narrows "covered" by
returning a boolean `coverage` frame (time × ticker) from `precompute_panel`;
the harness forwards the **chosen cell's** frame in each window to every
benchmark defining `set_coverage`. Their results JSON under
`reference/results/` are the post-D06 dev-period numbers; each ETF benchmark
is evaluated against the other, `eq_ew_covered` against cash.

## Tests

`tests/research/` runs on a temporary DuckDB pair and family root: loader
isolation, DST-aware as-of fundamentals, ledger schema on every path, the
known answers (identical-to-benchmark → FAIL; injected drift → PASS; t+1
readers in any cell or benchmark refused; sub-unit weights leave cash earning
zero), per-cell coverage, calendar schedule, fixed-cell mode, CLI exit codes.
The D06 cash guard is pinned in `tests/test_panel_cash.py`.
