# Low-frequency research harness (`trader/research/lowfreq`)

Evaluates a **registered** daily-bar panel strategy honestly and emits a
machine-readable `PASS / PROMISING / FAIL` verdict. It searches for nothing
itself: the families `xasset/` (C02, cross-asset ETFs, D04) and `equity/`
(C03, point-in-time equities, D05) register candidates here and read the
verdicts. Design decisions D01–D05 are in the program design record; this
file says how they are implemented.

## Run

```bash
# walk-forward protocol on the dev period declared by the family
uv run python -m trader.research.lowfreq evaluate <family>/<id> [--json] [--quiet]
# fixed-cell mode: ONE parameter cell, no selection, warm-up before --start
uv run python -m trader.research.lowfreq evaluate <family>/<id> \
    --fixed-params '{"LOOKBACK": 126}' --start 2024-07-01 --end 2025-06-30
```

Exit 0 = the evaluation completed (whatever the verdict). Exit 1 = it errored
or the id is unregistered. Exit 2 = bad arguments (never reaches the ledger).
Python API: `trader.research.lowfreq.evaluate(target, fixed_params=None,
start=None, end=None)` returns the results dict; `load_etf_panel(tickers=None,
start, end)` and `load_equity_panel(start, end, top_n=500)` are the loaders.

## Isolation (D03)

Everything reads the two DuckDB files named by `LOWFREQ_RESEARCH_CONFIG`
(default `~/.local/share/mmr/research_dev/trader_research.yaml`), opened
**read-only**. The PIT and fundamentals loaders in `scripts/` are wrapped
(`config.bound_container`, `config.read_only_duckdb_store`) rather than
copied, so there is one membership rule and one as-of rule in the codebase.
Nothing writes `backtest_runs`; no network is touched. Cash earns zero — a
book that wants a cash yield holds BIL explicitly, benchmarks included.
`LOWFREQ_RESEARCH_ROOT` relocates the family directories (tests use it).

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

Per-entry overrides of `universe`, `dev_period`, `walk_forward`,
`warmup_days`, `tickers` (ETF subset) and `top_n` are allowed. `evaluate`
refuses an id that is not in the file. The **registry-entry sha256** is the
hash of the entry's canonical JSON: reordering the YAML is not a new trial,
changing any field is. Strategy classes subclass
`trader.trading.strategy.Strategy` and implement `precompute_panel` /
`on_panel` (weights keyed by ticker; `None` = hold, `{}` = flatten). Upper-case
class attributes are the tunables the grid overrides. A strategy that defines
`set_research_data(dict)` receives the equity panel's as-of fundamentals
(`fields`: shares_diluted, revenue_ttm, …; `signals`: sales_to_price, …)
before the run; `reference_strategies.LowFreqStrategy` is a ready base.

## Protocol (D01), in execution order

1. **Lookahead probes** on the default cell before any metric exists:
   `assert_no_panel_lookahead` (hiding the last bars must not change a past
   precomputed value) plus an `on_panel` probe (weights at index *i* must not
   depend on bars after *i*). Failure raises, is ledgered as `error`, and
   writes no results.
2. **Rolling walk-forward** via `trader/simulation/walk_forward.py`: folds of
   `train_days`/`test_days`; the cell with the best train Sharpe is run on the
   test window; every benchmark is run on the identical window through
   `Backtester.run_panel` (fills at t+1 open, same costs, gross ≤ 1). With one
   cell no fit is run. OOS series are stitched across folds.
3. **Decisive benchmark**: per fold, the benchmark with the higher OOS Sharpe
   (D04: a candidate must beat BOTH). Active return = strategy − decisive,
   daily, OOS only. Gate (a) compares against the max stitched benchmark
   Sharpe.
4. **3× cost stress**: the chosen cells and the decisive benchmark re-run on
   the test windows at 3× slippage, commission and borrow (no re-selection).
5. **Full period**: the default cell, all benchmarks, and — for grids of ≥ 4
   cells — every cell, whose aligned daily returns feed `pbo_cscv` (CSCV, 16
   splits). Selection stability = fraction of consecutive folds keeping the
   same cell.
6. **Statistics**: annualised Sharpe (√252), arithmetic annualised active
   mean, Newey-West t with Bartlett bandwidth 4(n/100)^(2/9), per-calendar-year
   active sums (years with < 60 OOS days are shown with `*` and not counted),
   max drawdown of the stitched OOS equity, and DSR = PSR of the active series
   against `expected_max_sharpe(N, V)` with **N = distinct candidate hashes in
   the family ledger including this run** and V measured from their recorded
   active Sharpes.
7. **Ledger line, results JSON, then output.**

Base costs: ETFs 3 bps + $0.005/share; equities 5 bps + $0.005/share; shorts
50 bps/yr. Capital $1M; whole shares.

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
(fail closed). Fixed-cell mode has one window and one cell, so (g) is n/a and
the fold table has one row.

## Ledger (`<family>/trials.jsonl`)

One JSON line per `evaluate` call, appended **before** anything is printed,
including refused ids (`status: refused`, no hash) and errors (`status:
error`, with the message). Fields: `timestamp`, `git_sha`, `family`, `id`,
`role`, `entry_sha256`, `strategy_sha256`, `mode`, `grid`, `fixed_params`,
`start`, `end`, `metrics` (OOS Sharpe/CAGR/DD, active return, t(NW), active
Sharpe, DSR, 3×-cost active return, PBO, stability), `n_trials`, `verdict`.
The trial count for DSR is the number of distinct `entry_sha256` values with
`role: candidate`; benchmark rows never count. Never edit or delete lines.

## Results (`<family>/results/<id>.json`)

Full record: entry, window, costs, fold table (chosen cell, OOS and benchmark
metrics per fold, decisive id), stitched OOS and benchmark summaries, active
statistics with the per-year table, DSR with its trial list, PBO, stability,
full-period metrics for the report cell and every grid cell, gate inputs,
gates, verdict, the ledger row, and the OOS/decisive daily return series.
Fixed-cell runs go to `results/<id>__fixed_<start>_<end>.json`.

## Reference family (`reference/`)

Benchmark-role entries only: `etf_ew` (monthly equal weight of listed ETFs),
`etf_6040` (60 % SPY / 40 % IEF monthly), `eq_ew_covered` (monthly equal
weight of the covered PIT universe; a candidate can narrow "covered" by
publishing a boolean `coverage` frame in its `precompute_panel` state). Their
results JSON under `reference/results/` are the dev-period numbers every
family entry is measured against; each is evaluated against the other ETF
benchmark, `eq_ew_covered` against cash.

## Tests

`tests/research/` runs on a temporary DuckDB pair and a temporary family
root: loaders read only the configured files read-only; refusal, error and
benchmark rows in the ledger; the known answers (identical-to-benchmark → zero
active return and FAIL; injected drift → PASS; a t+1 reader is refused before
any metric; sub-unit weights leave cash earning zero); fixed-cell mode; CLI
exit codes.
