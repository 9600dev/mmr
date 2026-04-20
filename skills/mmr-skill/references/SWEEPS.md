# Sweep Manifests — Cron-able Backtest Runs

`mmr sweep run <manifest.yaml>` expands a declarative YAML into a
cartesian product of backtests, runs them in parallel, persists every
run under a single parent ``sweeps`` row, and drops a markdown digest
in `~/.local/share/mmr/reports/`.

One manifest = one or more sweeps. One sweep = one strategy class × a
symbol set × a param grid. Everything gets executed + persisted.

## The YAML shape

```yaml
# nightly.yaml — commit this alongside your strategies
sweeps:
  - name: orb_cross_sectional          # required, free text
    strategy: /abs/path/to/strategy.py # required, absolute path
    class: OpeningRangeBreakout        # required, Python class name

    # Exactly ONE of symbols / conids / universe
    symbols: [SPY, QQQ, NVDA, GOOGL]   # ticker strings (resolved via local universe DB)
    # conids: [756733, 320227]         # OR raw IB contract ids
    # universe: portfolio              # OR a named universe

    # Optional param grid — cartesian product expanded per symbol
    param_grid:
      RANGE_MINUTES: [15, 30, 45]
      VOLUME_MULT: [1.2, 1.3, 1.5]
      # 4 symbols × 3 × 3 = 36 jobs for this sweep

    days: 365                          # default 365
    bar_size: "1 min"                  # default "1 min"
    concurrency: 8                     # default: auto = cpu_count - 1, cap 16
    note: "nightly 2026-04-19"         # shows up on every child run

  - name: keltner_param_tune
    strategy: /abs/path/to/keltner_breakout.py
    class: KeltnerBreakout
    symbols: [SPY, QQQ]
    param_grid:
      EMA_PERIOD: [10, 20, 30]
      BAND_MULT: [1.5, 2.0, 2.5]
    days: 180
```

Omitting ``param_grid`` runs one backtest per symbol with strategy
defaults (still useful for cross-sectional robustness checks).

## Running

```bash
# Inspect before committing the compute
mmr sweep run nightly.yaml --dry-run
# → "total jobs: 1314, est CPU-time: 1970 min, est wall time: 197 min
#   • orb_cross_sectional  324 jobs  concurrency=8"

# Execute
mmr sweep run nightly.yaml

# Bypass the stale-data guard (default refuses if any conid lacks a
# bar from the last 3 trading days — catches "IB Gateway was down")
mmr sweep run nightly.yaml --skip-freshness

# Override every sweep's concurrency from the command line
mmr sweep run nightly.yaml --concurrency 12
```

### From Python / llmvm

```python
result = await MMRHelpers.sweep_run(
    "/Users/you/mmr-sweeps/nightly.yaml",
    dry_run=False,
    concurrency=8,
)
```

### Detached overnight launch

`sweep run` blocks until complete. For truly unattended overnight
execution:

```python
import subprocess
subprocess.Popen(
    ["mmr", "sweep", "run", "/Users/you/mmr-sweeps/nightly.yaml"],
    start_new_session=True,
    stdout=open("/tmp/sweep.stdout.log", "w"),
    stderr=open("/tmp/sweep.stderr.log", "w"),
)
# Your conversation / llmvm loop keeps going; the sweep runs on its own.
```

## Progress + review

```bash
# Live progress while the sweep is running — single snapshot
mmr sweep show <id>
# → "Sweep #17: orb_cross_sectional   running  42/324 (13%, ETA 78m)"
#   elapsed: 12.1m
#   digest: (empty until finalized)

# Continuous refresh — polls every 5s, re-renders until terminal state
# or Ctrl-C. Shows header, top-N leaderboard, AND most-recent completions
# so a stalled sweep is visible (no new rows = nothing happening).
mmr sweep watch <id>
mmr sweep watch <id> --interval 2 --top 20

mmr sweep list                     # every sweep ever run, newest first
mmr sweep show <id>                # leaderboard of one sweep by composite score
mmr sweep show <id> --top 20       # more of the leaderboard

# Bulk statistical confidence across top-N runs from a sweep
mmr backtests list --sweep <id> --limit 0    # all runs, ranked by quality
mmr backtests confidence <run1> <run2> ...   # PSR / CI / skew bulk
```

### The morning digest

When a sweep finishes, a markdown file lands at
`~/.local/share/mmr/reports/sweep_<id>_<name>_<ts>.md`:

- **Header**: status, duration, N/M ok
- **Strong candidates**: top runs that cleared both quality and
  statistical-confidence thresholds
- **All successful runs (ranked)**: full leaderboard by composite score
- **Failures**: each failure with a `<details>` block containing the
  full traceback (last 3KB of stderr, enough for nested Python
  exceptions)

Read it with any markdown viewer or `cat`. The LLM can read it via
`read_file` for post-morning-review triage.

## CPU / wall time math

Budget ~90 seconds per 1-min × 365-day backtest on a modern laptop
(varies per strategy — precompute/vectorbt is fast; Python-loop
indicators can blow past 180 seconds).

| jobs | concurrency | wall time |
|------|-------------|-----------|
| 30   | 8           | ~6 min    |
| 100  | 8           | ~19 min   |
| 300  | 8           | ~56 min   |
| 1000 | 8           | ~3 hours  |
| 1000 | 16          | ~1.6 hours |

Dry-run prints an estimate before you commit. On a 32-core workstation
you can push `concurrency: 24+` and halve these numbers.

## Gotchas

- **Path must be absolute.** CLI subprocesses don't inherit cwd; a
  relative ``strategies/foo.py`` resolves relative to the container's
  root and fails.
- **Strategy must use the precompute API for anything on 1-min data
  longer than ~90 days.** `on_prices`-only strategies are O(N²) per
  backtest; a 1-year × 1-min sweep will time out. Check dispatch mode
  with `mmr strategies inspect` first.
- **Param grid values must be JSON-serializable literals.** No Python
  expressions. `EMA_PERIOD: [10, 20, 30]` ✓ ; `EMA_PERIOD: list(range(10, 30, 5))` ✗.
- **One sweep is one strategy class.** For multi-strategy nightly runs,
  list several `sweeps:` entries in the same manifest — they execute
  sequentially. For heterogeneous one-offs (different days, different
  universes) use `MMRHelpers.backtest_batch` instead.
- **Freshness guard is per-conid.** If any conid in any sweep of the
  manifest lacks a recent daily bar, the whole manifest refuses
  unless `--skip-freshness` is passed.

## Related helpers

- `MMRHelpers.strategies_inspect()` → returns dispatch mode + tunables
  for every strategy file. Run this **before** writing the manifest to
  pick sweep candidates.
- `MMRHelpers.backtest_sweep(path, class, param_grid=..., conids=...)` →
  single-strategy sweep without the YAML overhead. Use for quick
  interactive param tuning on one strategy.
- `MMRHelpers.backtest_batch(jobs=[...], concurrency=N)` → heterogeneous
  parallel fanout (different strategies, days, conids per job).
  For when the cartesian-grid shape doesn't fit.
- `MMRHelpers.backtests_confidence([ids])` → compact bulk PSR / t-stat /
  bootstrap CI / skew / streak across N run ids. Run post-sweep to
  filter candidates statistically.
