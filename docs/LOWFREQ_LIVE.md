# Low-frequency live path: monthly runbook (C02)

The sales-to-price sleeve (`equity/s2p_top_quintile`) is paper-traded through
MMR's `propose -> approve` pipeline with weights computed by **exactly the
research code path** (`trader.research.lowfreq`: same loader, same strategy
class, same `precompute_panel -> on_panel -> normalise_weights` sequence
`Backtester.run_panel` runs). The paper ledger values the sleeve and its
virtual decisive benchmark (`eq_ew_covered`, D05) from the same stored bars, so
the sleeve accumulates honest out-of-sample evidence at zero capital risk.

Every command below runs inside the container:

```
docker exec -u trader -w /home/trader/mmr mmr-mmr-1 <command>
```

The harness reads ONLY the databases named by `LOWFREQ_RESEARCH_CONFIG` (D03),
read-only. In the container point it at the live config so the refresh and the
signal see the same tables:

```
export LOWFREQ_RESEARCH_CONFIG=/home/trader/.config/mmr/trader.yaml
```

## Once a month, on the FIRST NYSE session of the month, after the close

The strategy rebalances on the first session of each calendar month
(`CoveredTilt.REBALANCE = 'month'`). Its decision uses that session's close,
and the backtest fills at the NEXT session's open — so the monthly cycle is:
refresh after the close, generate targets, propose, review, approve before the
next open.

### 1. Refresh data (incremental, append-only)

```
python3 scripts/build_pit_universe.py --incremental
SEC_API_KEY=... python3 scripts/fetch_fundamentals.py --incremental
```

`build_pit_universe.py --incremental` resumes from the day after the last
stored session in `pit_daily_bars`; stored sessions are never re-fetched or
rewritten. `fetch_fundamentals.py --incremental` asks sec-api, for every
current point-in-time top-500 member, only for filings accepted after the
latest acceptance instant already stored for that name, and appends them with
`INSERT OR IGNORE` (a restatement arrives as a new accession number; no stored
row changes). Both are idempotent: a second run fetches and writes nothing.

### 2. Generate targets (the D08 file)

```
python3 -m trader.lowfreq_live targets equity/s2p_top_quintile \
    --sleeve paper_s2p --capital 25000 --as-of YYYY-MM-DD
```

Writes `~/.local/share/mmr/data/targets/paper_s2p/<as_of>.json`
(`mmr.targets.v1`, carrying the strategy source sha256) and a
`<as_of>.benchmark.json` sidecar with the decisive benchmark's weights on the
identical panel and coverage. It **refuses** (exit 2, nothing written) when:

- the latest stored session is not `--as-of` (STALE BARS: re-run step 1);
- the newest stored filing acceptance is more than 7 days before `--as-of`
  (STALE FUNDAMENTALS; `--max-fundamentals-lag-days` adjusts);
- `--as-of` is not a rebalance session for the strategy (it holds: nothing to trade);
- the result would violate D08 (weights outside (0, 1], sum above 1.0).

`--out PATH` and `--targets-dir DIR` redirect the file; `--overwrite` is
required to replace an existing decision record.

### 3. Plan, propose, review, approve (rebalancer, C01)

```
python3 -m trader.rebalance plan    --sleeve paper_s2p     # dry run: orders, cash after, refusals
python3 -m trader.rebalance propose --sleeve paper_s2p     # PENDING proposals in group sleeve_paper_s2p
mmr proposals                                              # review sizing and risk
mmr approve <id>                                           # one by one, SELLs first
```

The rebalancer only creates PENDING proposals (D09); approval is the operator's
act. See `docs/REBALANCE.md`.

### 4. Journal the fills

The paper ledger needs the executed fills (submission is not a fill — see
`CLAUDE.md`, broker acceptance is not inventory). Read them from
`mmr trades` / `mmr strategies fills` and record each:

```
python3 -m trader.lowfreq_live record-fill --sleeve paper_s2p --date YYYY-MM-DD \
    --ticker AAPL --side BUY --quantity 12 --price 231.40 --commission 1.00
python3 -m trader.lowfreq_live fills --sleeve paper_s2p
```

## Every session, after the close

```
python3 scripts/build_pit_universe.py --incremental
python3 -m trader.lowfreq_live track  --sleeve paper_s2p
python3 -m trader.lowfreq_live report --sleeve paper_s2p
```

`track` appends one row per stored session to `lowfreq_paper_ledger` (in the
trading DuckDB named by `trader.yaml`): sleeve holdings x closes, cash, sleeve
value, and the virtual benchmark's value. Both sides are replayed from
inception on every run from durable inputs only (the fill journal, the sleeve's
target files and sidecars, `pit_daily_bars`); rows already stored are never
rewritten, and a replay that disagrees with a stored row is printed as a
DISAGREEMENT (exit 2) for the operator to investigate — for example a fill
journaled late.

Benchmark rules: fills its new weights at the open of the first stored session
after each decision session (t+1 open, as `run_panel` does), pays the equity
base costs (5 bps + $0.005/share, D01), scales buys pro rata to available cash
(D06), holds fractional shares, marks at every close, and carries a missing
print at its last close (counted in `benchmark_n_unpriced`). Cash earns zero
(D03).

`report` prints since-inception sleeve and benchmark return, active return,
its annualised mean, and tracking error (annualised standard deviation of the
daily active return). `--json` on any command gives machine-readable output.

## Where things live

| What | Where |
|---|---|
| Point-in-time bars | `pit_daily_bars` in `history_duckdb_path` |
| As-of fundamentals | `fundamentals` in `duckdb_path` |
| Target files + benchmark sidecars | `~/.local/share/mmr/data/targets/<sleeve>/` |
| Fill journal | `lowfreq_sleeve_fills` in `duckdb_path` |
| Paper ledger | `lowfreq_paper_ledger` in `duckdb_path` |

## Tests

`tests/test_lowfreq_live_incremental.py` (fake Massive / sec-api / Polygon
clients on a temporary DuckDB), `tests/test_lowfreq_live_targets.py` (D08
validity, refusals, parity with `run_panel` on a fake research DB),
`tests/test_lowfreq_live_parity.py` (parity on the research dev DB for
2023-10-02, 2023-11-01, 2023-12-01), `tests/test_lowfreq_live_ledger.py`
(ledger known answers, append-only, CLI round trip).
