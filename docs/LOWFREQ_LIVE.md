# Low-frequency live path: monthly runbook (C02)

The sales-to-price sleeve (`equity/s2p_top_quintile`) is paper-traded through
MMR's `propose -> approve` pipeline with weights computed by **exactly the
research code path** (`trader.research.lowfreq`: same loader, same strategy
class, same `precompute_panel -> on_panel -> normalise_weights` sequence
`Backtester.run_panel` runs). The paper ledger values the sleeve and its
virtual decisive benchmark (`eq_ew_covered`, D05) from the same stored bars,
and the benchmark is executed by `Backtester.run_panel` itself, so the sleeve
accumulates honest out-of-sample evidence at zero capital risk.

> **2026-11-15: the strategy this sleeve runs has no research result behind
> it.** The development result that started it (+12.0%/yr over its benchmark)
> came from a market cap computed on two bases at once — split-adjusted prices
> times as-reported share counts — which is lookahead toward every stock that
> later split. On a correct market cap the candidate FAILS in development and
> held out (`docs/RESEARCH_LOG.md`, 2026-11-15). The sleeve's 15 holdings were
> bought on 2026-09-30 from targets computed the old way; targets generated
> from 2026-11-15 on use the corrected market cap, so the next rebalance will
> turn much of the book over. What follows is the operating runbook for the
> machinery, which is sound; whether to keep running a book on this signal is
> a decision, not a default.

Every command below is complete and runs from the host. `EXEC` carries the
persistent environment: the harness reads only the databases named by
`LOWFREQ_RESEARCH_CONFIG` (D03), which in the container is the live config
(both DuckDB paths), opened read-only.

```
EXEC="docker exec -u trader -w /home/trader/mmr \
  -e LOWFREQ_RESEARCH_CONFIG=/home/trader/.config/mmr/trader.yaml \
  -e SEC_API_KEY=<your sec-api.io key> mmr-mmr-1"
```

## What runs by itself (since 2026-11-15)

pycron runs `scripts/sleeve_upkeep.py` at 23:30 ET every weekday (job
`sleeve_upkeep`). For the last completed NYSE session it does the "every
session" steps below for every sleeve that has a targets file — bars, the
split history, the split adjustment of the stored prices, `reconcile`,
`track` — and, when that session was the first of its month and
the sleeve has no targets file for it, the monthly steps 1 and 2 plus a
dry-run `plan`, saved to
`~/.local/share/mmr/reports/sleeve_<sleeve>_<session>_plan.txt`. Candidate,
capital and params come from the sleeve's newest targets file.

**What is still yours:** step 3 from `propose` on. The job never proposes,
approves or trades (D15). The morning after a month's first session, read the
saved plan, then `propose`, review and approve before the open; the fills are
journaled by that evening's run.

A failed step stops the later steps of that sleeve and fails the job, which
`scripts/pycron_status.py` and `scripts/monitor_health.sh` report. The monthly
part needs `SEC_API_KEY` inside the container: `docker-compose.yml` forwards it
from the host shell like the other vendor keys, so it must be exported where
`./docker.sh` is run. By hand: `python3 scripts/sleeve_upkeep.py --dry-run`
prints the commands for today, `--session YYYY-MM-DD` for another day.

Why it exists: none of the manual steps had been run for six weeks. The bars
stopped at the first rebalance, `sleeve_fills` had never been written (so the
sleeve read as empty and the next plan would have bought it again — the
rebalancer now refuses that, see `docs/REBALANCE.md`), the ledger was empty and
November's decision session passed with no targets file.

## Once a month: the FIRST NYSE session of the month, after the close

The strategy rebalances on the first session of each calendar month
(`CoveredTilt.REBALANCE = 'month'`). Its decision uses that session's close and
the backtest fills at the NEXT session's open, so the cycle is: refresh after
the close, generate targets, propose, review, approve before the next open.
`D` below is the decision session, e.g. `2026-10-01`.

### 1. Refresh data through the decision session (incremental, append-only)

```
$EXEC python3 scripts/build_pit_universe.py --incremental --end D
$EXEC python3 scripts/fetch_splits.py
$EXEC python3 scripts/pit_split_adjust.py --apply --accept-refusals
$EXEC python3 scripts/fetch_fundamentals.py --incremental
```

The two middle lines keep price and share count on one basis. Every stored
bar is split-adjusted as of the day it was fetched, so a split executed since
the last fetch leaves the days before it on the old basis: a split-sized jump
in the series (MNST 97.74 -> 48.62 on 2026-07-29) and a market cap wrong by
the ratio. `fetch_splits.py` brings `corporate_splits` up to today (one or two
requests; it fails loudly), and `pit_split_adjust.py --apply` puts the stored
prices on today's basis against the vendor's current adjusted series and
records the day in `pit_price_basis`. `targets` and `track` refuse a store
that does not record that day. Without `--apply` it prints what it would
change. A series it cannot explain is left exactly as it was and named in
`pit_split_refusals` (`--accept-refusals`: record it and move on the same
night); those tickers get no market cap, so they are never ranked or held.
Exit status 1 means a refusal that was not already on record, or a vendor or
write failure - read the output; a ticker refused before and refused again is
listed and is not a failure.

`--end D` is required: without it the refresh stops at yesterday and `targets`
refuses with STALE BARS. `build_pit_universe.py --incremental` resumes from
the day after the last stored session; stored sessions are never re-fetched or
rewritten (a session whose bars are not yet published comes back empty and is
fetched on the next run). `fetch_fundamentals.py --incremental` asks sec-api,
for every current point-in-time top-500 member, only for filings accepted
after the newest acceptance instant already stored for that name (newest per
period wins, decided by the instant; a late-filing notice is not a filing),
and appends them with `INSERT OR IGNORE`. Both are idempotent: a second run
fetches and writes nothing.

The numbers come from the vendor's `/stocks/financials/v1` endpoints, which
serve the LATEST version of a period: once a later filing restates a quarter
as a comparative, that is what comes back. A row is stored only when the
vendor's numbers were taken from the filing being dated, so this must run
within months of a filing (monthly is ample) and it cannot backfill a name's
history. Its summary line counts what it refused and why. It exits non-zero
when the vendor or sec-api could not be read for any name — until 2026-11-15
the vendor's endpoint had been retired for some time and every run reported
"0 new rows".

### 2. Generate targets (the D08 file)

```
$EXEC python3 -m trader.lowfreq_live targets equity/s2p_top_quintile \
    --sleeve paper_s2p --capital 50000 --as-of D
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

**Operating decisions (2026-09-30).**
- **Capital US$50,000.** The paper account DUM422056 is CAD-based, NetLiquidation
  CA$92.9k (about US$65k at USD/CAD 1.42) with `max_leverage: 1.0`. US$50k uses about 77% of it. At
  US$25k each name is about $400, so three names cannot be bought at all under floor-and-refuse
  sizing and 19% stays in cash; at US$50k, 61 of 62 names fill and about 8.6% stays in cash.
- **Approve in chunks of at most 15.** Proposals are `MARKET`/`DAY`, regular hours only, and
  `risk_limits.max_open_orders` is 15. After the close every approved order stays OPEN until
  the next session, so approve SELLs first, then BUYs, at most 15 open at a time. Approve the
  next chunk only after `mmr orders` shows the previous one filled.
- Before approving, `mmr status` must show `account_values.NetLiquidation`, and `mmr proposals`
  must hold nothing except this batch.
```

The rebalancer only creates PENDING proposals and never approves, places or
cancels anything (D15); approval is the operator's act. See `docs/REBALANCE.md`.
`--sleeve` resolves the newest `<as_of>.json` under the sleeve's targets
directory (the `.benchmark.json` sidecar is ignored); a file path works too.
Without trader_service, `plan <file> --offline` dry-runs the file against an
EMPTY sleeve priced at the stored `pit_daily_bars` closes of its
`prices_as_of` (the end-to-end check in `research/lowfreq/SUMMARY.md`);
`propose --offline` is refused.

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

`track` reads IB positions through `trader_service` first and **refuses**
(exit 2, nothing appended) when the journaled holdings exceed what IB shows or
when IB cannot be read - there is no bypass (D13: a sleeve may never own more
than IB shows). It then replays both sides from inception from durable inputs
only (`sleeve_fills`, the target files and their sidecars, `pit_daily_bars`)
and writes one row per stored session to `lowfreq_paper_ledger` (in the
trading DuckDB named by `trader.yaml`): sleeve holdings x closes, cash, sleeve
value, and the virtual benchmark's value; its output line also carries the
benchmark's running count of D07 exits. The last 5 stored sessions are
**provisional**: a D07 exit on one of them may still be booked with hindsight
(below), and `track` then revises that row's benchmark columns and prints each
revision. Every earlier session is **settled**, and the replay must reproduce
it exactly, as it must the sleeve side of every row; if it does not (a fill
reconciled late, corrected bars) `track` **refuses before writing anything**
and names the disagreements. Settled history is rewritten only by an attested
operator act:

```
$EXEC python3 -m trader.lowfreq_live rebuild --sleeve paper_s2p \
    --reason "AAPL fill of 09-05 reconciled on 09-08" --attest
```

`rebuild` reconciles against IB, drops the sleeve's rows, replays from
inception and records the act (time, reason, rows dropped/written, the
disagreements it resolved) in `lowfreq_ledger_rebuilds`; `report` lists it.

Benchmark rules: `Backtester.run_panel` executes the sidecar weights with the
sleeve's capital and the equity base costs (D01) - whole shares truncated off
the decision close, fills at the next session's open, buys scaled pro rata to
cash and floored (D06), missing prints carried at the last close and counted.
Cash earns zero (D03). D07: a stopped name is exited ON the session of its
last stored print, at that close less costs - the harness's own fill. The
harness knows a print was the last with hindsight over the complete dataset;
live, the ledger learns it once the name has been silent for 5 consecutive
stored sessions, which is why the trailing 5 sessions stay provisional.
Applied per episode: a name that stops, re-enters through a later rebalance
and stops again is exited at the last print of each episode, and a gap before
the benchmark first held it changes nothing (`run_panel` sells only what it
holds). A name that prints again after 5 silent sessions (a long halt) has
been exited at its last print and re-enters only through the next sidecar's
weights - the one departure from the hindsight rule, which would have carried
it. A name absent from `pit_daily_bars` on a session (below the top-800
storage cutoff that day) is carried at its last close and counted in
`n_unpriced` / `benchmark_n_unpriced` until it prints again or five silent
sessions decide its exit.

`report` prints since-inception sleeve and benchmark return, active return,
its annualised mean, tracking error (annualised standard deviation of the
daily active return), how many trailing sessions are provisional, and any
rebuilds. `--json` on any command gives
machine-readable output.

## Where things live

| What | Where |
|---|---|
| Point-in-time bars | `pit_daily_bars` in `history_duckdb_path` |
| As-of fundamentals | `fundamentals` in `duckdb_path` |
| Target files + benchmark sidecars | `~/.local/share/mmr/data/targets/<sleeve>/` |
| Fills journal (C01 writes, D13) | `sleeve_fills` in `duckdb_path` |
| Paper ledger | `lowfreq_paper_ledger` in `duckdb_path` |
| Rebuild audit | `lowfreq_ledger_rebuilds` in `duckdb_path` |

## Tests

`tests/test_lowfreq_live_incremental.py` (fake Massive / sec-api / Polygon
clients on a temporary DuckDB, including same-period multi-filing reruns),
`tests/test_lowfreq_live_targets.py` (D08 validity, refusals, parity with
`run_panel` on a fake research DB), `tests/test_lowfreq_live_parity.py`
(parity on the research dev DB for 2023-10-02, 2023-11-01, 2023-12-01 against
the instructions `run_panel` consumed and the book it then held),
`tests/test_lowfreq_live_ledger.py` (harness-derived known answers: whole
shares, D06 floor, an opening gap, a stopped name exited ON its last print
once five silent sessions decide it - revising only provisional rows - a name
that stops, re-enters and stops again exited twice while an unheld gap changes
nothing, prefix-stable replay of settled rows, config parity
with `engine._config`, mandatory reconciliation, disagreement refusal, attested
single-transaction rebuild, sidecar/D08 consistency, CLI round trip), and
`tests/test_lowfreq_live_terminal_exits.py` (`run_panel` with several terminal
prints per name; a single timestamp behaves exactly as before).
