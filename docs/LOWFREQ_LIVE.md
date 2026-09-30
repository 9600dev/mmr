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
