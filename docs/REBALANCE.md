# Target-weights rebalancer (`trader/rebalance`)

The rebalancer bridges a low-frequency panel strategy to MMR's
`propose -> approve` pipeline. It reads a **targets file** (D08), works out
what one **sleeve** needs to buy and sell, and on `propose` creates **PENDING
proposals** through the ordinary pipeline. It never approves, places, cancels
or closes anything (D15): the operator approves each proposal by hand with
`mmr approve`, and the server-side gates apply unchanged.

```
uv run python -m trader.rebalance plan      <targets.json> | --sleeve <name> [--targets-dir DIR] [--band-pct 0.25] [--sells-only] [--json]
uv run python -m trader.rebalance plan      <targets.json> --offline [--bars-db DB] [--today YYYY-MM-DD]   # no trader_service: empty sleeve, stored closes
uv run python -m trader.rebalance propose   <targets.json> | --sleeve <name> [same flags; --offline is refused]
uv run python -m trader.rebalance reconcile --sleeve <name> [--dry-run] [--json]
uv run python -m trader.rebalance status    <sleeve> [--targets-dir DIR | --targets-file F] [--json]
```

Exit codes: `0` success, `1` a refusal (nothing created) or a `propose` that
stopped midway, `2` a malformed targets file or usage error. Run it **inside
the container** (`./docker.sh -e`) with trader_service up: it reads the
proposal store, IB positions and IB execution receipts through `trader.sdk`.

Only `propose` (PENDING proposals) and `reconcile` without `--dry-run` (journal
rows) write anything; `plan`, `status` and `reconcile --dry-run` write nothing —
not even the journal table's schema.

`--sleeve <name>` takes the newest `<as_of>.json` under
`~/.local/share/mmr/data/targets/<name>/` (or `--targets-dir`); the
`<as_of>.benchmark.json` sidecar C02 writes beside it is ignored. `plan
--offline` needs no trader_service at all: the sleeve is taken as EMPTY (no
positions, proposals or journal rows) and every ticker is priced at its stored
`pit_daily_bars` close on the file's `prices_as_of` (`--bars-db`, default the
`history_duckdb_path` of `LOWFREQ_RESEARCH_CONFIG`); NetLiquidation is reported
as unreadable, never guessed, and a name without a stored close is
`UNPRICEABLE`. It is the dry run for a historical targets file — the
staleness check runs as of the file's own `as_of` unless `--today` is given —
and can create nothing: `propose --offline` is refused (`OFFLINE`) before a
gateway exists.

## The workflow

1. **Generate targets.** A strategy (the sales-to-price sleeve from C02, or any
   other) writes `~/.local/share/mmr/data/targets/<sleeve>/<as_of>.json`:

   ```json
   {"schema": "mmr.targets.v1", "sleeve": "s2p", "as_of": "2026-09-28", "generated_at": "2026-09-28T21:05:00Z",
    "strategy": {"family": "equity", "id": "s2p_top_quintile", "module": "strategies.lowfreq_s2p",
                 "class": "S2PTopQuintile", "params": {"quantile": 0.2}, "source_sha256": "<sha256>"},
    "capital_usd": 10000.0, "prices_as_of": "2026-09-28", "weights": {"AAPL": 0.05, "MSFT": 0.05}, "notes": ""}
   ```

   Weights are long-only, each in (0, 1], summing to at most 1.0 **exactly as
   written** (parsed as decimals: `0.1 + 0.2 + 0.7` loads, `1.0000000001` does
   not); the remainder is cash.

2. **Plan (dry run).** `plan` prints the full order list — SELLs first, with
   price, notional, held and target shares — then expected cash after
   execution and post-execution gross exposure against capital. Read the
   `WARN:` lines (journal and IB disagree; an earlier batch still open), the
   `POSITION LIMIT would refuse` block and the `skipped:` block (no-trade band,
   or BUYs deferred by `--sells-only`).

3. **Propose.** `propose` recomputes the plan, checks for an open batch, and
   creates one PENDING proposal per order: explicit whole-share quantity, group
   `sleeve_<name>`, source `rebalance:<sleeve>:<as_of>`, reasoning naming the
   targets file and the strategy's `source_sha256`. If the store fails midway it
   stops, prints the ids it did create (real PENDING rows) and exits 1.

4. **Review.** `mmr proposals show <id>` shows the trusted fields and the
   rebalance metadata (target weight and shares, held-before, plan price);
   `mmr portfolio-risk` shows the batch's concentration effect.

5. **Approve by hand.** `mmr approve <id>` per proposal, SELLs first so the BUYs
   are funded; the normal gates run. `mmr reject <id> --reason ...` otherwise.
   The rebalancer never does either.

6. **Reconcile.** After the fills, `reconcile --sleeve <name>` journals every IB
   execution of the sleeve's EXECUTED proposals into `sleeve_fills` (D13), once
   each; `--dry-run` previews it. Until it runs the sleeve reads as flat.

7. **Check.** `status <sleeve>` prints holdings, market value, open proposals
   and per-name drift against the latest targets file.

## How the sleeve is accounted (D13, D15)

The proposal store records submissions, not fills, so holdings come from the
**`sleeve_fills` journal** in the main DuckDB: one row per IB execution
(`sleeve, ticker, trade_date, quantity` signed, `price, commission, exec_id`
primary key, `order_id, proposal_id, recorded_at`). C02 reads it through
`SLEEVE_FILLS_DDL` / `SleeveFillsStore` and never writes it.

`reconcile` takes trader_service's read-only `execution_snapshot` for the
broker order ids of the sleeve's EXECUTED proposals: IB's **execution
receipts** (`execDetails`: one per execution, with IB's execution id, shares,
price and time) and the tracker's **order observations** (status, cumulative
fill). Each receipt becomes one journal row dated by IB's execution time. It is
deliberately not the event store's `ORDER_FILLED` rows, which are cumulative
deltas derived from `orderStatus` messages, stamped with observation time and
sometimes without an execution id.

Every EXECUTED order must be resolved affirmatively: an observation with a
known fill quantity that the receipts match exactly (zero filled, cancelled or
still working, counts), or receipts covering the whole submitted quantity.
Anything else refuses and journals nothing — an order the tracker never saw, an
`Unknown` fill quantity, receipts short of the broker's fill (`orderStatus`
before `execDetails`, or a fill outside IB's bounded history window) or
exceeding it, an unreadable snapshot, an incomplete replay or open-orders
refresh, or trader_service's own `complete: false` left unexplained. Treating
"no evidence" as "unfilled" would make the next plan buy the name again.
Orders still working at the broker are reported: reconcile again once they
finish.

Holdings are the journal's per-ticker sums **clamped to IB**: `min(journal, IB
position)`, never more. A name IB no longer shows is flat (and warned about);
IB positions the journal does not know are outside the sleeve, never touched.

Sizing is `floor(weight × capital / price)` through the contracted
`order_math.whole_shares_for_notional` kernel; a target worth less than one
share is zero. Prices come from the live snapshot (last, else bid/ask midpoint,
else close). A name that left the targets is sold in full even inside the band;
other changes smaller than `--band-pct` of capital (default 0.25%) are skipped.
Turnover within a batch is not capped. There is no repair mode: every order is
exactly target minus held. If banded names holding more than target push
post-execution gross exposure above capital, the plan is **refused**
(`GROSS_EXPOSURE_EXCEEDS_CAPITAL`) and the message names the remedy — a smaller
`--band-pct`, or targets that leave cash. Expected cash after execution is
capital minus that exposure, never negative on an accepted plan.

**Position limit.** The live risk gate refuses an opening order whose resulting
position exceeds `risk_limits.max_position_size_pct` of NetLiquidation. `plan`
reads both, values each BUY's resulting position (outside shares included) and
marks the ones the gate would refuse; an unreadable NetLiquidation is reported,
never guessed.

## Refusals (non-zero exit, nothing created)

| code | when |
|---|---|
| `STALE_TARGETS` / `FUTURE_TARGETS` | `as_of` more than 5 NYSE sessions ago, or after today |
| targets refused (exit 2) | malformed file, weights summing above 1.0, non-long-only weights, bad sleeve name |
| `UNPRICEABLE` | any target or held name has no usable price |
| `IB_UNREADABLE` / `JOURNAL_UNREADABLE` | the IB book or the journal cannot be read |
| `GROSS_EXPOSURE_EXCEEDS_CAPITAL` | post-execution gross exposure > `capital_usd` |
| `DUPLICATE_BATCH` | PENDING/APPROVED proposals already exist for this `sleeve/as_of` |
| `SLEEVE_BUSY` | PENDING/APPROVED proposals exist in the sleeve from an earlier batch |
| `NOTHING_TO_DO` | `propose` on a plan with no orders |
| `EXECUTIONS_UNREADABLE` | `reconcile`: no snapshot, receipts unreadable, replay or open-orders refresh incomplete, account unconfirmed |
| `EXECUTIONS_INCOMPLETE` | `reconcile`: no evidence or unknown fill quantity for an order, receipts not matching the broker's fill, unexplained `complete: false` |
| `EXECUTION_MISMATCH` | `reconcile`: a broker order contradicts its proposal's symbol or side |

## What it cannot do, by construction

The package reaches the world only through the ports in `ports.py`: read
proposals, IB positions, NetLiquidation and the execution snapshot; price a
ticker; create a proposal; write its own journal. `tests/test_rebalance_static.py`
walks the package AST and fails on any name or import of a trading action, the
proposal state machine, the event store or a raw RPC chain;
`tests/test_rebalance_propose.py` asserts the exact SDK calls and that no
trading method is ever invoked. Only `sdk_gateway.py` imports `trader.sdk`.
