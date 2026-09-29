# Target-weights rebalancer (`trader/rebalance`)

The rebalancer is the bridge from a low-frequency panel strategy to MMR's
`propose -> approve` pipeline. It reads a **targets file** (design decision
D08), works out what one **sleeve** needs to buy and sell, and on `propose`
creates **PENDING proposals** through the ordinary proposal pipeline. It never
approves, places, cancels or closes anything (D09): the operator approves each
proposal by hand with `mmr approve`, and the existing server-side gates (trading
filter, risk gate, approval tier) apply unchanged.

```
uv run python -m trader.rebalance plan    <targets.json> [--band-pct 0.25] [--sells-only] [--json]
uv run python -m trader.rebalance propose <targets.json> [same flags]
uv run python -m trader.rebalance status  <sleeve> [--targets-dir DIR | --targets-file F] [--json]
```

Exit codes: `0` success, `1` a D09 refusal (nothing created), `2` a malformed
targets file or usage error. Run it **inside the container** (`./docker.sh -e`):
it reads the proposal store and IB positions through `trader.sdk`, and the live
DuckDB is in the container volume (a host run hits `ShadowedDatabaseError`).

## The workflow

1. **Generate targets.** A strategy (the sales-to-price sleeve from C02, or any
   other) writes `~/.local/share/mmr/data/targets/<sleeve>/<as_of>.json`:

   ```json
   {"schema": "mmr.targets.v1", "sleeve": "s2p", "as_of": "2026-09-28",
    "generated_at": "2026-09-28T21:05:00Z",
    "strategy": {"family": "equity", "id": "s2p_top_quintile",
                 "module": "strategies.lowfreq_s2p", "class": "S2PTopQuintile",
                 "params": {"quantile": 0.2}, "source_sha256": "<sha256 of the strategy source>"},
    "capital_usd": 10000.0, "prices_as_of": "2026-09-28",
    "weights": {"AAPL": 0.05, "MSFT": 0.05}, "notes": "monthly rebalance"}
   ```

   Weights are long-only, each in (0, 1], summing to at most 1.0; the remainder
   is cash. The loader refuses anything else and names the offending field.

2. **Plan (dry run).** `plan` prints the full order list and creates nothing:

   ```
   REBALANCE PLAN  sleeve=s2p  as_of=2026-09-28  group=sleeve_s2p
     capital $10,000.00  holdings value $8,000.00  cash before $2,000.00
      #  ACTION     QTY SYMBOL        PRICE     NOTIONAL   HELD  TARGET  WEIGHT  FLAGS
      1  SELL         5 AAPL         200.00     1,000.00     30      25  0.5000
      2  BUY          2 MSFT         400.00       800.00      5       7  0.3000
     sells 1 ($1,000.00)  buys 1 ($800.00)  gross $1,800.00 of capital $10,000.00
     expected cash after execution $2,200.00  book after $7,800.00
   ```

   Read the `WARN reconcile:` lines (the sleeve ledger and IB disagree), the
   `WARN in flight:` lines (proposals from an earlier batch still open), the
   `POSITION LIMIT would refuse` block (orders the configured per-position cap
   would stop at approval time) and the `skipped:` block (changes inside the
   no-trade band, or BUYs deferred by `--sells-only`).

3. **Propose.** `propose` recomputes the plan, checks for an open batch, and
   creates one PENDING proposal per order, SELLs first. Each proposal carries an
   explicit whole-share quantity, group `sleeve_<name>`, source
   `rebalance:<sleeve>:<as_of>`, and reasoning naming the targets file and the
   strategy's `source_sha256`. It prints the proposal ids.

4. **Review.** `mmr proposals` / `mmr proposals show <id>` show each proposal
   with its trusted fields and the rebalance metadata (target weight, target
   shares, held-before, plan price, capital). `mmr portfolio-risk` shows what the
   batch would do to concentration and group budgets.

5. **Approve by hand.** `mmr approve <id>` for each proposal, SELLs first so the
   BUYs are funded. Approval runs the normal gates; an order the risk gate
   refuses stays refused. `mmr reject <id> --reason ...` for anything you do not
   want. The rebalancer never does either.

6. **Check.** `status <sleeve>` prints the sleeve's reconciled holdings, market
   value, open proposals, and per-name drift against the latest targets file
   for that sleeve.

## How the sleeve is accounted (D09)

A sleeve's holdings are the **net EXECUTED quantities of proposals tagged with
its position group** `sleeve_<name>`, **reconciled with IB**: for each name the
sleeve holds `min(ledger, IB position)`, never more. A name IB no longer shows
is treated as flat (and warned about); IB positions the ledger does not know
are outside the sleeve and never touched. Broker acceptance is not inventory,
so the IB clamp is what stops the sleeve selling shares it does not have.

Sizing is `floor(weight × capital / price)` through the contracted
`order_math.whole_shares_for_notional` kernel; a target worth less than one
share is zero. Prices come from the live snapshot (last, else bid/ask
midpoint, else close). A name that left the targets is sold in full even if the
amount is inside the band; other changes smaller than `--band-pct` of capital
(default 0.25%) are skipped. Expected cash after execution is never negative:
if names left inside the band overhang capital, BUYs are trimmed first, then the
banded SELLs are restored, and if that still cannot balance the plan is refused.

## Refusals (non-zero exit, nothing created)

| code | when |
|---|---|
| `STALE_TARGETS` / `FUTURE_TARGETS` | `as_of` more than 5 NYSE sessions ago, or after today |
| targets refused (exit 2) | malformed file, weights summing above 1.0, non-long-only weights, bad sleeve name |
| `UNPRICEABLE` | any target or held name has no usable price |
| `IB_UNREADABLE` | the IB book cannot be read, so the sleeve cannot be reconciled |
| `GROSS_EXCEEDS_CAPITAL` | Σ order notional > `capital_usd`; split the transition with `--sells-only` |
| `DUPLICATE_BATCH` | PENDING/APPROVED proposals already exist for this `sleeve/as_of` |
| `SLEEVE_BUSY` | PENDING/APPROVED proposals exist in the sleeve from another batch |
| `NOTHING_TO_DO` | `propose` on a plan with no orders |
| `CASH_NEGATIVE` / `BOOK_EXCEEDS_CAPITAL` | fail-closed guards; should be unreachable |

## What it cannot do, by construction

The package talks to the world through four narrow ports (`ports.py`): read
the sleeve's proposals, read IB stock positions, price a ticker, create a
proposal. `tests/test_rebalance_static.py` walks the package AST and fails if
any module names `approve`, `buy`, `sell`, `cancel`, `close_position`, the
proposal state machine, or a raw RPC chain; `tests/test_rebalance_propose.py`
drives `propose` against a fake SDK and asserts the exact calls made and that
no trading method was ever invoked. `sdk_gateway.py` is the only module that
imports `trader.sdk`.

Testing: `uv run --extra test python -m pytest -q tests/test_rebalance_*.py`;
types: `uv run --extra dev ty check trader/rebalance`.
