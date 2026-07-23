# MMR Safety Roadmap

Staged hardening plan for the trading stack. Tranche 1 **shipped 2026-07-23**
(in the repo, **not yet deployed** to the running container — see the
2026-07-23 entry in [`OPERATIONAL_STATE.md`](OPERATIONAL_STATE.md) for the
deployment checklist). Tranches 2-3 are designed, not built.

---

## Principles

1. **Convert conventions into invariants that can't be silently skipped.**
   A safety property that lives in a code-review comment, a client-side flag,
   or a "we always do it this way" habit will eventually be skipped by an
   agent, a script, or a refactor. Tranche 1 is largely this conversion:
   `skip_risk_gate=True` (a client's *claim* of being a liquidation) became
   `Trader.order_reduces_exposure()` (the server *checking* the live broker
   position); `getattr(trader, 'risk_gate', None)`-and-proceed became a
   declared `risk_gate: Optional[RiskGate] = None` that fails **closed**;
   "round the quantity, bump to 1" became floor-and-refuse with an asserted
   postcondition; "we validated it before deploying" became a SHA-256-keyed
   gauntlet PASS record that deploy/enable/arm all check.

2. **Exit-class orders are never refusable.** An order that reduces the live
   broker position must never be blocked by a risk limit, a trading filter, a
   missing gate, an unreadable account feed, or an approval requirement —
   refusing an exit is worse than any limit it would enforce. Every gate in
   the system applies to exposure-*increasing* orders only. The classifier is
   server-side and conservative: "can't prove it reduces exposure" (unknown
   conId, oversized quantity that would flip the position, unreadable
   portfolio) means it's treated as an open and gated.

3. **Counterexample-first verification.** Every property claimed above is
   backed by a test that encodes the exact failure it prevents (the BRK.A
   bump, the poisoned pickle, the gated close, the not-evaluable-PnL open).
   When a property is violated in the field, the counterexample becomes a
   pinned regression test before the fix lands. The `tests/invariants/` suite
   (human-owned — see below) is the durable statement of what must stay true.

4. **Fail closed on exposure, fail open on exits.** The two halves of the same
   rule: when a critical input can't be read (daily PnL, NetLiquidation, a
   price to value a market order), opening exposure is refused with a reason
   naming the missing datum; closing is never affected.

---

## Tranche 1 — SHIPPED (2026-07-23)

### 1. Exit-class trust boundary

Files: `trader/trading/trading_runtime.py` (`order_reduces_exposure`,
`_signed_position`, `gather_risk_inputs`, `place_expressive_order`,
`place_standalone_order`), `trader/trading/executioner.py` (`place_order`),
`trader/messaging/trader_service_api.py` (`place_order_simple`),
`trader/trading/risk_gate.py` (`RiskGate.evaluate`, `RiskInputs`,
`RiskLimits.load`).

- **`Trader.order_reduces_exposure(contract, action, quantity)`** is the
  single server-side exit-class predicate: True iff SELL with qty ≤ held
  long, or BUY (cover) with qty ≤ |held short|, against the live broker
  position in the pinned account. No position, unknown conId, a
  position-flipping oversize, or an unreadable portfolio all classify as an
  *open* (fail-closed).
- **Gates apply only to exposure-increasing orders.** Exit-class orders skip
  the trading filter, leverage check, risk gate, and the
  `require_proposal_approval` gate entirely (the filter still runs in
  observability mode on exits — a would-have-blocked exit is logged, never
  refused).
- **`skip_risk_gate` is deprecated and IGNORED** — kept in RPC signatures for
  wire compatibility only. Passing it logs a warning; whether gates apply is
  decided server-side by the exit-class predicate, never by a client flag.
- **`place_standalone_order` is exit-class ONLY.** The protective-order path
  (STP/TRAIL/LMT covering an existing position) refuses anything that does
  not reduce the live position — it was an ungated exposure door. No risk
  limits beyond that: protectives must never be refusable by limits.
- **Fail-closed when the gate is missing.** `Trader.__init__` declares
  `risk_gate = None`; the executioner and `place_expressive_order` use hard
  attribute access and refuse exposure-increasing orders when the gate is
  None (previously a `getattr(..., None)` default let everything through).
- **Tri-state checks.** `RiskGate.evaluate` records per-check results
  (`pass` / `fail` / `skipped:<reason>`) in `RiskGateResult.checks`, and
  `RiskInputs` carries per-field `*_evaluable` flags distinguishing "read
  succeeded, value is 0" from "could not read". A not-evaluable critical
  input (daily PnL, NetLiquidation, no price to value the order) **REFUSES
  the open**, naming the missing datum — previously those checks silently
  degraded to no-ops. Market orders are now valued off a snapshot (ask for
  BUY, bid for SELL) so the concentration check is evaluable for them too.
  - *Live implication*: for a brief window after connect, while IB's PnL /
    account-value feeds warm up, market opens can be refused with
    "could not be read — fail-closed". This is by design; it self-clears
    within seconds and **closes are never affected**. Expect these lines in
    the first minutes of a session log after restart.
- **Rate limit counts real orders.** The hourly rate check counts
  `ORDER_SUBMITTED` events (which the order paths actually write) instead of
  `SIGNAL` events (which they never wrote — the old check was dead code).
- **`RiskLimits` are operator-configurable** via a `risk_limits:` mapping in
  `~/.config/mmr/trader.yaml` (`RiskLimits.load()`). Missing file/section =
  code defaults (unchanged). A present section with an unknown key or
  malformed value raises loudly — never trade under limits the operator
  didn't actually set.
- **Wrongly-gated closes fixed**: single-position `close`, `resize-positions`
  trims, and AutoExecutor strategy exits (which route through
  propose→approve→`place_expressive_order`) previously ran the full gate on
  their SELLs and could be refused mid-exit; all are exit-class now.

**Known consequence — resize grows are gated.** `resize-positions
--min-bound` BUY deltas *increase* exposure, so under
`require_proposal_approval: true` they are refused like any other direct
open (trims are unaffected). This is accepted for now: growing the book
should be a reviewed decision. If grow-resize is ever needed it gets its own
reviewed path (tranche 2).

### 2. Order math

Files: `trader/trading/order_math.py` (new), `trader/sdk.py` (`approve`),
`trader/trading/executioner.py` (`helper_create_order`),
`trader/trading/position_sizing.py`.

- **`whole_shares_for_notional(amount, price, multiplier)`** is the single
  amount→shares conversion: floor, postcondition
  `1 <= shares` and `shares × price × multiplier <= amount` (with a
  float-boundary step-down), and **refuse-at-zero** (`ValueError`) when the
  amount doesn't cover one whole share. Never bumps to 1 — the old
  `round(...)`-plus-bump turned a ~$340 auto-sized amount on a >$510 stock
  into a full share at inflated notional (the BRK.A-class bug). Fixed in
  **both** conversion sites: `sdk.approve()` (proposal fails with a clear
  error, transitions APPROVED→FAILED) and `executioner.helper_create_order`.
- **BUY sized by ask, SELL by bid** in `helper_create_order` (previously both
  sized off bid — a BUY was sized against the price it *wouldn't* pay).
- **Position-sizing hardening**: `spread_penalty_threshold <= 0` now disables
  the spread penalty instead of dividing by zero, and a penalty factor > 1
  can no longer produce a *negative* size that escaped the min-size refusal
  (clamped to 0).

### 3. Restricted unpickler in the ZMQ layer

File: `trader/messaging/clientserver.py`. Tests:
`tests/test_serialization_security.py`, `tests/test_serialization.py`,
`tests/test_clientserver_rpc.py`.

- **`_RestrictedUnpickler` is ALWAYS ON** — the actual deserialization
  boundary for every dill payload (msgpack `EXT_OBJECT` *and* the raw-dill
  legacy fallback, both routed through `_decode_payload`). `find_class` is
  **type-first** (hardened after the adversarial review — see Remediation
  below): a resolved global is admitted only if it is a *class/type* whose
  module matches the prefix allowlist (`trader`, `ib_async`, `numpy`,
  `pandas`, `datetime`, …), OR a member of an explicit hand-audited
  `_DILL_ALLOWED_RECONSTRUCTORS` frozenset (`copyreg._reconstructor`, the
  `dill._dill` array/namedtuple helpers, `numpy…_reconstruct`/`scalar`, …),
  OR a curated safe builtin. **Every other callable — every plain function
  under any allowed prefix — is refused**, which is what closes the
  `pandas.read_pickle`/`numpy.load` re-entrant-`pickle.load` escape. Module
  gating runs *before* resolution, so an attacker-named module is never
  imported; `_DILL_DENIED_GLOBALS` additionally blocks socket-opening
  messaging classes (`RPCClient`, `MessageBus*`, …) that are types under the
  `trader` prefix. `getattr` and `dill._dill._load_type` resolve to guarded
  stand-ins (no dunder access, data types + exceptions only).
- **Known residual (tracked for tranche 2).** The type-first rule still
  admits *construction* of any non-denied class under an allowed prefix. No
  reachable RCE was found on re-verification, but two surfaces are not
  `find_class`-gated: side-effectful `__init__` (file-I/O / DoS, e.g.
  `pandas.HDFStore`), and the pickle `BUILD` opcode applying attacker state
  via `__setstate__` without passing through `find_class`. The allowlist is
  also dependency-version-dependent. The robust end state is replacing the
  dill fallback with an **explicit codec for the known wire types** (tranche
  2); until then the denylist/frozenset must be revisited on dependency bumps.
- **Poisoned payloads produce structured errors, not silent death.** An
  undeserializable RPC request gets an error *reply* (req_id recovered from
  the msgpack framing) so the client sees `DillDeserializationError` with the
  reason instead of a blind timeout; an unserializable RPC *response* also
  degrades to an error reply. On PubSub and the MessageBus a poisoned message
  is dropped loudly and the read loop continues — **one bad message no longer
  tears down the ticker subject or the signal subscription**.
- `MMR_DILL_STRICT=1` and `set_dill_whitelist(...)` are unchanged as outer
  layers (strict mode disables the dill fallback entirely; the whitelist
  additionally restricts the top-level loaded type).

### 4. Strategy gauntlet — "no hash, no live"

Files: `trader/data/gauntlet_store.py` (new),
`trader/simulation/synthetic_markets.py` (new), `trader/mmr_cli.py`
(`strategies gauntlet` + deploy/enable refusals),
`trader/strategy/strategy_runtime.py` (`_gauntlet_allows_arming`). Tests:
`tests/test_gauntlet.py`.

- `mmr strategies gauntlet strategies/foo.py --class Foo [--min-psr F]
  [--name NAME]` runs four stages and records the verdict:
  - **S1 — import allowlist (AST, no exec)**: denied imports (`os`, `sys`,
    `socket`, `subprocess`, `requests`/`urllib`/`http`, `ib_async`,
    `trader.messaging`, `trader.sdk`, `importlib`, `ctypes`,
    `multiprocessing`, `threading`, `pickle`, `dill`, `shutil`, …) **fail**
    with line numbers; unknown-but-not-denied imports are warnings only. S1
    failure skips S2/S3 — the module is never imported.
  - **S2 — lookahead**: `assert_no_lookahead` walk-forward consistency over
    seeded trending + choppy synthetic series. `not_evaluable` (vacuous for
    `on_prices`-only strategies) counts as pass-with-note, not fail.
  - **S3 — nasty-market battery**: the strategy runs over every synthetic
    frame in `trader/simulation/synthetic_markets.py` (gaps/halts, high
    volatility, zero volume, NaN rows, trending, choppy — the same seeded
    frames the conftest fixtures now delegate to). Any exception or
    malformed `Signal` fails the stage.
  - **S4 — PSR**: probabilistic Sharpe of the latest backtest run for this
    *exact source hash*, **record-only** by default (never fails on absence);
    `--min-psr F` makes it enforcing.
- **Verdicts are keyed by the SHA-256 of the source bytes** in the
  append-only `gauntlet_runs` table (`GauntletStore`). Edit one byte and the
  PASS no longer applies.
- **`strategies deploy` and `strategies enable` refuse without a PASS record
  for the current file's hash.** There is deliberately **no override flag** —
  edit the file, run the gauntlet again. Any failure to *verify* (unhashable
  file, unreachable store) also refuses.
- **Load-time arm gate** (`StrategyRuntime._gauntlet_allows_arming`) is the
  authoritative server-side check for `auto_execute` strategies (the CLI
  checks alone are bypassable by hand-editing the YAML). Default is
  **warn-only** (arms anyway, logs current-hash vs last-PASS-hash and the
  exact command to run) so the live roster doesn't silently disarm on a
  restart before PASS records exist on that host. `MMR_GAUNTLET_ENFORCE=1`
  (strict `'1'`) flips it to enforcing: an unverified `auto_execute` strategy
  loads **DISARMED** — still able to close attributed positions, never able
  to open.
- **Roster smoke status** (host run, 2026-07-23): OpeningRangeBreakout and
  KeltnerBreakout **PASS**; SMICrossOver **FAILS S1** (imports `ib_async`)
  and needs that import removed before it can ever be armed again.

### 5. Invariants suite (`tests/invariants/`)

A separate, **human-owned** test suite stating the safety properties as
executable spec: exit-class orders are never refused, gates fail closed,
share conversion never exceeds the sized notional, the unpickler refuses
gadget globals, no-PASS strategies can't arm. Policy:

- The suite is the spec. **Agents may not weaken, delete, or loosen a
  property to make an implementation pass** — a red invariant means the
  implementation is wrong (or a human explicitly revises the spec).
- **Counterexamples become pinned regressions**: any field incident or
  discovered violation is added as a concrete test case before its fix
  lands, and stays forever.

---

## Remediation — adversarial review of tranche 1 (2026-07-23)

Before commit, the tranche-1 diff was run through a 3-lens adversarial review
(capital-safety, bypass-hunting, regression) with refute-first verification.
1,693 passing tests had missed **9 confirmed defects**; all were fixed and
re-verified in the same session:

- **[critical] Unpickler callable escape** — the original module-*prefix*
  allowlist admitted `pandas.read_pickle` (→ re-entrant unrestricted
  `pickle.load`) and every other side-effecting callable under an allowed
  package. Fixed by the type-first rule above; independently re-verified that
  hand-assembled `read_pickle`/`numpy.load`/`os.system`/`eval` payloads are
  refused with no code execution. Residual class-construction surface tracked
  above.
- **[major] Order-rate limit was still dead code** — `ORDER_SUBMITTED` events
  were stamped `manual` but the approve/AutoExecutor path queried `proposal`,
  so the per-hour cap never fired on the automated open path (and the direct
  path false-refused off a shared bucket). Now stamped with the real
  `orderRef` and counted per-source, exit-class submissions excluded.
- **[major] Grow-resize stranded positions naked** — `place_standalone_order`'s
  new exit-class check refused the re-created (larger) protective before the
  BUY delta filled. `execute_resize_plan` now never cancels the old stop
  without a confirmed replacement (bounded fill-poll, else the smaller old
  stop is retained).
- **[major] `approve()` ignored the contract multiplier** — an OPT/FUT
  proposal sized by bare premium was 100× oversized. Multiplier now flows into
  `whole_shares_for_notional`.
- **[major] Daily-loss gate blind after a restart** — an empty PnL cache read
  as an evaluable `0.0`. Now treated as *not-evaluable* (fail-closed on opens)
  when the account has positions or fills today, self-clearing as the feed
  warms.
- **[minor] Forex concentration regression** (`--sectype CASH` exempted),
  **gauntlet S1 dynamic-import bypass** (`__import__`/`eval`/`exec`/`compile`
  Call-node checks added; S1 documented as a static advisory scan, not a
  sandbox), **gauntlet PASS keyed by `(hash, class)`** so a sibling class
  can't inherit a verdict, **enable-gate scoped to `auto_execute`** strategies,
  and **`RiskLimits.load` falls back to defaults** on a bad key instead of
  crash-looping trader_service into the supervise circuit breaker.

## Tranche 2 — designed, not built

Priority order is roughly as listed; items are independent unless noted.

1. **Proposer/approver split.** The agent that proposes trades must not be
   the agent that approves them. Concretely: the `approve` capability is
   *absent from the proposing agent's environment* (not merely
   policy-forbidden); a **fresh-context approver** reviews with an input
   surface of exactly proposal + manifest + risk report — never fetched
   text, news, or scanner output (prompt-injection cannot reach the
   approver through content it never sees); **tiered human approval** above
   a notional threshold.
2. **`ApprovedOrder` capability token.** The gate *mints* an unforgeable
   token when a proposal passes review; the executioner *accepts nothing
   else*. Turns "all orders came through the gate" from a call-graph
   convention into a type/possession invariant — a code path that never
   touched the gate cannot construct the argument the executioner requires.
3. **Leverage check on the direct order path.** `place_order_simple` /
   `executioner.place_order` currently run filter + risk gate but not the
   whatIf margin/leverage check that `place_expressive_order` has; unify so
   every exposure-increasing path is leverage-checked.
4. **Strategy runtime isolation ladder.** Step 1: the gauntlet's import
   allowlist becomes *enforced at load time* (today it's a deploy-time scan —
   a passing file could still `__import__` dynamically). Step 2:
   per-strategy **no-network subprocess workers**, so a strategy cannot
   reach the SDK, the message bus, or the internet even in principle.
5. **Deal contracts + CrossHair** on the pure kernels (`order_math`, sizing
   clamps, gate arithmetic): machine-checked pre/postconditions.
   **Blocked on a container image rebuild** — new runtime dependency.
6. **Grow-resize reviewed path.** A deliberate, review-carrying flow for
   `resize-positions --min-bound` BUY deltas (today refused under
   `require_proposal_approval` — see tranche 1 known consequence).
7. **Strategy YAML manifest.** Each strategy declares its universe,
   direction, and expected turnover in `strategy_runtime.yaml`; the runtime
   enforces `min(declared, global)` — a strategy that declared "long-only,
   ≤ 3 orders/day, these 2 conids" physically cannot short, churn, or trade
   anything else, whatever its code does.

## Tranche 3 — kernel extraction

1. **Extract `trader/kernel/`**: the risk gate, sizing clamp, proposal state
   machine, and order construction as **pure, closed-world** functions — no
   I/O, no clock, no globals; inputs in, decision out. Everything effectful
   (IB, DuckDB, ZMQ) stays outside and feeds the kernel explicit snapshots
   (the `RiskInputs` shape is the seed of this). Pure + closed-world is what
   makes exhaustive property testing and contract checking tractable.
2. **Optional Rust port** of the kernel behind the same signatures, verified
   with **Kani** (model checking the no-overflow / postcondition properties),
   callable from Python. Only worth it once the kernel boundary has been
   stable for a while; the Python kernel remains the reference
   implementation.

---

## Rollout sequencing — gauntlet enforcement flag

`MMR_GAUNTLET_ENFORCE` stays **unset** (warn-only) until every step below is
done, in order:

1. **Deploy tranche 1** to the container (sync + restart both services,
   outside market hours; `mmr verify` after — full checklist in
   `OPERATIONAL_STATE.md` 2026-07-23).
2. **Run the gauntlet in-container for the armed roster** (`mmr strategies
   gauntlet <module> --class <Class>` per armed strategy). PASS records live
   in the DuckDB the strategy_service reads — the container's `mmr_db_data`
   volume — so host-side runs do **not** satisfy the in-container arm gate.
3. **Observe one full session warn-only.** Every `auto_execute` load logs
   either nothing (PASS on file) or a warning naming the missing hash;
   confirm zero warnings for the intended roster and that the warnings for
   anything else are expected.
4. **Set `MMR_GAUNTLET_ENFORCE=1`** and restart. Verify the roster arms
   (strategies RUNNING with auto_execute on) and that unverified strategies
   load DISARMED, not dead — they must still be able to close.
5. **From then on, every source edit changes the hash**: re-run the gauntlet
   after any strategy change or the next reconcile/restart disarms it. Make
   gauntlet-then-deploy the only path (it already is in the CLI — deploy and
   enable refuse without the PASS).
