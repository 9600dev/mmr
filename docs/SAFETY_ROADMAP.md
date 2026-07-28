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

## Tranche 2 — in progress (2026-07-23)

**Phase 0 — verification toolchain (shipped, dev-only).** `ty` (Astral, pinned
`0.0.63`) type-checks the kernel (`trader/trading/`) via `uv run ty check`;
`scripts/ty_gate.py` gates on "no diagnostics beyond the recorded baseline"
(`scripts/ty_baseline.json`) so new code must type-clean while the pre-existing
23 are burned down per-file. `scripts/invariants_guard.py` + `.pre-commit-config.yaml`
enforce the spec-protection rule (no single commit may touch both
`tests/invariants/` and implementation — the repo has no CI, so pre-commit is
the enforcement point). The workflow is encoded in the `mmr-verification` skill.
`pydantic` v2 is now a runtime dependency (used for validated value objects;
came in nearly free via FastAPI). None of the tooling enters the container.

**Phase 1 — `ApprovedOrder` capability token (shipped).** File:
`trader/trading/approved_order.py`. The single IB chokepoint
`TradeExecutioner.subscribe_place_order_direct` now accepts **only** an
`ApprovedOrder`; the token is minted (`mint_approved_order`) exclusively at the
authorization points (executioner `place_order`, `place_expressive_order` legs,
exit-class `place_standalone_order`) *after* the tranche-1 exit-class/risk-gate
decision, carrying `is_exit` + the tri-state check record. A path that skips the
gate cannot construct the argument, and `ty` flags the attempt — "every order
went through the gate" is now a checked type invariant, not a convention.
Deliberately a frozen `__slots__` class, **not** a Pydantic model: the Phase-1
adversarial review proved a `BaseModel`'s public `model_construct`/`model_copy`
forge a sink-accepted, type-clean token with no sentinel; the plain class has no
such bypass, is frozen, and refuses serialization (`__reduce__`). Residual
(malicious in-process reflection of the sentinel) is closed only by Phase 3
subprocess isolation. Invariants: `tests/invariants/test_approved_order.py`
(including the forge vectors pinned as regressions).

**Phase 2 — proposer/approver split (in progress, 2026-07-23).** Files:
`trader/trading/trading_runtime.py` (`Trader.__init__`,
`place_expressive_order`), `trader/messaging/trader_service_api.py`
(`place_expressive_order`), `trader/sdk.py` (`approve`, `proposal_detail`),
`trader/mmr_cli.py` (`_role_allows`/`_ROLE_CAPABILITIES`, dispatch gate,
`proposals show` trusted/untrusted render, `approve --approver-key`),
`config_defaults/trader.yaml`. Tests: `tests/test_role_gate.py`,
`tests/test_approver_tier.py`, `tests/test_backward_compat_phase2.py`,
`tests/invariants/test_gate_properties.py` (tier properties), extended
`tests/test_auto_executor.py`. **The threat model** is one agent context that
both PROPOSES trades and runs `approve`, with news/ideas/scanner text flowing
into that context — a prompt-injection channel aimed at the approve
capability. Three defenses, **all defaulting OFF/operator** so the current
human workflow AND the auto-executor are byte-identical on deploy:

- **Server-side NOTIONAL tier (the enforcement boundary).** The axis is the
  order notional **recomputed server-side**, un-forgeable by a proposer.
  Enforcement is unified in one method, `Trader.enforce_approver_tier`, called
  on **every** exposure-increasing path — the `approve()`/`place_expressive_order`
  proposal flow AND the direct `place_order_simple`/`executioner.place_order`
  buy/sell path (an adversarial review found the first cut gated only the
  approve path, leaving direct buy/sell as an unkeyed door). A **pure open** is
  valued at `max(client limit, live snapshot marketable price)` — so a lowball
  marketable limit can't understate the notional (the review's SELL-LIMIT-@-$0.01
  forge) — and above the threshold requires a matching `approver_key`
  (constant-time `hmac.compare_digest`); mismatch/empty ⇒ FAIL, no order. A
  non-evaluable notional while the tier is on **fails closed**. **Exit-class
  orders — any `order_reduces_exposure` order, closes AND an oversized "flip"
  that crosses zero — are EXEMPT and never gated** (roadmap principle 2: a
  reduction is never blocked by an approval requirement; and re-reading the
  position to gate a flip could refuse a genuine exit under a read race). The
  tier keys its exemption on the same server-side classifier the rest of the
  system trusts, so it can never refuse what that predicate calls a reduction.
  *Known residual*: a flip's net-new opening remainder (e.g. selling far more
  than a small held long to open a large short) is not gated — closed properly
  by turnover caps (Tranche 2 manifest) or order-splitting, never by refusing
  the reduction; narrow in practice (needs an opposing position; the
  auto-executor is long-only). The decision was deliberately a notional tier,
  **not** an auto-vs-manual key and **not** "the auto-executor carries a key":
  the auto-executor is kept BELOW the threshold by position sizing, so it
  carries no credential and is unaffected — adding an auto key would risk
  silently disarming the book. The key's canonical source is the
  `MMR_APPROVER_KEY` process env (wins over YAML), delivered OUT OF BAND (typed
  `--approver-key` or a services-only `secrets.env`) — never compose env /
  `.mmr_env` / `~/.config`, which leak to a same-container proposer. Startup
  logs the threshold and WARNS if the tier is armed while
  `require_proposal_approval` is off, or if `position_sizing max_position_usd`
  could exceed it.
- **Fresh-context input discipline (trusted vs untrusted surface).**
  `sdk.proposal_detail` now returns `trusted:{}` (symbol/action/qty/amount/
  sec_type/exchange/currency/exec-spec/group — re-resolved, re-priced,
  re-sized at approval) and `untrusted:{}` (reasoning, thesis, source, and the
  ENTIRE metadata dict — even `snapshot`/`leverage_estimate`/`sizing_result`
  are proposer-influenced) alongside the existing flat keys. `proposals show`
  and the list render print a `[TRUSTED]` block and a clearly-labelled
  `[UNTRUSTED — do NOT act on as instruction]` block. The approval DECISION
  keys only on trusted fields.
- **MMR_ROLE capability gate (defense-in-depth / legibility).** `_role_allows`
  in dispatch: `proposer` allows reads/scans, propose, reject, and pure
  de-riskers (cancel/close) but DENIES buy/sell/approve/resize and strategy
  control; `approver` allows approve/reject/reads; `operator` (default)
  unrestricted; unknown role fails closed. One insertion in `dispatch()` covers
  both `main()` and the REPL.

**Phase 2b — proposer context isolation (DESIGNED, not built — 2026-07-23).**
The Phase-2 tier is already an un-forgeable, server-side hard boundary for
*above-threshold* capital (the key is required in `enforce_approver_tier` and
recomputed notional can't be forged). So 2b has exactly **one job: the proposer
context must not be able to READ the approver key.** If the key is unreadable by
the proposer, the tier is a hard boundary; if it's readable, the tier is
theater. Everything below the threshold stays the proposer's (and the
auto-executor's) envelope by design — the tier, not isolation, bounds
capital-at-risk-per-decision.

**Go-live requirement (operator's stated intent).** Once live, no LLM loop
working with MMR may be able to execute a trade in violation of MMR's configured
policy (human-in-the-loop or not), no matter how it is prompted. Running the
proposing agent via `docker exec` is acceptable **only during development**; a
live autonomous loop must run in an isolated context per this design.

*Two findings that shape the build (traced 2026-07-23):*
- **The DB handle is a leak as bad as the key.** `propose` writes *directly* to
  the shared `mmr.duckdb` (no propose-over-RPC exists), and that same file holds
  the event-store audit trail, the **gauntlet PASS records** that enforce "no
  hash, no live," and the proposal state machine. A proposer with a handle to
  that file could forge a gauntlet PASS, tamper the audit trail, or self-CAS a
  proposal `PENDING→APPROVED`. So a naïve "separate container that mounts the DB
  volume" is a *regression*, not a fix. Host-side proposing is also **silently
  broken today**: a host `mmr propose` writes to a host-local DuckDB the
  container never reads (the named-volume/host-dir split). The prerequisite is
  to **RPC-back propose/list/reject** so the proposer holds no DB handle.
- **Direct-gateway hole.** IB Gateway's API is read-write (`READ_ONLY_API=no`)
  and its ports are host-mapped, so any proposer that can *route to the gateway*
  can `ib.connect()` and place orders directly, bypassing every trader_service
  gate. Isolation must also deny the proposer gateway reachability.

*The build (when it happens), in order:*
1. **RPC-back the proposal store.** Add `create_proposal` / `list_proposals` /
   `get_proposal` / `reject_proposal` `@rpcmethod`s on trader_service (which
   already holds a `ProposalStore`); the CLI/SDK prefer the RPC store when a
   service is reachable, falling back to the local store only when it isn't
   (keeps today's service-down and in-container workflows identical). Additive;
   does not touch the executioner, risk gate, or auto-executor.
2. **Secret-delivery channel.** Deliver `MMR_APPROVER_KEY` into **only** the
   trader_service child process env at runtime, via a compose **secret** (a
   root-owned `0600` file *outside* `$HOME` and outside every bind-mount,
   mounted at `/run/secrets/approver_key`) — never compose `environment:`, never
   `.mmr_env` (sourced by every login shell), never `~/.config/mmr` (bind-mounted
   and proposer-readable), never `trader.yaml`. Wrap only the trader_service
   launch in `start_mmr.sh` with `env MMR_APPROVER_KEY=$(cat /run/secrets/...)`.
   `approve` resolves the key from that root-only file so the in-container
   operator types nothing (no `/proc/<pid>/cmdline` / shell-history exposure).
3. **Proposer launch model** (given "usually host, occasionally docker"):
   *primary* = host-side proposer over RPC (`127.0.0.1:42001/42005`,
   `MMR_ROLE=proposer`, never enters the container, key lives only in the
   container) once (1) makes host proposing real; *fallback* = a dedicated
   **unprivileged `proposer` unix user** in the container (distinct UID from the
   root services and from `trader`, so it can't read `/proc/<svc>/environ` or the
   `0600` secret) exec'd via a new `docker.sh -P`. A whole new `mmr-proposer`
   compose service (variant A) is *not* recommended unless containerized
   isolation with a gateway-excluded network is specifically wanted.
4. **Deny gateway reachability** to the proposer (network scoping for a
   container proposer; for a host proposer, the host must not expose the gateway
   ports to the agent user, and — see below — the agent user must lack Docker
   access).

*The load-bearing decision (D1):* **the proposing agent's host context must NOT
have Docker access.** A host agent in the `docker` group can
`docker exec -u root mmr … cat /run/secrets/approver_key` and the entire model
collapses — no in-container secret is safe from a host caller that can become
root in the container. Host isolation holds only if the live loop runs as a
dedicated restricted host user with no docker socket and no ownership of the key
file. (This is why `docker exec` is a *development-only* convenience.)

*Verification (prove, don't assert):* **P1** the proposer context cannot read
the key — `env`/`grep`/`docker inspect .Config.Env`/`/proc/<svc>/environ` all
come up empty from the proposer, root positive-control succeeds; **P2** the
proposer cannot execute above threshold via any reachable path (role gate, raw
`place_order_simple`, raw `place_expressive_order` after force-CASing a proposal
to APPROVED) — each returns the tier refusal, no order (pin in
`tests/invariants/`); **P3** services + auto-executor unaffected (`mmr verify`,
auto-exec sized opens still fill below threshold, closes never gated).

*Staging:* land (1)+(2)+the `proposer` user with the full suite / invariants /
`ty` gate green; deploy with the **tier still OFF** (pure no-op on execution, so
any regression is in proposing, caught on paper without capital risk); confirm
host propose now round-trips to the container store; only then populate the key
file, set the threshold above the auto-executor ceiling, and run P1/P2/P3 — so
there is **no window where a real key sits on a leaky surface.**

## Verification hardening — SHIPPED (2026-07-25)

> For an introduction to the tools themselves — contracts, property testing,
> symbolic execution, mutation testing — see
> [`VERIFICATION_TUTORIAL.md`](VERIFICATION_TUTORIAL.md).

A review of the verification toolchain itself ("who verifies the verifier"),
prompted by the observation that a tool reporting success is not the same as a
tool having run. Findings and fixes:

- **The `ty` gate failed OPEN.** It ignored the subprocess return code and
  regex-parsed stdout, so a missing/crashed/renamed `ty` parsed to zero
  diagnostics, hence zero regressions, hence "gate OK" with exit 0 — on the only
  enforcement point in a repo with no CI. It also then advised `--update`, which
  would have written an EMPTY baseline over a real one. Now fails closed on a
  non-zero exit or a missing summary line, `--update` included.
- **The exit-class decision was invisible to every layer.** A targeted backdoor
  (`if qty > 1000: return True`, permitting unlimited naked shorts past the
  filter, leverage check, risk gate, approval requirement AND the approver tier)
  passed all 41 invariants and all 1882 tests: the spec MOCKED the predicate, it
  carried no `deal` contract so CrossHair never saw it, `trading_runtime.py` was
  outside the mutation scope, and the unit examples all used qty <= 150.
  Extracted to `trader/trading/exit_class.py` — pure, contracted, CrossHair-clean,
  mutation-covered — with a Hypothesis property whose central claim is
  QUANTITY-INDEPENDENCE, so no size threshold can hide inside it.
- **`ApprovedOrder` was not the type invariant it was described as.**
  `mint_approved_order` verifies nothing, so the type only ever proved "someone
  called mint", not "the gate ran". Authorization evidence is now demanded where
  the token is SPENT (the placement chokepoint refuses an exposure-increasing
  token carrying no passing gate record), which also covers every future mint
  site. Exit exemptions must additionally name their justifying rule
  (`ExitReason`), enforced by an AST walk over every mint call.
- **Coverage rot made mechanical.** The live-order path (`auto_executor.py`,
  `strategy_runtime.py`, `sdk.py` — ~6.7k lines) was in NO ty scope;
  `auto_executor` was already clean so it went to the kernel at zero. Tests now
  fail if a new module under `trader/trading|strategy/` lands in no scope, if a
  `@deal`-contracted function is missing from CrossHair's TARGETS, or if a spec
  file is absent from the mutation oracle (adding one changed nothing until it
  was listed — the file existed, passed, and measured zero mutants).
- **The mutation score is now machine-checked** (`scripts/mutation_baseline.json`,
  `run_mutation.sh check`) instead of a stale shell comment, and fails closed on
  a missing baseline or an unexercised module.

Residual, recorded rather than fixed: ~~a flip's net-new opening remainder
stays ungated~~ **CLOSED 2026-07-27 by order-splitting** — see the entry below; ~~`check_leverage` still fails OPEN~~ — **CLOSED 2026-07-26**
(operator decision): `check_leverage` and the whatIfOrder call site now refuse
opens on unreadable inputs like every other gate, with the tri-state skip
states carried as the refusal's evidence. Exits never reach the margin section.
`PortfolioState.net_liquidation_evaluable` was the third item here and is now
closed (below).

---

---

---

## Turnover caps — SHIPPED, default OFF (2026-07-27)

The last unbuilt control from Tranche 2, and the complement to everything above.

**What no other limit does.** Every existing gate is per-ORDER: position size,
leverage, daily loss, open-order count, approver notional. None bounds
cumulative activity, so this passes every check indefinitely:

```
open 100 shares   (within size limit, gated, approved)
close             (exit-class, ungated, unrefusable)
open 100 shares   (gated again, approved again)
...
```

No single order is wrong. The damage is commissions, slippage and market impact
bleeding out over hours, plus a wash-trade pattern that looks bad to a broker.
`max_signals_per_hour` is the nearest existing control and counts ORDERS, not
value: 20/hour is trivial at $500 each and $1M at $50k each.

**Why it can exist at all.** The obvious objection is that a turnover cap could
refuse an exit, which breaks the rule the whole system rests on. It resolves
cleanly: **you cannot churn without opening.** Cap the OPENING side and the loop
halts at the cap while every close stays untouchable. That also means the check
drops into `risk_gate.evaluate()` and inherits exits-exempt, tri-state
recording, and fail-closed behaviour with no new exemption path — which matters,
since three of the four bugs found on 2026-07-27 were exemption paths
disagreeing with each other.

**A prerequisite that nearly made it useless.** `ORDER_SUBMITTED` recorded
`price=order.lmtPrice`, and ib_async leaves an unused numeric field at
`UNSET_DOUBLE` = `sys.float_info.max`. Verified in the live event store: market
submissions sit at `1.7976931348623157e+308`. Nothing looked broken, because the
only consumer was the order-rate limit, which counts rows and never reads value.

The sentinel is the dangerous part, and not for the predicted reason. It is
FINITE and POSITIVE, so it passes the natural `isfinite(price) and price > 0`
guard — including the first version of the new valuation. At a quantity of 1.0
the product stays finite, so that version reported a $1.8e308 notional as VALID.
One of those in a cumulative total exceeds every conceivable cap forever: not a
fail-open, a system-bricking fail-closed. Now rejected in the pure kernel AND
stripped at the boundary, and pinned in `tests/invariants/test_order_notional.py`.

**Design.**

| decision | choice |
|---|---|
| Unit | Notional USD (shares are not comparable across instruments) |
| Scope | Per-strategy AND account — per-strategy catches a runaway, the account cap catches ten strategies each just under their own |
| Window | Per trading day: predictable reset, and a refusal an operator can explain |
| Source | Event store, so it survives restarts. In-memory counters would reset on every crash, and `supervise()` restarts automatically — a crash loop would hand back a fresh budget each time |
| Default | `0.0` = OFF, so deploying is byte-identical until an operator opts in |

**Calibration decides whether it works.** A cap that trips in normal operation
gets raised or switched off, and then protects nothing. Measured from 7 live
paper days: peak day $5,856 (mostly manual probe traffic that day), peak genuine
strategy-day $1,241. Recommended values are in `config_defaults/trader.yaml`
with that rationale attached, and the instruction to re-measure rather than
copy them.

**Operational note, found by enabling it live.** The cap refuses opens while
ANY of the day's opening submissions cannot be valued, since a lower bound is
not a bound. History written before this change carries no notional, so
switching the cap on mid-session refuses every open for the rest of that day
and then clears by itself at the day boundary. Verified on the deploy day:
approving a 1-share proposal returned "14 of today's opening submissions cannot
be valued, so today's turnover (>= $1,859) is only a lower bound". Intended
fail-closed behaviour, and the reason the live config was left OFF rather than
suppressing a session's trading without the operator asking for it. Enable at a
day boundary.

**What it does not cover.**

* Forex (`sec_type` CASH) is exempt, for the same reason concentration exempts
  it — a currency conversion is not a position and its notional is not
  comparable. Turnover in a CASH pair is real, so this is a gap, not a
  judgement that it does not matter.
* A single oversized order: that is sizing, the approver tier, the risk gate.
* A bad position held too long: that is the protective stop.
* Cancel/replace storms on the EXIT side: bounded in notional (you can only
  close to flat) but not in order count, and the count limit skips exits.

**Verification.** 27 behavioural tests, and the mutation gate did the real work.
The first measurement dropped both touched modules well below baseline (87.7% /
85.6% against 95.9% / 90.9%) with 51 survivors, nearly all genuine test gaps
rather than equivalents. Two findings came out of the code rather than the
tests: a fail-closed branch that was UNREACHABLE (concentration already refuses
that input upstream), i.e. protection-shaped dead code, now removed with a note
so it is not re-added; and a zero-notional order that was refused and now
correctly contributes nothing. Final: 97.3% / 91.9%, both above baseline, 8
documented equivalents. The full survivor triage is in
`scripts/run_mutation.sh`.

---

## Flip residual CLOSED — order-splitting (2026-07-27)

The last documented hole in the exit-class rule. Exit-class is direction-aware
and not size-clamped, so with 3 shares held a `SELL 5` was labelled an exit and
all five shares skipped every gate: three closed a position, two opened a short
that nothing checked.

**Confirmed live before fixing.** `mmr sell QBTS --limit 19.40 --quantity 5`
against 3 held was accepted and submitted with no refusal from the trading
filter, the leverage check, the risk gate, the approval requirement or the
approver tier. It failed to execute only because IB held it for the next
session, which was a separate bug (the missing `outsideRth`, fixed in the same
session). The resting order was cancelled before it could fire unattended.

**Why "clamp the classifier" is the wrong fix.** Clamping makes an oversized
close *refusable*, and a refusal blocks the whole order including the
legitimate reduction. Someone fat-fingering a close in a falling market would
have their exit rejected for being too big. Refusing a reduction is worse than
any limit the refusal would enforce, which is the rule that created the
residual in the first place.

**The fix.** The order was always two economically different things wearing one
label. `trader/trading/order_split.py` decomposes it:

```
held +3, SELL 5  ->  reduce 3  (exit-class, ungated, never refusable)
                   + open   2  (new short exposure, fully gated)
```

- The reduction is placed **first**. A refused remainder therefore leaves the
  caller flat rather than stuck in the position they asked to leave.
- The remainder carries `force_open=True` so it cannot be re-classified as an
  exit by a position read that still shows the pre-reduction size.
- Both order paths split: `place_expressive_order` and the direct
  `place_order_simple` (the path a human uses, and the likeliest place for an
  oversized close to be typed by accident).
- Any exit spec (bracket, trailing stop) rides with the **remainder**, since
  the new position is what needs protection; the reduction is a plain close.

**It did not work the first time.** Splitting was correct and still left the
hole open on the direct path, because a gate UPSTREAM of the splitter had
already exempted the order. The RPC layer asked
`order_reduces_exposure(contract, action, FULL quantity)` — direction-aware and
not size-clamped, so `SELL 3` against 1 held answered "exit" and the whole
order skipped `require_proposal_approval`, opening half included. The splitter
downstream then placed both halves past a gate that had decided not to look.

Found live within minutes of deploying the split, by running the control
alongside the probe:

| probe | held | result |
|---|---|---|
| `mmr sell GOOGL --limit 900 --quantity 3` | 1 | **placed both halves** |
| `mmr sell AAPL --limit 900 --quantity 2` | 0 | correctly refused |

Same path, same session, same config. Without the control the first result
looks like the split working.

The fix makes `Trader.split_for_order()` the ONE place that answers "how much
of this opens exposure" — the proposal gate, the approver notional tier and
both order paths now call it, and the gate passes `allow_open=False` down
rather than computing a quantity upstream and trusting it. The tier's
`_opening_exposure_quantity` had been a second implementation of the same
question, resolving the position slightly differently; it is now a thin
accessor.

**The transferable lesson.** A decomposition is only as strong as the agreement
between everyone who classifies the thing being decomposed. Two components each
asking a locally reasonable question, in the wrong order, restored the exact
hole the decomposition removed. Look for the OTHER askers before declaring a
classification bug fixed.

**Applying that lesson immediately found a third.** An audit of every caller of
`order_reduces_exposure` turned up `enforce_approver_tier` doing the same
thing: it exempts anything the classifier calls a reduction, and a split flip's
opening half still reads as one, because the reduction it follows may not have
filled. So the notional tier — the control meant to stand between a compromised
proposer context and a large position — handed the remainder the very exemption
the split exists to remove. Measured: a $50,000 opening short with no approver
key was EXEMPTED as a flip remainder while the identical order from flat was
refused.

Latent rather than live, since the tier is off by default
(`approver_required_above_usd = 0`). Fixed by passing `force_open` through, not
by recomputing the opening remainder there: a second position read that came
back unreadable would gate a GENUINE exit, which is the failure this tier's
design deliberately avoids. Pure exits keep their exemption; the three cases
are pinned together so a later "simplification" cannot trade one for the other.

Worth noting how the two were found. The RPC gate needed a live probe WITH A
CONTROL — the flip alone looked like the split working, and only the plain open
short beside it showed the difference. The tier needed no probe at all, just
the grep the first bug earned.

**Verification.** Pure, `deal`-contracted (conservation, non-negativity, and
"the reduction never exceeds the position"), CrossHair-clean, 95.4% mutation
with three proven equivalents. The `deal` postcondition then rejected exact
float conservation as unachievable (`held=-1.2890519581908961`,
`BUY 513.7350762451457`): one half is pinned to `|held|` and the other is a
subtraction, so they can miss by an ULP. The spec was revised to be asymmetric
and STRICTER where it counts — summing to MORE than the request is refused
exactly, summing to less is tolerated to one ULP — and `split_order` steps the
opening half down to guarantee the exact half. Mutation testing then showed
that step-down was never exercised: it only runs when `qty > 2*|held|`, which
at ordinary sizes forces a remainder far above 1.0, so its sub-1.0 path is
reachable only at denormal magnitudes. Pinned. `tests/invariants/test_order_split.py` pins both
directions, because each alone is satisfiable by a broken splitter: one that
puts everything in `open_qty` closes the hole and makes exits refusable, and
one that puts everything in `reduce_qty` preserves exits and leaves the hole
untouched. Conservation (the halves sum to the request) is the property that
catches both, plus any arithmetic slip.

**The complementary control, turnover caps, is now BUILT** (2026-07-27) — see
the section below. Splitting bounds *classification*; a turnover cap bounds
*volume* regardless of classification, which limits the damage from any ungated
path rather than this one specifically.

---

## Verification hardening, second pass — SHIPPED (2026-07-26)

Two gaps, both found by asking the same question the first pass asked ("does the
tool actually see this?") of the code rather than the tooling.

- **The sizer could not tell "offline" from "worthless".** `PortfolioState`
  carried a bare `net_liquidation: float = 0.0`, and `sdk._get_portfolio_state()`
  returns a default instance when the RPC fails — the documented offline
  `propose` path. So a real account reading zero and an unreachable
  trader_service were the same input, and the zero case bypassed the percentage
  cap and returned the flat default size against a cap of $0. Fixed with the
  `*_evaluable` flag pattern `RiskInputs` already uses: `evaluable=False`
  (the default) keeps the offline fallback byte-identical, `evaluable=True` with
  a non-positive value refuses to size. Zero test breakage, versus 45 for the
  naive fix that refused on the value alone.

  The spec's Hypothesis strategies could not reach the degenerate region at all
  — widening the range barely helped (a uniform draw over [0, 5M] lands in (0,1]
  about once in five million), so the generators became a MIXTURE sampling the
  bands explicitly. A property was also missing from the other side: a measured
  POSITIVE account must never be refused as worthless. Cap properties
  structurally cannot catch an over-eager refusal, because refusing always
  satisfies an upper bound. Mid-change the mutation gate FAILED on a regression
  this introduced — the first time it caught something rather than merely
  recording a number.

- **The structural check guarded the wrong path.** `OrderValidator` rejects
  malformed orders (bad action, non-positive/NaN quantity, a priced order type
  with no price) and has done so correctly for years. It ran only under
  `ExecutorCondition.SANITY_CHECK`, whose sole caller is `place_order_simple` —
  `mmr buy` / `mmr sell`, the path with a human watching. Every automated order
  (approve, the AutoExecutor's opens and closes, every bracket leg, the
  protective stop) reaches IB through `place_expressive_order` →
  `subscribe_place_order_direct` and was never structurally checked. Meanwhile
  `place_expressive_order` takes `quantity: float` from the client without
  checking it is positive or finite, and `ExecutionSpec.validate()` rejects a
  missing limit price but not a zero one — the same "the server must not trust
  client numbers" argument that motivates the approver notional tier.

  The decision is now `trader/trading/order_structure.py`: pure, `deal`-
  contracted, CrossHair-clean, mutation-tested (84.3% → **96.6%**, three
  documented equivalents), and enforced at the CHOKEPOINT so every path is
  covered by construction. It applies to exits too, which is not a breach of
  "an exit is never refused": a malformed order is not a working exit — a SELL
  of NaN shares reduces nothing, IB rejects it from the far side of the wire,
  and refusing it here at least names the reason. `tests/invariants/
  test_order_structure.py` pins both directions, because "nothing malformed
  reaches the broker" is trivially satisfied by a validator that refuses
  everything — and that failure mode (a close that cannot be placed) is the one
  this codebase treats as worse than any limit breach.

  Every survivor of the first measurement was a `getattr` DEFAULT mutant, i.e.
  the behaviour when an order object is MISSING a field — untested, and six of
  the fourteen changed real behaviour (three turned a refusal into an
  `AttributeError` inside the placement path; three made the default permissive,
  so an order with no quantity places one share and one with no limit price
  places a limit nobody chose). One property killed eleven of them.

- **The disaster stop could be placed AT the entry price.** The broker-side GTC
  STP SELL on every attributed open is the only protection that survives a dead
  feed or a dead strategy_service while holding — the stale-bar gate guards
  opens, nothing else guards a position already on the book. Its arithmetic lived
  inline in `AutoExecutor._ensure_protective` between two SDK calls, and every
  test of it used a $100 entry and a healthy position.

  Extracted to `trader/trading/protective_stop.py` (pure, contracted,
  CrossHair-clean, 81.8% → **92.3%** mutation) and measuring it immediately
  found a real bug: `round(avg_cost * (1 - pct/100), 2)` returns the entry price
  itself for any entry under about $0.0625 at the default 8% —
  `round(0.05 * 0.92, 2) == 0.05` — so the disaster stop sold at market the
  moment IB accepted it. Now FLOORS (rounding may only ever step away from
  entry) and refuses outright when no two-decimal price sits strictly below
  entry; the caller retries next bar. The Hypothesis property then found a
  second: flooring alone does not guarantee "never nearer than requested",
  because `target * 100` can round up onto an exact integer (at
  99999.99999999999 and 1.00001% it lands on 9899999.0). Fixed with the same
  step-down loop `order_math._floor_shares_for_notional` uses, and pinned.

  The contracts state the two things it must never do: **oversell** (an
  oversized protective SELL is exit-class, hence exempt from every gate, and
  would open a SHORT out of the mechanism whose job is to close one) and **sit
  at or above entry**. The spec states the converse too — a normal position
  always gets a stop — since both safety properties are satisfied by a function
  that plans nothing, and a naked position is what the mechanism exists to
  prevent.

- **The never-oversell clamp was not a clamp.** Both the executor's close path
  and the protective stop wrote `min(attributed_qty, broker_qty)` and then
  guarded with `if qty <= 0`. Neither survives a NaN: `min(140.0, nan)` returns
  `140.0` (the comparison is False, so `min` keeps its first argument) and
  `nan <= 0` is `False`. An unreadable broker quantity therefore passed the
  clamp AND the guard and would have sold the full attributed size against a
  position of unknown size — as an exit-class order, so the filter, the
  leverage check, the risk gate and the approver tier would all have waved it
  through. Now one contracted definition (`order_math.reducible_quantity`,
  CrossHair-checked) used by both call sites: every non-number, NaN, inf, zero
  or negative input yields 0.0.

- **Reconciliation could disarm every protective stop it owns.** The first-work
  reconcile marks attributed positions absent at the broker as
  `CLOSED_EXTERNALLY` and cancels their stops. It read absence from an empty
  dict — and `Trader.get_positions` falls back to MMR's own portfolio cache
  when `ib.positions()` is empty, with both empty for the first moments after
  trader_service connects. That is exactly when strategy_service starts pushing
  bars and the executor reconciles. The failure is silent and permanent: real
  positions lose their attribution (so no strategy will ever close them) and
  lose their disaster stop (so nothing protects them), and nothing
  re-attributes. An all-empty read is now inconclusive until it has persisted
  for `MMR_EMPTY_BROKER_GRACE_S` (default 120s); a readable book resets the
  clock. Bounded rather than unlimited, because a stale attribution blocks new
  opens — refusing forever would trade one silent failure for another.

  Tooling note: CrossHair crashed on this target (`ValueError: Exceeds the limit
  (4300 digits)`) because realizing a symbolic float can make z3 return an exact
  rational with thousands of digits, which CPython refuses to stringify. The
  gate fails closed, so a tooling limit read as a contract violation.
  `scripts/crosshair_check.py` now raises `PYTHONINTMAXSTRDIGITS` in the checker
  subprocess.

---

## Tranche 2 — designed, not built

Priority order is roughly as listed; items are independent unless noted.

**Verification-deepening pass (2026-07-23, in progress):** items **3** (deal
contracts + CrossHair), **5** (strategy manifest), plus **mutation testing** of
the kernel + invariants and **widened `ty` coverage** are being built this
session. Items **1** (leverage check on the direct path) and **2** (strategy
runtime isolation ladder) are **explicitly deferred to a future session** — the
two next-up items after this pass.

1. **Leverage check on the direct order path.** `place_order_simple` /
   `executioner.place_order` currently run filter + risk gate but not the
   whatIf margin/leverage check that `place_expressive_order` has; unify so
   every exposure-increasing path is leverage-checked. **(Deferred — next up.)**
2. **Strategy runtime isolation ladder.** Step 1: the gauntlet's import
   allowlist becomes *enforced at load time* (today it's a deploy-time scan —
   a passing file could still `__import__` dynamically). Step 2:
   per-strategy **no-network subprocess workers**, so a strategy cannot
   reach the SDK, the message bus, or the internet even in principle.
   **(Deferred — next up. Arguably a bigger real-world risk than any missing
   proof tool: the gauntlet is deploy-time, but at runtime a passing strategy
   is still arbitrary in-process Python.)**
3. **Deal contracts + CrossHair** on the pure kernels (`order_math`, sizing
   clamps, gate arithmetic): machine-checked pre/postconditions. CrossHair is
   *symbolic* — strictly stronger than Hypothesis's sampling on the pure cores;
   the contracts serve as both the CrossHair oracle and a Hypothesis oracle.
   `deal` is a new **runtime** dependency (contracts live on runtime functions)
   → deploying it needs a container image rebuild, like `pydantic`.
4. **Grow-resize reviewed path.** A deliberate, review-carrying flow for
   `resize-positions --min-bound` BUY deltas (today refused under
   `require_proposal_approval` — see tranche 1 known consequence).
5. **Strategy YAML manifest.** Each strategy declares its universe,
   direction, and expected turnover in `strategy_runtime.yaml`; the runtime
   enforces `min(declared, global)` — a strategy that declared "long-only,
   ≤ 3 orders/day, these 2 conids" physically cannot short, churn, or trade
   anything else, whatever its code does. This is the *strategy* verifiability
   lever — strategy profitability is not provable (markets aren't a closed
   world; statistical confidence is the honest ceiling), but strategy
   *behavior* can be made a checked invariant.

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
