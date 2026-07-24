---
name: mmr-verification
description: The MMR safety-verification workflow — how to change trading-critical code without breaking the invariants that keep the book safe. Use when modifying the risk gate, order construction, position sizing, the proposal state machine, the strategy gauntlet, or anything under trader/trading/; when adding or changing a strategy; or when a type-check / property / gauntlet gate fails and you need to know how to respond. Covers the ty type-check gate, the human-owned invariants spec, the Hypothesis property layer, deal contracts + CrossHair symbolic execution on the pure kernel, mutmut mutation testing, the strategy gauntlet, the pre-commit spec-protection guard, and the counterexample-to-regression norm.
metadata:
  author: mmr
  version: "1.0"
---

# MMR Verification Skill

MMR trades real money behind a double-arm. Safety here is not "write careful
code" — it's a set of gates that make the unsafe change fail loudly before it
ships. This skill is the operating manual for those gates. Read it before
touching trading-critical code; follow it when a gate goes red.

The governing idea: **convert conventions into invariants that can't be
silently skipped.** A rule enforced by "remember to check X" rots; a rule
enforced by a type, a property, or a gate cannot.

## The division of labor (read this first)

- **`tests/invariants/` is the human-owned spec.** It states the safety
  properties as executable checks (exit-class orders are never refused, gates
  fail closed, a sized order never exceeds its notional, no-PASS strategies
  can't arm, the proposal state machine's terminal states are immutable).
- **You own the implementation; you do NOT own the spec.** If your change
  makes an invariant go red, the implementation is wrong — fix it. Do **not**
  weaken, delete, or loosen a property to make code pass. A pre-commit guard
  (`scripts/invariants_guard.py`) refuses any commit that stages both
  `tests/invariants/` and `trader/**`, precisely so a property can't be edited
  in the same breath as the code it checks. Changing the spec is a deliberate,
  separately-reviewed human act.
- **Every counterexample becomes a pinned regression.** When a property (or a
  field incident) surfaces a concrete failing case, add that exact case as a
  named regression test *before* landing the fix. It stays forever.

## The gates, and how to run them

Run everything through **`uv run`** (the project is uv-managed; `ty` and
`pre-commit` are dev-only and never enter the container).

### 1. Type-check gate — `ty` on the kernel

`trader/trading/` is the enforced scope. The capability types (e.g. the
`ApprovedOrder` token the gate mints and the executioner requires) make
"this order went through the gate" a type invariant — a path that skips the
gate cannot construct the argument, and `ty` says so.

```
uv run ty check trader/trading/            # see all diagnostics
uv run python scripts/ty_gate.py           # the gate: fails on NEW diagnostics
```

The gate baselines the pre-existing diagnostics (`scripts/ty_baseline.json`)
and fails only on regressions, so new code must type-clean while the legacy
set is burned down per-file. When you legitimately reduce the count by fixing
a file, re-record the baseline — a human-reviewed act:

```
uv run python scripts/ty_gate.py --update
```

Do not add blanket `# ty: ignore`. A genuine false positive (e.g. `**kwargs`
unpacking, ib_async stub strictness) gets a *targeted* `# ty: ignore[rule]`
with a one-line reason; anything that looks like a real type error is a real
type error until proven otherwise.

### 2. Property layer — Hypothesis

Properties live in `tests/invariants/`. They assert universally-quantified
facts, not examples. Run them like any test, in the canonical env:

```
~/miniforge3/envs/mmr/bin/python3 -m pytest tests/invariants/ -q
```

When Hypothesis prints a falsifying example, that example is the spec telling
you the code is wrong. Reproduce it, add it as a pinned regression, then fix.

### 3. Contracts + symbolic execution — `deal` + CrossHair (pure kernel)

The pure kernel functions carry `deal` pre/postconditions —
`order_math.whole_shares_for_notional` / `_floor_shares_for_notional`, the
`position_sizing` helpers (`_confidence_scale` / `_volatility_multiplier` /
`compute_atr`), and `data/proposal_transitions.py`
(`is_valid_transition` / `is_known_status`). They run at runtime (a violation
raises) and are the spec CrossHair checks *symbolically*:

```
uv run python scripts/crosshair_check.py
```

CrossHair reasons about whole input domains, not sampled examples (it already
caught a denormal `price*multiplier` underflow that crashed instead of
refusing). A counterexample is either a real bug (fix + pin the case) or a
mis-stated contract (correct the `@deal.pre`/`@deal.ensure` to the true
documented behavior) — **never weaken a contract to hide a bug**. When you add
or change a contracted pure function, add its `@deal` contract and re-run this.
It's also a manual-stage pre-commit hook (too slow per-commit):
`uv run pre-commit run crosshair-check --hook-stage manual`. Note: `deal` is a
RUNTIME dep — deploying contracted code needs a container image rebuild.

### 4. Mutation testing — `mutmut` ("verify the verifier")

Confirms the tests actually *catch* bugs, by mutating the kernel and checking
the suite fails. Config lives in `pyproject.toml` `[tool.mutmut]` (scope = the
pure kernel). Run and score:

```
scripts/run_mutation.sh                     # run the pass (all 4 kernel modules)
uv run python scripts/mutation_score.py     # per-module killed/survived table
```

**Always via `scripts/run_mutation.sh`, never a bare `mutmut run`** — mutmut 3.x
silently *skips* every `@deal`-decorated function (which is the entire
contracted safety kernel: `whole_shares_for_notional`, the `position_sizing`
helpers, the proposal transitions) and reports a false 100%. The runner patches
mutmut to mutate their bodies (never the contract lambdas). A naive run gives a
reassuring score that tested nothing that matters.

A surviving mutant = a code change no test noticed = a real test gap (add a
test that kills it) OR a documented **equivalent** mutant (state the reason
explicitly — an unreachable boundary, a semantically-identical rewrite). Do
**not** edit production code to raise the score — that games the metric; only
ADD tests. Run this after changing a kernel function or its tests.

### 5. Strategy gauntlet — "no hash, no live"

No strategy deploys or arms without a PASS recorded for the exact source hash.
Before deploying/enabling a strategy (especially `auto_execute`):

```
mmr strategies gauntlet strategies/<file>.py --class <Class>
```

Stages: S1 import allowlist (AST — denies socket/subprocess/os/ib_async/
importlib/eval/exec/… and dynamic-import call forms; S1 is a *static advisory
scan, not a sandbox*), S2 lookahead (`assert_no_lookahead`), S3 nasty-market
battery (gaps/halts/NaN/zero-volume — must not crash, must emit well-formed
signals), S4 PSR (record-only unless `--min-psr`). `deploy`/`enable` refuse
without a PASS for the current `(hash, class)`; there is no override flag —
edit the file, re-run the gauntlet.

### 6. Full suite

```
~/miniforge3/envs/mmr/bin/python3 -m pytest tests/ --timeout=60 -q --ignore=tests/test_ibrx_async.py
```

## The workflow, end to end

1. **Before editing** trading-critical code, know which invariant governs it.
   If none does and the change is safety-relevant, the property is missing —
   flag it (a human adds it to `tests/invariants/`).
2. **Make the change** in the implementation only.
3. **Run the gates**: `ty_gate.py`, the invariants suite, the full suite. A
   red property means your code is wrong, not the property.
4. **Any counterexample** → pinned regression test first, then fix.
5. **Commit** — the pre-commit hooks (`uv run pre-commit run --all-files` to
   check ahead of time) enforce the ty gate and the spec-protection guard. If
   the guard fires, split your spec and implementation changes into separate
   commits.
6. **Deploying trading code** (not dev tooling) follows the operational
   checklist in `docs/OPERATIONAL_STATE.md`: outside market hours, snapshot
   DBs, sync/rebuild, restart, `mmr verify`, gauntlet the roster in-container.

## What NOT to do

- Do not weaken a property, widen an allowlist, or raise a limit to make a
  test pass. That inverts the entire safety model.
- Do not gate-bypass in trading code (`skip_risk_gate` is already deprecated
  and ignored server-side; exit-class classification, not a flag, decides what
  the gate exempts).
- Do not add a runtime dependency casually — it forces a container image
  rebuild and widens the trusted surface. Dev tooling (`ty`, `pre-commit`)
  stays dev-only.
- Do not silence `ty` broadly. Targeted, reasoned `# ty: ignore[rule]` only.

See also: `docs/SAFETY_ROADMAP.md` (what shipped and what's next),
`CLAUDE.md` (architecture + the exit-class boundary), and the `mmr-skill`
skill (operating the platform).
