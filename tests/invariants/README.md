# tests/invariants — the human-owned spec

This directory is the Stage-1 "small spec, large implementation" layer: a
compact set of properties that state what the trading system **must** do,
independent of how any implementation happens to do it. The implementation is
large and machine-written; this spec is small and human-owned. When the two
disagree, the spec wins by default.

## Policy (binding on agents and humans alike)

1. **Agents must not modify existing properties to make an implementation
   pass.** Weakening a property — loosening a bound, narrowing a generator,
   deleting an assertion, adding an `assume()` that dodges the failing region —
   requires explicit human review and sign-off. An agent that believes a
   property is wrong must say so in its report and leave the property intact.

2. **A failing property is a finding, not an obstacle.** If a property uncovers
   a real bug, do not weaken the property. Mark the property
   `xfail(strict=True)` with a reason naming the bug, list the bug in the
   report's concerns, and fix the implementation in a separate change. The
   `strict=True` makes the xfail expire loudly the moment the bug is fixed.

3. **Every counterexample Hypothesis finds becomes a pinned regression test
   before the fix lands.** Shrunk counterexamples get promoted to named,
   deterministic test cases (e.g. the BRK.A case in `test_order_notional.py`)
   so the exact failure can never silently return — even if the property is
   later reorganised.

4. **Properties assert what is TRUE and DOCUMENTED.** The spec here mirrors
   documented behaviour (docstrings, CLAUDE.md, module headers) — for example
   the proposal transition table is written out literally in
   `test_proposal_machine.py` rather than imported from the implementation,
   so a change to the implementation's table cannot silently rewrite the spec
   into a tautology.

5. **Tolerant comparisons where the docs promise tolerance.** Sizing amounts
   are rounded to cents and have documented floors/clips, so monotonicity
   properties use small epsilons rather than strict inequalities. A property
   must not be *stricter* than the documented contract either — that produces
   noise that trains people to ignore this directory.

6. **A property may only be revised through the four-point protocol below.**
   Occasionally a property turns out to be wrong rather than the code: most
   often because it is unsatisfiable, or because it contradicts another
   property in this directory. That is a real category and it needs a route,
   or the pressure to "just adjust the test" finds an unofficial one. The
   route requires the agent to supply evidence a human can check in seconds,
   and to reduce the judgement call to a single sentence.


## Revising a property: the four-point protocol

The spec guard (`scripts/invariants_guard.py`) stops an agent changing a
property and its implementation in one commit. It does not stop an agent
persuading a human to accept a bad revision across two commits. The guard is
mechanical; this protocol is what makes the review real.

An agent proposing a revision MUST supply all four. The human only has to judge
the fourth.

1. **The counterexample, as runnable values.** Not a description. The concrete
   inputs that break the property.

2. **A falsification the reviewer can run in seconds.** Output, not argument.
   Something that demonstrates the old form cannot hold, ideally by showing
   there is no implementation that satisfies it rather than that this one does
   not.

3. **What the new form still catches.** Everything the old one did, or an
   explicit list of what it no longer catches.

4. **What is now allowed that was not.** One sentence, in units the reviewer
   cares about. THIS is the line to judge. If it is acceptable, the revision is
   sound. If it is not, the answer is to change the design, not to restore an
   unsatisfiable property.

If a revision cannot produce point 2, it is not a revision. It is a weakening,
and policy 1 applies.

### Worked example (2026-07-27, `test_order_split.py`)

The conservation property was written as exact float equality:

```python
assert plan.reduce_qty + plan.open_qty == qty
```

1. **Counterexample:** `held = -1.2890519581908961`, `BUY 513.7350762451457`,
   found by the `deal` postcondition under Hypothesis.

2. **Falsification:** with the reduction pinned to the full position
   (`1.2890519581908961`), no float value of `open_qty` within 6 ULP in either
   direction makes the halves sum to the request. The only way to reach exact
   equality is to let the reduction grow to `1.289051958190953`, which is
   LARGER than the position being closed. That is an oversell, and this same
   file already pins "the reduction never exceeds the position it claims to
   reduce". The old property did not merely fail to hold; it contradicted a
   more important property beside it.

3. **Still caught:** everything. Summing to MORE than the request is now
   refused with no tolerance at all, which is the direction that invents
   exposure out of rounding.

4. **Newly allowed:** a split may lose up to one ULP of the request (about
   1.1e-13 shares on a 513-share order). It may not gain any.

Point 4 was the whole review. The revision was accepted on that sentence.

## Contents

| File | Invariant of record |
|------|---------------------|
| `test_approved_order.py` | The IB placement chokepoint accepts only an `ApprovedOrder`, which only `mint_approved_order` can construct, so a path that never met the gate cannot build the argument. |
| `test_auto_execute_decision.py` | Auto-execution arming: kill switch, live double-arm, RUNNING/paper_only, per-bar dedup, cooldown, stale-bar gate. Closes are never gated. |
| `test_exit_class.py` | The exit-class decision: direction-aware, not size-clamped; an order that reduces the live position is never refusable. |
| `test_gate_properties.py` | RiskGate: approval implies no failed check; not-evaluable critical inputs refuse opens naming the missing datum; exit-class orders are never refusable by gates; rate limit counts ORDER_SUBMITTED only. |
| `test_manifest.py` | The strategy manifest is an OPENS-ONLY envelope: allowed conids, direction and per-strategy turnover may refuse an ENTRY, but must be structurally incapable of blocking an exit. |
| `test_order_notional.py` | `whole_shares_for_notional` never returns a quantity whose notional exceeds the sized amount; refuses (never 0, never bump-to-1). An unvaluable order is distinguishable from a $0 one, and a float sentinel is never mistaken for a price. |
| `test_order_split.py` | A position-crossing order decomposes into an unrefusable reduction and a gated remainder; the halves never exceed the request; the reduction never exceeds the position. |
| `test_order_structure.py` | Structural refusals before IB: non-positive or non-finite quantity, bad action, priced order type with no price. Applies to exits too. |
| `test_proposal_gate_split.py` | No opening quantity survives `require_proposal_approval`, at any size from any position, AND a reduction is never blocked. One resolver answers "how much of this opens exposure" for every gate. |
| `test_proposal_machine.py` | Proposal state machine: exactly the documented transition table; terminal rows immutable (status, metadata, deletion-without-force); proposals are born PENDING. |
| `test_protective_stop.py` | The disaster stop never oversells and never sits at or above entry. |
| `test_sizing_properties.py` | PositionSizer: amount respects every active cap; ATR↑ ⇒ size non-increasing; confidence↑ ⇒ size non-decreasing; amount ≥ 0; degenerate spread configs never crash. |

Deeper example-based cases live in the ordinary unit-test files
(`tests/test_order_math.py`, `tests/test_proposal_store.py`, ...); the files
here are the invariants of record.

## Running

```bash
~/miniforge3/envs/mmr/bin/python3 -m pytest tests/invariants/ -q --timeout=120
```

Requires `hypothesis` (in the `test` extra: `pip install -e '.[test]'`).
