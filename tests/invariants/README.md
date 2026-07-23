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

## Contents

| File | Invariant of record |
|------|---------------------|
| `test_proposal_machine.py` | Proposal state machine: exactly the documented transition table; terminal rows immutable (status, metadata, deletion-without-force); proposals are born PENDING. |
| `test_order_notional.py` | `whole_shares_for_notional` never returns a quantity whose notional exceeds the sized amount; refuses (never 0, never bump-to-1). |
| `test_sizing_properties.py` | PositionSizer: amount respects every active cap; ATR↑ ⇒ size non-increasing; confidence↑ ⇒ size non-decreasing; amount ≥ 0; degenerate spread configs never crash. |
| `test_gate_properties.py` | RiskGate: approval implies no failed check; not-evaluable critical inputs refuse opens naming the missing datum; exit-class orders are never refusable by gates; rate limit counts ORDER_SUBMITTED only. |

Deeper example-based cases live in the ordinary unit-test files
(`tests/test_order_math.py`, `tests/test_proposal_store.py`, ...); the files
here are the invariants of record.

## Running

```bash
~/miniforge3/envs/mmr/bin/python3 -m pytest tests/invariants/ -q --timeout=120
```

Requires `hypothesis` (in the `test` extra: `pip install -e '.[test]'`).
