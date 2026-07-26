# A Tutorial on MMR's Verification Toolchain

**Audience:** you know Python and OO programming. You've written unit tests. You
have not used design-by-contract, property-based testing, symbolic execution, or
mutation testing, and you'd like to know what they actually do — and why a
trading system bothers.

Everything below is real code from this repository and real output from these
tools. Where a tool caught a bug, the bug was real.

---

## Table of contents

1. [The problem with tests](#1-the-problem-with-tests) · [finance primer](#a-five-minute-finance-primer)
2. [Layer 0 — Types that encode permission (`ty`)](#2-layer-0--types-that-encode-permission-ty)
3. [Layer 1 — Property-based testing (Hypothesis)](#3-layer-1--property-based-testing-hypothesis)
4. [Layer 2 — Contracts (`deal`)](#4-layer-2--contracts-deal)
5. [Layer 3 — Symbolic execution (CrossHair)](#5-layer-3--symbolic-execution-crosshair)
6. [Layer 4 — Mutation testing (`mutmut`)](#6-layer-4--mutation-testing-mutmut)
7. [Layer 5 — Gates that cannot lie](#7-layer-5--gates-that-cannot-lie)
8. [Putting it together: how a change flows through](#8-putting-it-together)
9. [Running everything](#9-running-everything)
10. [When *not* to reach for these](#10-when-not-to-reach-for-these)

---

## 1. The problem with tests

Here is an ordinary, sensible unit test:

```python
def test_shares_for_amount():
    assert whole_shares_for_notional(1000.0, 100.0) == 10
```

(*Notional* is the cash value of an order — `quantity × price`. This function
answers "I want to spend $1,000 and the price is $100 — how many whole shares?"
There is a short [finance primer](#a-five-minute-finance-primer) below if terms
like this are unfamiliar.)

It passes. It will keep passing. And it tells you almost nothing, because it
checks *one point* in a two-dimensional input space that you chose while you
already believed the code was correct.

This matters more than usual here. MMR converts a dollar amount into a share
count, and a wrong answer spends real money. The bug that motivated
`order_math.py` was exactly this shape: an ad-hoc `round(amount / price)` plus a
"bump to at least 1 share" turned a ~$340 sized order on a >$510 stock into a
full share — an overspend, produced by code that passed its tests.

The tools below each attack a different blind spot:

| Blind spot | Tool | What it is |
|---|---|---|
| "I only tested the inputs I thought of" | **[Hypothesis](https://hypothesis.readthedocs.io/)** ([repo](https://github.com/HypothesisWorks/hypothesis)) | property-based testing — generates inputs and shrinks failures |
| "My assumptions live in my head, not the code" | **[deal](https://deal.readthedocs.io/)** ([repo](https://github.com/life4/deal)) | design-by-contract — pre/postconditions that execute |
| "Random sampling never hits the pathological corner" | **[CrossHair](https://crosshair.readthedocs.io/)** ([repo](https://github.com/pschanely/CrossHair)) | symbolic execution — an SMT solver *solves* for a counterexample |
| "My tests pass, but do they actually *check* anything?" | **[mutmut](https://mutmut.readthedocs.io/)** ([repo](https://github.com/boxed/mutmut)) | mutation testing — breaks the code, sees if the tests notice |
| "A whole category of call sites is unprotected" | **[ty](https://github.com/astral-sh/ty)** | static type checker (Astral, same family as `ruff`/`uv`) |

All five are open source and installable from PyPI. Four of them are **dev-only**
and never enter the trading container: `ty`, Hypothesis, CrossHair and mutmut are
things you *run against* the code.

**`deal` is the exception, and deliberately so — it is a RUNTIME dependency.**
Contracts are not a test harness; they execute on every real call in production,
so a violated postcondition raises in the live system rather than passing a bad
value downstream. That is why adding a contracted module needs a container image
rebuild, not just a code sync.

Of the dev-only four, only `ty` runs on every commit — it is the only one fast
enough. The rest run on demand or as manual-stage hooks.

They compose. None replaces unit tests.

### A five-minute finance primer

The examples below are all real trading code, so a handful of terms recur. If
you already know them, skip ahead.

- **Position** — how much of an instrument you currently hold, as a *signed*
  number. `+75` means you own 75 shares (you are **long**). `-75` means you owe
  75 shares (you are **short** — you sold shares you didn't own, and must buy
  them back later). `0` means **flat**.
- **Notional** — the cash value of an order: `quantity × price`. A "notional of
  $5,000" is a $5,000 order, whatever the share price. The word matters because
  position sizing works in *dollars* ("risk $5,000 on this idea") while the
  broker works in *shares*, so something has to convert — and that conversion is
  where money gets lost if it rounds the wrong way.
- **Multiplier** — some instruments trade in bundles. One options contract
  typically covers 100 shares, so its notional is `quantity × price × 100`.
  Forget the multiplier and you under-count your exposure 100-fold.
- **Opening vs closing** — an order that *increases* your position (buying when
  flat or long) takes on new risk. An order that *reduces* it (selling what you
  hold) removes risk. This distinction drives nearly every safety rule below.
- **NetLiquidation** — what the account is worth if you closed everything now.
  Percentage limits ("never put more than 10% in one position") are computed
  against it.
- **Leverage / margin cushion** — leverage is borrowed exposure: holding
  $200,000 of stock against a $100,000 account is 2×. The cushion is how much
  spare equity remains before the broker force-liquidates you.
- **The risk gate** — MMR's pre-trade checks: position too big, daily loss
  exceeded, too many open orders, order rate too high. It can **refuse** an
  order.

---

## 2. Layer 0 — Types that encode permission (`ty`)

> **[ty](https://github.com/astral-sh/ty)** · `uv add --dev ty` · static type checker
>
> **When it runs:** development + every commit. **Never at runtime** — it reads your source, it does not execute it. Zero production impact.

`ty` is Astral's type checker (same family as `ruff`/`uv`). Fast enough to run
on every commit.

### The ordinary use

Catch the boring stuff — `str` where an `int` belongs. Useful, unremarkable.

### The interesting use: making a *rule* into a *type*

MMR's rule is: **every order placed with the broker must have passed the risk
gate.** Normally that's a call-graph convention — you enforce it by everyone
remembering. Instead:

```python
# trader/trading/approved_order.py
_MINT_KEY = object()          # module-private; never exported

class ApprovedOrder:
    """A frozen, mint-only capability token proving an order was authorized."""
    __slots__ = ('contract', 'order', 'is_exit', 'checks', 'exit_reason')

    def __init__(self, _key: object = None, /, *, contract, order, ...):
        if _key is not _MINT_KEY:
            raise RuntimeError(
                'ApprovedOrder is mint-only — construct it via the gate')
        object.__setattr__(self, 'contract', contract)
        ...

    def __setattr__(self, name, value):        # frozen after mint
        raise AttributeError('ApprovedOrder is frozen')

    def __reduce__(self):                      # no pickle reconstruction path
        raise TypeError('ApprovedOrder is not serializable')
```

And the single placement chokepoint accepts *only* that type:

```python
# trader/trading/executioner.py
async def subscribe_place_order_direct(self, approved: ApprovedOrder) -> Observable[Trade]:
```

Now a code path that never reached the gate **cannot construct the argument the
placement function requires**, and `ty` flags the attempt statically. There's a
test that runs the real type checker on a probe to prove it:

```python
# tests/invariants/test_approved_order.py
_TY_PROBE = """
    async def bad(ex: TradeExecutioner):
        await ex.subscribe_place_order_direct(Contract(), Order())   # must be a type error

    async def good(ex: TradeExecutioner):
        tok = mint_approved_order(Contract(), Order(), is_exit=False)
        await ex.subscribe_place_order_direct(tok)                   # OK
"""
```

### Be honest about what this buys

This is the part most write-ups get wrong, so it's worth stating plainly:
`mint_approved_order()` performs **no verification**. It's an unconditional
constructor. So the type proves *"someone called mint"*, **not** *"the gate
ran"*. A new code path could mint without gating and `ty` would be perfectly
happy.

The real enforcement therefore lives where the token is *spent*:

```python
# trader/trading/executioner.py — inside the chokepoint
if not is_exit:
    recorded = approved.checks or {}
    failed = sorted(k for k, v in recorded.items()
                    if str(v).split(':', 1)[0] == 'fail')
    if not recorded or failed:
        return rx.throw(ValueError('placement refused: ...'))
```

An exposure-increasing order must carry the gate's tri-state record, non-empty
and with nothing in the `fail` state. Checking at the *consumption* point rather
than the constructor covers every future mint site automatically — including the
ones nobody remembers to audit.

**Where MMR applies it:** two scopes with separate baselines, so an existing
codebase can adopt type checking without a big-bang rewrite.

```python
# scripts/ty_gate.py
SCOPES = {
    "kernel":   {"dirs": ["trader/trading/", "trader/strategy/auto_executor.py"],
                 "baseline": "scripts/ty_baseline.json"},          # held at ZERO
    "advisory": {"dirs": ["trader/data/", "trader/simulation/",
                          "trader/strategy/strategy_runtime.py", "trader/sdk.py"],
                 "baseline": "scripts/ty_baseline_advisory.json"},  # ratchets down
}
```

The kernel (order construction, risk gate, and the code that actually places
live orders) is held at **0 diagnostics**. Advisory scopes have their existing
diagnostics recorded; a *new* one fails the gate, so the count can only fall.

---

## 3. Layer 1 — Property-based testing (Hypothesis)

> **[Hypothesis](https://hypothesis.readthedocs.io/)** · `pip install hypothesis` · [source](https://github.com/HypothesisWorks/hypothesis)
>
> **When it runs:** test time only. It is a pytest plugin; nothing it does reaches production.

Instead of asserting one input/output pair, you state something true of **all**
inputs, and the library hunts for a counterexample.

### The shape

```python
from hypothesis import given, strategies as st

@given(
    amount=st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
    price=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
    multiplier=st.sampled_from([1.0, 100.0]),
)
def test_notional_never_exceeds_amount(amount, price, multiplier):
    try:
        shares = whole_shares_for_notional(amount, price, multiplier)
    except ValueError:
        # Refusal is only legitimate when one share is genuinely unaffordable.
        assert amount < price * multiplier * (1 + 1e-9)
        return
    assert shares >= 1, 'a returned quantity of 0 must be a refusal, not a result'
    assert shares * price * multiplier <= amount, 'overspend'
```

Read that as English: *either it returns a whole-share quantity whose notional
fits inside the amount, or it refuses. It never returns 0, and it never
overspends.* Hypothesis will try hundreds of combinations, and when it fails it
**shrinks** the counterexample to the smallest one that still breaks:

```
Falsifying example: test_never_returns_more_than_requested(
    price=1.0,
    qty=1,
)
```

### The property that catches what examples can't

Sometimes the *right* property isn't about a value at all — it's about an
**invariance**.

MMR's most important boolean answers: *does this order reduce a position I
already hold, or take on new risk?* An order that reduces one is called
**exit-class**, and exit-class orders are deliberately exempt from every safety
gate — because refusing to let someone close a position is worse than any limit
that refusal would protect. One `True` therefore switches off the trading
filter, the leverage check, the risk gate, the approval requirement *and* the
approver notional tier: five gates at once. [Section 8](#8-putting-it-together)
works through why in full; for now, all you need is that a wrongly-`True` answer
is the worst single outcome in the system.

```python
# tests/invariants/test_exit_class.py
@given(held=_HELD, action=st.sampled_from(['BUY', 'SELL']), q1=_QTY, q2=_QTY)
def test_answer_does_not_depend_on_quantity(self, held, action, q1, q2):
    assert reduces_exposure(action, held, q1) == reduces_exposure(action, held, q2)
```

The classification must be **independent of quantity**. Why phrase it that way?
Because a backdoor needs a threshold, and *no threshold can exist inside a
constant function*. This exact property is what makes the following unhideable:

```python
if act == 'SELL':
    if qty > 1000:
        return True        # unlimited naked shorts, past every gate
    return held > 0
```

That backdoor passed **all 41 invariants tests and all 1,882 suite tests** when
the predicate was only covered by example-based tests — because every quantity
those examples asserted was ≤ 150.

**Where MMR applies it:** `tests/invariants/` is the human-owned executable
spec. Properties there state safety facts: exit-class orders are never refused,
gates fail closed, share conversion never exceeds the sized notional, a strategy
without a PASS record can't arm.

### A caveat you must internalise

Hypothesis only explores the space **your strategies describe**. MMR's sizing
spec had good properties that missed 70 mutants for exactly this reason:

```python
net_liq = draw(st.floats(min_value=10_000.0, max_value=5_000_000.0))  # never 0
price   = st.floats(min_value=1.0, max_value=5_000.0)                 # never < 1
```

With `net_liq ≥ 10,000`, a guard written `> 0` and a guard written `> 1` can
**never disagree**, so mutating one into the other is undetectable. The property
was fine; the *input space* was too polite.

---

## 4. Layer 2 — Contracts (`deal`)

> **[deal](https://deal.readthedocs.io/)** · `pip install deal` · [source](https://github.com/life4/deal)
>
> **When it runs:** ⚠️ **RUNTIME — in production, on every call.** The only tool here that does. A violated contract raises inside the live trading system. That is the feature, not a side effect: see [Why this is more than a fancy assert](#why-this-is-more-than-a-fancy-assert).

A contract states pre/postconditions **as executable code attached to the
function**, instead of prose in a docstring that drifts.

### The basics

```python
import deal

@deal.pure                                              # no I/O, no mutation, no raises
@deal.pre(lambda price, qty: price > 0 and qty > 0)     # caller's obligation
@deal.ensure(lambda _: _.result <= _.qty)               # function's promise
def affordable_shares(price: float, qty: int) -> int:
    return qty + 1                                      # deliberately wrong
```

`_` in `@deal.ensure` is a namespace holding the arguments **and** `result`.
These execute at runtime — here is genuine output:

```
PostContractError: expected result <= qty (where result=6, price=10.0, qty=5)
PreContractError:  expected price > 0 and qty > 0 (where price=-1.0, qty=5)
```

Note the two are different kinds of failure. A `PreContractError` blames the
**caller**; a `PostContractError` blames the **function**.

### Why this is more than a fancy assert

A contract is simultaneously three things:

1. **Documentation that cannot rot** — it runs.
2. **A runtime guard** in production.
3. **A test oracle.** Combine with Hypothesis and you don't even need to write
   the assertion — feed the function generated inputs and the contract does the
   judging. In the run above, `deal` raised *before* the test's own `assert`:

   ```
   E  deal.PostContractError: expected result <= qty (where result=2, price=1.0, qty=1)
   E  Failing test case: test_never_returns_more_than_requested(price=1.0, qty=1)
   ```

### The real thing in MMR

```python
# trader/trading/order_math.py
@deal.has()                                   # side-effect free
@deal.raises(ValueError)                      # the ONLY exception it may raise
@deal.pre(lambda amount, price, multiplier=1.0:
          _all_finite_positive(amount, price, multiplier))
@deal.ensure(lambda _: _.result >= 1 and _.result * _.price * _.multiplier <= _.amount)
def _floor_shares_for_notional(amount: float, price: float, multiplier: float = 1.0) -> int:
    ...
```

That `@deal.ensure` **is** the safety property: *if it returns, you get at least
one whole share and the notional never exceeds the amount.* Note it constrains
only the returning case — when the function raises `ValueError`, `ensure`
doesn't apply, which is exactly right for "refuse rather than overspend".

### A subtlety worth copying

The public wrapper deliberately has **no** `@deal.pre`:

```python
@deal.has()
@deal.raises(ValueError)
@deal.ensure(lambda _: _.result >= 1 and _.result * _.price * _.multiplier <= _.amount)
def whole_shares_for_notional(amount, price, multiplier=1.0) -> int:
    for name, value in (('amount', amount), ('price', price), ('multiplier', multiplier)):
        ...
        if not valid:
            raise ValueError(f'{name} must be a finite number > 0, got {value!r}')
    return _floor_shares_for_notional(amount, price, multiplier)
```

Why? A fatal `@deal.pre` would convert those documented `ValueError`s into
`PreContractError`s and change observable behaviour for every caller. So the
**pure core** carries the precondition (and is what CrossHair verifies), while
the **public function is defensively total** — it validates its own inputs and
fails loudly with the offending parameter named.

The rule of thumb: contract the *pure* parts. `PositionSizer.compute` reads
config and portfolio state and threads a human-readable reasoning string through
many branches — a bad contract target. So its genuinely pure sub-steps were
extracted and contracted instead:

```python
@deal.pure
@deal.pre(lambda min_confidence_scale, confidence: 0.0 <= min_confidence_scale <= 1.0)
@deal.ensure(lambda _: _.min_confidence_scale - 1e-9 <= _.result <= 1.0 + 1e-9)
def _confidence_scale(min_confidence_scale: float, confidence: float) -> float:
    confidence = max(0.0, min(1.0, confidence))
    return min_confidence_scale + (1.0 - min_confidence_scale) * confidence
```

Each encodes "the result stays inside its documented band" — the same shape the
sizing safety story depends on.

---

## 5. Layer 3 — Symbolic execution (CrossHair)

> **[CrossHair](https://crosshair.readthedocs.io/)** · `pip install crosshair-tool` · [source](https://github.com/pschanely/CrossHair)
>
> **When it runs:** on demand, by a developer. Too slow for a commit hook (tens of seconds per function) and never at runtime.

Hypothesis *samples*. CrossHair *solves*.

It runs your function with symbolic values instead of concrete ones and asks an
SMT solver (z3): **"is there any input satisfying the preconditions that
violates a postcondition?"** It explores paths rather than points.

Run it on the buggy demo above:

```console
$ crosshair check demo.affordable_shares --per_condition_timeout 15
/tmp/vtut/demo.py:3: error: false when calling affordable_shares(0.5, 1) (which returns 2)
```

No test written. No inputs guessed. It read the contract and constructed a
violation.

### Why it's worth the extra seconds

Hypothesis draws from distributions that are, sensibly, biased toward realistic
values. Some bugs live where realistic values never go. In MMR, CrossHair found
this:

```python
denom = price * multiplier
ratio = amount / denom          # ← ZeroDivisionError
```

Both `price` and `multiplier` are finite and > 0 — the precondition holds — but
their **product can underflow to exactly 0.0**. For example
`3.0765742648370966e-154 * 3.034084836703205e-308 == 0.0`. That's an undeclared
crash on the single conversion every order path uses, and random sampling would
essentially never generate it.

The fix, plus the counterexample pinned forever as a regression test:

```python
if not math.isfinite(denom) or denom <= 0:
    raise ValueError(
        f'price {price} x multiplier {multiplier} is degenerate ({denom!r})')
```

```python
# tests/invariants/test_order_notional.py
def test_denormal_product_underflow_refuses_not_crashes(self):
    """These are the exact symbolic inputs CrossHair reported."""
    with pytest.raises(ValueError):
        whole_shares_for_notional(2.0, 3.0765742648370966e-154, 3.034084836703205e-308)
```

**This is the workflow, and it's the whole point:** tool finds counterexample →
counterexample becomes a named, deterministic test → *then* the fix lands. The
test outlives the tool run.

### What it is not

It is **not a proof**. Each condition runs under a timeout:

```python
# scripts/crosshair_check.py
TARGETS = [
    "trader.trading.exit_class.reduces_exposure",
    "trader.trading.order_math.whole_shares_for_notional",
    "trader.trading.order_math._floor_shares_for_notional",
    "trader.trading.position_sizing._confidence_scale",
    ...
]
```

A clean result means *"no counterexample found within the budget"*, not *"no
counterexample exists"*. Stronger than sampling on this code; weaker than
verification. Also: it only works on small, referentially-transparent functions
— which is another reason the pure kernel was extracted.

---

## 6. Layer 4 — Mutation testing (`mutmut`)

> **[mutmut](https://mutmut.readthedocs.io/)** · [source](https://github.com/boxed/mutmut) · pin `mutmut==3.6.0`, installed into the **same interpreter that runs pytest**
>
> **When it runs:** on demand, by a developer — minutes per pass. Never at runtime. It works on a throwaway *copy* of your source in `mutants/`; your working tree is never modified.

The previous layers check the code. This one checks **the tests**.

The idea is beautifully blunt: deliberately break the source — flip `>` to
`>=`, change a constant, swap `and` for `or` — then run the test suite. If the
tests still pass, they never actually checked that behaviour.

- **Killed** mutant = a test failed = good, your tests caught it.
- **Survived** mutant = every test passed = **a gap**.

### Reading a real run

`scripts/run_mutation.sh all` prints this:

```
module                                    killed   survived  timeout    score
trader/data/proposal_transitions.py            8          0        0   100.0%
trader/trading/exit_class.py                  29          2        0    93.5%
trader/trading/order_math.py                  56          3        2    94.9%
trader/trading/risk_gate.py                  294         16        0    94.8%
trader/trading/position_sizing.py            395        162        0    70.9%
```

Reading it row by row:

- **`killed`** — mutants where at least one test failed. Your tests noticed the
  change. This is the good column.
- **`survived`** — mutants where the whole suite still passed. **Nothing you
  wrote can tell the difference between the real code and the broken code.**
- **`timeout`** — the mutant made the suite hang (usually an infinite loop). MMR
  counts these as caught: a hang is a detected difference.
- **`score`** — `killed / (killed + survived)`. The share of injected bugs your
  tests actually catch.

The score is **not** a target to maximise, and this table shows why.
`proposal_transitions.py` at 100% is a 20-line state machine where every branch
matters. `position_sizing.py` at 70.9% looks alarming until you look at *where*
its 162 survivors live: **85 of them are inside a `session_summary()` reporting
method**, and another 6 mutate human-readable explanation strings. A mutant that
changes a log message cannot lose a single dollar. Chasing that module to 95%
would mean writing dozens of tests asserting the exact wording of status text.

So the useful question is never "what's the number?" but:

> **Of the mutants that survived, which ones could change something that
> matters?**

For MMR the classification is by *consequence*: does this mutant change the
number of shares ordered, or the approve/refuse decision? For
`position_sizing.py`, 70 of the 162 could touch the sized amount — those got
examined; the other 92 were classified as cosmetic and written down as such.

A score dropping is meaningful even when the absolute value isn't: it means a
mutant that *used* to be caught no longer is. That's why the score is recorded
in `scripts/mutation_baseline.json` and checked by `run_mutation.sh check`.

### Surviving mutants are the interesting part

A survivor is one of two things, and you must decide which:

1. **A real test gap** → add a test.
2. **An equivalent mutant** → the change is genuinely unobservable. Document
   *why*, or the next person redoes the analysis.

Here is a real gap this found. Five survivors clustered in `check_leverage`, all
missing-key defaults or guard boundaries — meaning nothing tested what happens
when margin data is absent. The answer turned out to be *silently approve*:

```python
init_margin_after = margin_impact.get('initMarginAfter', 0)
if net_liquidation > 0 and init_margin_after:     # absent key → 0 → skipped
    ...
return RiskGateResult(approved=True)              # ← nothing recorded the skip
```

And here is a real **equivalent** mutant, with the derivation that proves it:

```python
# original
while shares >= 1 and shares * price * multiplier > amount:
# mutant
while shares > 1  and shares * price * multiplier > amount:
```

They differ only when `floor(amount/denom) == 1` while `1*denom > amount`. That
state is unreachable in IEEE-754: `denom > amount` implies an exact quotient
< 1, and rounding a quotient up to exactly 1.0 requires it to exceed
`1 − 2⁻⁵⁴` — but double spacing near `amount` is `2⁻⁵²` relative, so the
smallest representable `denom > amount` already yields `0.9999999999999998`,
which floors to 0, never 1. **True equivalent.**

That reasoning lives in an *equivalent-mutant ledger* in
`scripts/run_mutation.sh`. Writing it down is not bureaucracy: without it, every
future run re-litigates the same three survivors.

> **Rule:** never change production code to raise the mutation score. Only add
> tests. A survivor tells you about your *tests*.

---

## 7. Layer 5 — Gates that cannot lie

This layer is the one most projects skip, and it's arguably the most important.
**A tool that reports success when it didn't run is worse than no tool**, because
it manufactures confidence.

### Fail closed

The `ty` gate used to do this:

```python
proc = subprocess.run(["uv", "run", "ty", "check", ...], capture_output=True, text=True)
out = proc.stdout + proc.stderr
counts = Counter()
for line in out.splitlines():
    if m := LINE_RE.match(line.strip()):
        counts[...] += 1
return counts        # ← proc.returncode never checked
```

If `ty` is missing, crashes, renames a flag, or changes its output format,
nothing parses → zero diagnostics → zero regressions → **"gate OK", exit 0**.
Demonstrated with a stub that exits 1:

```console
ty gate OK [kernel] — 0 diagnostics, none beyond baseline
ty gate OK [advisory] — 0 diagnostics ... (49 fewer than baseline — consider --update)
EXIT: 0
```

Worse: it then *invites* `--update`, which would overwrite the real baseline
with nothing. The fix:

```python
if proc.returncode != 0:
    raise GateError(f"ty exited {proc.returncode} ... the tool failed, not that it found problems")

if not any(_SUMMARY_RE.match(ln.strip()) for ln in out.splitlines()):
    raise GateError("no recognisable summary line — cannot distinguish a clean "
                    "scan from a scan that never happened")
```

The same principle is applied to the mutation gate: a missing baseline, absent
mutation data, or a baselined module a partial run didn't exercise are each a
**failure**, never a silent pass.

### Baseline the right thing

```python
# scripts/mutation_score.py
def _score(counts):
    """(killed + timeout) / (killed + timeout + survived)."""
```

Two decisions hide in that one line:

- **Score, not survivor names.** Mutant identifiers renumber whenever a mutated
  function changes — two documented equivalents moved from `12/14` to `13/15`
  when their function grew a few lines, with nothing actually changing. A
  name-keyed baseline would cry wolf on every edit until nobody read it.
- **Timeouts count as caught.** A timed-out mutant made the suite hang — a
  detected difference. And mutmut's killed-vs-timeout split is *timing*
  dependent: one module was observed moving `57 killed/1 timeout → 56/2` with no
  code change, while `killed+timeout` stayed fixed at 58. Excluding timeouts made
  the score wobble ~0.1% between identical runs; in a gate, that means spurious
  failures and then a gate nobody trusts.

### Protect the spec from the agent

`tests/invariants/` is human-owned. If one commit could edit a property *and*
the code it checks, an agent (or a tired human) can weaken the property to make
the code pass and everything stays green — self-consistent garbage.

```python
# scripts/invariants_guard.py
out = subprocess.run(["git", "diff", "--cached", "--name-only", "--diff-filter=ACMRD"], ...)
spec = [f for f in files if f.startswith("tests/invariants/")]
impl = [f for f in files if f.startswith("trader/")]
if spec and impl:
    print("spec-protection guard FAILED — split them", file=sys.stderr)
    return 1
```

The `D` in `ACMRD` matters: without it a staged **deletion** was invisible, so
`git rm tests/invariants/test_x.py` plus an implementation change sailed
through — the most direct way to weaken the spec was the one the guard couldn't
see.

**Be honest about its strength:** this is review hygiene, not a security
control. Two sequential commits bypass it, `--no-verify` bypasses it, and the
override env var is settable by anything that can run `git`. Its value is that
weakening a property becomes a *separate, visible, reviewable act* instead of a
line buried in a large diff.

### Make coverage rot impossible

Every layer above is configured by a hand-maintained list with a "keep in sync"
comment and nothing enforcing it. When such a list drifts, the failure is silent
*and flattering*: the tool still runs, still passes, and quietly stops looking.

```python
# tests/test_verification_wiring.py
def test_every_contracted_function_is_symbolically_checked(self):
    """A @deal contract that CrossHair never runs is decoration."""
    missing = _deal_contracted_functions() - _crosshair_targets()
    assert not missing

def test_every_invariants_file_is_in_the_oracle_or_documented_as_excluded(self):
    """A spec file absent from the oracle does not constrain any mutant."""
    ...
```

That second one is not hypothetical. Adding a new property file left the
module's mutation score unchanged at 61.3%, because mutmut only runs the
hand-listed oracle. Adding the file to `pytest_add_cli_args_test_selection` took
it to 93.5%. **The file existed, passed, and measured nothing.**

---

## 8. Putting it together

### First: what "exit-class" means, and why it is the scariest boolean here

Every order either **increases** your exposure or **reduces** it.

- You hold **+75 shares** of WDS. Selling 75 → position goes to 0. That is an
  **exit**: risk removed.
- You hold **0** shares. Selling 75 → position goes to **−75**, a short. You now
  owe 75 shares you never had. That is an **open**: risk *added*, and the loss on
  a short is theoretically unbounded, because a price can rise forever.

Notice both are `SELL` orders. The action word tells you nothing; only the
action *combined with the position you already hold* does.

**Why the system must treat these completely differently.** MMR refuses orders
for good reasons: the position would be too large, the daily loss limit is hit,
the instrument is denylisted, the account value can't be read. But every one of
those reasons is an argument for *not taking on more risk*. Applying them to an
exit is perverse — you'd be refusing to let someone **close a losing position
because they're losing money**. A blocked exit can turn a bad day into a
catastrophic one, and a stop-loss you refuse to place is not a stop-loss.

So the rule is: **exit-class orders are never refusable.** They skip the trading
filter, the leverage check, the risk gate, the approval requirement, and the
approver notional tier.

Which means this one boolean decides whether *five safety gates apply at all*:

```
reduces_exposure(...) == True   →  all five gates SKIPPED (correct for a genuine exit)
reduces_exposure(...) == False  →  all five gates APPLY   (correct for an open)
```

A wrong `False` is an annoyance: a legitimate exit gets gated, and might be
refused. A wrong `True` is the whole system's failure mode in one value — an
*opening* order that faces no checks whatsoever. That's why it gets every layer
in the toolchain pointed at it, and why the property in section 3 is about
quantity-*independence*: a backdoor needs a threshold to hide behind.

### The function


```python
# trader/trading/exit_class.py
@deal.has()
@deal.pure
def reduces_exposure(action: str, held: float, quantity: float) -> bool:
    try:
        qty = float(quantity)
        position = float(held)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(qty) or qty <= 0:
        return False
    if not math.isfinite(position) or position == 0.0:
        return False
    act = str(action).strip().upper()
    if act == 'SELL':
        return position > 0
    if act == 'BUY':
        return position < 0
    return False
```

Every layer touches it:

| Layer | What it contributes |
|---|---|
| **Extraction** | The *decision* is pure — no `self`, no I/O — so tools can reach it. The position lookup stays in the runtime. |
| **`deal`** | `@deal.pure` — side-effect free, total. |
| **CrossHair** | In `TARGETS`; symbolically checked, clean. |
| **Hypothesis** | Quantity-independence + the direction rule + fail-closed on every degenerate input. |
| **mutmut** | Injects bugs to check the tests notice. It first scored **61.3%** — nearly 4 in 10 injected bugs went undetected, including three that flipped a fail-safe `return False` into `return True`, i.e. *fail-open* versions of the guards. After the property above was wired into the mutation oracle: **93.5%**. The 2 remaining survivors are *proven* equivalent — they differ only at `position == 0.0`, which the guard above already excluded, so no input can tell them apart. |
| **`ty`** | The module is in the kernel scope, held at zero. |
| **Wiring tests** | Fail if it ever drops out of TARGETS or the mutation scope. |

Before this treatment the predicate lived inline in a 2,000-line module, was
*mocked* in the spec, wasn't contracted, and wasn't in the mutation scope. The
`qty > 1000` backdoor shown earlier survived the entire suite. Now it turns two
properties red.

---

## 9. Running everything

Fastest → slowest. All dev-only; none of it ships in the container.

```bash
# type gate — seconds. Runs on every commit via pre-commit.
uv run python scripts/ty_gate.py
uv run python scripts/ty_gate.py --update      # re-record baselines (human-reviewed)

# the test suite, including the human-owned spec
~/miniforge3/envs/mmr/bin/python3 -m pytest tests/ --timeout=60 -q

# symbolic execution over the contracted kernel — tens of seconds per function
uv run python scripts/crosshair_check.py
uv run python scripts/crosshair_check.py order_math --timeout 30

# mutation testing — minutes
scripts/run_mutation.sh cores        # the fast pure cores
scripts/run_mutation.sh all          # everything
scripts/run_mutation.sh survivors    # list what survived — the interesting part
scripts/run_mutation.sh check        # compare to the recorded baseline
scripts/run_mutation.sh baseline     # re-record (human-reviewed, full pass)
```

Enforcement is `pre-commit` (this repo has no CI):

```yaml
- id: invariants-guard      # spec and implementation may not move together
- id: ty-gate               # no new diagnostics beyond baseline
- id: crosshair-check
  stages: [manual]          # too slow for every commit; run deliberately
```

---

## 10. When *not* to reach for these

Proportion matters. The whole toolchain is aimed at a **small, pure, dangerous
core** — a few hundred lines where being wrong costs money.

- **Contracts** need pure functions. Don't contract something that reads config,
  hits a database, and builds a human-readable string; extract the pure part and
  contract that.
- **CrossHair** needs small, referentially-transparent functions. It will time
  out on anything wide, and a timeout tells you nothing.
- **Mutation testing** on a UI or a reporting method mostly produces string
  mutants. In MMR's own numbers, 85 of `position_sizing`'s 161 survivors live in
  a `session_summary()` reporting method and cannot affect a single traded
  dollar. Classify by *consequence*, not by count.
- **Property tests** are wasted on functions with no interesting invariant.
  "Returns a dict with these keys" is not a property worth the machinery.

The honest summary of what this bought MMR: a `ZeroDivisionError` on the single
conversion every order uses (CrossHair), an ungated-order class made impossible
by construction (types + chokepoint), an unexamined fail-open path in the
leverage check (mutation), and a size-triggered backdoor that would have passed
1,882 tests (property). Every one was invisible to a normal test suite that was
entirely green.

### Further reading

- Hypothesis — <https://hypothesis.readthedocs.io/>
- deal — <https://deal.readthedocs.io/>
- CrossHair — <https://crosshair.readthedocs.io/>
- mutmut — <https://mutmut.readthedocs.io/>
- ty — <https://github.com/astral-sh/ty>

In-repo companions: [`SAFETY_ROADMAP.md`](SAFETY_ROADMAP.md) for what is shipped
and what is designed, [`AUDIT_ROADMAP.md`](AUDIT_ROADMAP.md) for outstanding
work, and `tests/invariants/README.md` for the spec-ownership policy.
