# Hardening the LLM programming loop

### A tutorial on verification tooling, using a live trading system as the worked example

**Audience:** you know Python and OO programming, and you have written unit
tests. You now write a lot of your code by directing an LLM. You have not used
design-by-contract, property-based testing, symbolic execution, or mutation
testing. This tutorial shows you what each one does.

Everything below is real code from this repository and real output from these
tools. Every bug shown here was a real bug. Several were caught in the sessions
that produced this document.

### What MMR is

**MMR ("Make Me Rich")** is the open-source algorithmic trading platform this
tutorial lives in: [github.com/9600dev/mmr](https://github.com/9600dev/mmr),
[README](../README.md). It is a Python system that connects to
[Interactive Brokers](https://www.interactivebrokers.com/), streams market
data, runs trading strategies, and places real orders with real money. No
human confirms each order.

Three properties make it a useful worked example:

1. **Mistakes are expensive and immediate.** A bad change places an order that
   should not exist, at a size that should not be used. A filled trade has no
   undo.
2. **An LLM wrote most of it.** A human directs a model, and the model writes
   the code. That is the loop this tutorial is about.
3. **The dangerous core is small.** A few hundred lines decide how many shares
   to buy and whether an order is allowed at all. Those lines get the full
   toolchain. The surrounding ~10,000-line CLI does not need it.

You do not need to know trading to follow along. The tutorial explains each
domain term where it first appears, and section 1 contains a
[finance primer](#a-five-minute-finance-primer).

---

## The loop this is really about

The old loop was:

```
human intent  →  human writes code  →  human reviews diff  →  ship
```

Review worked because the reviewer had written similar code, at human speed,
and could hold the change in their head. The new loop is:

```
human intent  →  LLM writes code  →  ??? →  ship
```

Be honest about what fills the `???`. It is not line-by-line review. Nobody
reads 5,000 lines of generated diff with the attention they give 50 lines of
their own. You skim, you spot-check, and you ship on the strength of "the
tests pass".

**The LLM also wrote the tests.** That is the problem.

An LLM is very good at producing plausible code and a green test suite. Those
two facts correlate with correctness, but they are not correctness, and they
fail together: the model tests what it just built, using the inputs it had in
mind while it built it. Green tests over a wrong assumption look exactly like
green tests over a right one.

So the loop has to become:

```
human intent
   ↓  stated in a form a machine can check   ← contracts, properties, types
LLM writes code
   ↓  adversarial search for where code ≠ intent   ← CrossHair, Hypothesis
   ↓  check the TESTS actually test something      ← mutation testing
   ↓  gates that cannot report success falsely     ← fail-closed baselines
ship
```

Each tool in this tutorial fills one of those arrows.

This framing also explains why a technical document stops to define finance
terms. The method depends on the human stating intent precisely in domain
language ("an order that reduces a position must never be refused") and then
encoding that sentence as something executable. If you cannot say it, you
cannot check it, and you are back to trusting the diff.

> **The uncomfortable version:** every technique here is a way of not trusting
> code you did not read. That includes code you wrote six months ago. It is
> acute when a model wrote the code ten seconds ago and is confident about it.

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
11. [Adopting this on a real project](#11-adopting-this-on-a-real-project)
12. [The loop, restated](#12-the-loop-restated)

Appendix A. [The day live testing outran the toolchain](#appendix-a-the-day-live-testing-outran-the-toolchain)

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
count on every automated trade, and a wrong answer spends real money. The bug that motivated
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

This is the least obvious idea in the tutorial, so it's built up in steps.

**The rule we want to enforce:** *every order that reaches the broker must have
passed the risk gate.*

#### Step 1 — how you'd normally do it, and why it leaks

```python
def place_from_dashboard(contract, order):
    if not risk_gate.approves(contract, order):     # remember to check!
        return
    broker.place(contract, order)
```

That works, for that function. The problem is what it does *not* do: nothing
stops the next function from being written like this —

```python
def place_from_new_feature(contract, order):
    broker.place(contract, order)                   # ...oops
```

— which compiles, runs, and trades. The rule lives in the reviewer's head and in
a docstring. It is a **convention**, and conventions are exactly what erodes when
code volume goes up and review depth goes down.

#### Step 2 — make the placement function demand *proof*

Instead of asking callers to check first, change what the placement function is
willing to *accept*:

```python
# trader/trading/executioner.py
async def subscribe_place_order_direct(self, approved: ApprovedOrder) -> Observable[Trade]:
```

Read the signature as a sentence: **"I do not place orders. I place
`ApprovedOrder`s."** You can no longer hand it a contract and an order at all —
there's no parameter for them.

#### Step 3 — make that value obtainable in exactly one place

`ApprovedOrder` values come from exactly one function:

```python
def mint_approved_order(contract, order, *, is_exit, checks=None) -> ApprovedOrder
```

and that function is called only from the gate's approve branch. So the chain
becomes:

```
you want to place an order
  └─ you must pass an ApprovedOrder
       └─ which only mint_approved_order() produces
            └─ which is only called after the gate approved
```

The rule *"you must pass the gate"* has been rewritten as *"you must be holding
this value"* — and **"which values may go here" is precisely the question a type
checker answers.** That's the whole trick: a process requirement has been
converted into a data requirement.

#### Step 4 — what the type checker actually does

Write the gate-skipping call and run `ty` on it. This is real output:

```console
$ uv run ty check probe.py --output-format concise
probe.py:7:43: error[invalid-argument-type] Argument to bound method
  `TradeExecutioner.subscribe_place_order_direct` is incorrect:
  Expected `ApprovedOrder`, found `Contract`
probe.py:7:55: error[too-many-positional-arguments] Too many positional arguments
  to bound method `TradeExecutioner.subscribe_place_order_direct`: expected 2, got 3
Found 2 diagnostics
```

That is the enforcement, and it's worth being precise about what changed:
skipping the gate is no longer *a thing a reviewer might notice* — it is a
**type error**, produced before the code runs, by a tool that runs on every
commit. There's a test in the suite that runs the real type checker on that
probe and asserts it fails, so the guarantee itself is regression-tested.

#### Step 5 — but Python has no private constructors

Here's the hole, and it's the reason `_MINT_KEY` exists. A type checker only
checks *types*. If anyone can build the value directly:

```python
tok = ApprovedOrder(contract=c, order=o)      # forged — and ty is perfectly happy!
await ex.subscribe_place_order_direct(tok)    # ...straight to the broker
```

A forged `ApprovedOrder` **is** an `ApprovedOrder`. Types get you *"you can't
pass the wrong thing"*; they do not get you *"you can't manufacture the right
thing"*. Python has no `private` keyword to lean on.

The fix is a **sentinel**: a secret value the constructor demands, which exists
only inside the defining module.

```python
# trader/trading/approved_order.py
_MINT_KEY = object()      # never in __all__, never returned, never passed out

class ApprovedOrder:
    __slots__ = ('contract', 'order', 'is_exit', 'checks', 'exit_reason')

    def __init__(self, _key: object = None, /, *, contract, order, ...):
        if _key is not _MINT_KEY:                      # note: `is`, not `==`
            raise RuntimeError(
                'ApprovedOrder is mint-only — construct it via the gate')
        object.__setattr__(self, 'contract', contract)
        ...
```

Three details in that snippet do real work:

- **`object()`** — a bare object whose only distinguishing feature is its
  *identity*. You cannot guess it, compute it, or construct an equal one.
- **`is` not `==`** — identity comparison, so a look-alike can't satisfy it.
  (`==` could be defeated by an object with a permissive `__eq__`.)
- **`/`** — makes `_key` **positional-only**, so `ApprovedOrder(_key=...)` isn't
  even expressible syntactically.

Real behaviour, run against the actual class:

```console
ApprovedOrder(contract=..., order=...)           -> RuntimeError: ApprovedOrder is mint-only ...
ApprovedOrder('guess', contract=..., order=...)  -> RuntimeError: ApprovedOrder is mint-only ...
mint_approved_order(...)                         -> ApprovedOrder(BUY 100 AMD, open)
tok.is_exit = True                               -> AttributeError: ApprovedOrder is frozen
pickle.dumps(tok)                                -> TypeError: ApprovedOrder is not serializable
```

#### Step 6 — so what enforces what?

Four mechanisms, four distinct jobs. This is the summary worth remembering:

| Mechanism | Prevents | Enforced by | When |
|---|---|---|---|
| `approved: ApprovedOrder` parameter | passing raw contract/order to the broker | `ty` | statically, every commit |
| `_MINT_KEY` sentinel | manufacturing a token without the gate | Python | at runtime |
| `__setattr__` raises (frozen) | altering a decision after it was made | Python | at runtime |
| `__reduce__` raises (no pickle) | rebuilding a token from serialized data | Python | at runtime |

The type system and the sentinel are **complementary, and neither is sufficient
alone**: types stop you passing the wrong value, the sentinel stops you creating
the right one illegitimately. The frozen/unpicklable pair closes the remaining
routes — mutating a token after it was approved, or reconstructing one from a
message off the wire.

> **Why not a Pydantic model?** An earlier version used one. Pydantic's *public*
> constructors `model_construct()` and `model_copy()` bypass `__init__`
> entirely — so a forged, sentinel-free, type-clean token was one documented
> API call away. A plain frozen `__slots__` class has no such bypass.

> **The honest threat model.** This stops *accidents* and *refactors* — an agent
> or a colleague wiring up a new placement path without the gate. It is **not**
> a defence against hostile code in the same process: anything that can
> `import` the module can reach `_MINT_KEY` by reflection. True unforgeability
> would need process isolation, which is a different (and much more expensive)
> project. Know which threat you've actually addressed.

### The gap the type cannot close

Steps 1–6 stop you *forging* a token. There is a second, subtler gap they do
not touch, and this is the part most write-ups of this pattern get wrong.

State it plainly: `mint_approved_order()` performs **no verification**. It's an
unconditional constructor. So the type proves *"someone called mint"*, **not** *"the gate
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

> **In the LLM loop.** Ask a model to "add a way to place a market order from the
> dashboard" and you will get a plausible, working function that calls the broker
> API directly — because that is the shortest path to the stated goal, and the
> gate is somewhere else in a 2,000-line file. A type makes that *impossible to
> express*: there is no way to obtain an `ApprovedOrder` except through the gate,
> so the model's shortcut doesn't compile. This is the cheapest layer, because it
> converts "reviewer must notice the missing check" into "the code does not
> typecheck", and the second one scales to diffs nobody reads.

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

That backdoor passed **all 41 invariants tests and all 1,882 suite tests** as
they stood at the time. The predicate was covered only by example-based tests,
and every quantity those examples asserted was 150 or less, so nothing ever
reached the threshold. The property below did not exist yet — writing it is what
turned the backdoor from invisible into two red tests.

**Where MMR applies it:** `tests/invariants/` is the human-owned executable
spec. Properties there state safety facts: exit-class orders are never refused,
gates **fail closed** (when a required input can't be read, refuse the trade
rather than wave it through — the opposite of *fail open*), share conversion
never exceeds the sized notional, and a strategy without a passing pre-deploy
check can't start trading.

> **In the LLM loop.** This is the direct antidote to *"the model tested the
> inputs it had in mind."* A model writing example tests draws from the same
> distribution it used to write the code, so its blind spots are correlated —
> it will test `qty=100` because it was thinking about `qty=100`. The backdoor
> in the next subsection survived 1,882 tests for exactly this reason: every
> example asserted a quantity of 150 or less. A property doesn't ask the model
> for inputs; Hypothesis generates them, and shrinks any failure to the minimal
> case. Note the division of labour: **you** state the invariant (that's intent,
> and it's the part a human must own), the library hunts for violations.

### A caveat you must internalise

Hypothesis only explores the space **your strategies describe** — the
`min_value`/`max_value` bounds you hand it. Anything outside those bounds is
never tried, so a bug living there is invisible no matter how many examples run.

Here is how MMR discovered that about its own sizing spec. Jumping ahead
slightly: [section 6](#6-layer-4--mutation-testing-mutmut) introduces **mutation
testing**, which deliberately injects small bugs — a *mutant* is one such
deliberately-broken copy of the code, e.g. a `>` changed to `>=` — and then
checks whether the test suite notices. A mutant nothing notices marks a gap.

Seventy mutants in the sizing code survived, and they all had the same shape:

```python
net_liq = draw(st.floats(min_value=10_000.0, max_value=5_000_000.0))  # never 0
price   = st.floats(min_value=1.0, max_value=5_000.0)                 # never < 1
```

With `net_liq ≥ 10,000`, a guard written `> 0` and a guard written `> 1` can
**never disagree** — you'd need a value between 0 and 1 to tell them apart, and
the strategy never generates one. So mutating `> 0` into `> 1` is undetectable,
not because the property is weak but because the input space is too polite.

**The property was fine. The generator was the bug.** That distinction is easy to
miss and worth holding onto: a property test can be simultaneously well-written
and near-useless if its inputs avoid the interesting region.

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
3. **A test oracle** — the thing that decides whether a given run passed or
   failed. Normally that's your `assert`. Here the contract plays the role, so
   combined with Hypothesis you don't even need to write the assertion: feed the
   function generated inputs and the contract does the judging. In the run above, `deal` raised *before* the test's own `assert`:

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

> **In the LLM loop.** A contract is intent written **once**, by the human, that
> then checks *every future call* — including calls in code generated months
> later by a model that never saw the discussion where the rule was agreed. A
> docstring saying "never overspend" is a suggestion to a reader; `@deal.ensure`
> is a tripwire in production. It also survives refactoring: when a model
> rewrites the body of a contracted function, the contract is still standing
> there judging the result. Of everything in this tutorial, contracts have the
> best ratio of *intent captured* to *characters typed*.

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

> **In the LLM loop.** Neither a human nor a model proposes
> `3.0765742648370966e-154` as a test input. It isn't a lack of skill — it's that
> both are reasoning about *plausible* trading inputs, and the bug lives outside
> plausibility. A solver has no such bias: it works backwards from the
> postcondition to any input that breaks it. Use it when the human's intent is
> already written down as a contract and you want to know whether the generated
> implementation actually satisfies it, rather than merely passing the examples
> that came with it.

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

> **In the LLM loop — this is the load-bearing one.** Every other layer checks
> the code. Mutation testing is the only one that answers the question the LLM
> loop actually raises: *the model wrote the tests too — do they check anything?*
>
> A worked example from the session that produced this document. Mutation found
> five survivors in `check_leverage`, so tests were written to kill them. Re-run:
> **only one of the five died.** The tests looked thorough, asserted sensible
> things, and passed — and four of them exercised paths where the mutant and the
> original produce identical output. One test used a large `net_liquidation`, so
> an injected default of `1` produced a leverage near zero that passed the limit
> anyway; both versions approved; nothing was proven. The discriminating inputs
> had to be *derived* rather than guessed:
>
> ```python
> # inputs where the original and the mutant actually disagree
> c9  = ({'equityWithLoanAfter': 0}, net_liq=1.0, max_lev=0.5)
> #     original: key absent -> 0 -> branch skipped -> APPROVE
> #     mutant:   default 1  -> 1/1.0 = 1.0x > 0.5  -> REFUSE
> ```
>
> Confident-looking tests that verify nothing is the characteristic failure of
> generated test suites, and it is invisible to code review — the tests *read*
> correctly. Only mutation testing surfaces it.

---

## 7. Layer 5 — Gates that cannot lie

This layer is the one most projects skip, and in an LLM loop it is arguably the
most important. **A tool that reports success when it didn't run is worse than no
tool**, because it manufactures confidence — and confidence is exactly what you
are substituting for review.

There is also a hazard here that simply does not exist when humans write all the
code: **the thing being verified and the thing doing the verifying are produced
by the same process.** An agent that can edit both the implementation and the
test that checks it can converge on something self-consistent and wrong, with
every light green. Everything in this section is about removing that degree of
freedom.

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

> **In the LLM loop — it works, and here is it working.** While writing the
> `ApprovedOrder` hardening described in section 2, the obvious implementation
> was to make `mint_approved_order()` validate its arguments. That change turned
> `tests/invariants/test_approved_order.py` **red**: a human-owned property pins
> that a token may legally be constructed with an empty record.
>
> The tempting move — and the one an unsupervised agent takes, because it makes
> the suite green — is to adjust the property. The policy forbids it: *a red
> invariant means the implementation is wrong unless a human revises the spec.*
> So the validation moved to where the token is **spent** instead, which turned
> out to be the better design anyway: it covers every future mint site rather
> than the ones someone remembered to audit.
>
> The guard didn't prevent a security breach. It prevented a worse outcome —
> a plausible change, a green suite, and a silently weakened guarantee that
> nobody would ever have gone looking for.

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

> **In the LLM loop.** Ask a model to "add the new test file to the verification
> setup" and it will add the file. Whether it also updated four hand-maintained
> registries — the CrossHair target list, the mutation scope, the mutation
> oracle, the type-check scope — depends on whether it happened to know they
> exist. Nobody notices the omission, because the failure mode is silent *and
> flattering*: the tool still runs, still passes, and quietly stops looking at
> something. These wiring tests convert "the agent must remember" into "the
> suite fails until it's wired", which is the only version that survives a
> hundred sessions.

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

The proportionality rule that follows from the loop framing: **apply this where
you would not accept "the tests pass" as an answer.** In MMR that is order
construction, the risk gate, position sizing and the exit-class decision — a few
hundred lines. The 10,000-line CLI next door has none of it, deliberately. If a
mistake there means a bad table on a terminal, review-by-skimming is a rational
trade; if it means an unbounded short position, it is not.

---

## 11. Adopting this on a real project

Everything above is *what* the tools do. This section is *how to get there* —
either on a new project or on one that already exists — plus what it actually
cost here.

### First, the load-bearing decision

**Pick the dangerous core, and be ruthless about how small it is.**

Every one of these techniques has a cost that scales with surface area, and a
benefit that scales with *consequence*. So the first question is not "which tool"
but "which few hundred lines would I be unable to sleep after shipping wrong?"

In MMR that's roughly:

| In the core | Not in the core |
|---|---|
| amount → share-count conversion | the 10,000-line CLI |
| the risk gate's decisions | reporting and summary output |
| exit-class classification | data downloads, scanners |
| position sizing caps | backtest plumbing |

That's a few hundred lines out of tens of thousands. If your answer is "all of
it", you haven't answered yet — and applying this everywhere is how the method
dies of exhaustion in week three.

A useful heuristic: **where would you refuse to accept "the tests pass" as an
answer?** That's the core.

### Path A — starting a new project

The order matters, because each layer makes the next cheaper.

1. **Type checker + a fail-closed gate, on day one.** Cheapest possible win, no
   test-writing required, and adopting it later means burning down a backlog of
   diagnostics instead of never creating one.
2. **Write the invariants before the implementation.** Not TDD exactly — you're
   not writing example tests, you're writing down the two or three sentences
   that must never stop being true. *"An order that reduces a position is never
   refused."* *"A returned share count never costs more than the budget."* Give
   those to the LLM as the specification.
3. **Make the dangerous decisions pure.** The single highest-leverage structural
   habit, and the one an LLM won't do unprompted: separate *deciding* from
   *doing*. A decision that takes plain values and returns a plain value can be
   contracted, property-tested, symbolically checked and mutation-tested. The
   same logic buried inside a method that also reads a database can be tested
   only through mocks.
4. **Contracts on those pure functions** as you write them.
5. **Mutation testing once the suite exists** — and treat the first run as a
   review of your tests, not your code.
6. **CrossHair last**, on the handful of numeric functions where a pathological
   float is plausible.

### Path B — retrofitting an existing codebase

The trap here is trying to reach a clean state before getting any value. Don't.

1. **Baseline, don't fix.** Run the type checker, record every existing
   diagnostic as accepted, and gate only on *new* ones. MMR's advisory scope
   started at 49 diagnostics and is still not zero — that was never the point.
   The ratchet is the point.
2. **Find the dead code first.** Retrofitting is a good excuse to delete. This
   session removed three packages — 19 files, **5,673 lines** — that could not
   even be imported (`ModuleNotFoundError: No module named 'arctic'`), and had been
   sitting next to the live code looking plausible. Verification you apply to
   dead code is pure waste, and worse, dead code is a trap for an LLM grepping
   for "the CLI".
3. **Extract one decision.** Pick the single scariest boolean or number in the
   system and pull the *decision* out into a pure function, leaving the I/O
   where it is. In MMR that was one afternoon's work and it changed the
   predicate from untestable to 93.5% mutation-covered.
4. **Write one property for it.** Just one, stating the thing that must never
   happen.
5. **Run mutation testing on that one module.** This is the moment the method
   sells itself: you will discover your brand-new property does less than you
   thought.
6. **Only then widen.**

> Steps 3–5 are a half-day and produce a complete worked example inside *your*
> codebase, which is worth more than any tutorial for convincing colleagues —
> or for giving an LLM a pattern to imitate.

### Prompts that actually change what you get back

Models default to example-based tests and to putting logic wherever the data
already is. These are the instructions that reliably move them, phrased as you'd
actually type them.

**Making decisions extractable:**

```
Before implementing: separate the DECISION from the I/O.
Put the decision in a module-level pure function that takes plain
values (no self, no config lookups, no network) and returns a plain
value. The caller can do the lookups and pass the results in.
```

**Getting properties instead of examples:**

```
Do not write example tests for this. Write a Hypothesis property.
State the invariant that must hold for ALL valid inputs, and let
Hypothesis generate them. If you find yourself writing a specific
number in an assertion, that is a sign you are still writing examples.
```

**The one that catches the backdoor class:**

```
Is there an input dimension this function's answer must NOT depend on?
If so, write that independence as the property.
```

That single question is what produced the strongest test in this repo. The
exit-class answer must not depend on order quantity — so a size-triggered
backdoor has nowhere to hide, because no threshold can exist inside a function
whose answer is constant in that variable.

**Widening a generator (the failure mode from section 3):**

```
Review the Hypothesis strategies in this test. For each bound, ask
whether excluding that region hides a bug: zero, negative, sub-1,
non-finite, empty. Widen them, then tell me which assertions break —
do NOT adjust the assertions to keep them passing.
```

**Turning a mutation run into work:**

```
Here are the surviving mutants. For each one, decide: real test gap,
or equivalent mutant. If it is a gap, add a test that KILLS it and
show me the mutation score afterwards. If you claim equivalence, prove
it — show me why no input can distinguish the original from the mutant.
Do not modify the source to raise the score.
```

That last sentence matters. Without it you will get production code "simplified"
until the mutant no longer exists, which raises the number and destroys the
signal.

**The standing rule, worth putting in your `CLAUDE.md` / `AGENTS.md`:**

```
tests/invariants/ is the specification and is human-owned. If a change
makes an invariant fail, the implementation is wrong — fix the code, or
stop and tell me the property needs revising. Never edit an invariant to
make an implementation pass.
```

### What it cost, and what it caught — this session, n=1

Being straight about the evidence: this is **one weekend on one codebase, with no
control group.** Nobody ran the counterfactual. What follows is what happened,
not a measured effect size.

**Caught, in a suite that was fully green beforehand:**

| Layer | Finding |
|---|---|
| CrossHair | `ZeroDivisionError` from denormal float underflow, on the conversion every order path uses |
| Property | A `qty > 1000` backdoor in the exit-class predicate that passed **every one of the 41 invariants and 1,882 suite tests then in place** |
| Mutation | An untested silently-approving path in the leverage check |
| Mutation | A brand-new set of tests that killed **1 of 5** mutants — confident tests that verified nearly nothing |
| Fail-closed gate | The type gate reporting `OK`/exit 0 on every run where the checker had not run at all |
| Spec guard | An implementation change that would have required weakening a human-owned property |
| Wiring test | A new spec file that measured **zero** mutants until it was added to the oracle |

**Rough costs, measured:**

- Type gate: **seconds**, every commit.
- Property tests: minutes to write, **~10s** for the invariants suite.
- CrossHair: **tens of seconds per function**; run deliberately, not on commit.
- Mutation, one module: **2–5 minutes**. Full kernel: **~15 minutes**.
- Retrofit of the scariest predicate (extract → contract → property → mutation
  → wire into the gates): **one session**, and it went 61.3% → 93.5% mutation
  coverage with two remaining survivors *proven* equivalent.

**The honest counter-evidence** — the method is not free and this session showed
its costs too:

- I proposed a "fix" to the sizer that **broke 45 tests**, because I'd
  misunderstood which of two callers relied on the existing behaviour. The tests
  caught it; my reasoning did not. That's the system working, but it's also an
  hour spent on a change that got reverted.
- My first attempt at killing five mutants killed one. The discriminating inputs
  had to be *derived numerically* rather than reasoned out.
- I misread which argument a type diagnostic referred to and wrote a wrong
  explanation into a commit message, corrected in the next commit.
- I hypothesised that mutation scores were nondeterministic and used it to
  explain an anomaly. Three controlled runs proved me wrong.

Every one of those was an LLM being confidently wrong inside a loop *designed to
catch that*. Which is the argument, really: the tooling is not there because
models are bad, it's there because **plausible-and-wrong is the characteristic
failure**, and it is invisible to review precisely because it reads well.

### Failure modes of the method itself

- **Applying it everywhere.** The fastest way to abandon it. Core only.
- **Chasing the mutation score.** `position_sizing` sits at 70.9% and that's
  correct — 85 of its survivors are in a reporting method. A number going up is
  not the goal; a *specific* survivor being explained is.
- **Letting the agent re-baseline.** `--update` on any of these gates is a
  human-reviewed act. An agent that can re-record the baseline can make any
  regression disappear.
- **Property theatre.** `assert result is not None` is a property in the same
  way a smoke alarm with no battery is a smoke alarm.
- **Forgetting the generators.** The single most likely way your property suite
  is weaker than you think. Re-read section 3's caveat.

### The smallest useful version

If you do nothing else on an existing project:

1. Turn on a type checker with a **baseline** and a **fail-closed** gate.
2. Pick the scariest function. Make its decision **pure**.
3. Write **one** property that says what must never happen.
4. Run **mutation testing on that one module** and read the survivors.

That is an afternoon, and step 4 will tell you something surprising about tests
you already trusted.

---

## 12. The loop, restated

The premise: `human intent → LLM → code → ??? → ship` needs something better
than "the tests pass" in the `???`. That something is five practices:

**1. State intent where a machine can check it, and where the LLM cannot
quietly edit it.**
Not a docstring, not a comment, not a ticket. A contract, a property, or a
type. In this repo those live in `tests/invariants/` and in `@deal`
decorators. A pre-commit guard stops implementation and spec from moving in
the same commit.

**2. Let the machine attack the code, not just exercise it.**
Hypothesis generates the inputs nobody thought of. CrossHair solves for the
inputs outside plausibility. Both search for places where code differs from
intent. That is a different activity from confirming that code does what its
author expected.

**3. Verify the verifier.**
The model wrote the tests. Mutation testing is the only cheap way to find out
whether those tests check anything. In one session it caught a set of freshly
written, sensible-looking tests that killed one mutant out of five.

**4. Make the gates incapable of lying.**
A gate that passes when it did not run manufactures false confidence. Fail
closed on a missing tool, a missing baseline, or a partial run.

**5. Make coverage rot mechanical, not remembered.**
Hand-maintained registries drift silently. Write a test that fails when a
module, contract, or spec file is left unwired. That test is worth more than
any instruction to "remember to update the list".

### What the static toolchain bought

Each of these was invisible to a green test suite:

| Found by | What it was |
|---|---|
| CrossHair | `ZeroDivisionError` from a denormal float underflow, on the single conversion every order path uses |
| Property test | A size-triggered backdoor in the exit-class predicate. It passed all 1,882 tests. |
| Mutation | An untested, silently-approving path in the leverage check. Also a set of new tests that verified almost nothing. |
| Fail-closed gate | The type gate reported "OK" with exit 0 on every run where the type checker never ran. |
| Spec guard | An implementation change that would have quietly weakened a human-owned safety property. |

### What the full loop bought: the 2026-07-27 case

The static toolchain is half of the method. The other half is live testing
with the LLM watching the run. One day of it, in paper trading, produced the
strongest safety result in this repo's history:

- **18 execution paths ran against the real broker for the first time.**
  A protective stop fired. A pyramid stack built, hit its cap, and closed. A
  real resize cancelled and re-created live protective orders. Every refusal
  gate (risk limits, proposal-approval, approver tier, kill switch,
  gauntlet-enforce, role matrix) refused a real request.
- **Nine real bugs surfaced. All nine were in code with passing tests, a
  clean type gate, and a recorded mutation baseline.** One example: the
  resize path cancelled the auto-executor's stop orders and re-created them
  without attribution. The executor then tracked dead order ids. The worst
  case was a live stop order left behind after a close, able to fire into a
  short position with no gate check.
- **The LLM monitored the run and closed the loop the same day.** For each
  bug: reproduce, fix, write the regression test, wire the test into the
  toolchain, deploy, and confirm on the live system. By the next session, all
  nine bugs had pinned regressions, the modules two of them lived in had
  joined the mutation scope, and the seams two of them crossed had composed
  integration tests.

The safety win came from the combination, not from either half:

- Live testing **discovers** the properties that matter. It samples reality,
  including facts about the broker's behaviour that no analysis of our own
  code can reach.
- The toolchain **retains** them. Every discovered failure becomes a
  permanent, machine-checked regression. Live testing without the toolchain
  finds a bug once. With the toolchain, each find is permanent.
- The LLM makes the cycle fast enough to run in one day. Reproduce, fix,
  pin, measure, deploy, verify: nine times, between market open and close,
  while the book stayed protected by broker-side stops the whole time.

Appendix A analyses this day in detail, including which bugs the static
toolchain could have caught on its own, and which needed live contact.

### The honest limits

None of this makes generated code correct. It makes a specific, narrow class
of wrongness loud, in a core small enough to be worth the effort. The
10,000-line CLI in this repo has none of this machinery. That is a deliberate
proportionality call: the tools aim at the few hundred lines where being
wrong costs money.

The human stays in the loop, at a different altitude. You no longer review
statements. You review intent: the properties, the contracts, the scopes, and
the decisions about what may fail open. Read those carefully. The
implementation, increasingly, is the part the machine checks for you.

---

## Appendix A: The day live testing outran the toolchain

*Written 2026-07-27, the evening after, while the evidence was fresh. Updated
after the follow-up work landed. This appendix is a case study in the
toolchain's blind spots.*

### What happened

Every safety layer was green. We then spent one day driving the system's
surface against the real (paper) broker: we fired a protective stop on
purpose, ran a real portfolio resize, forced a pyramiding add with a
synthetic strategy, armed and tripped every refusal flag, and round-tripped a
forex order. Eighteen paths executed in production for the first time.
**Nine real bugs fell out.** No verification session in this repo has matched
that rate.

That result demands an uncomfortable question, answered honestly below:
could a harder push on the toolchain have found these bugs instead?

### The scoreboard

Each bug is scored. "Findable" means: a discipline this tutorial already
teaches, applied more thoroughly, catches the bug with no broker contact.

| # | Bug (commit) | Findable? | What it would have taken |
|---|---|---|---|
| 1 | CLI resize plan crashed rendering float deltas (`7214dac`) | **Yes, trivially** | Any test that executed the render path. The CLI's render paths were in no test and no mutation scope. |
| 2 | The *approver* role could propose (`3ea0947`) | **Yes** | The spec existed in the code's own capability table: "approver: approve, reject, reads". A test that enumerates every role × command pair against that table finds the missing branch at once. The example tests checked the denials that existed. Nothing could notice a denial that was absent. |
| 3 | Forex contracts routed to SMART, IB error 200 (`1cbbe2f`) | **Yes** | CLAUDE.md states the rule: "forex pairs are constructed on IDEALPRO." That sentence was never turned into an assertion. One unit test on `_resolve_contract('EUR', sec_type='CASH')` catches it. |
| 4 | Resize re-created stops without `orderRef` (`bef98e8`) | **Yes** | A round-trip property: a re-created protective equals the old order except quantity. `execute_resize_plan` had zero tests. Only its pure sibling `compute_resize_deltas` was verified. |
| 5 | Executor tracking pointed at dead stop ids after resize (`bef98e8`) | **Yes, with composition** | Each side passed its own tests in isolation. The resize tests contained no executor. The executor tests contained no resize. Composing the two fake harnesses reproduces the bug exactly. The bug lived in the seam. |
| 6 | A `Cancelled → Filled` status sequence dropped the fill from the ledger (`d23746d`) | **Plausibly** | A Hypothesis property over arbitrary status sequences ("any sequence that ends in Filled records a fill") finds it at once. Writing that property requires suspecting that IB's stream is non-monotonic. Sequence-fuzzing the environment model is a discipline, not a tool. |
| 7 | 10197 recovery re-requested **0** subscriptions (`99c8adb`) | **Borderline** | Both ends are our code: the registry and the subscription path. An end-to-end wiring test could catch it. The unit test injected the registry state, so it tested the mock. |
| 8 | `whatIfOrderAsync` returns a *list* for CASH (`fd4ddc8`) | **No** | ib_async's own types say `OrderState`. No property, contract, or stub could know that the real API hands back a list. This is an environment fact. |
| 9 | IB returns *no* whatIf state at all for CASH (`fd4ddc8`) | **No** | Same. Only the real IB can tell you this. |

Tally: six findable, one borderline, two that require the live environment.

### The three doors

Every escape used one of three doors:

**Door 1: unstated specs.** Bugs 1 through 4 violated rules that already
existed as prose: a CLAUDE.md sentence, a docstring capability table, an
"obviously" symmetric matrix. The toolchain verifies properties you state.
These were never stated where a machine could see them. The forex rule sat in
the documentation for the life of the repo while the code violated it on one
of two paths.

**Door 2: seams.** Bugs 5 and 7 lived between verified components. Mutation
scores, contracts, and properties are all per-component. Nothing asked "what
happens when the resize path mutates state the executor tracks?" The
components passed in isolation and composed into a hazard: a stale tracking
id whose worst case was a GTC stop firing into a flat book, which is an
unattended short position.

**Door 3: wrong environment models.** Bugs 6, 8, and 9. The code's model of
IB was wrong: it assumed statuses are monotonic, whatIf returns one state,
and every instrument has margin data. No analysis of our code can reveal
facts about theirs. Note also: the fail-closed flip from earlier the same day
is what made two of these visible at all. Fail-open had swallowed the whatIf
crash silently, forever. A fail-closed gate is, among other things, an
environment-mismatch detector.

### What "pushing harder" means, and what happened when we did

Each door implies a discipline. The follow-up work ran the first two to
completion; the results are themselves evidence.

1. **Cover the found bugs first** (done, `1d21da4`). Auditing the nine bugs
   against the toolchain found three coverage gaps. Closing one of them
   repeated the day's lesson at small scale: `order_lifecycle.py`, the module
   that writes the ledger rows, entered the mutation scope and scored 61.7%.
   About 90 survivors sat inside `_record_event`. Every field of the ledger
   row could be corrupted without a test noticing, because the tests asserted
   event types and never content. The row is now pinned field by field
   (78.1%).
2. **Seam tests** (done, `83c01bf`). One shared fake broker that both
   components mutate for real: `execute_resize_plan` acts on it through a
   stubbed RPC, and the executor reads it through its SDK. The harness
   reproduces the live resize sequence end to end, including the
   counterfactual (strip the attribution the way the old code did, and the
   orphaned stop provably survives the close). On its first run the harness
   also surfaced a three-way interaction nobody planned to test: the
   trim-to-zero seam collided with the empty-broker grace period, which
   correctly refused to believe an all-empty book.
3. **Docs-as-spec extraction** (queued). Mine CLAUDE.md and the docstrings
   for must / never / always / only sentences. Verify each against actual
   behaviour first, because a wrong doc pinned as a test enshrines the error.
   Each claim lands in one of three buckets: pinned (true, becomes a test),
   doc-bug (false, doc corrected), or flagged (cannot verify cheaply).
4. **Exhaustive matrices for capability tables** (queued). Any
   role/permission table gets an enumeration test over its full
   cross-product, derived from the documented table.
5. **Sequence-fuzz the environment** (queued). Hypothesis over IB status
   streams and error sequences into the lifecycle tracker and the recovery
   loop. This cannot conjure unknown facts. It removes the monotonicity
   assumptions we did not know we were making.
6. **Record reality into fixtures** (ongoing). When live contact reveals an
   environment shape (a list-valued whatIf, an empty CASH response, a
   `Cancelled → Filled` stream), that shape goes into a fixture. The test
   suite accumulates a museum of true IB behaviour.

### The synthesis

The tempting reading is "live testing 9, toolchain 0". That reading is wrong.
The day was the loop from section 12 running at a larger radius:

- **Live contact is property discovery.** It samples reality, including the
  parts no analysis of our own code can reach, and it tells you which
  properties matter and which models are wrong.
- **The toolchain is property retention.** Every one of the nine bugs now has
  a pinned regression: a supersession test, a role matrix, a routing
  assertion, a round-trip check, an adoption-and-sweep suite, a seam harness.
  None of them can return silently. Live testing without the toolchain finds
  a bug once. With it, each find is permanent.
- **The LLM in the monitoring seat is what makes the radius affordable.** The
  find-fix-pin-deploy-verify cycle ran nine times in one day because the same
  agent that watched the logs also wrote the fix, the regression, and the
  toolchain wiring, and then confirmed the deployed behaviour live.

The honest asymmetry is search space. Verification exhausts the properties
you conceived. Live testing samples the ones you did not. A day like this one
generates the next month's invariants.

### Further reading

- Hypothesis: <https://hypothesis.readthedocs.io/>
- deal: <https://deal.readthedocs.io/>
- CrossHair: <https://crosshair.readthedocs.io/>
- mutmut: <https://mutmut.readthedocs.io/>
- ty: <https://github.com/astral-sh/ty>

In-repo companions: [`SAFETY_ROADMAP.md`](SAFETY_ROADMAP.md) for what is shipped
and what is designed, [`AUDIT_ROADMAP.md`](AUDIT_ROADMAP.md) for outstanding
work, and `tests/invariants/README.md` for the spec-ownership policy.
