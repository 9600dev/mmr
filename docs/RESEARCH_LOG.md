# Research log

Dated findings, with the numbers that produced them. Distinct from
[`STRATEGY_EVALUATION.md`](STRATEGY_EVALUATION.md), which is the *method*;
this is *what we measured and what we concluded*.

Entries are append-only. A conclusion that later turns out wrong gets a
correction underneath it rather than an edit, because the reasoning that
produced the wrong answer is usually the more useful record.

---

## 2026-07-28/29 — the intraday roster was never real

**Finding: every armed strategy failed honest evaluation.**

`OpeningRangeBreakout`, armed on most roster slots, scored **PBO 58-70% across
four independent sweeps** on four different symbol universes. Above 50% means
the in-sample winner lands in the *bottom* half out of sample more often than
not: the search was anti-predictive, not merely uninformative.

| measurement | result |
|---|---|
| headline sweep (best cell) | Sharpe 3.46 |
| PSR | 0.977 |
| PBO, four sweeps | **58 / 70 / 69 / 70%** |
| DSR vs. all 875 ORB evaluations | **0.111** |
| walk-forward, GOOGL, live semantics | **+0.097%** over 5 folds |
| GOOGL buy-and-hold, same window | **+13.97%** |
| live paper ledger, 6 trades | **-$128.76** |

The per-fold detail is the clearest single artefact: the two folds with the
**highest** in-sample scores (train Sharpe 1.16, 1.25) were the **only two
losing folds** out of sample. The selection signal pointed the wrong way.

**Walk-forward across the whole library** (5 strategies x 2 instruments, live
semantics): every result within +/-0.5% over ten months, against GOOGL's
+13.97% buy-and-hold. `opening_drive_fade`, the one flagged as most promising
on PBO (22-39%), returned **+0.013%**.

**Root cause was statistical power, not strategy quality.** These designs
produced 20-44 trades per ten-month walk-forward. At 44 trades you can only
detect a per-trade Sharpe above 0.30, which is enormous. The experiments could
not have found a realistic edge if one existed.

> **NOT the cause:** the store held 2,580 structurally impossible bars. Real,
> now quarantined, and 0.01% of 24.6M rows - far too few to explain a
> systematic PBO of 70%. The data defects were real; the *selection* was what
> was wrong.

**Action taken:** entire auto-execute roster disarmed 2026-07-28.

---

## 2026-07-29 — cross-sectional: a real signal, and a low ceiling

**Finding: 12-1 momentum is real. Almost nothing else is, and the ceiling is
around Sharpe 0.4-0.5 gross.**

### Signal scan (490 instruments x 2,572 days, 1.15M observations)

| signal | h | mean IC | t(NW) | turnover |
|---|---|---|---|---|
| **momentum_12_1** | 1 | **0.0181** | **3.45** | 0.036 |
| reversal_5d | 1 | 0.0105 | 2.65 | 0.144 |
| momentum_6_1 | 1 | 0.0095 | 2.00 | 0.045 |
| low_vol_63d | 21 | -0.0257 | -1.21 | 0.010 |
| random_control | 1 | 0.0011 | 1.01 | 0.333 |

Pairwise correlation between the three real signals: **rho ~ +0.01**.
Essentially orthogonal.

> **The noise control earned its place immediately.** At horizon 21 it scored
> t = 2.07 - "significant" - which meant the statistic was wrong, not the
> market predictable. Overlapping forward returns inflate every t-stat at h>1
> by ~sqrt(h). Newey-West correction dropped momentum_12_1 at h=21 from
> **4.06 to 1.19**. Three findings would have been reported without it.

### Panel backtests (480 names, 2016-2026, 5bps + $0.005/sh + 50bps borrow)

| | CAGR | Sharpe |
|---|---|---|
| buy-and-hold equal-weight | +18.52% | 0.96 |
| momentum long-only | +28.88% | 1.09 |
| momentum long/short | +3.50% | 0.33 |

Long-only beats the benchmark by ~10pp/yr, but both are inflated by
survivorship (the universe is today's most liquid names) and momentum's long
leg benefits disproportionately. The long/short line is the honest alpha
estimate.

### Cost decomposition - the important one

| variant | no costs | with costs | turnover |
|---|---|---|---|
| momentum | 0.42 | 0.35 | 14.0x |
| **momentum, sector-neutral** | **0.19** | 0.09 | **13.8x** |
| mom+rev | **0.52** | 0.19 | 61.8x |
| mom+rev, sector-neutral | 0.53 | 0.11 | 60.2x |

Two different answers to two different questions:

**Combination works, execution eats it.** Zero-cost Sharpe rises 0.42 -> 0.52
when reversal is added, exactly as the IC t-statistic predicted (3.29 ->
4.87). But turnover goes 14x -> 62x. **We capture 0.19 of a 0.52 signal.**

**Sector-neutralisation removes real return.** Identical turnover (13.8x vs
14.0x), less than half the return, *before any cost*. This is not an execution
problem. A large share of what we have been calling momentum alpha is **sector
momentum** - the book was long technology through a decade when technology
rose. That is a real phenomenon, but it is more concentrated, more
market-correlated and more crowded than stock-level selection, and it must not
be described as market-neutral alpha.

### Best honest configuration

**Plain momentum, rebalance every 10 days: Sharpe 0.39, CAGR 5.54%, turnover
9.9x.** Better than 5-day (0.35) - less churn beats fresher signal. At 21 days
the signal decays faster than costs fall (0.26).

Not deployable. But it is a real, measured number, which is more than existed
the day before.

### Conclusions

1. **A 0.19 -> 0.52 gap is construction, not prediction.** The signal exists;
   two-thirds is thrown away in trading costs. Attacking that needs no new
   data and no new signal.
2. **More price-derived signals will not raise the ceiling.** Three were
   tested, near-orthogonal, and combination still lands ~0.5 gross. Raising
   the ceiling needs genuinely different information.
3. **Nothing here has been through walk-forward or PBO yet.** Momentum's
   parameter cliff (lookback 252 works at 3.29%, 126 gives -0.09%) is an
   undischarged warning.

---

## 2026-07-29 (later) - the momentum parameter search has no skill

**Finding: PBO 47.7%. DSR 0.745. The cross-sectional grid is a coin flip.**

Nine cells, lookback x rebalance, full period, T=2,511:

| lookback | reb 5 | reb 10 | reb 21 |
|---|---|---|---|
| 189 | 0.43 | 0.43 | 0.30 |
| 252 | 0.43 | **0.44** | 0.35 |
| 315 | **0.12** | **0.11** | **0.10** |

PBO 47.7% is the coin-flip line: choosing the best-performing cell in sample
tells you nothing about which will perform out of sample. DSR 0.745 on the
best cell, below the 0.95 bar.

**A correction to an earlier claim in this log.** The parameter surface was
described as "a plateau and a gradient, not a spike", and that was wrong. With
lookback 126 (-0.09%) and 315 (0.11) both collapsing, only 189-252 works. That
is a narrow ridge between two cliffs. The earlier reading came from sampling
only 126/189/252, which made the failure look one-sided.

**Walk-forward, first attempt: INVALID.** Reported -18.22%, but three of seven
folds returned exactly 0.00% because they made no trades. Cause was the
experiment design, not the strategy: each test window is ~252 bars and the
backtest began AT test_start with no prior history, so a strategy needing 252
bars of lookback never reached its first tradeable index. Folds choosing
LOOKBACK 252 did nothing; folds choosing 189 traded only at the very end.

Re-run with a warm-up period preceding each test window. The strategy holds
nothing during warm-up (on_panel returns None below its lookback), so the
equity curve is flat there and the measured return is the test period's.

**Reading the PBO result on its own:** the in-sample Sharpe of 0.44 was
selected from nine cells whose ordering carries no out-of-sample information.
Whatever walk-forward returns, the *selection procedure* for this strategy is
already known to be uninformative - which means any deployed configuration
would have to be chosen on grounds other than backtested rank.

---

## Method errors worth not repeating

Recorded because they cost real time and two of them were invisible to every
automated gate.

| error | caught by |
|---|---|
| conId 756733 called "GOOGL" (it is SPY) - through a full analysis, twice | **nothing; only re-checking by hand** |
| a deadlock diagnosis invented from a bad `ps` grep, then written into a code comment as fact | **nothing; only re-checking by hand** |
| describing a run as started when it had not been | **nothing** |
| sector map injected onto a class that `run_from_module` re-imports, silently making neutralisation a no-op | identical results across variants |
| `BacktestTrade` missing required fields | ty gate, statically |
| holdings erased from equity on a missing bar (-96% drawdown) | `deal` precondition three frames away |
| `migrate-symbols` duplicating 1,255 daily bars | the standing data audit |
| every t-statistic at h>1 inflated by sqrt(h) | the pure-noise control |

The pattern in the top three: they are **narrative** errors, not code errors.
The code was correct; the claim about it was wrong. No type system, contract
or property has anything to say about a claim. Mitigations are procedural -
resolve identifiers at point of use, separate observation from inference, and
prefer a second measurement to a better explanation.
