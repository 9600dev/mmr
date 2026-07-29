# Evaluating a strategy honestly

> **Short version.** A backtest number means nothing on its own. Before you
> believe one, you need to know three things it cannot tell you: what the same
> pipeline reports on data with no edge in it, how much of the result came
> from having searched, and what the procedure earns when it does not know the
> future. This document is how to get those three numbers, and what happened
> when we finally did.

## Why this exists

Through July 2026 this codebase accumulated a serious verification toolchain —
design-by-contract, property tests, symbolic execution, mutation testing, a
gauntlet that refuses to arm an unverified strategy. All of it verifies that
the **code** does what the spec says.

None of it asked whether the **strategy** was real.

On 2026-07-28 we asked. Every armed strategy failed, and the failure was not
subtle: the deployable out-of-sample return of the flagship strategy was
**+0.097%** against a backtest that had reported Sharpe 3.46. The gap was
entirely selection.

The tools below exist so that gap is visible *before* something arms, not
after it has been trading for six weeks.

---

## The three questions, in order

### 1. What does the pipeline report on nothing?

`scripts/negative_control.py`

Reading a backtest without knowing what the same machine produces from noise
is like reading a scale without knowing its zero. A sweep runs N
configurations and keeps the best; the best of N draws from a random walk
looks good, and looks better the larger N is.

```bash
python3 scripts/negative_control.py \
    --strategy strategies/opening_range_breakout.py \
    --class OpeningRangeBreakout \
    --grid '{"RANGE_MINUTES":[15,30,45],"VOLUME_MULT":[1.2,1.5,2.0]}' \
    --instruments 20 --days 250 --annual-drift 0.162
```

It runs the *real* pipeline — same class, same grid, same Backtester, same
execution semantics — over instruments whose minute returns are i.i.d.
zero-mean by construction.

**Always pass `--annual-drift`.** A long-biased strategy on real equities
earns the market's rise for free. A zero-drift control hands the real result
the entire equity risk premium and calls it edge. Set it to the realised
median buy-and-hold of the instruments over the same window:

```sql
SELECT first(close ORDER BY date), last(close ORDER BY date)
FROM tick_data WHERE symbol = ? AND bar_size = '1 min'
  AND date >= ? AND date < ?
```

**Measured, 2026-07-28** — 180 runs, the real ORB grid, 250 days:

| null | best-of-180 Sharpe | PF | return |
|---|---|---|---|
| pure noise (0% drift) | 2.04 | 2.13 | +18.6% |
| beta-matched (+16.2%/yr) | **2.26** | **2.40** | **+22.0%** |

A +22% backtest with profit factor 2.40 is what this machine produces from
data containing nothing. Any result in that neighbourhood carries no
information about the strategy.

> The null's own emptiness is pinned by `tests/invariants/test_null_market.py`
> — no drift, no serial correlation at lags 1/5/15/30/60, no cross-instrument
> factor. A control that is quietly wrong looks exactly like one that is
> right: it would set the zero too high and reject genuine strategies against
> a rigged benchmark.

### 2. How much of the result came from searching?

`mmr sweep overfit <sweep_id>`

Two numbers, answering different halves.

**Deflated Sharpe (DSR)** re-benchmarks PSR against the Sharpe you should
*expect* from the best of N trials under the null of no skill. Deflate against
**every evaluation in the strategy's history**, not one sweep — you do not
un-search a configuration by running another sweep:

```sql
SELECT class_name, count(*) FROM backtest_runs GROUP BY 1
```

**PBO** (Probability of Backtest Overfitting, via CSCV) scores every symmetric
train/test split and asks how often the in-sample winner lands in the *bottom*
half out of sample. **0.5 means the search had no skill**, however good the
winner's headline looks.

PBO is the more damning of the two, because it indicts the *method*. You can
re-run a strategy on better data; you cannot rescue a selection procedure that
was never informative.

| reading | meaning |
|---|---|
| PBO < 0.2 | the search finds something that persists |
| PBO ≈ 0.5 | the winner is a coin flip |
| PBO > 0.5 | **the search is anti-predictive** — the in-sample winner is worse than average |

**Below 20 trials the estimate carries a caveat on the result object**, because
its spread (sd 0.32 at N=5, 0.22 at N=12, measured against known-noise
families) is set by the rank grid's N+1 positions and does *not* shrink with
more observations. Do not read a narrow sweep's PBO as a point value.

### 3. What does the procedure earn without knowing the future?

`mmr walk-forward`

PBO and DSR *detect* selection bias. Walk-forward *removes* it.

```bash
mmr walk-forward -s strategies/opening_range_breakout.py \
    --class OpeningRangeBreakout --conids 208813719 \
    --days 250 --train-days 90 --test-days 30 --live-semantics \
    --grid '{"RANGE_MINUTES":[15,30,45],"VOLUME_MULT":[1.2,1.5,2.0]}'
```

At every fold the parameters are chosen using only data preceding the window
they are judged on. The result is out-of-sample by construction, so **there is
nothing left to deflate** — the trial count is one (the rule), however many
cells it considered.

It answers a question you can act on — *"if I refit every 30 days on the
previous 90 and traded the result, what happened?"* — instead of the one a
sweep answers, which has no honest answer at all.

**Read `selection stability` before the return.** It is the fraction of
consecutive folds that kept the same cell. A procedure that changes its mind
every fold has found noise, and its equity being positive is luck rather than
evidence.

Always pass `--live-semantics`. Validating under `accumulate` (which pyramids
*and* compounds sizing) and deploying under single-lot live execution compares
two different trading processes.

---

## The gates, as thresholds

Nothing arms unless all of these hold. They are deliberately blunt; a gate
with a judgement call in it becomes a gate someone argues past.

| gate | threshold | why |
|---|---|---|
| walk-forward OOS return | **> the instrument's buy-and-hold** over the same window | if holding the asset beats the strategy, the strategy is a worse way to own it |
| walk-forward OOS return | **> the beta-matched negative control's best-of-N** | otherwise it is inside what the machine invents from nothing |
| PBO | **< 0.35** | at 0.5 the search is uninformative; above it, anti-predictive |
| selection stability | **≥ 0.5** | below this the procedure is not finding a parameter, it is chasing noise |
| DSR (vs. full evaluation history) | **≥ 0.95** | the conventional bar, applied to the number that accounts for the search |
| PSR | *not a gate* | it blessed all thirteen sweeps at ≥ 0.96, including PBO 90% ones |

That last row is the important one. **`strategies gauntlet --min-psr` is not a
statistical gate.** It has never rejected anything and on this evidence never
could. Treat a passing PSR as evidence of nothing.

---

## What happened when we ran all three

### The ORB post-mortem

`OpeningRangeBreakout` was armed on most roster slots. Every layer said the
same thing, and each said it more sharply than the last:

| measurement | result |
|---|---|
| headline sweep (best cell, sweep 21) | Sharpe **3.46** |
| PSR | **0.977** — "almost certainly real" |
| PBO, four independent sweeps, four universes | **58 / 70 / 69 / 70%** |
| DSR vs. all 875 ORB evaluations | **0.111** |
| beta-matched negative control, best-of-180 | Sharpe 2.26, +22.0% |
| **walk-forward, GOOGL, live semantics** | **+0.097%** over 5 folds, stability 50% |
| GOOGL buy-and-hold, same window | **+13.97%** |
| live paper ledger, 6 closed trades | **−$128.76**, 2 wins |

The per-fold detail is the clearest thing in this document:

| fold | chosen | train Sharpe | OOS return | OOS Sharpe |
|---|---|---|---|---|
| 0 | R=30 V=1.2 | 0.77 | +0.007% | 0.03 |
| 1 | R=30 V=1.2 | 0.10 | +0.227% | 2.41 |
| 2 | R=15 V=1.2 | 0.57 | +0.110% | 1.46 |
| 3 | R=15 V=1.5 | **1.16** | −0.133% | −2.19 |
| 4 | R=15 V=1.5 | **1.25** | −0.113% | −1.92 |

The two folds with the **highest** in-sample scores were the **only two losing
folds**. Fold 1 was selected on a train Sharpe of 0.10 and produced the best
out-of-sample result. Over these five folds the selection signal points the
wrong way — which is precisely what PBO 70% measures.

SPY, same procedure: **+0.11%** against SPY's own +8.22%, stability 25%.

> The +8.22% figure was initially reported as GOOGL's. It is SPY's — the
> same conId confusion, caught a second time in the same session. GOOGL
> returned +13.97% over that window, so the real gap is wider than first
> stated. Resolve the conId every time; the habit is cheaper than the
> correction.

### What was NOT the problem

Worth recording, because we spent real effort there first. The store held
2,580 structurally impossible bars (highs between open and close, quote-
midpoint synthesis). Finding and quarantining them was correct and they are
now excluded — but they were **0.01% of 24.6M bars** and cannot explain a
systematic PBO of 70%.

**The data defects were real. They were not what was wrong with the roster.
The selection was.**

---

## Method notes, learned the hard way

**Verify the conId against the symbol before attributing anything.** We
reported a full set of results for "GOOGL" that were actually SPY (756733 is
SPY; GOOGL is 208813719), and the error propagated through a per-instrument
PBO table, a deployed-config comparison, and a complete walk-forward run
before it was caught. `mmr resolve <conid>` costs a second.

**Match N when comparing distributions.** The maximum of 495 draws exceeds the
maximum of 180 draws even when both are noise. Compare medians and
percentiles, or match the counts.

**Check the tie/degenerate case before trusting a statistic.** A family of
flat equity curves would inflate PBO through tie handling. Confirm it is not
that (zero flat columns, non-trivial trade counts) before believing the
number.

**Two metrics disagreeing is information, not an error to resolve.** ORB's
armed configs sat at the 24–37th percentile of noise on Sharpe but the 1–8th
on profit factor, and it was not a trade-count artifact (corr(trades, PF) =
−0.046 in the null). The honest reading is that whatever the trade filter does
well shows up in win/loss ratio and does not survive into risk-adjusted
return — *and* that PBO says you cannot select it anyway.

**Equity curves are decimated to daily on write.** Anything that annualises
them using the run's `bar_size` overstates by √(98280/252) ≈ 19.7× for 1-min
runs. Infer the period from the curve (`infer_periods_per_year`).

---

## Where to look next

The one strategy that was never armed is the one with the best evidence:
`OpeningDriveFade` scored PBO **22 / 35 / 39%** across three sweeps — the only
strategy consistently below the coin-flip line. It deserves the full
three-question treatment before anything else does.

More generally, the useful conclusion from all of this is not "these six
strategies are bad". It is that **a 9-cell grid search over one instrument
cannot manufacture an edge**, and no amount of statistical hygiene applied
afterwards will change that. The hygiene tells you when you have nothing. It
does not tell you where to find something.

## See also

- [`VERIFICATION_TUTORIAL.md`](VERIFICATION_TUTORIAL.md) — the *code*
  verification toolchain (contracts, properties, CrossHair, mutation testing)
- [`SAFETY_ROADMAP.md`](SAFETY_ROADMAP.md) — execution-path hardening
- `tests/invariants/test_selection_bias.py`, `test_walk_forward.py`,
  `test_null_market.py` — the human-owned properties behind these tools
- `docs/evidence/` — raw negative-control outputs
