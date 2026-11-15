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

## 2026-07-29 (later still) - fundamentals carry no signal; the denominator does

**Finding: what looked like a value effect is the size effect, and the size
effect here is survivorship.**

Point-in-time fundamentals ingested for 496 instruments, 2009-2026, 25,326
filings keyed by EDGAR acceptance instant. 58.5% of filings are accepted at or
after 16:00 ET, so more than half are NOT tradeable on their filing date - a
date-only source would trade those a day early.

As-of IC, Newey-West corrected:

| signal | h=63 IC | t(NW) |
|---|---|---|
| sales_to_price | 0.0425 | **2.58** |
| cash_flow_yield | 0.0283 | 1.77 |
| low_accruals | 0.0223 | 1.85 |
| gross_profitability | 0.0173 | 0.93 |
| earnings_yield | 0.0161 | 1.27 |
| book_to_price | 0.0176 | 1.05 |
| random_control | 0.0005 | 0.56 |

Two cleared t=2 and sales_to_price looked like the strongest result of the
day - more than double momentum's IC, at a horizon that decays over quarters
rather than days, which is exactly the shape that could survive costs.

**It was not real.** Decomposing the ratio:

| variant | IC (h=63) | t(NW) |
|---|---|---|
| sales_to_price as measured | 0.0425 | 2.58 |
| **revenue FROZEN per name** | **0.0591** | **3.40** |
| shares FROZEN per name | 0.0216 | 0.93 |
| 1/price alone | 0.0458 | 2.34 |
| **1/marketcap alone** | **0.0637** | **4.07** |

Replacing each company's actual sales with a constant makes the signal
STRONGER. The fundamental was subtracting, not adding. `1/marketcap` alone
beats every variant.

So the signal is the size effect (Banz 1981), and four of the six signals
tested - earnings yield, book-to-price, cash-flow yield, sales-to-price - share
market cap in the denominator. They were not six tests of fundamental
information; they were four measurements of size with different noise on top.
The two that divide by total assets instead scored t=1.32 and t=1.76.

**And the size effect here is survivorship.** The universe is today's top 500
by dollar volume, so its "small" names are the ones that GREW into the list.
They are small-and-present precisely because they went up.

### The leak probe was the wrong instrument

Delaying every acceptance instant by 7 days changed nothing (0.0425 ->
0.0427). That was designed to detect lookahead, and its flatness was read as
suspicious - wrongly. A slow valuation ratio does not depend on announcement
timing, because its numerator moves quarterly while its denominator moves
daily. The probe tests something the signal never claimed.

**New standing rule:** a ratio signal must be DECOMPOSED before it is
believed. Freeze the numerator per name, freeze the denominator, and test each
component alone. If the ratio does not beat both parts, you have found a part,
not the ratio. This is now in STRATEGY_EVALUATION.md.

Without that decomposition the reported finding would have been
"sales-to-price works, IC 0.0425, t=2.58" - wrong in the most expensive
direction, since it is the only positive result that survived a full day of
honest testing.

### Where this leaves the search

Every positive result found today has resolved to one of three things: a
parameter search with no skill (PBO 47.7%), sector momentum rather than stock
selection, or survivorship expressed as a factor. Nothing has survived.

**Survivorship is now the binding constraint.** It contaminated long-only
momentum (Sharpe 1.07 vs benchmark 0.96 - the excess was concentration) and it
contaminates the size effect entirely. Until the universe is point-in-time,
"edge" and "we selected the winners" are indistinguishable, and no further
signal work is worth doing on top of it.

---

## 2026-07-29 - swing reversal in tech: no effect, and a lesson about narrow grids

**Hypothesis (operator's own observation):** after a drop of 5-6% a tech name
climbs back, and after a spike it falls back. Tested on 138 tech names (SIC
35/36/73), 2021-07 to 2026-07, 1,335 days.

**Finding: no effect, in any variant, at any threshold or horizon.**

Run as an EVENT STUDY rather than a backtest - the prior question is whether
the conditional mean differs from an ordinary day, not what a portfolio would
have earned. Three confounds handled up front:

* **Drift.** Tech's unconditional daily mean over this window is +0.079%, so
  the 21-day baseline forward return is +2.18% for ANY random day. Every number
  is an excess over that. Without the subtraction the -6% bucket's raw +4.57%
  looks like a strong bounce when more than half is simply tech rising.
* **Clustering.** Hundreds of names fall 6% on the same market-wide day. At the
  -6% threshold there are 4,119 events but only 784 distinct DATES, so
  statistics are computed on per-date means.
* **Overlap.** Consecutive dates' h-day forward windows share h-1 days.
  Newey-West applied; see below, because this one bit twice.

### The wide sweep is the result

196 cells (7 thresholds x 7 horizons x drop/spike x raw/idiosyncratic):
**4 beyond |t|=2, against ~9 expected from noise.** Fewer significant cells
than chance produces. Individual cells are not findings at that width.

| variant | positive cells | mean t |
|---|---|---|
| raw after drop | 38/49 | +0.53 |
| raw after spike | 45/49 | +0.89 |
| idio after drop | 23/49 | -0.03 |
| idio after spike | 46/49 | +1.21 |

Raw drops AND raw spikes are both followed by above-baseline returns (38/49,
45/49). No directional effect can do that. A VOLATILITY effect can: a large
move in either direction marks an elevated-vol period, and in this window
elevated vol was followed by recovery - the 2022 drawdown and its rebound.

### Two of my own errors, both caught by widening the test

**1. Overlap manufactured significance, again.** The first run showed t =
2.19-2.48 at h=21 across every threshold, monotone in threshold, starred as
significant. Corrected for the 20-day window overlap, all fell to 0.89-1.27.
The tell was that significance appeared ONLY at h=21, where overlap is largest,
and vanished at h=1 where there is none. This was the FOURTH time in one day
that raw statistics on overlapping windows produced a false positive, and the
second time it fooled me personally after I had documented it that morning.

**2. A narrow grid produced a false story.** I reported idiosyncratic drops as
showing "consistent continuation - all 12 cells negative", from thresholds
3-6% and horizons 1-10. Widened to 49 cells the same measurement is 23/49
positive: no consistent sign. Twelve adjacent cells agreed because they are
nested subsets of the same data, not because the effect was real.

**Standing rule added:** sign consistency across nested cells is NOT
independent evidence. Nested thresholds on nested horizons over one panel are
perhaps 2-3 effective tests, and a real effect shows as a CONTIGUOUS region
across a wide grid, not as agreement within a narrow one.

### What the operator observed is real; the inference was not

Big drops in tech during 2021-2026 were often followed by recovery. That is
what tech DID - the unconditional 21-day forward return was already +2.18%.
The bounce was not predicted by the drop. And the variant that would isolate a
genuine reversal - the name falling alone while the market holds - shows
nothing at all (t between -0.90 and +0.87 across 12 cells).

**Survivorship still biases "drops recover" upward** and the effect still did
not appear, so the true result is weaker than shown.

---

## 2026-07-29 - point-in-time universe built; momentum's IC rose, its P&L fell

**2,496 sessions, 1,996,800 rows, 3,176 names** after filtering 17 exchange
TEST tickers (ZWZZT closed at 199,999.00 in the raw tape). Membership decided
monthly from the PRIOR month's dollar-volume rank, so it uses only what was
known. Validated by a clean monotone survivorship decay: 24% of the 2016
universe is absent from the recent liquid list, falling to 0% for 2026. If the
build were still survivorship-conditioned every year would read near 0%.

### Same signals, two universes (h=1, non-overlapping)

| signal | survivorship IC | t | point-in-time IC | t |
|---|---|---|---|---|
| **momentum_12_1** | 0.0174 | 3.29 | **0.0247** | **4.42** |
| reversal_5d | 0.0101 | 2.54 | 0.0100 | 2.24 |
| low_vol_63d | -0.0000 | 0.00 | 0.0019 | 0.35 |
| inv_price (size proxy) | -0.0024 | -0.78 | 0.0006 | 0.25 |
| random_control | - | - | -0.0000 | 0.00 |

**Momentum got STRONGER on the honest universe** - IC +42%, t 3.29 -> 4.42. I
predicted the opposite and said so in advance. The mechanism: momentum's SHORT
leg wants the losers, and survivorship had removed exactly those. The bias was
holding momentum DOWN, not up. My reasoning (winners selected after winning)
is right for the long-only version and wrong for long/short.

The size effect was never there: -0.0024 -> 0.0006.

### But the traded result got WORSE

| | survivorship | point-in-time |
|---|---|---|
| 12-1 L/S | Sharpe 0.26 | **0.16** |
| 12-1 L/S + band | 0.35 | **0.26** |
| 12-1 long-only | 1.07 | **0.75** |
| max drawdown (L/S) | -32.2% | **-46.4%** |

Long-only lost a third of its return and a third of its Sharpe, which is
survivorship being removed from the long leg as expected. But long/short fell
too, while its IC rose - rank accuracy and realised P&L came apart. Restoring
the dead names gives the short leg the losers it wants (better IC) and those
are precisely the names that gap, squeeze and become unborrowable (worse P&L,
-46% drawdown).

**Surviving number: Sharpe 0.26**, pre-registered parameters, survivorship-free
universe, costs and borrow charged. The same 0.26 as before, now re-derived
rather than re-quoted.

### Two corrections to my own claims, both caught by the operator or the data

**1. "The drawdown is probably understated because a delisting-at-zero short
shows up as a position that simply stops rather than one you couldn't close."**
Wrong twice. Delisting does not trap you - for a short, a name going to zero is
the BEST case, closed out near zero. And empirically the residual gap is not
collapses: of names stopping while still members, 82% end within 20% of their
own 1-year high and the median final/high ratio is 1.00. They are acquisitions
closing. The missing piece is a further jump to the deal price, so the bias is
DOWNWARD - a missed gain on longs, a missed loss on shorts.

  The real unmodelled short-side risk is BORROW RECALL and forced buy-in, which
  does bias optimistically. I conflated "the position stops" with "you are
  trapped"; only the second would have supported the claim.

**2. Two survivorship metrics reported as one.** I first said 84% of historical
top-500 membership was gone, then 79% of names "stop mid-sample". Both were
inflated: 1,520 tickers entered the top 500 on exactly ONE day, and most
"stops" were names leaving the liquid set rather than delisting. A name you
would have stopped holding is correctly excluded, not lost. The honest figure
is names that stop while STILL members: 413 in the filtered panel. The first
claim was 16x too high.

### Remaining limits

* 812 of 3,176 names carry the 252-day lookback (300+ sessions required), which
  reintroduces a mild frame-level survivorship filter - much weaker than
  survived-to-today, but not zero.
* Borrow recall / forced buy-in unmodelled, biasing long/short optimistically.
* 413 terminal-return gaps, biasing downward.

---

## 2026-07-30 — long-only momentum through walk-forward: the benchmark kills it

**Finding: long-only 12-1 momentum on the point-in-time universe does not beat
owning the universe. Walk-forward out-of-sample Sharpe 0.71 vs 0.94 for an
equal-weight book of the same panel over the identical windows, through the
identical execution path. The last undischarged positive number is
discharged — not by overfitting this time, but by the benchmark.**

Setup held identical to the prior work: same PIT panel (monthly membership
from the prior month's dollar-volume rank), same execution (fill at t+1's
open, 5bps slippage + $0.005/sh), same grid as the L/S PBO run
(lookback 189/252/315 x rebalance 5/10/21), `SHORT_ENABLED=0`. Borrow does
not apply long-only. Script: `scripts/momentum_wf_pbo.py`; full results in
`~/.local/share/mmr/reports/momentum_wf_pbo.json`. 87 panel runs, 4m33s.

### Phase A — full-period grid (2018-01 measured start, costs charged)

| Sharpe | reb 5 | reb 10 | reb 21 |
|---|---|---|---|
| lookback 189 | 0.66 | 0.65 | 0.67 |
| lookback 252 | **0.78** | 0.74 | 0.77 |
| lookback 315 | 0.63 | 0.62 | 0.62 |
| **equal-weight benchmark** | | | **0.99** |

* Best cell lb252/rb5 at 0.78 is the 07-29 "Sharpe 0.75" re-derived (the
  small difference is the measured-window start). The harness is faithful.
* The surface is FLAT (0.62–0.78) — no 315-cliff like the L/S grid, because
  the long-only book carries the equity premium everywhere; parameters barely
  matter. PBO 0.11/0.21/0.21 at S=8/12/16 (9-trial caveat applies): the
  parameter selection is NOT the problem.
* DSR 0.976, and misleading BY CONSTRUCTION: it deflates against a
  zero-Sharpe null, and a long-only book's honest null is the market — the
  same beta-matched-null lesson the negative control taught on 07-28. The
  equal-weight benchmark IS the beta-matched null here, and it wins: Sharpe
  0.99, return +324.5%, maxDD −34.0% vs the best cell's −39.7%.

### Phase B — walk-forward (rolling, train 2y, trade 1y, 7 folds)

| fold | test window | chose | train SR | test SR | test ret |
|---|---|---|---|---|---|
| 0 | 2019-07 → 2020-07 | lb189/rb21 | 0.94 | 0.64 | +18.9% |
| 1 | 2020-07 → 2021-07 | lb315/rb21 | 0.53 | 1.40 | +47.8% |
| 2 | 2021-07 → 2022-07 | lb189/rb10 | **1.13** | **−0.97** | **−28.8%** |
| 3 | 2022-07 → 2023-07 | lb252/rb21 | 0.42 | 0.50 | +7.6% |
| 4 | 2023-07 → 2024-07 | lb252/rb10 | **−0.15** | 1.34 | **+33.7%** |
| 5 | 2024-07 → 2025-07 | lb252/rb5 | 1.02 | 1.11 | +33.5% |
| 6 | 2025-07 → 2026-07 | lb252/rb5 | 1.14 | 1.06 | +38.9% |

| | Sharpe | CAGR | maxDD |
|---|---|---|---|
| walk-forward momentum | 0.71 | +18.97% | −42.0% |
| equal-weight, same windows | **0.94** | +17.92% | (−34.0% full-period) |

* **Selection stability 0.17** — five distinct cells across seven folds. This
  coexists with the low PBO without contradiction: on a flat surface the
  cells are nearly interchangeable, so the in-sample winner's out-of-sample
  rank is weakly preserved (low PBO) while the identity of the winner is
  noise (no stability). Low PBO here means "the choice doesn't matter", not
  "the choice is skilled".
* The train score carries no information about the test outcome: the highest
  train Sharpe to that point (1.13) preceded the only losing fold (−28.8%,
  the 2021-22 momentum crash), while a NEGATIVE train Sharpe (−0.15, best
  available in fold 4) preceded +33.7%.
* Net: the ranking buys +1.0pp of CAGR over owning the universe, and pays
  0.23 of Sharpe and ~8pp of extra drawdown for it. That is concentration,
  not selection — the same decomposition the 07-29 survivorship comparison
  suggested (long-only 1.07 vs benchmark 0.96), now confirmed with honest
  selection on the honest universe.

### Honesty notes, stated before the result was known

The grid prices 9 trials. It does not price the upstream choices made after
seeing data — long-only itself (chosen after L/S disappointed), quintiles,
the 21-day skip. And fold 0's warm-up is slightly short (the panel starts
2016-08); cells with lookback 315 start a few weeks late inside a 2-year
training window.

### Where this leaves the search

Momentum long/short: real signal, Sharpe 0.16–0.26 traded, undeployable.
Momentum long-only: the market plus concentration risk, dominated by
equal-weight ownership of the same universe. **The price-signal well is dry —
both remaining configurations of the one surviving signal are now discharged.
Raising the ceiling requires genuinely different information, not more
price-derived signals or better parameters on this one.**

---

## 2026-08-17 — OpeningDriveFade under live semantics: the last pre-registered thread closes negative

The one intraday strategy that consistently scored BELOW the PBO coin-flip
line (35/39/22% across sweeps 16/22/25) had never been re-run under the two
corrections built since: AutoExecutor live semantics (no pyramiding, fixed
notional, cooldown) and the quarantine-cleaned store. Sweep 43 is that re-run,
pre-registered: grid and symbols identical to sweep 25 (10 mega-caps, 1-min,
365d, DRIVE_WINDOW_MIN × DRIVE_ATR_MULT 3×3), `live_semantics: true`, nothing
new searched. 90/90 runs completed.

| | sweep 25 (accumulate, 2026-06) | sweep 43 (live, clean store) |
|---|---|---|
| PBO | 22% | **67.8%** (S=16, 12,870 splits, T=248) |
| best Sharpe | — | 0.91 (GOOGL W=15 ATR=2.0, 174 trades) |
| best DSR | — | 0.426 (deflated for 90 trials) |
| median Sharpe | — | **−0.35**; 33/90 cells positive |

Calibration: the negative control's best-of-180 on pure i.i.d. noise was
Sharpe 2.04 (beta-matched 2.26). A best-of-90 at 0.91 sits far below what a
noise search of this size routinely produces.

**Conclusion: the below-coin-flip PBO did not survive live semantics + clean
data — OpeningDriveFade looks like every other intraday family, and its
earlier promise was a property of pyramiding/compounding execution measured
on a dirtier store, not of the signal.** Walk-forward was not run: with the
sweep-level surface this flat there is nothing left for a selection rule to
select. This closes the last pre-registered price-signal thread; the
2026-07-30 conclusion stands unqualified — raising the ceiling requires
genuinely different information.

(Not attributable to the trade-count reliability penalty: median 122 trades
per cell. Runs are `_execution_mode=live`, `_trade_notional=2000`, stamped in
params for reproducibility; `mmr sweep overfit 43` reproduces the PBO/DSR.)

---

## 2026-08-17 — fundamentals on the honest universe: size is NOT survivorship

The 07-29 conclusion had two parts: "the value ratios are the size effect"
(measured, decomposition) and "the size effect here is survivorship"
(hypothesis — the universe was today's top-500, so its small names were the
ones that grew into the list). The second part could only be tested on a
universe that keeps the losers. It now has been, and it is FALSE.

**Setup.** Fundamentals extended from 496 present-day names to the
point-in-time membership: every name with >=3 member-months (rank<=500 at a
month-end), 2,339 fetched, 1,394 with 10-K/Q filings -> 81,120 filings across
1,890 tickers, all EDGAR-acceptance-keyed as before. Coverage now includes
the failures' final filings: SIVB's last 10-K before the collapse, TWTR to
going-private, ATVI/VMW/XLNX to acquisition. (Known gaps: FRC returned
nothing from sec-api's ticker index; AABA correctly absent — investment
company, no 10-K/Q. Ticker-recycling merges two companies' filings into one
column — same accepted residual as pit_daily_bars.) Scan machinery:
`fundamental_signals.py --pit`, ticker-joined, same as-of rule (acceptance
instant strictly before the 16:00 ET read).

**Honest-universe scan (h=63, NW-corrected):** sales_to_price IC 0.0477
(t 2.83), cash_flow_yield 0.0402 (t 2.40), earnings_yield h=21 0.0200
(t 2.25); random control clean (t 0.16); +7d leak probe flat, as expected
for slow ratios. Ratio-signal coverage is ~10% of the full frame against a
~17% membership ceiling (only ~500 of ~3,000 columns are live members on any
day; TTM warm-up and missing shares_diluted account for the rest).

**Decomposition (all variants on sales_to_price's own cells, h=63):**

| variant | IC | t(NW) |
|---|---|---|
| sales_to_price as measured | 0.0477 | 2.83 |
| revenue FROZEN per name | 0.0431 | 2.43 |
| shares FROZEN per name | 0.0266 | 1.32 |
| 1/price alone | 0.0371 | 2.06 |
| **1/marketcap alone** | **0.0443** | **3.34** |

Two reversals from 07-29:

1. **Size survives the honest universe.** 1/marketcap, WITH the delisted
   losers included, is the strongest and most consistent line in the table.
   The survivorship explanation predicted it would collapse; it did not.
2. **Revenue now ADDS (a whisker).** On the survivor universe, freezing
   revenue made the signal STRONGER (0.0425 -> 0.0591) — the fundamental was
   subtracting. Here freezing it weakens the ratio (0.0477 -> 0.0431): the
   actual revenue numbers contribute ~10% of the IC. First time a
   fundamental has added anything, and it is small, with a LOWER t than size
   alone.

**What this is NOT yet:** tradeable. The next test is the one that killed
momentum: rank accuracy and tradeability came apart there (IC rose on the
honest universe while traded Sharpe fell), and a small-cap tilt is long the
names that gap, halt and cost the most to trade. Required before belief:
panel backtest of the size tilt vs EQUAL-WEIGHT ownership of the same
universe (the benchmark that killed long-only momentum), walk-forward, with
costs. A size tilt that cannot beat owning the universe is concentration,
not selection — we have measured that decomposition once already.

---

## 2026-08-17 (later) — the size tilt is TRADEABLE: first construction to survive its benchmark

The gap momentum died in — IC rose on the honest universe, traded Sharpe
fell — is where the size finding had to be tested next. It survived.

**Pre-registered design** (`scripts/size_tilt_backtest.py`, written before
the result): two trials, no grid — bottom-marketcap quintile and top
sales_to_price quintile of the covered PIT universe, monthly rebalance,
equal weight within the slice, long only, `run_panel` execution identical to
the momentum test (t+1 open, 5bps, $0.005/sh). Decisive benchmark:
equal weight over the SAME covered names. No fitted parameters, so PBO does
not apply; the benchmark comparison is the test.

| book | Sharpe | CAGR | maxDD |
|---|---|---|---|
| small-cap quintile (1/mcap) | 1.15 | 32.7% | −42% |
| **top sales_to_price quintile** | **1.26** | 31.3% | −42% |
| EW covered set (benchmark) | 1.04 | 20.2% | −37% |
| EW whole panel (context) | 1.04 | 18.5% | −34% |

Momentum LOST 0.23 Sharpe to this benchmark; the size tilt beats it by
+0.11 and +12.5pp CAGR, and s2p by +0.22 Sharpe.

**The bias that had to be priced first: truncated terminal losses.** A held
name whose series ends exits at its last stored price. 224 tilt-held names'
series end while held (9.9y) — but most are NOT deaths: they fell below the
PIT build's top-800-by-volume storage cutoff while continuing to trade
(TROW, IRM, FLS...), for which last-price exit is approximately realizable.
The genuine-collapse class (fall ≥80% from 1y peak: AMC, BBBY, NVAX, RAD,
SPCE, SPWR...) is 13 events, and zero-stressing their ENTIRE entry weight —
an overstatement, since 80–97% of each fall is already inside the data —
costs −2.0%/yr. The absurd bound (every ≥50% faller worthless at exit,
51 events) costs −7.8%/yr. The +12.5pp edge survives both.

**Per-year excess vs the covered-EW benchmark:** size tilt positive 8 of 10
years; **s2p positive 9 of 10, including +17.2% in the 2022 bear** (the raw
size tilt's one bad year, −9.2%) and +11.6% in 2026 YTD. 2021 is the
largest single contribution (+54/+35pp — the meme era) but the excess does
not depend on it. s2p is the better book on every robustness cut, which is
consistent with the decomposition: revenue adds, and it adds most exactly
when smallness alone is being punished.

**Honesty notes.** Two trials priced; quintile width, monthly cadence and
long-only are unpriced upstream choices. 5bps flat slippage is optimistic
for the smallest names in a liquid universe (monthly cadence keeps the
sensitivity modest: tripling the cost assumption costs well under 1pp/yr).
Absolute levels reflect an equal-weight liquid-universe book in a strong
decade — the CLAIM is the spread over the benchmark, nothing else. Coverage
gaps (FRC absent from sec-api; ticker recycling) are inherited from the
fundamentals store.

**Where this leaves the search:** the 2026-07-30 "the well is dry"
conclusion was about PRICE-derived signals, and it stands. This is the
new-information direction it called for, and it is the first result in the
program's history to clear: an honest universe, a pre-registered design,
its own benchmark, a bias stress, and a per-year robustness cut. Next
decisions are operational, not statistical: whether/how a monthly ~60-name
rebalance book fits MMR's execution machinery, and paper-trading it before
any capital.

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

---

## 2026-09-29 — correction: membership-filtered PIT prices froze every leaver in the book

Found while building the low-frequency harness (`trader/research/lowfreq`,
C01). `scripts/pit_panel.load_pit_panel` returns prices ONLY on days a name
is a member, so the month a name leaves the top-500 its series vanishes and
the sell a monthly book issues can never fill (`run_panel` skips fills it
cannot price). The position then sits in the book forever, marked at a stale
carry-forward, and every later rebalance sizes new buys off equity that
includes it — implicit, cost-free leverage.

Measured on the covered-EW benchmark, 2016-08..2024-06, `run_panel`, 5 bps +
$0.005/sh (`research/lowfreq/reference/trials.jsonl`, rows for
`eq_ew_covered`; command `uv run python -m trader.research.lowfreq evaluate
reference/eq_ew_covered`): with membership-filtered prices 511 of 857 held
names had no price at the end, worth **102 % of reported equity**, and cash
was **−$3.2M on 99 % of fill dates** (full-period Sharpe 0.88, CAGR 16.7 %).
With prices kept for every name ever a member (`fields['member']` carries
membership; opens refused outside it) and a stopped name's last price
standing for 30 sessions so the exit fills: 0 frozen names, min cash +0.01×,
**full-period Sharpe 0.59, CAGR 10.0 %**
(`research/lowfreq/reference/results/eq_ew_covered.json`).

Consequence for earlier entries: the 2026-08-17 size-tilt / sales-to-price
books AND their covered-EW benchmark were run through the same
membership-filtered loader, so both sides carried frozen leavers and implicit
leverage. The *spread* may survive (both books were affected), the absolute
levels quoted there (Sharpe 1.26 vs 1.04, CAGR 31 % vs 20 %) should not be
believed until re-run through the harness. Not re-run here.

**2026-09-29 (C01, D06 labelling).** Every `Backtester.run_panel` figure in this log dated before 2026-09-29 — including the 2026-08-17 size-tilt / sales-to-price books and their covered-EW benchmark (Sharpe 1.26 vs 1.04) and the 12-1 momentum panel runs — is **pre-D06**: produced before the panel executor's cash guard (BUY fills scaled to available cash) and before stopped names were exited at their last stored print (D07), so those books could run on implicit leverage and stale marks. Read them as pre-fix until re-run through `trader.research.lowfreq`.

---

## 2026-09-29 — low-frequency program: cross-asset null, sales-to-price PROMISING at N = 20

Program `20260928-185549` (summary `research/lowfreq/SUMMARY.md`): a
pre-registered, trial-counted harness (`trader/research/lowfreq`, protocol
D01–D07 in `research/lowfreq/README.md`) applied to two information sets the
program had not exploited well — a 49-ETF total-return panel and the
point-in-time equity universe with EDGAR-acceptance-keyed fundamentals.
Rolling walk-forward, t+1-open fills, decisive benchmark through the identical
path, Newey-West t on active return, DSR deflated by distinct-entry count, 3×
cost stress. Dev data end 2024-06-28; the period from 2024-07-01 is held out
and will be evaluated by the orchestrator with the fixed-cell commands in
`SUMMARY.md`. **Neither family produced a PASS.**

| family | trials | champion | verdict | OOS Sharpe vs decisive | active/yr | t(NW) | DSR |
|---|---|---|---|---|---|---|---|
| xasset (5 mechanisms, 24 cells, 12 folds 2012–24) | 7 | `xa_vol_target`, TARGET_VOL 0.15 | **FAIL** | 0.897 vs 0.950 (60/40) | +1.22 % | 0.77 | 0.04 |
| equity (7 hypotheses + 13 controls, 4 folds 2019–23) | 20 | `s2p_top_quintile` | **PROMISING** | 0.878 vs 0.564 (covered-EW) | +12.0 % | 2.01 | 0.64 |

Commands: `uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target`
and `... evaluate equity/s2p_top_quintile`; results
`research/lowfreq/<family>/results/<id>.json`; every trial in each family's
`trials.jsonl` and `REPORT.md`.

**Cross-asset (C02).** All seven candidates fail gate (a); none reaches
60/40's OOS Sharpe and only the champion has positive active return. Unlevered
long-only trend, risk parity and vol-targeting on this universe put 49–72 % of
the book in bonds/BIL and earned bond-like returns (Sharpe 0.59–0.65, CAGR
2.3–3.7 %) against a 60/40 that compounded at 9 %; the champion is 85 % SPY on
average and its Sharpe is lower than 60/40's because de-risking follows the
fall (2020 active −18.6 %). Dual momentum and carry failed the drawdown gate
(−40.8 %, −34.4 %). PBO on the 4-cell grids 0.34 / 0.73 / 0.51.

**Equity (C03).** The sales-to-price quintile beats owning its covered
universe on every gate but DSR: +12.0 %/yr active in all five counted years,
+11.5 % at 3× cost, max DD −36.8 % vs −32.7 %, D07 terminal-loss stress
(all stopped names to zero, both books) active +13.8 %, t 2.25. Decomposition
(`s2p_frozen_revenue` 0.88 / +11.9 %; `size_smallcap` 0.76 / +8.7 %;
`revenue_raw` 0.53 / −0.4 %): the signal is the LEVEL of sales relative to
price — not revenue updates, not only size. Momentum 12-1, low volatility and
gross profitability do not beat covered-EW. Fragility stated before belief:
t ≈ 2 on four years of monthly returns, every OOS drawdown containing March
2020. This supersedes the 2026-08-17 figures (Sharpe 1.26 vs 1.04), which
were pre-D06/D07 (see the correction above): after honest execution the
spread survives, the levels do not (OOS CAGR 22.9 % vs 10.5 %).

**Bugs fixed on the way (C01):** `run_panel` could overdraw cash — buys now
scale to available cash (D06, `tests/test_panel_cash.py`); membership-filtered
PIT prices froze every leaver in the book — prices are kept for every name
ever a member and stopped names exit at their last stored print (D07,
`tests/research/test_lowfreq_round4.py`). Details and consequences in
`SUMMARY.md` and the correction entry above.

**Conclusion.** Low-frequency allocation on liquid ETFs is a clean null under
this protocol. The one construction in the program's history that beats its
own universe honestly is a monthly sales-to-price tilt, PROMISING at N = 20;
the held-out period, not more search, decides whether it is real.

---

## 2026-09-29 — held-out verdict: neither champion survives 2024-07 onward

Run by the orchestrator on data no agent saw (research DB extended past the
2024-06-28 dev cut; fixed-cell mode, the champions' pre-registered cells, no
selection; commands in `research/lowfreq/SUMMARY.md`):

| champion | window | OOS Sharpe | decisive benchmark Sharpe | active/yr | t(NW) | 3x-cost active | verdict |
|---|---|---|---|---|---|---|---|
| equity `s2p_top_quintile` | 2024-07-01..2026-07-28 | 1.30 | 1.17 (covered-EW) | **+0.15%** | 0.03 | −0.30% | FAIL |
| xasset `xa_vol_target` 0.15 (primary) | 2024-07-01..2026-09-28 | 0.99 | 1.40 (EW) / 1.12 (60/40) | +0.85% | 0.16 | +0.73% | FAIL |
| xasset `xa_vol_target` 0.12 (secondary) | same | 0.99 | same | −0.81% | −0.17 | −1.01% | FAIL |

The sales-to-price tilt earned +12.0%/yr over covered-EW in the dev
walk-forward (2019-2024, t 2.01, PROMISING) and **+0.15%/yr held out** — its
absolute Sharpe of 1.30 is the market's, not the tilt's (the benchmark made
1.17 owning the same names). Two years is a short window (the standard error
of a two-year active Sharpe is ~0.7), so this does not prove the effect is
zero; it does mean nothing here clears the bar for capital. The cross-asset
null was expected and holds. Results:
`research/lowfreq/{equity,xasset}/results/*__fixed_2024-07-01_*.json`.

Along the way: the fixed-cell results file was keyed by window only, so a
second cell over the same window overwrote the first (fixed in `fee9e31`,
regression test pinned).

## 2026-09-29 — SEC Form 4 insider purchases: a null against the PIT equal-weight basket (N = 16)

Program `20260929-131516` (C03 data, C04 family; `research/lowfreq/insider/`).
A genuinely new information source, tested under the harness protocol
(D01/D11): open-market purchases (code P) from the SEC's quarterly insider
data sets, 2.19 M filings in the dev copy, usable only from the close of the
first session AFTER `FILING_DATE` (D14 — filings are frequently accepted after
the close), amendments superseding their originals through six deterministic
matching tiers (2,592 unmatched, counted). Long-only overlays on the D11
monthly equal-weight PIT basket, walk-forward train 3 y / test 1 y, 4 OOS
folds 2019-08..2023-07 (1,005 sessions), costs 5 bps + $0.005/share.

| rule (H) | OOS Sharpe | bench | active/yr | t(NW) | DSR (16) | its size control, active/yr | verdict |
|---|---|---|---|---|---|---|---|
| `ins_purchase_count` (C0) | 0.40 | 0.49 | −0.65 % | −0.25 | 0.01 | +4.69 % | FAIL |
| `ins_cluster` (H2) | 0.38 | 0.49 | −1.64 % | −0.71 | 0.00 | +1.49 % | FAIL |
| `ins_officer` (H4a) | 0.36 | 0.49 | −2.34 % | −1.21 | 0.00 | +0.54 % | FAIL |
| `ins_director` (H4b) | 0.47 | 0.49 | +0.97 % | 0.33 | 0.03 | +6.12 % | FAIL |
| `ins_net_buy_ratio` (H5, champion) | 0.51 | 0.49 | **+1.75 %** | **0.56** | 0.05 | +3.81 % | **FAIL** |
| `ins_value_to_mcap` (H6) | 0.44 | 0.49 | +0.37 % | 0.12 | 0.02 | +1.79 % | FAIL |

(`ins_opportunistic` and `ins_conviction` sit between C0 and H4b.) Eight
rules, eight market-cap-matched controls holding NO insider information, one
purchase-count control: **every control beats the basket by more than its
rule does**, and seven of the eight controls are PROMISING on their own
(director control t 2.94). The matching pool is covered names with an as-of market cap
(operating companies with EDGAR fundamentals, 58 % of covered name-days), so
the overlays' excess over the basket is the operating-company/size tilt the
2026-08-17 fundamentals entry already found — not information in the
purchases. The pre-registered eligibility rule (beat the size control AND C0)
admitted nobody; the champion is the fallback (highest active t), and it is
FAIL on gates (b) and (c). D07 stress (all 303 stopped names to zero, both
books): active +1.07 %/yr, t 0.64 — still nothing. Full tables and per-trial
artefacts: `research/lowfreq/insider/REPORT.md`, `results/`.

Held-out (orchestrator only, insider data to 2026-03-31):
`LOWFREQ_RESEARCH_CONFIG=~/.local/share/mmr/research_full/trader_research.yaml uv run python -m trader.research.lowfreq evaluate insider/ins_net_buy_ratio --fixed-params '{}' --start 2024-07-01 --end 2026-03-31`.

Method note worth keeping: the size-matched control was pre-registered
(PREREG addendum 1, before the first ledger row), and it is what turned "five
PROMISING overlays" into a null in one table. Without it, the report would have
read as an insider effect.

#### Insider held-out and live dry run (2026-09-29)

**Held-out result (orchestrator, 2026-09-29): FAIL.** `ins_net_buy_ratio`,
fixed cell, 2024-07-01..2026-03-31 (the full insider data's end), full
research DB: OOS Sharpe 0.90 vs `eq_ew_pit` 0.84; active **+0.72 %/yr, t 0.30**,
DSR 0.12 (N = 16), +0.50 %/yr at 3x costs; per year 2024 +0.8 %, 2025 −0.6 %,
2026 (Q1) +1.1 %; 136 forced exits in both books. Ledger row 49, artefact
`research/lowfreq/insider/results/ins_net_buy_ratio__fixed_2024-07-01_2026-03-31_44136fa3.json`.
The dev null holds out of sample: the insider family is closed.

Live path dry run (container, 2026-09-29): PIT bars and fundamentals caught up
(44 sessions, 1,418 new filings, 0 errors); `lowfreq_live targets` correctly
refuses a non-rebalance session; a dry run as of 2026-09-01 (written to /tmp,
not the live targets directory) plans 62 names at 1/62. At the runbook's
$25,000 sleeve capital each name is $403, three names cost more than that
and cannot be bought at all under floor-and-refuse sizing, and 19 % stays in
cash; at $100,000 all 62 are bought and cash is 4.7 %. The paper sleeve should
start at $100k (or with a quintile cap on names), not $25k.

---

## 2026-11-15 — correction: the market cap under every price ratio was wrong, and nothing fundamental survives it

Found while porting the fundamentals ingest off a retired vendor endpoint
(audit `docs/AUDIT_2026-11-14.md` A7.5). Comparing the replacement endpoint
field by field with the stored rows showed share counts that disagreed by the
ratio of a later stock split, which is how the first defect below surfaced;
the other three were found measuring it. **None is a timing leak, so the
leak probe this program relies on (delay every acceptance instant by a week,
see whether the signal degrades) could not see any of them**: it moves WHEN a
number is known, not whether the number is right or on the same basis as the
price beside it.

### What was wrong

`market cap = pit_daily_bars.close x fundamentals.shares_diluted` is the
denominator of sales-to-price, book-to-price, earnings yield, cash-flow yield
and of `1/marketcap`.

| defect | what it is | measured |
|---|---|---|
| **split basis** | `pit_daily_bars` prices are split-ADJUSTED (the vendor's `adjusted=True`, as of the fetch); a filing's share count is AS REPORTED on the filing day. Their product understates the market cap by the cumulative ratio of every LATER split. | NVDA on 2021-07-01: stored close 20.21 (it traded at 808), stored shares 632M: $12.8B against a true $511B, a factor of 40. 100 names that were top-500 for more than 250 sessions have split since 2016-08: CMG 50x, NVDA 40x, BKNG 25x, AMZN and GOOG 20x, ANET 16x, TSLA and ORLY 15x, MNST 12x, AVGO / KLAC / LRCX / TTD / SHOP 10x. Reverse splits err the other way (MARA, NVAX, LCID: cap overstated 10-20x). |
| **fiscal-year-end share counts** | The retired endpoint derived a fiscal Q4 by subtraction from the annual figures. Right for a flow; for a weighted-average share count it stored `annual average - mean(Q1..Q3)`. | AAPL FY2025: -47,029,000. Of 19,153 stored 10-K rows, 3,022 have no count, 7,393 a non-positive one, and 7,152 of the 8,709 that can be compared with the previous period are outside 0.5-2x of it. Negative for a company buying back stock (dropped from coverage for the quarter after its 10-K); small and POSITIVE for one issuing stock (market cap near zero, top of every price ratio). |
| **late-filing notices** | The sec-api form filter `formType:"10-Q"` also matches `NT 10-Q`, a notice that the report will be late, filed BEFORE it. Rows dated to one put the numbers ahead of their filing. | 952 stored rows (758 in the dev snapshot), 304 tickers. |
| **units** | Quarterly share counts off by a power of a thousand. | 432 of 44,489 (0.97%) outside [1/3, 3] of the centred five-filing median; 270 tickers. MCD 2024-03-31: 725.9. |
| **trailing sums** | "TTM" was the sum of the last four ROWS. | 1,800 of 66,663 period-to-period steps skip a quarter, so those sums span fifteen months or more. |

The first is lookahead in the plain sense: it ranks the 2019 universe by which
names would have split by 2026, and stocks split after they have gone up.

What it did to the book, on six month-ends (as-stored top quintile against
the corrected one, same code path, full snapshot):

| date | covered | top quintile | in the as-stored book ONLY | of those, market cap wrong by >1.5x |
|---|---|---|---|---|
| 2019-06-28 | 293 | 59 | 18 | 18: AXP, VFC, MCHP, GIS, TTWO (unit / 10-K errors), CMG 50x, NVDA 40x, BKNG 25x, AMZN 20x, ANET 16x, ORLY 15x, TSLA 15x, AVGO, KLAC, LRCX 10x |
| 2020-06-30 | 281 | 56 | 14 | 14: incl. CMG, CRM, NVDA, BKNG, AMZN, TSLA, AVGO, KLAC, LRCX |
| 2021-01-29 | 248 | 50 | 19 | 17: incl. NFLX, AVGO, ADI, DIS, DE, CMG, NVDA, BKNG, AMZN, KLAC, LRCX |
| 2022-01-31 | 283 | 57 | 20 | 20: incl. NFLX, INTC, DIS, SBUX, BA, CMG, BKNG, AMZN, ANET, KLAC, LRCX, MSTR |
| 2023-06-30 | 344 | 69 | 13 | 13: incl. ORCL, CMG, BKNG, ORLY, AVGO, KLAC, LRCX, NFLX, SMCI |
| 2024-05-31 | 324 | 65 | 18 | 16: incl. MCD, GOOG, MRVL, CRWD, SNOW, CRM, CMG, BKNG, KLAC, LRCX, NFLX, SMCI |

A quarter to a third of the "cheapest on sales" book was mega-cap growth
winners that were not cheap on sales.

### The registered candidate, re-run

`equity/s2p_top_quintile`, the same registry entry and harness
(`trader.research.lowfreq evaluate`), on copy-on-write clones of the frozen
research snapshots with the complete split history added to their
`corporate_splits` table and the prices' adjustment date recorded
(`pit_price_basis`, 2026-07-29). Results went to scratch roots; the committed
ledger (`research/lowfreq/equity/trials.jsonl`) was not written to — this is a
data correction to an existing trial, not a new one, and `n_trials` stays 20.
Each correction was added in turn (`MMR_FUNDAMENTALS_CORRECTIONS`), so its
effect can be read off. Development walk-forward, OOS 2019-2023:

| corrections applied | OOS Sharpe | covered-EW | active /yr | t(NW) | positive years | 2019 / 20 / 21 / 22 / 23 | verdict |
|---|---|---|---|---|---|---|---|
| none (rows as stored) | 0.878 | 0.564 | +11.99% | 2.01 | 5/5 | +2.1 +4.8 +27.7 +8.1 +5.2 | PROMISING |
| notices | 0.879 | 0.564 | +12.03% | 2.02 | 5/5 | +2.1 +4.9 +27.7 +8.0 +5.2 | PROMISING |
| + fiscal-year-end shares | 0.880 | 0.564 | +11.97% | 2.04 | 4/5 | +3.2 +4.9 +25.2 +15.6 -1.1 | PROMISING |
| + split basis | 0.754 | 0.564 | +9.01% | 1.29 | 3/5 | +1.3 -3.5 +26.0 +19.0 -6.9 | PROMISING |
| + units | 0.738 | 0.565 | +8.58% | 1.24 | 3/5 | +1.4 -3.7 +24.6 +18.0 -6.1 | PROMISING |
| + consecutive quarters (ALL) | **0.727** | **0.570** | **+8.44%** | **1.20** | **3/5** | +1.3 -6.0 +25.7 +18.7 -6.0 | **FAIL** |
| split basis alone | 0.797 | 0.564 | +9.45% | 1.47 | 4/5 | +0.7 -1.0 +23.7 +11.5 +2.8 | PROMISING |

The first row reproduces the 2026-09-29 entry to the digit, so the scratch
setup is the registered one. With every correction the candidate fails three
gates (active t >= 2, DSR 0.39, drawdown -40.4% against -34.7%). What is left
is a value tilt: strongly positive in the two years value beat growth (2021,
2022), negative in the two it did not (2020, 2023).

Held out (fixed cell, 2024-07-01..2026-07-28):

| | OOS Sharpe | covered-EW | active /yr | t(NW) | verdict |
|---|---|---|---|---|---|
| rows as stored (the 2026-09-29 entry) | 1.301 | 1.169 | +0.15% | 0.03 | FAIL |
| all corrections | 1.336 | 1.051 | +2.09% | 0.37 | FAIL |

The puzzle the 2026-09-29 held-out entry left — +12%/yr in development,
+0.15%/yr held out — has its explanation: the development figure was inflated
by knowledge of splits that had happened by the time the prices were fetched;
in the held-out window almost none had happened yet.

### The information coefficient, decomposed again

`scripts/fundamental_signals.py --pit --decompose --horizons 21`, full
snapshot (2016-08..2026-07), every variant scored on sales-to-price's cells:

| variant | IC as stored | t(NW) | IC corrected | t(NW) |
|---|---|---|---|---|
| sales_to_price | 0.0204 | 2.13 | **-0.0020** | **-0.13** |
| revenue frozen per name | 0.0178 | 1.72 | -0.0054 | -0.34 |
| shares frozen per name | 0.0151 | 1.40 | -0.0053 | -0.37 |
| 1/marketcap alone | 0.0168 | 1.98 | **0.0022** | **0.22** |
| 1/price alone | 0.0168 | 1.66 | 0.0211 | 2.05 |

With a market cap that is a market cap, sales-to-price ranks nothing and
neither does size. `1/price` still scores, and that is the same defect seen
from the other side, not a finding: the LEVEL of a split-adjusted price is
itself forward-looking (a low adjusted price marks a stock that later split),
so any signal that uses an adjusted price level rather than a return carries
it. Returns are unaffected by a consistent adjustment, which is why the
price-return work (12-1 momentum, Sharpe 0.26) is not touched by this entry.

### Consequences for earlier entries

Every figure below was computed with the uncorrected market cap. None has been
re-run except where stated.

- **2026-07-29, "fundamentals carry no signal; the denominator does"** —
  `sales_to_price` IC 0.0425 (t 2.58), `1/marketcap` 0.0637: the conclusion
  that the ratio's power was its denominator stands, and the denominator's
  power was this defect.
- **2026-08-17, "size is NOT survivorship"** (`1/marketcap` t 3.34 with
  delisted names) — **withdrawn**. Corrected: IC 0.0022, t 0.22.
- **2026-08-17 (later), "the size tilt is TRADEABLE: first construction to
  survive its benchmark"** (Sharpe 1.26 vs 1.04, excess positive 9 of 10
  years) — **withdrawn**. Already marked pre-D06/D07 on 2026-09-29; the
  spread itself does not survive a correct denominator.
- **2026-09-29, "sales-to-price PROMISING at N = 20"** (+12.0%/yr, t 2.01,
  positive in all five counted years; and the controls `s2p_frozen_revenue`,
  `size_smallcap`, `revenue_raw` beside it) — **withdrawn: FAIL on corrected
  data** (table above). The controls were not re-run; they share the
  denominator and their figures should not be quoted.
- **2026-09-29, held-out verdict** — the verdict (FAIL) stands; its numbers
  move to the second row above.
- The insider-purchase null (2026-09-29) used `eq_ew_pit` and a
  **size-matched** control built on the same market cap. Not re-run. The null
  is unlikely to become a positive, but the size matching was matching on a
  wrong size for later-split names.

**Where the program stands: there is no surviving positive result.** The one
durable figure is still pre-registered 12-1 momentum at Sharpe 0.26.

### Limits of this correction, stated before anyone leans on it

- The cleaning rules are judgement calls: a share count is believed when it
  is within 3x of the median of the last four believed counts (a second
  agreeing count confirms a real change of level), and consecutive quarters
  end 70-120 days apart. They were fixed before any re-run was looked at and
  the table shows they barely matter (split basis alone moves t from 2.04 to
  about 1.3-1.5; the two rules together move it 1.29 to 1.20).
- Split ratios come from the vendor's table, which also lists spin-off price
  adjustments (A 1000 -> 1398) and applies them to its prices; they are
  applied here for the same reason. A split the vendor's adjusted prices never
  carried (the raw jump is in the stored series: WLL 2017-11-09, CGNX
  2017-12-04, ESV 2019-04-11) is left out of the share factor — and remains a
  fake -54% / +322% day in the price series of both the book and its
  benchmark.
- The snapshots' prices were taken to be adjusted through 2026-07-29 (the
  build). The live `pit_daily_bars` is NOT uniformly adjusted: days appended
  after a split meet days fetched before it (MNST 97.74 -> 48.62 on
  2026-07-29, APH 143.85 -> 75.16, five ETFs on 2026-10-02). Being repaired
  (`scripts/pit_split_adjust.py`); no research figure above reads those days.
- Ticker recycling (two companies under one ticker) is unchanged and can pair
  a split with the wrong company's filings.

### Method errors worth not repeating

- **A probe tests the failure it was built for.** The +7-day delay catches a
  number known too early. It passed a number that was wrong by a factor of
  forty. The check that would have caught this in July is one line: print the
  market cap the pipeline computes for a name everyone knows, on a date
  everyone remembers, and look at it.
- **An adjusted price LEVEL is forward-looking.** Use adjusted prices for
  returns and for nothing else; anything that needs a level needs the same
  basis on both sides.
- **A vendor's "quarterly" row at a fiscal year end is derived, and the
  derivation only makes sense for flows.**
- **`except Exception` around a vendor call turns a retired endpoint into a
  quiet year.** The ingest reported "0 new rows" for as long as the endpoint
  answered 404.

### Reproduce

```
# one stage (development walk-forward); drop names from the list to stage it
LOWFREQ_RESEARCH_ROOT=<scratch copy of research/lowfreq> \
MMR_FUNDAMENTALS_CORRECTIONS=notices,annual_shares,split_basis,units,periods \
  uv run python -m trader.research.lowfreq evaluate equity/s2p_top_quintile
# held out: LOWFREQ_RESEARCH_CONFIG=~/.local/share/mmr/research_full/trader_research.yaml,
#           plus  --fixed-params '{}' --start 2024-07-01 --end 2026-07-28
# decomposition
TRADER_CONFIG=~/.local/share/mmr/research_full/trader_research.yaml \
  uv run python scripts/fundamental_signals.py --pit --decompose --horizons 21
```

Two reference tables were ADDED to the history database of both research
snapshots (`~/.local/share/mmr/research_dev`, `research_full`) on 2026-11-15,
because the corrected loader cannot run without them and refuses to guess:
`corporate_splits` now holds the complete split history through 2026-07-29
(the snapshots' own table stopped at their data cut - the splits their prices
are adjusted for but they could not see; it is kept as
`corporate_splits_as_snapshotted`), and `pit_price_basis` records that day as
the one the prices are adjusted through. Nothing else in the snapshots was
touched, and no strategy can read either table. The staged runs above were
made on copy-on-write clones (`research_{dev,full}_splits`) carrying the same
two tables; a run on the default configuration afterwards reproduces the
"ALL" row. With `MMR_FUNDAMENTALS_CORRECTIONS` unset every correction is on,
which is how everything else (the harness, `lowfreq_live targets`) now runs.
Code: `trader/data/share_basis.py`, `scripts/fundamental_signals.py`
(`prepare_rows`), `trader/data/statements_v1.py`, commit `c2e332e`.

**The paper sleeve (`paper_s2p`)** holds 15 names bought on 2026-09-30 from
targets computed with the uncorrected market cap. It is a paper exercise of
the monthly machinery; as of this entry it has no research result behind it.
