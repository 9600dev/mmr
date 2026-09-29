## Reading the decomposition

`s2p_frozen_revenue` holds each name's revenue at its first observed value in
the window, so it removes the information in revenue UPDATES while keeping the
cross-sectional LEVEL of sales relative to price. TTM revenue moves slowly
over a four-year window, so the control tracks the as-measured book closely
by construction; the margin between them is the value of the updates, and the
margin over `size_smallcap` is the value of the level. The pre-registered
guard is literal (as-measured must exceed both controls) and is applied
literally; the size of the margins is stated in the tables above so the
reader can weigh it. The 2026-07-29 standing rule's phrase "what you measured
is the denominator" over-reads this instrument: a frozen numerator still
carries the per-name level.

## What the forced-exit numbers mean

`pit_daily_bars` stores the top ~800 names by dollar volume, so a holding
that stops printing has usually fallen below the STORAGE cutoff, not gone to
zero; a few are delistings and acquisitions. D07 sells every such holding at
its last stored print. The stress above marks all of them to zero, so it is
an upper bound on the damage from genuine collapses, harshest for the
small-cap books (their forced-exit value fractions are the largest). The
benchmark is exposed to the same names; the fair comparison (ii) stresses
both books. Names sold at a terminal print are counted per run in every
results JSON (`stopped_names`).

## Limits stated before belief

* The dev period is short (OOS 2019-08..2023-07, four folds) and the OOS
  drawdowns of every book include March 2020; a t(NW) near 2 on four years of
  monthly-rebalanced returns is fragile, and the held-out period the
  orchestrator holds is the real test.
* The as-measured 2026-08-17 figures (Sharpe 1.26 vs 1.04) were pre-D06/D07
  and are not quoted as a baseline anywhere in this report.
* Costs are 5 bps + $0.005/share; the small names inside a top-500-dollar-
  volume universe are liquid (median holding 21-day $ADV in the tables), but
  the model still understates real small-cap cost; the 3x column is the
  bound offered.
* Ticker recycling in `pit_daily_bars` / `fundamentals` can merge two
  companies' filings into one column (documented residual, not fixed here).
* `value_momentum_filter` and `low_volatility` hold nothing in the first
  year of the FULL-PERIOD run (their price history needs ~273 sessions
  from 2016-08); the OOS folds are unaffected (400 days of warm-up before
  every test window).
* Four duplicate pass-1 processes were started by a driver restart and
  killed before completion; a killed run writes no ledger row. Every
  completed run, including partial-N pass-1 rows, is in the ledger table.

## Bugs fixed

none

## Requests to the orchestrator

* Integrate the family outcome into `docs/RESEARCH_LOG.md` and
  `research/lowfreq/SUMMARY.md` (shared paths; not edited by C03).
