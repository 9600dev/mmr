## Reading the decomposition

`s2p_frozen_revenue` holds each name's revenue at its first observed value in
the window: it removes the information in revenue UPDATES but keeps the
cross-sectional LEVEL of sales relative to price, and TTM revenue moves
slowly, so the control tracks the as-measured book closely by construction.
The margin over it (a few bp/yr) is the value of the updates — almost nothing;
the margin over `size_smallcap` is the value of the level; `revenue_raw` (the
numerator alone) is a large-cap tilt that does not beat covered-EW. So H1 is
not its numerator and not only size: what it buys is the level of sales
relative to price. The rule-2 guard is literal and applied literally; the
margins are in the tables so the reader can weigh them.

`pead_earnings` barely exceeds its raw numerator; the asset scaling adds
little and neither reaches the bar. `low_accruals` beats its frozen and raw
numerators and its asset denominator, so the accrual information is real, but
t(NW) < 2 and DSR < 0.90. `quality_value_blend` sits below plain
sales-to-price and below its own frozen control: blending in profitability
removed value, not junk. The momentum filter is below value alone, and
momentum alone does not beat covered-EW, as on 2026-07-30.

## What the forced-exit numbers mean

`pit_daily_bars` stores the top ~800 names by dollar volume, so a holding
that stops printing has usually fallen below the STORAGE cutoff, not gone to
zero; a few are delistings and acquisitions. D07 sells every such holding at
its last stored print; the stress marks all of them to zero, an upper bound
on genuine collapses, harshest for small-cap books. The benchmark holds the
same names, so the fair comparison (ii) stresses both books.

## Limits stated before belief

* OOS is 2019-08..2023-07 (four folds) and every OOS drawdown includes March
  2020; t(NW) ≈ 2 on four years of monthly returns is fragile. The held-out
  period after 2024-06-30 is the real test.
* The 2026-08-17 figures (Sharpe 1.26 vs 1.04) were pre-D06/D07 and are not a
  baseline anywhere here.
* 5 bps + $0.005/share understates real small-cap cost even inside a
  top-500-dollar-volume universe; the 3x column is the bound offered.
* Ticker recycling in `pit_daily_bars` / `fundamentals` can merge two
  companies' filings into one column (documented residual, not fixed here).
* `value_momentum_filter`, `momentum_12_1` and `low_volatility` hold nothing
  in the first year of the FULL-PERIOD run (~273 sessions of price history
  needed); the OOS folds are unaffected (400 days of warm-up per window).
* Four duplicate pass-1 processes started by a driver restart in round 2 were
  killed before completion; a killed run writes no ledger row.

## Bugs fixed

none

## Requests to the orchestrator

* Integrate the family outcome into `docs/RESEARCH_LOG.md` and
  `research/lowfreq/SUMMARY.md` (shared paths; not edited by C03).
