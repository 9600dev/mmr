# Pre-registration — cross-asset ETF allocation family (`xasset`, D04)

Written 2026-09-29 **before any candidate was evaluated** (no row with
`role: candidate` existed in `trials.jsonl` when this file was first
committed; `git log --follow` on both files is the evidence). Nothing below
was chosen with knowledge of a dev-period result. Every rule, grid and
expected sign comes from the cited literature, not from this panel.

## Protocol (FIXED by D01/D04 — restated, not decided here)

Universe: the 49 `etf_daily_bars` tickers, total-return closes, each usable
only from its first bar. Long-only / long-flat, gross <= 1.0, monthly
rebalance (first NYSE session of each calendar month, `schedule_mask`), fills
at the next session's open, ETF costs 3 bps + $0.005/share. Walk-forward:
train 5 years / test 1 year, rolling over 2007-07-01..2024-06-28 (12 OOS
years, 2012-06-30..2024-06-27). Decisive benchmark per fold: the better OOS
Sharpe of monthly equal-weight of the eligible universe (`etf_ew`) and
60% SPY / 40% IEF monthly (`etf_6040`). Cash earns zero (D03); a cash yield
means holding BIL. Reference dev-period numbers (already public in
`research/lowfreq/reference/results/`): `etf_6040` OOS Sharpe 0.95, CAGR
9.1%, max DD -21.2%; `etf_ew` OOS Sharpe 0.59, CAGR 5.9%, max DD -25.5%.
PASS therefore needs OOS Sharpe >= 0.95, active t(NW) >= 2 vs a 60/40 that
compounded at 9% through 2012-2024, and a drawdown no deeper than about -26%.
We expect this bar to be hard for any unlevered long-only allocation rule.

## Conventions shared by every candidate (C0)

- **Decision bar**: the close of the rebalance session; the harness fills at
  t+1's open. Between rebalances the strategy returns `None` (hold).
- **Eligible(t)**: a ticker with a close at t whose signal is fully defined
  at t (every rolling window uses `min_periods` = window length; a late
  starter joins once it has the history). BIL is never a risk asset.
- **Risk-off leg**: any weight not allocated to risk assets goes to **BIL**
  when BIL is eligible, else stays in cash (weight 0, yield 0).
- **Returns**: `R_L(t) = tr_close_t / tr_close_{t-L} - 1` over L sessions;
  excess return = asset `R_L` minus BIL's `R_L` over the same span.
- **Volatility**: `sigma(t)` = std of daily TR returns over `VOL_DAYS`
  sessions x sqrt(252), `min_periods = VOL_DAYS`.
- **Weights**: long-only, sum exactly 1.0 when BIL is eligible; every other
  column is an explicit 0.0 so a name that leaves the book is sold.
- **Months** are 21 sessions: 3m=63, 6m=126, 8m=168, 10m=210, 12m=252.
- Grids are the values the cited papers report as robustness ranges. They
  are not to be extended after results are seen; any later addition is a
  new registry entry, dated, labelled post-hoc in `REPORT.md`, and counted.

## Hypotheses

### H1 `xa_tsmom_rp` — time-series momentum on a risk-parity base
- **Mechanism**: trend persistence at the 1-12 month horizon in every asset
  class (Moskowitz, Ooi & Pedersen 2012; Hurst, Ooi & Pedersen 2013), with
  inverse-volatility sizing so bonds and commodities carry risk comparable
  to equities rather than being drowned by them.
- **Rule**: risk asset i is ON when its excess return over `LOOKBACK_DAYS`
  is > 0. Base weight `b_i = (1/sigma_i) / sum_j (1/sigma_j)` over all
  eligible risk assets (`VOL_DAYS = 63`, fixed). `w_i = b_i` if ON else 0;
  the OFF weight goes to BIL.
- **Grid**: `LOOKBACK_DAYS in {63, 126, 252}`; default 252. (3 cells: gate g
  is not applicable.)
- **Expected**: active return > 0 vs `etf_ew` and vs `etf_6040`, max drawdown
  shallower than both. Sharpe above 60/40: plausible, not assured. Best
  prior for the family.

### H2 `xa_faber_sma` — Faber's 10-month SMA timing of an equal-weight book
- **Mechanism**: the same trend persistence, expressed as price vs its
  moving average (Faber 2007 "A Quantitative Approach to Tactical Asset
  Allocation"); Levine & Pedersen 2016 show MA and return-sign filters are
  related but not identical, so this checks the trend result's dependence
  on signal construction. Registered as trend-family, not a new mechanism.
- **Rule**: base weight `1/N_eligible` per eligible risk asset. ON when
  `tr_close_t > SMA(tr_close, SMA_DAYS)_t`. OFF weight to BIL.
- **Grid**: `SMA_DAYS in {126, 168, 210, 252}`; default 210 (Faber's 10m).
- **Expected**: drawdown well below `etf_ew`, active return > 0 vs `etf_ew`;
  vs `etf_6040` uncertain (an equal-weight base is commodity- and
  bond-heavy relative to 60/40).

### H3 `xa_dual_mom` — dual (relative + absolute) momentum
- **Mechanism**: cross-sectional return continuation across asset classes
  (Asness, Moskowitz & Pedersen 2013 "Value and Momentum Everywhere";
  Antonacci 2012 "Risk Premia Harvesting Through Dual Momentum"), a ranking
  effect distinct from the time-series sign used by H1/H2 (MOP 2012, sec. 4).
- **Rule**: rank eligible risk assets by `R_{LOOKBACK_DAYS - 21}` measured to
  t-21 (skip the latest month, the 12-1 convention). Hold the top `TOP_N`
  at `1/TOP_N` each; a top-N name whose excess return over BIL on the same
  span is <= 0 fails the absolute filter and its `1/TOP_N` goes to BIL.
- **Grid**: `TOP_N in {5, 10}` x `LOOKBACK_DAYS in {126, 252}`; default
  TOP_N 5, LOOKBACK_DAYS 252. (4 cells: gate g applies.)
- **Expected**: active return > 0 vs `etf_ew`; vs `etf_6040` positive but
  with a deeper drawdown and the highest turnover in the family.

### H4 `xa_risk_parity` — unlevered inverse-volatility (control for H1)
- **Mechanism**: leverage aversion makes low-risk assets offer higher
  risk-adjusted returns and balancing risk across classes diversifies
  better than balancing capital (Asness, Frazzini & Pedersen 2012).
- **Rule**: `w_i = (1/sigma_i) / sum_j (1/sigma_j)` over eligible risk assets
  (BIL excluded), fully invested.
- **Grid**: `VOL_DAYS in {21, 63, 252}`; default 63.
- **Expected**: Sharpe >= `etf_ew`; **active return vs `etf_6040` negative**
  (without leverage the book is bond-heavy and earns bond-like returns), so
  the pre-registered expected verdict is FAIL on gate (b). Its purpose is
  decomposition: H1 minus H4 is what the trend filter adds to the base.

### H5 `xa_vol_target` — volatility-managed equity exposure
- **Mechanism**: variance is persistent while the equity premium does not
  rise proportionally with it, so scaling exposure by 1/variance raises the
  Sharpe ratio (Moreira & Muir 2017). Without leverage only the de-risking
  half survives (Cederburg, O'Doherty, Wang & Yan 2020), so the effect is
  expected to be weak.
- **Rule**: `e_t = min(1, TARGET_VOL / sigma_SPY(21 days))`; weight `e_t` in
  SPY, `1 - e_t` in BIL.
- **Grid**: `TARGET_VOL in {0.10, 0.12, 0.15}`; default 0.12.
- **Expected**: Sharpe above SPY's; vs `etf_6040` positive active return but
  t below 2; drawdown comparable to 60/40.

### H6 `xa_carry` — carry via trailing distribution yield
- **Mechanism**: carry — the return if prices do not change — predicts
  returns across asset classes because prices do not fully offset yield
  differences (Koijen, Moskowitz, Pedersen & Vrugt 2018). For an ETF, carry
  is its distribution yield.
- **Rule**: `yield_i(t) = (TR_t / TR_{t-252}) / (P_t / P_{t-252}) - 1` where
  TR is the total-return close and P the price close (recovered exactly as
  `dollar_volume / volume`, zero zero-volume rows in the panel). Rank
  eligible risk assets by yield; hold the top `TOP_N` at `1/TOP_N`. No
  trend filter (pure carry).
- **Grid**: `TOP_N in {5, 10, 15}`; default 10.
- **Expected**: active return > 0 vs `etf_ew`; vs `etf_6040` uncertain and
  the book concentrates in credit, EM and REITs, so drawdown gate (d) is at
  risk in 2020.

### H7 `xa_trend_6040` — trend-filtered 60/40
- **Mechanism**: the decisive benchmark's drawdowns are equity bear markets,
  which trend; a time-series filter on the equity leg alone (Faber 2007;
  Antonacci's GEM uses the same switch) targets that failure mode without
  the basis risk of the whole universe. Trend-family, registered separately
  because it tests the filter on the benchmark's own composition.
- **Rule**: IEF weight 0.40 always. SPY weight 0.60 when SPY's excess return
  over BIL over `TREND_DAYS` is > 0; otherwise that 0.60 goes to `OFF_ASSET`.
- **Grid**: `TREND_DAYS in {126, 252}` x `OFF_ASSET in {BIL, IEF}`; default
  TREND_DAYS 252, OFF_ASSET BIL. (4 cells: gate g applies.)
- **Expected**: return within +-1 pp/yr of 60/40 with a shallower drawdown,
  so Sharpe >= 60/40; active t(NW) positive but modest (few switches in 12
  years); the most likely PROMISING in the family.

Five mechanisms (trend: H1/H2/H7; cross-sectional momentum: H3; risk
balancing: H4; volatility timing: H5; carry: H6). Seven registry entries,
seven trials for DSR if each is evaluated once with an unchanged entry.

## Champion-selection rule (fixed before results)

1. Eligible: candidates whose evaluation completed (`status: ok`).
2. Rank by verdict class: PASS > PROMISING > FAIL.
3. Within a class, the higher Newey-West t of OOS active return vs the
   decisive benchmark (`active.t_nw`); tie -> higher OOS Sharpe (`oos.sharpe`);
   further tie -> the lower registry order (H1 first).
4. The champion's **final parameter cell** is the cell the walk-forward chose
   in the majority of folds (`folds[*].chosen`); a tie goes to the last
   fold's choice. This is the cell handed to `--fixed-params` for the
   held-out evaluation. The default cell's full-period metrics are reported
   next to it, not in place of it.

## Stop rule

Evaluate H1..H7 once each, in registry order. Stop when the champion's
verdict is PASS, or when all seven have completed — then `REPORT.md` states
the null with the best PROMISING/FAIL evidence. A candidate fixed for a code
error is re-evaluated with a new dated registry entry (new trial); the
erroring row stays in the ledger. No hypothesis is added after this point
without a dated "post-hoc" section here.

## What `REPORT.md` will contain

Every ledger row with role candidate and its verdict; the champion named by
the rule above with OOS Sharpe/CAGR/max DD vs both benchmarks, per-year
active returns, 3x-cost active return, DSR with N, PBO/stability where the
grid has >= 4 cells, final cell; every number copied from
`results/<id>.json` with the producing command.
