# Cross-asset ETF allocation family (`xasset`) — evaluation report

Pre-registration: `PREREG.md` (commit e82afb3, amended in 5177081 before any
candidate row existed in `trials.jsonl`). Every number below is copied from
`results/<id>.json` / `results/snapshots/*.json` by `make_report.py`
(`uv run python research/lowfreq/xasset/make_report.py`; pass comparison:
`... make_report.py check`). Reproduce any candidate with
`uv run python -m trader.research.lowfreq evaluate xasset/<id>`.

## Protocol (D01/D04, as run)

49 total-return ETFs (`etf_daily_bars`), each usable from its first bar;
long-only, gross <= 1, monthly rebalance at the first NYSE session, fills at
the next open, 3 bps + $0.005/share (3x for the stress). Rolling
walk-forward, train 5 y / test 1 y over 2007-07-01..2024-06-28: 12 OOS folds,
2012-06-30..2024-06-27, 3,015 OOS days; the cell with the best train Sharpe is
traded on each test year. Decisive benchmark per fold = better OOS Sharpe of
monthly equal weight (`etf_ew`) and 60/40 SPY/IEF (`etf_6040`); gate (a) uses
the higher stitched benchmark Sharpe. Family benchmark rows (warm-up 500 d):
`etf_6040` OOS Sharpe 0.950, CAGR 9.11%, max DD −21.2%; `etf_ew` 0.590,
5.90%, −25.5% (`results/etf_6040.json`, `results/etf_ew.json`, identical to
the C01 reference family). Cash earns zero; the risk-off leg of every
candidate is BIL.

## Hypotheses evaluated (details and grids in `PREREG.md`)

| id | mechanism | rule (one line) | grid (cells) | default cell |
|---|---|---|---|---|
| `xa_tsmom_rp` | H1 time-series momentum (MOP 2012, HOP 2013) | hold each ETF whose L-day excess return over BIL > 0, inverse-vol base (63 d), rest BIL | LOOKBACK_DAYS {63,126,252} (3) | 252 |
| `xa_faber_sma` | H2 trend via moving average (Faber 2007) | equal-weight base; hold while TR close > SMA, else BIL | SMA_DAYS {126,168,210,252} (4) | 210 |
| `xa_dual_mom` | H3 cross-sectional + absolute momentum (AMP 2013, Antonacci 2012) | top-N by 12-1 / 6-1 return at 1/N; a slot below BIL's return -> BIL | TOP_N {5,10} x LOOKBACK_DAYS {126,252} (4) | 5 / 252 |
| `xa_risk_parity` | H4 unlevered inverse volatility (AFP 2012; control for H1) | w ∝ 1/σ over all eligible risk assets, fully invested | VOL_DAYS {21,63,252} (3) | 63 |
| `xa_vol_target` | H5 volatility-managed equity (Moreira-Muir 2017) | SPY weight min(1, target/σ21), rest BIL | TARGET_VOL {0.10,0.12,0.15} (3) | 0.12 |
| `xa_carry` | H6 carry = trailing 12 m distribution yield (KMPV 2018) | top-N yielders at 1/N | TOP_N {5,10,15} (3) | 10 |
| `xa_trend_6040` | H7 trend filter on the benchmark's equity leg | IEF 0.40 always; SPY 0.60 while its excess return > 0, else to OFF_ASSET | TREND_DAYS {126,252} x OFF_ASSET {BIL,IEF} (4) | 252 / BIL |

Five mechanisms (trend x3, cross-sectional momentum, risk balancing,
volatility timing, carry), seven registry entries, 24 grid cells. Each
strategy is `strategies/lowfreq_xasset_<name>.py` on the shared causal base
`lowfreq_xasset_base.py`; `tests/research/test_xasset_candidates.py` proves
long-only, gross <= 1, no weight before a first bar, cash never overdrawn,
and the harness's own lookahead check on the real dev panel for every
candidate; `tests/research/test_xasset_report.py` pins the champion and
final-cell rules.

## Workflow actually followed (PREREG A1.2)

Pass 1 ran H1..H6 in registry order on 2026-09-29 08:27-08:30 UTC (ledger
rows 3-8). The session running it was terminated before H7 and before the
snapshots; H7's pass-1 run followed at 08:37 (row 9), so pass 1 was
completed before any snapshot or pass-2 call, as A1.2 requires. Pass-1
snapshots (7 files, `results/snapshots/<id>__pass1.json`) were taken at
08:37:28; pass 2 ran the same seven commands with unchanged entries at
08:37-08:40 (rows 10-16; hashes identical, N stays 7); pass-2 snapshots
followed; `make_report.py check` found NO deterministic field differing
between passes for any candidate (only `dsr`, at N = 1..7 in pass 1 and
N = 7 in pass 2). The champion rule was applied to the pass-2 files.
The two benchmark rows at the top of the ledger are the family's own
benchmark evaluations, kept deliberately (they document the benchmark
numbers under this family's 500-day warm-up; benchmark rows never count).

## Pass 1 vs pass 2 (`make_report.py check`)

| id | pass-1 DSR (N) | pass-2 DSR (N) | pass-1 verdict | pass-2 verdict | deterministic fields differing |
|---|---|---|---|---|---|
| xa_tsmom_rp | 0.00 (1) | 0.00 (7) | FAIL | FAIL | none |
| xa_faber_sma | 0.00 (2) | 0.00 (7) | FAIL | FAIL | none |
| xa_dual_mom | 0.01 (3) | 0.00 (7) | FAIL | FAIL | none |
| xa_risk_parity | 0.00 (4) | 0.00 (7) | FAIL | FAIL | none |
| xa_vol_target | 0.03 (5) | 0.04 (7) | FAIL | FAIL | none |
| xa_carry | 0.00 (6) | 0.00 (7) | FAIL | FAIL | none |
| xa_trend_6040 | 0.00 (7) | 0.00 (7) | FAIL | FAIL | none |

## Every trial (research/lowfreq/xasset/trials.jsonl)

| # | id | role | status | verdict | entry sha256[:12] | pass | snapshot | n_trials | OOS Sharpe | active ann | t(NW) | DSR | timestamp (UTC) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | etf_ew | benchmark | ok | FAIL | 92ea92ba0f6b | - | - | 0 | +0.590 | -2.87% | -2.23 | 0.01 | 2026-09-29T08:13:55 |
| 2 | etf_6040 | benchmark | ok | PASS | 6ab9596875e6 | - | - | 0 | +0.950 | +2.87% | 2.23 | 0.99 | 2026-09-29T08:14:05 |
| 3 | xa_tsmom_rp | candidate | ok | FAIL | 8ce2f38ba43a | 1 | snapshots/xa_tsmom_rp__pass1.json | 1 | +0.650 | -7.56% | -3.86 | 0.00 | 2026-09-29T08:27:59 |
| 4 | xa_faber_sma | candidate | ok | FAIL | 06a4a17d3342 | 1 | snapshots/xa_faber_sma__pass1.json | 2 | +0.619 | -6.15% | -3.53 | 0.00 | 2026-09-29T08:28:31 |
| 5 | xa_dual_mom | candidate | ok | FAIL | 49516e9af9ff | 1 | snapshots/xa_dual_mom__pass1.json | 3 | +0.302 | -4.72% | -1.46 | 0.01 | 2026-09-29T08:29:04 |
| 6 | xa_risk_parity | candidate | ok | FAIL | 7767f140e256 | 1 | snapshots/xa_risk_parity__pass1.json | 4 | +0.594 | -6.40% | -5.08 | 0.00 | 2026-09-29T08:29:36 |
| 7 | xa_vol_target | candidate | ok | FAIL | 320965185b28 | 1 | snapshots/xa_vol_target__pass1.json | 5 | +0.897 | +1.22% | 0.77 | 0.03 | 2026-09-29T08:30:03 |
| 8 | xa_carry | candidate | ok | FAIL | 971e09774198 | 1 | snapshots/xa_carry__pass1.json | 6 | +0.600 | -2.05% | -1.11 | 0.00 | 2026-09-29T08:30:30 |
| 9 | xa_trend_6040 | candidate | ok | FAIL | a360adfb0410 | 1 | snapshots/xa_trend_6040__pass1.json | 7 | +0.827 | -2.95% | -2.01 | 0.00 | 2026-09-29T08:37:28 |
| 10 | xa_tsmom_rp | candidate | ok | FAIL | 8ce2f38ba43a | 2 | snapshots/xa_tsmom_rp__pass2.json | 7 | +0.650 | -7.56% | -3.86 | 0.00 | 2026-09-29T08:37:53 |
| 11 | xa_faber_sma | candidate | ok | FAIL | 06a4a17d3342 | 2 | snapshots/xa_faber_sma__pass2.json | 7 | +0.619 | -6.15% | -3.53 | 0.00 | 2026-09-29T08:38:23 |
| 12 | xa_dual_mom | candidate | ok | FAIL | 49516e9af9ff | 2 | snapshots/xa_dual_mom__pass2.json | 7 | +0.302 | -4.72% | -1.46 | 0.00 | 2026-09-29T08:38:53 |
| 13 | xa_risk_parity | candidate | ok | FAIL | 7767f140e256 | 2 | snapshots/xa_risk_parity__pass2.json | 7 | +0.594 | -6.40% | -5.08 | 0.00 | 2026-09-29T08:39:17 |
| 14 | xa_vol_target | candidate | ok | FAIL | 320965185b28 | 2 | snapshots/xa_vol_target__pass2.json | 7 | +0.897 | +1.22% | 0.77 | 0.04 | 2026-09-29T08:39:39 |
| 15 | xa_carry | candidate | ok | FAIL | 971e09774198 | 2 | snapshots/xa_carry__pass2.json | 7 | +0.600 | -2.05% | -1.11 | 0.00 | 2026-09-29T08:40:02 |
| 16 | xa_trend_6040 | candidate | ok | FAIL | a360adfb0410 | 2 | snapshots/xa_trend_6040__pass2.json | 7 | +0.827 | -2.95% | -2.01 | 0.00 | 2026-09-29T08:40:30 |

Candidate-role trial count (distinct entry hashes): **7** (budget 40, D04); 14 candidate rows in total.

## Candidates at the final trial count (results/<id>.json, pass 2)

| id | verdict | cells | OOS Sharpe | bench max Sharpe | OOS CAGR | OOS maxDD | decisive maxDD | active ann | t(NW) | DSR (N) | 3x-cost active | pos. years | PBO | stability | gates failed |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| xa_tsmom_rp | **FAIL** | 3 | +0.650 | +0.950 | +2.35% | -11.1% | -18.6% | -7.56% | -3.86 | 0.00 (7) | -7.89% | 15% | n/a | 0.64 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90, e_positive_years_ge_60pct, f_active_positive_at_3x_cost |
| xa_faber_sma | **FAIL** | 4 | +0.619 | +0.950 | +3.68% | -10.8% | -18.6% | -6.15% | -3.53 | 0.00 (7) | -6.42% | 8% | 0.34 | 0.27 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90, e_positive_years_ge_60pct, f_active_positive_at_3x_cost, g_pbo_and_stability |
| xa_dual_mom | **FAIL** | 4 | +0.302 | +0.950 | +3.79% | -40.8% | -18.6% | -4.72% | -1.46 | 0.00 (7) | -5.34% | 38% | 0.73 | 0.64 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90, d_drawdown_within_5pp, e_positive_years_ge_60pct, f_active_positive_at_3x_cost, g_pbo_and_stability |
| xa_risk_parity | **FAIL** | 3 | +0.594 | +0.950 | +3.43% | -15.4% | -18.6% | -6.40% | -5.08 | 0.00 (7) | -6.53% | 8% | n/a | 0.55 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90, e_positive_years_ge_60pct, f_active_positive_at_3x_cost |
| xa_vol_target | **FAIL** | 3 | +0.897 | +0.950 | +10.95% | -21.3% | -18.6% | +1.22% | 0.77 | 0.04 (7) | +1.05% | 69% | n/a | 0.64 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90 |
| xa_carry | **FAIL** | 3 | +0.600 | +0.950 | +7.28% | -34.4% | -18.6% | -2.05% | -1.11 | 0.00 (7) | -2.14% | 38% | n/a | 0.82 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90, d_drawdown_within_5pp, e_positive_years_ge_60pct, f_active_positive_at_3x_cost |
| xa_trend_6040 | **FAIL** | 4 | +0.827 | +0.950 | +6.87% | -18.7% | -18.6% | -2.95% | -2.01 | 0.00 (7) | -3.03% | 46% | 0.51 | 0.64 | a_sharpe_vs_benchmark, b_active_t_ge_2, c_dsr_ge_0_90, e_positive_years_ge_60pct, f_active_positive_at_3x_cost, g_pbo_and_stability |

## Champion: `xa_vol_target` — verdict **FAIL**

Chosen by the PREREG rule (verdict class, then active t(NW)). Results file: `results/xa_vol_target.json` (entry sha256 `320965185b28`, strategy sha256 `5772e4027424`).

| series | OOS Sharpe | OOS CAGR | OOS max DD | OOS days |
|---|---|---|---|---|
| xa_vol_target | +0.897 | +10.95% | -21.3% | 3015 |
| etf_6040 | +0.950 | +9.11% | -21.2% | 3015 |
| etf_ew | +0.590 | +5.90% | -25.5% | 3015 |

Decisive benchmark per fold: etf_6040, etf_6040, etf_6040, etf_6040, etf_6040, etf_6040, etf_6040, etf_6040, etf_ew, etf_ew, etf_6040, etf_6040.
Active (vs decisive): ann +1.22%, t(NW, 8 lags) 0.77, active Sharpe +0.210, DSR 0.04 with N = 7 trials, 3x-cost active ann +1.05%, positive years 69%, PBO n/a, selection stability 0.64.

| year | active return | counted |
|---|---|---|
| 2012 | +0.90% | yes |
| 2013 | +7.47% | yes |
| 2014 | -1.16% | yes |
| 2015 | -1.83% | yes |
| 2016 | +2.16% | yes |
| 2017 | +6.92% | yes |
| 2018 | +0.13% | yes |
| 2019 | +3.45% | yes |
| 2020 | -18.55% | yes |
| 2021 | +9.42% | yes |
| 2022 | -4.66% | yes |
| 2023 | +4.46% | yes |
| 2024 | +5.85% | yes |

Per-fold chosen cells: {'{"TARGET_VOL": 0.1}': 3, '{"TARGET_VOL": 0.15}': 7, '{"TARGET_VOL": 0.12}': 2}. **Final parameter cell (modal; tie -> latest fold in the tied set): `{"TARGET_VOL": 0.15}`**; pre-registered default cell: `{"TARGET_VOL": 0.12}`.

| gate | result |
|---|---|
| a_sharpe_vs_benchmark | FAIL |
| b_active_t_ge_1 | FAIL |
| b_active_t_ge_2 | FAIL |
| c_dsr_ge_0_90 | FAIL |
| d_drawdown_within_5pp | ok |
| e_positive_years_ge_60pct | ok |
| f_active_positive_at_3x_cost | ok |
| g_pbo_and_stability | n/a |

Full dev period, default cell: Sharpe +0.729, CAGR +8.61%, max DD -28.8%, turnover/yr 2.93, names held (median) 2, min cash +0.00x capital, cash-guard scaled buys 61 (D06), forced exits 0 (D07).

## Reading the result

**Verdict of the family: NULL.** All seven candidates FAIL at N = 7; none
is PROMISING, because gate (a) fails for every one of them (no candidate
reaches 60/40's OOS Sharpe of 0.950) and only one has a positive active
return at all. The champion by the pre-registered rule is `xa_vol_target`
(verdict **FAIL** in `results/xa_vol_target.json`, matching this report):
OOS Sharpe 0.897 vs 0.950, CAGR 10.95% vs 9.11%, max DD −21.3% vs the
stitched decisive series' −18.6% (within 5 pp), active +1.22%/yr with
t(NW) 0.77, DSR 0.04, positive in 69% of years, +1.05%/yr at 3x cost.
Its final cell is `TARGET_VOL = 0.15` (chosen in 7 of 12 folds; the default
0.12 was chosen in 2). For the held-out period:
`uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target --fixed-params '{"TARGET_VOL": 0.15}' --start 2024-07-01 --end <end>`.

**Why the champion is not an edge.** The book averages 85% SPY (rest BIL)
on rebalance days, so its 1.8 pp/yr of extra CAGR over 60/40 is the equity
premium it carried, not timing; its Sharpe is *lower* than 60/40's because
the de-risking arrives after the fall (2020: active −18.55%, the one year
that decided the t-statistic). This is the Cederburg-O'Doherty-Wang-Yan
(2020) result reproduced: without leverage the volatility-managed rule
keeps only the half that cannot add Sharpe. Pre-registered expectation was
"positive but weak"; measured: weak and insignificant.

**Why the trend and risk-balancing books lost 3-8 pp/yr.** (Composition figures in this
section: `results/composition_diagnostics.json`, from `make_report.py composition` -
default-cell weights on OOS rebalance days.) The universe is
49 ETFs of which about half are bonds, currencies, commodities and miners
that returned little over 2012-2024. Inverse-vol sizing puts the book where
the volatility is lowest: `xa_tsmom_rp` was 64% invested on average with
72% of the book in bonds or BIL and 1% in SPY; `xa_risk_parity` 55% in
bonds; `xa_faber_sma` 65% invested, 49% in bonds or BIL. Such books earn
bond-like returns (CAGR 2.3-3.7%) with low drawdowns (−11% to −15%); their
Sharpe ratios (0.59-0.65) match or beat equal weight (0.590) but sit far
below a 60/40 that compounded at 9%. The control worked as pre-registered:
the trend filter raised the risk-parity base's Sharpe from 0.594 to 0.650
and cut its drawdown from −15.4% to −11.1% while lowering CAGR from 3.43%
to 2.35% - a defensive overlay on a base that cannot keep up.

**The filter on the benchmark itself (`xa_trend_6040`)** lost 2.95%/yr with
t(NW) −2.01: the 2012-2024 equity drawdowns (2018Q4, 2020, 2022) were
short and V-shaped, so a 6-12 month filter exited after the fall and
re-entered after the recovery; the whipsaw cost exceeded what it saved in
the one prolonged bear market (2022). Its cell choice moved from IEF/126 to
BIL/252 across folds and its PBO is 0.51 (n = 4 cells, wide caveat): no
skill in the selection either. `xa_dual_mom` (Sharpe 0.30, max DD −40.8%,
2023 active −27.5%, PBO 0.73) concentrated in 2022's commodity and energy
winners into 2023; `xa_carry` (Sharpe 0.60, max DD −34.4%) concentrated in
credit, EM and REITs into 2020. Both fail the drawdown gate outright.

**Fold pattern.** Every candidate beat 60/40 in the fold covering
2020-07..2021-06 (fold 9, where equal weight was also the decisive
benchmark) and lost in 2013-14, 2019, 2021 and 2023 - the years US equities
ran. Beating a 60/40 that had that decade requires more equity, not a
signal, and the one candidate that carried more equity did so without a
Sharpe gain.

**Selection and search accounting.** Seven distinct candidate hashes, 24
grid cells, 14 evaluations (two identical passes), zero errors, zero
post-hoc entries; PREREG's grids were not changed after results. PBO for the
4-cell grids: 0.34 (Faber), 0.73 (dual momentum), 0.51 (trend 60/40);
stability 0.27-0.82. DSR is 0.00-0.04 for every candidate at N = 7 because
six of the seven active-return Sharpes are negative (−0.31 to −1.33) and
the seventh is 0.21.

**Diagnostics.** Cash was never overdrawn in any run (min cash ≥ 0). The D06
guard trimmed whole-share buys on 61 of the champion's full-period fill
dates: a month at 100% SPY plus 3 bps slippage and commission exceeds cash
by a few dollars, so one share is dropped - the guard working, not
leverage. No forced exits (D07): no ETF in the panel stopped printing.

**What this rules out, and what it does not.** On this universe, with
monthly rebalancing, no leverage and no shorts, the standard cross-asset
allocation mechanisms - time-series trend (three constructions),
cross-sectional momentum, unlevered risk parity, volatility targeting and
distribution-yield carry - do not beat 60/40 over 2012-2024 after costs.
Not tested (would be new, post-hoc trials): leverage (excluded by D01),
an equity-heavy base with a trend overlay, or shorter horizons.

**MMR bugs found: none.** The harness, loaders and panel executor behaved
as documented throughout.
