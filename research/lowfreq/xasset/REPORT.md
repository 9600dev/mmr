# Cross-asset ETF allocation family (`xasset`) — evaluation report

Pre-registration: `PREREG.md` (commit e82afb3, amended in 5177081 before any
candidate row existed in `trials.jsonl`). Every table below is rendered from
`results/<id>.json`, `results/snapshots/*.json` and `trials.jsonl` by
`uv run python research/lowfreq/xasset/make_report.py`
[`check` | `folds` | `holdings` | `composition`];
`tests/research/test_xasset_report.py::test_report_tables_match_current_artifacts`
fails if this file and those files disagree. Reproduce any candidate with
`uv run python -m trader.research.lowfreq evaluate xasset/<id>`.

## Protocol (D01/D04, as run)

49 total-return ETFs (`etf_daily_bars`), each usable from its first bar;
long-only, gross <= 1, monthly rebalance at the first NYSE session, fills at
the next open, 3 bps + $0.005/share (3x for the stress). Rolling
walk-forward, train 5 y / test 1 y over 2007-07-01..2024-06-28: 12 OOS folds,
2012-06-30..2024-06-27, 3,015 OOS days; the best-train-Sharpe cell is traded
on each test year. Decisive benchmark per fold = the better OOS Sharpe
of monthly equal weight (`etf_ew`) and 60/40 SPY/IEF (`etf_6040`); gate (a)
uses the higher stitched benchmark Sharpe. Benchmark rows (warm-up 500 d):
`etf_6040` OOS Sharpe 0.950, CAGR 9.11%, max DD −21.2%; `etf_ew` 0.590,
5.90%, −25.5% (`results/etf_*.json`, identical to the C01 reference family).
Cash earns zero; the risk-off leg is BIL.

## Hypotheses evaluated (exact rules and rationale in `PREREG.md`)

| id | mechanism (source) | grid (cells) | default |
|---|---|---|---|
| `xa_tsmom_rp` | H1 time-series momentum (MOP 2012, HOP 2013) | LOOKBACK_DAYS {63,126,252} (3) | 252 |
| `xa_faber_sma` | H2 trend via moving average (Faber 2007) | SMA_DAYS {126,168,210,252} (4) | 210 |
| `xa_dual_mom` | H3 relative + absolute momentum (AMP 2013, Antonacci 2012) | TOP_N {5,10} x LOOKBACK_DAYS {126,252} (4) | 5 / 252 |
| `xa_risk_parity` | H4 unlevered inverse volatility (AFP 2012; control for H1) | VOL_DAYS {21,63,252} (3) | 63 |
| `xa_vol_target` | H5 volatility-managed equity (Moreira-Muir 2017) | TARGET_VOL {0.10,0.12,0.15} (3) | 0.12 |
| `xa_carry` | H6 carry = trailing 12 m distribution yield (KMPV 2018) | TOP_N {5,10,15} (3) | 10 |
| `xa_trend_6040` | H7 trend filter on the benchmark's equity leg | TREND_DAYS {126,252} x OFF_ASSET {BIL,IEF} (4) | 252 / BIL |

Five mechanisms (trend x3, cross-sectional momentum, risk balancing,
volatility timing, carry), seven entries, 24 grid cells. Each is
`strategies/lowfreq_xasset_<name>.py` on a shared causal base;
`tests/research/test_xasset_candidates.py` proves long-only, gross <= 1, no
weight before a first bar, no overdraft and the harness's lookahead check on
the real dev panel for every candidate.

## Workflow actually followed (PREREG A1.2)

Pass 1 ran H1..H6 in registry order on 2026-09-29 08:27-08:30 UTC (ledger
rows 3-8); that session was terminated before H7 and the snapshots, so H7's
pass-1 run followed at 08:37 (row 9) — pass 1 was complete before any
snapshot or pass-2 call, as A1.2 requires. Pass-1 snapshots at 08:37:28;
pass 2 ran the same seven commands with unchanged entries at 08:37-08:40
(rows 10-16; identical hashes, N stays 7), then pass-2 snapshots. Ledger
row (id, pass k) maps to `results/snapshots/<id>__pass<k>.json`.
`make_report.py check`: no deterministic field differs between passes for
any candidate and each `results/<id>.json` matches its pass-2 snapshot
(deterministic fields, verdict, DSR); only
`dsr` changed (N = 1..7 in pass 1, 7 in pass 2) and every verdict is FAIL in
both. The champion rule was applied to the pass-2 files. The two
benchmark-role rows never count as trials.

## Every trial (research/lowfreq/xasset/trials.jsonl)

| # | id | role | status | verdict | entry sha256[:12] | pass | n_trials | OOS Sharpe | active ann | t(NW) | DSR | timestamp (UTC) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | etf_ew | benchmark | ok | FAIL | 92ea92ba0f6b | - | 0 | +0.590 | -2.87% | -2.23 | 0.01 | 2026-09-29T08:13:55 |
| 2 | etf_6040 | benchmark | ok | PASS | 6ab9596875e6 | - | 0 | +0.950 | +2.87% | 2.23 | 0.99 | 2026-09-29T08:14:05 |
| 3 | xa_tsmom_rp | candidate | ok | FAIL | 8ce2f38ba43a | 1 | 1 | +0.650 | -7.56% | -3.86 | 0.00 | 2026-09-29T08:27:59 |
| 4 | xa_faber_sma | candidate | ok | FAIL | 06a4a17d3342 | 1 | 2 | +0.619 | -6.15% | -3.53 | 0.00 | 2026-09-29T08:28:31 |
| 5 | xa_dual_mom | candidate | ok | FAIL | 49516e9af9ff | 1 | 3 | +0.302 | -4.72% | -1.46 | 0.01 | 2026-09-29T08:29:04 |
| 6 | xa_risk_parity | candidate | ok | FAIL | 7767f140e256 | 1 | 4 | +0.594 | -6.40% | -5.08 | 0.00 | 2026-09-29T08:29:36 |
| 7 | xa_vol_target | candidate | ok | FAIL | 320965185b28 | 1 | 5 | +0.897 | +1.22% | 0.77 | 0.03 | 2026-09-29T08:30:03 |
| 8 | xa_carry | candidate | ok | FAIL | 971e09774198 | 1 | 6 | +0.600 | -2.05% | -1.11 | 0.00 | 2026-09-29T08:30:30 |
| 9 | xa_trend_6040 | candidate | ok | FAIL | a360adfb0410 | 1 | 7 | +0.827 | -2.95% | -2.01 | 0.00 | 2026-09-29T08:37:28 |
| 10 | xa_tsmom_rp | candidate | ok | FAIL | 8ce2f38ba43a | 2 | 7 | +0.650 | -7.56% | -3.86 | 0.00 | 2026-09-29T08:37:53 |
| 11 | xa_faber_sma | candidate | ok | FAIL | 06a4a17d3342 | 2 | 7 | +0.619 | -6.15% | -3.53 | 0.00 | 2026-09-29T08:38:23 |
| 12 | xa_dual_mom | candidate | ok | FAIL | 49516e9af9ff | 2 | 7 | +0.302 | -4.72% | -1.46 | 0.00 | 2026-09-29T08:38:53 |
| 13 | xa_risk_parity | candidate | ok | FAIL | 7767f140e256 | 2 | 7 | +0.594 | -6.40% | -5.08 | 0.00 | 2026-09-29T08:39:17 |
| 14 | xa_vol_target | candidate | ok | FAIL | 320965185b28 | 2 | 7 | +0.897 | +1.22% | 0.77 | 0.04 | 2026-09-29T08:39:39 |
| 15 | xa_carry | candidate | ok | FAIL | 971e09774198 | 2 | 7 | +0.600 | -2.05% | -1.11 | 0.00 | 2026-09-29T08:40:02 |
| 16 | xa_trend_6040 | candidate | ok | FAIL | a360adfb0410 | 2 | 7 | +0.827 | -2.95% | -2.01 | 0.00 | 2026-09-29T08:40:30 |

Candidate-role trial count (distinct entry hashes): **7** (budget 40, D04); 16 candidate rows in total.

Ledger rows 17-18 (integration, after selection): the champion's reproduction
(FAIL) and a one-year `--fixed-params` command check; same hash, N stays 7.

## Candidates at the final trial count (results/<id>.json, pass 2)

| id | verdict | cells | OOS Sharpe | OOS CAGR | OOS maxDD | decisive maxDD | active ann | t(NW) | DSR (N) | 3x-cost active | pos. years | PBO | stability | gates failed |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| xa_tsmom_rp | **FAIL** | 3 | +0.650 | +2.35% | -11.1% | -18.6% | -7.56% | -3.86 | 0.00 (7) | -7.89% | 15% | n/a | 0.64 | abcef |
| xa_faber_sma | **FAIL** | 4 | +0.619 | +3.68% | -10.8% | -18.6% | -6.15% | -3.53 | 0.00 (7) | -6.42% | 8% | 0.34 | 0.27 | abcefg |
| xa_dual_mom | **FAIL** | 4 | +0.302 | +3.79% | -40.8% | -18.6% | -4.72% | -1.46 | 0.00 (7) | -5.34% | 38% | 0.73 | 0.64 | abcdefg |
| xa_risk_parity | **FAIL** | 3 | +0.594 | +3.43% | -15.4% | -18.6% | -6.40% | -5.08 | 0.00 (7) | -6.53% | 8% | n/a | 0.55 | abcef |
| xa_vol_target | **FAIL** | 3 | +0.897 | +10.95% | -21.3% | -18.6% | +1.22% | 0.77 | 0.04 (7) | +1.05% | 69% | n/a | 0.64 | abc |
| xa_carry | **FAIL** | 3 | +0.600 | +7.28% | -34.4% | -18.6% | -2.05% | -1.11 | 0.00 (7) | -2.14% | 38% | n/a | 0.82 | abcdef |
| xa_trend_6040 | **FAIL** | 4 | +0.827 | +6.87% | -18.7% | -18.6% | -2.95% | -2.01 | 0.00 (7) | -3.03% | 46% | 0.51 | 0.64 | abcefg |

Gates (D01): a Sharpe >= 0.950; b active t(NW) >= 2; c DSR >= 0.90; d
drawdown within 5 pp; e >= 60% positive years; f active > 0 at 3x cost;
g PBO < 0.35 and stability >= 0.5 (>= 4 cells).

## Per-fold and per-year comparison (`make_report.py folds`)

Metric: Sharpe inside one test window — the candidate's `folds[k].oos.sharpe`
beside both benchmarks' `folds[k].benchmarks.<b>.sharpe` over the same days.
`*` = above 60/40 by more than 0.005 (visible at two decimals), `!` = above
both by that margin; closer is a tie (`xa_trend_6040` equals 60/40 to ~1e-4
in folds it never switched). Columns: zero-based fold index, test-window
years.

| series | 0:12-13 | 1:13-14 | 2:14-15 | 3:15-16 | 4:16-17 | 5:17-18 | 6:18-19 | 7:19-20 | 8:20-21 | 9:21-22 | 10:22-23 | 11:23-24 | >60/40 | >both |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 60/40 | +1.52 | +2.44 | +0.88 | +0.55 | +2.17 | +0.98 | +1.25 | +0.60 | +2.25 | -0.71 | +0.66 | +1.80 | - | - |
| EW | +0.66 | +2.27 | -0.59 | +0.02 | +1.49 | +0.89 | +0.76 | -0.02 | +2.64 | -0.43 | +0.41 | +1.23 | - | - |
| tsmom_rp | +0.88 | +1.90 | +0.05 | +0.46 | +1.05 | +0.89 | +1.18 | -0.28 | +2.01 | -0.54* | -0.05 | +1.80 | 1/12 | 0/12 |
| faber_sma | +0.95 | +1.53 | -0.30 | -0.20 | +1.46 | +0.73 | +0.15 | -0.37 | +2.29* | -0.25*! | -0.16 | +0.93 | 2/12 | 1/12 |
| dual_mom | +1.26 | +1.80 | +0.03 | +0.23 | +0.39 | +0.90 | -0.16 | -0.38 | +2.01 | -0.23*! | -0.94 | +0.79 | 1/12 | 1/12 |
| risk_parity | +0.57 | +2.48*! | -0.38 | +0.26 | +1.61 | +0.88 | +1.40*! | +0.11 | +2.28* | -1.01 | +0.52 | +1.38 | 3/12 | 2/12 |
| vol_target | +1.38 | +1.83 | +0.55 | -0.05 | +2.38*! | +1.22*! | +0.62 | +0.11 | +1.82 | -0.07*! | +0.95*! | +2.11*! | 5/12 | 5/12 |
| carry | +1.01 | +2.11 | -0.44 | +0.20 | +2.08 | +0.60 | +0.87 | -0.15 | +2.35* | +0.02*! | +0.78*! | +1.08 | 3/12 | 2/12 |
| trend_6040 | +1.52 | +2.44 | +0.88 | +0.52 | +2.17 | +0.98 | +0.32 | -0.29 | +2.25 | -0.54* | +0.79*! | +1.80 | 2/12 | 1/12 |

In eleven of twelve folds at most three of seven candidates beat 60/40; the
exception is fold 9 (2021-22, 60/40 Sharpe −0.71): six did and four also
beat equal weight — by falling less (every fold-9 CAGR is negative; 60/40's
−8.8%). In fold 8 three beat 60/40 and none beat equal weight (decisive
there). No candidate beat 60/40 in more than 5 of 12 folds. Calendar-year
active returns (`active.per_year`, tabulated by the same command): none
positive in 2014 or 2020; exactly one in 2015 (`xa_trend_6040`), 2019 and
2023 (`xa_vol_target`); the best years were 2021 and 2022 (four of seven
positive).

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

Gates (D01): a_sharpe_vs_benchmark FAIL; b_active_t_ge_1 FAIL; b_active_t_ge_2 FAIL; c_dsr_ge_0_90 FAIL; d_drawdown_within_5pp ok; e_positive_years_ge_60pct ok; f_active_positive_at_3x_cost ok; g_pbo_and_stability n/a.

Full dev period, default cell: Sharpe +0.729, CAGR +8.61%, max DD -28.8%, turnover/yr 2.93, names held (median) 2, min cash +0.00x capital, cash-guard scaled buys 61 (D06), forced exits 0 (D07).

## Reading the result

**Verdict of the family: NULL.** All seven candidates FAIL at N = 7 and none
is PROMISING: gate (a) fails for every one (none reaches 60/40's OOS Sharpe
of 0.950) and only one has a positive active return at all. The champion by
the pre-registered rule is `xa_vol_target`, verdict **FAIL** in
`results/xa_vol_target.json` and here (metrics above); final cell
`TARGET_VOL = 0.15` (7 of 12 folds; the default 0.12 in 2). Held-out command:
`uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target --fixed-params '{"TARGET_VOL": 0.15}' --start 2024-07-01 --end <end>`.

**Why the champion is not an edge.** Its default-cell book averages 85% SPY
(rest BIL) on OOS rebalance days (`results/composition_diagnostics.json`),
so the 1.8 pp/yr of extra CAGR over 60/40 is
the equity premium it carried, not timing; its Sharpe is *lower* than
60/40's because de-risking arrives after the fall (2020: active −18.55%, the
year that decided the t-statistic). This is Cederburg-O'Doherty-Wang-Yan
(2020): without leverage the volatility-managed rule keeps only the half
that cannot add Sharpe. Pre-registered "positive but weak"; measured: weak,
insignificant.

**Why the trend and risk-balancing books lost 6-8 pp/yr** (composition:
same file). About half of the 49 ETFs are bonds, currencies, commodities and
miners that returned little over 2012-2024, and inverse-vol sizing puts the
book where volatility is lowest:
`xa_tsmom_rp` was 64% invested with 72% of the book in bonds or BIL and 1%
in SPY; `xa_risk_parity` 55% in bonds; `xa_faber_sma` 65% invested, 49% in
bonds or BIL. Such books earn bond-like returns (CAGR 2.3-3.7%) with shallow
drawdowns (−11% to −15%): Sharpes of 0.59-0.65 match or beat equal weight
(0.590) but sit far below a 60/40 that compounded at 9%. The control behaved
as pre-registered: the trend filter raised the risk-parity base's Sharpe
from 0.594 to 0.650 and cut its drawdown from −15.4% to −11.1% while
lowering CAGR from 3.43% to 2.35% (approximate: H1 fixes a 63-day vol base,
H4's walk-forward chose 21-day vol in 8 of 12 folds).

**The filter on the benchmark itself (`xa_trend_6040`)** lost 2.95%/yr with
t(NW) −2.01. Calendar-year active: −8.5% in 2019, −16.8% in 2020, −5.6% in
2023 — years it left SPY after a fall and re-entered after the recovery
(mean SPY weight in `holdings_diagnostics.json`: 50% in fold 6, 45% in fold
7 (table below), 5% in fold 10, i.e. out of SPY for 2, 3 and 11 of 12
rebalances) — and −4.7% in 2022, when it held SPY for 11 of fold 9's 12
rebalances (55%), exiting only after most of the fall (decisive: equal
weight to June 2022, 60/40 after). In fold 9 it beat 60/40 on Sharpe (−0.54
vs −0.71) but not equal weight (−0.43), decisive that year. Its cell moved from IEF/126 (folds 0-2, 4) via
IEF/252 (5-8) to BIL/252 (3, 9-11); PBO 0.51 (4 cells, wide caveat): no
skill in the selection either.

**Dated holdings** (`results/holdings_diagnostics.json`, `make_report.py
holdings`: mean target weight over the fold's rebalance days, with the cell
the walk-forward chose):

| candidate | fold | test window | chosen cell | top holdings (mean target weight) |
|---|---|---|---|---|
| xa_carry | 7 | 2019-06-29..2020-06-28 | `{"TOP_N": 15}` | XLE 7%, VGK 7%, XLU 7%, XLRE 7%, LQD 7%, EFA 7%, EMB 7%, EWA 7% +4 more |
| xa_dual_mom | 10 | 2022-06-28..2023-06-28 | `{"LOOKBACK_DAYS": 252, "TOP_N": 5}` | XLE 18%, USO 13%, DBC 13%, UNG 12%, UUP 10%, EWZ 5%, BIL 5%, EWG 3% +4 more |
| xa_trend_6040 | 7 | 2019-06-29..2020-06-28 | `{"OFF_ASSET": "IEF", "TREND_DAYS": 252}` | IEF 55%, SPY 45% |

`xa_dual_mom` (Sharpe 0.30, max DD −40.8%, PBO 0.73) lost 27.5% against
60/40 in calendar 2023: its fold-10 book was 2022's energy, commodity and
dollar winners (XLE, USO, DBC, UNG, UUP = 66% of the book) held through
their reversal. `xa_carry` (Sharpe 0.60, max DD −34.4%) entered 2020 with 15
equal-weight high-yielders — energy, European/UK/Australian equities,
utilities, staples, REITs, IG/HY/EM credit — the complex that fell hardest
in March 2020 (2020 active −16.8%). Both fail the drawdown gate.

**Selection and search accounting.** Seven distinct candidate hashes, 24
grid cells, 14 evaluations (two identical passes), zero errors, zero
post-hoc entries or grid changes. PBO for the
4-cell grids: 0.34 (Faber), 0.73 (dual momentum), 0.51 (trend 60/40);
stability 0.27-0.82. DSR is 0.00-0.04 at N = 7 because six of the seven
active-return Sharpes are negative (−0.31 to −1.33) and the seventh is 0.21.

**Diagnostics.** Cash was never overdrawn (min cash > 0 in every fold,
benchmark and full-period cell). The D06 guard trimmed whole-share buys on
61 of the champion's full-period fill dates (a month at 100% SPY plus costs
exceeds cash by a few dollars, so one share is dropped — the guard working,
not leverage). No forced exits (D07): no ETF stopped printing.

**What this rules out.** On this universe, monthly, unlevered and long-only,
time-series trend (three constructions), cross-sectional momentum, risk
parity, volatility targeting and distribution-yield carry do not beat 60/40
over 2012-2024 after costs. Not tested (would be new trials): leverage
(excluded by D01), an equity-heavy base with a trend overlay, shorter
horizons.

**MMR bugs found: none.** Harness, loaders and panel executor behaved as
documented.
