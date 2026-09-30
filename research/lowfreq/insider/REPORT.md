# Insider family — REPORT (C04; D11, D14)

Generated 2026-09-30T00:12:48+00:00 by `make_report.py` from `results/*.json` (pass 2), `trials.jsonl`, `candidates.yaml`, `results/_calibration_counts.json`. Pre-registration: `PREREG.md` (git `f0658e5`, addendum 1 before the first ledger row). Dev period 2016-08-01..2024-06-28, walk-forward train 1095 / test 365 days, 4 folds, OOS 1005 sessions; costs 5 bps + $0.005/share, stress x3. Distinct candidate trials in the ledger: **16** (budget 30); every pass-2 file is deflated by 16.

## Verdict

**Champion: `ins_net_buy_ratio` (H5), selected by rule 4 (no rule passed the guards; highest OOS active t(NW) among the rules). Harness verdict: FAIL.** This is the null: no pre-registered insider rule beat the equal-weight PIT basket to the D01 standard. Gates: a_sharpe_vs_benchmark=ok, b_active_t_ge_1=FAIL, b_active_t_ge_2=FAIL, c_dsr_ge_0_90=FAIL, d_drawdown_within_5pp=ok, e_positive_years_ge_60pct=ok, f_active_positive_at_3x_cost=ok, g_pbo_and_stability=n/a.

## Every candidate (pass 2, final trial count)

| id | H | OOS Sharpe | bench Sharpe | active ann | t(NW) | DSR (N) | 3x-cost active | +years | OOS maxDD | bench maxDD | turnover/yr | names | verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `ins_purchase_count` | C0 | 0.40 | 0.49 | -0.65% | -0.25 | 0.01 (16) | -1.09% | 60% | -35.0% | -31.6% | 7.11 | 554 | **FAIL** |
| `ins_opportunistic` | H1 | 0.40 | 0.49 | -0.73% | -0.27 | 0.01 (16) | -1.17% | 60% | -35.0% | -31.6% | 7.11 | 554 | **FAIL** |
| `ins_cluster` | H2 | 0.38 | 0.49 | -1.64% | -0.71 | 0.00 (16) | -1.87% | 0% | -33.7% | -31.6% | 5.59 | 557 | **FAIL** |
| `ins_conviction` | H3 | 0.40 | 0.49 | -0.57% | -0.20 | 0.01 (16) | -0.99% | 40% | -35.1% | -31.6% | 6.97 | 554 | **FAIL** |
| `ins_officer` | H4a | 0.36 | 0.49 | -2.34% | -1.21 | 0.00 (16) | -2.61% | 20% | -33.0% | -31.6% | 5.83 | 556 | **FAIL** |
| `ins_director` | H4b | 0.47 | 0.49 | +0.97% | 0.33 | 0.03 (16) | +0.66% | 80% | -36.3% | -31.6% | 7.07 | 554 | **FAIL** |
| `ins_net_buy_ratio` | H5 | 0.51 | 0.49 | +1.75% | 0.56 | 0.05 (16) | +1.50% | 80% | -35.8% | -31.6% | 5.55 | 555 | **FAIL** |
| `ins_value_to_mcap` | H6 | 0.44 | 0.49 | +0.37% | 0.12 | 0.02 (16) | +0.03% | 40% | -34.7% | -31.6% | 6.24 | 556 | **FAIL** |
| `ins_size_matched_count` | D | 0.68 | 0.49 | +4.69% | 2.39 | 0.55 (16) | +3.78% | 80% | -33.7% | -31.6% | 11.64 | 555 | **PROMISING** |
| `ins_size_matched_opportunistic` | D | 0.69 | 0.49 | +4.76% | 2.43 | 0.57 (16) | +3.84% | 80% | -33.6% | -31.6% | 11.64 | 555 | **PROMISING** |
| `ins_size_matched_cluster` | D | 0.56 | 0.49 | +1.49% | 1.20 | 0.11 (16) | +1.04% | 80% | -32.5% | -31.6% | 7.30 | 557 | **PROMISING** |
| `ins_size_matched_conviction` | D | 0.61 | 0.49 | +2.99% | 1.56 | 0.25 (16) | +2.11% | 60% | -33.5% | -31.6% | 11.08 | 555 | **PROMISING** |
| `ins_size_matched_officer` | D | 0.50 | 0.49 | +0.54% | 0.37 | 0.03 (16) | -0.03% | 60% | -32.5% | -31.6% | 8.05 | 556 | **FAIL** |
| `ins_size_matched_director` | D | 0.74 | 0.49 | +6.12% | 2.94 | 0.78 (16) | +5.15% | 100% | -33.8% | -31.6% | 11.83 | 555 | **PROMISING** |
| `ins_size_matched_net_buy_ratio` | D | 0.66 | 0.49 | +3.81% | 2.15 | 0.47 (16) | +3.13% | 100% | -33.6% | -31.6% | 8.86 | 556 | **PROMISING** |
| `ins_size_matched_value_to_mcap` | D | 0.56 | 0.49 | +1.79% | 1.01 | 0.09 (16) | +1.19% | 60% | -32.0% | -31.6% | 8.67 | 556 | **PROMISING** |

Benchmark `eq_ew_pit` = monthly equal weight of the covered PIT members (D11), identical path and costs; decisive benchmark in every fold: ['eq_ew_pit']. `names` = median names held (full period); size controls (D) mirror their rule with market-cap neighbours.

## Champion selection (pre-registered rule, PREREG.md)

| rule | active ann | its size control | beats control | beats C0 | eligible |
|---|---|---|---|---|---|
| `ins_purchase_count` | -0.65% | +4.69% | no | - | no |
| `ins_opportunistic` | -0.73% | +4.76% | no | no | no |
| `ins_cluster` | -1.64% | +1.49% | no | no | no |
| `ins_conviction` | -0.57% | +2.99% | no | yes | no |
| `ins_officer` | -2.34% | +0.54% | no | no | no |
| `ins_director` | +0.97% | +6.12% | no | yes | no |
| `ins_net_buy_ratio` | +1.75% | +3.81% | no | yes | no |
| `ins_value_to_mcap` | +0.37% | +1.79% | no | yes | no |

Eligible: none. Champion `ins_net_buy_ratio` by rule 4 (no rule passed the guards; highest OOS active t(NW) among the rules).

## Champion `ins_net_buy_ratio` in detail

* OOS: Sharpe 0.51 vs benchmark 0.49; CAGR +9.37% vs +8.11%; max drawdown -35.8% vs -31.6%.
* Active return: +1.75%/yr, Newey-West t 0.56 (6 lags), active Sharpe 0.28; positive in 80% of OOS calendar years; under 3x costs +1.50%/yr.
* DSR 0.05 deflated by N = 16 trials.
* Turnover 5.55x/yr (full period); median names held 555; median $ADV of holdings $301,976,631; min cash fraction 0.00; cash-guard scaled buys 26671 (D06).
* Stopped names (D07): 303 forced exits over the full period worth 58.80% of initial capital (224 inside OOS windows; benchmark {'eq_ew_pit': 309}). Terminal-loss stress: had every forced exit gone to zero, the full-period return would be lower by 58.80%, about -7.44%/yr over 7.9 years.
* Lookahead: 1 cell(s) and ['eq_ew_pit'] checked with 75 fresh-state prefixes; PBO n/a (single cell).

### Per-year active return (OOS)

| year | 2019 | 2020 | 2021 | 2022 | 2023 |
|---|---|---|---|---|---|
| active | +3.02% | -4.70% | +0.86% | +5.47% | +2.33% |

`*` = partial calendar year, not counted in gate (e).

## Decomposition against the confounders

Purchase count alone = C0 (`ins_purchase_count`). Size = each rule's market-cap-matched control (same K, same sleeve, PREREG addendum 1). Calibration columns are qualifier counts, not returns.

| rule | H | K median (calibration) | qualifier mcap pctile | rule active ann | size control active ann | rule - control | rule - C0 |
|---|---|---|---|---|---|---|---|
| `ins_purchase_count` | C0 | 40 | 0.50 | -0.65% | +4.69% | -5.34% | - |
| `ins_opportunistic` | H1 | 40 | 0.50 | -0.73% | +4.76% | -5.49% | -0.08% |
| `ins_cluster` | H2 | 11 | 0.42 | -1.64% | +1.49% | -3.13% | -0.99% |
| `ins_conviction` | H3 | 26 | 0.51 | -0.57% | +2.99% | -3.56% | +0.08% |
| `ins_officer` | H4a | 13 | 0.44 | -2.34% | +0.54% | -2.88% | -1.69% |
| `ins_director` | H4b | 34 | 0.51 | +0.97% | +6.12% | -5.16% | +1.62% |
| `ins_net_buy_ratio` | H5 | 18 | 0.44 | +1.75% | +3.81% | -2.06% | +2.40% |
| `ins_value_to_mcap` | H6 | 17 | 0.26 | +0.37% | +1.79% | -1.42% | +1.02% |

Reading the controls: every control beats the equal-weight basket although it holds no insider information. The matching pool is covered names WITH an as-of market cap (operating companies with EDGAR fundamentals; 58 % of covered name-days per the calibration file), so part of a control's excess over the basket is "operating companies vs the ETFs/ADRs/trusts in the PIT basket" - a tilt every rule shares by construction (addendum 1). The honest reading is therefore rule minus control: negative for all eight rules, i.e. conditional on size and on being an operating company, the names insiders bought did WORSE than their neighbours over 2019-2024. The controls are not candidates (PREREG: never champions) and their PROMISING verdicts are reported, not claimed.

## Every ledger trial

32 rows (`trials.jsonl`, append-only; pass 1 then pass 2).

| # | timestamp (UTC) | id | mode | status | trials at run | verdict | OOS Sharpe | active ann | t(NW) | DSR |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 2026-09-30T00:02:34 | `ins_cluster` | walk_forward | ok | 1 | FAIL | 0.38 | -1.64% | -0.71 | 0.23 |
| 2 | 2026-09-30T00:02:38 | `ins_purchase_count` | walk_forward | ok | 1 | FAIL | 0.40 | -0.65% | -0.25 | 0.40 |
| 3 | 2026-09-30T00:02:39 | `ins_opportunistic` | walk_forward | ok | 1 | FAIL | 0.40 | -0.73% | -0.27 | 0.39 |
| 4 | 2026-09-30T00:02:40 | `ins_officer` | walk_forward | ok | 1 | FAIL | 0.36 | -2.34% | -1.21 | 0.11 |
| 5 | 2026-09-30T00:02:40 | `ins_value_to_mcap` | walk_forward | ok | 1 | FAIL | 0.44 | +0.37% | 0.12 | 0.55 |
| 6 | 2026-09-30T00:02:40 | `ins_director` | walk_forward | ok | 1 | FAIL | 0.47 | +0.97% | 0.33 | 0.63 |
| 7 | 2026-09-30T00:02:41 | `ins_conviction` | walk_forward | ok | 1 | FAIL | 0.40 | -0.57% | -0.20 | 0.42 |
| 8 | 2026-09-30T00:02:48 | `ins_net_buy_ratio` | walk_forward | ok | 1 | FAIL | 0.51 | +1.75% | 0.56 | 0.71 |
| 9 | 2026-09-30T00:06:03 | `ins_size_matched_cluster` | walk_forward | ok | 9 | PROMISING | 0.56 | +1.49% | 1.20 | 0.51 |
| 10 | 2026-09-30T00:06:07 | `ins_size_matched_officer` | walk_forward | ok | 9 | FAIL | 0.50 | +0.54% | 0.37 | 0.29 |
| 11 | 2026-09-30T00:06:08 | `ins_size_matched_value_to_mcap` | walk_forward | ok | 9 | PROMISING | 0.56 | +1.79% | 1.01 | 0.46 |
| 12 | 2026-09-30T00:06:09 | `ins_size_matched_opportunistic` | walk_forward | ok | 9 | PROMISING | 0.69 | +4.76% | 2.43 | 0.80 |
| 13 | 2026-09-30T00:06:09 | `ins_size_matched_count` | walk_forward | ok | 9 | PROMISING | 0.68 | +4.69% | 2.39 | 0.79 |
| 14 | 2026-09-30T00:06:09 | `ins_size_matched_director` | walk_forward | ok | 9 | PROMISING | 0.74 | +6.12% | 2.94 | 0.88 |
| 15 | 2026-09-30T00:06:10 | `ins_size_matched_conviction` | walk_forward | ok | 9 | PROMISING | 0.61 | +2.99% | 1.56 | 0.63 |
| 16 | 2026-09-30T00:06:17 | `ins_size_matched_net_buy_ratio` | walk_forward | ok | 9 | PROMISING | 0.66 | +3.81% | 2.15 | 0.77 |
| 17 | 2026-09-30T00:09:21 | `ins_cluster` | walk_forward | ok | 16 | FAIL | 0.38 | -1.64% | -0.71 | 0.00 |
| 18 | 2026-09-30T00:09:25 | `ins_conviction` | walk_forward | ok | 16 | FAIL | 0.40 | -0.57% | -0.20 | 0.01 |
| 19 | 2026-09-30T00:09:26 | `ins_officer` | walk_forward | ok | 16 | FAIL | 0.36 | -2.34% | -1.21 | 0.00 |
| 20 | 2026-09-30T00:09:26 | `ins_purchase_count` | walk_forward | ok | 16 | FAIL | 0.40 | -0.65% | -0.25 | 0.01 |
| 21 | 2026-09-30T00:09:26 | `ins_opportunistic` | walk_forward | ok | 16 | FAIL | 0.40 | -0.73% | -0.27 | 0.01 |
| 22 | 2026-09-30T00:09:26 | `ins_director` | walk_forward | ok | 16 | FAIL | 0.47 | +0.97% | 0.33 | 0.03 |
| 23 | 2026-09-30T00:09:27 | `ins_value_to_mcap` | walk_forward | ok | 16 | FAIL | 0.44 | +0.37% | 0.12 | 0.02 |
| 24 | 2026-09-30T00:09:36 | `ins_net_buy_ratio` | walk_forward | ok | 16 | FAIL | 0.51 | +1.75% | 0.56 | 0.05 |
| 25 | 2026-09-30T00:12:29 | `ins_size_matched_cluster` | walk_forward | ok | 16 | PROMISING | 0.56 | +1.49% | 1.20 | 0.11 |
| 26 | 2026-09-30T00:12:31 | `ins_size_matched_count` | walk_forward | ok | 16 | PROMISING | 0.68 | +4.69% | 2.39 | 0.55 |
| 27 | 2026-09-30T00:12:34 | `ins_size_matched_officer` | walk_forward | ok | 16 | FAIL | 0.50 | +0.54% | 0.37 | 0.03 |
| 28 | 2026-09-30T00:12:35 | `ins_size_matched_conviction` | walk_forward | ok | 16 | PROMISING | 0.61 | +2.99% | 1.56 | 0.25 |
| 29 | 2026-09-30T00:12:35 | `ins_size_matched_director` | walk_forward | ok | 16 | PROMISING | 0.74 | +6.12% | 2.94 | 0.78 |
| 30 | 2026-09-30T00:12:36 | `ins_size_matched_opportunistic` | walk_forward | ok | 16 | PROMISING | 0.69 | +4.76% | 2.43 | 0.57 |
| 31 | 2026-09-30T00:12:44 | `ins_size_matched_value_to_mcap` | walk_forward | ok | 16 | PROMISING | 0.56 | +1.79% | 1.01 | 0.09 |
| 32 | 2026-09-30T00:12:45 | `ins_size_matched_net_buy_ratio` | walk_forward | ok | 16 | PROMISING | 0.66 | +3.81% | 2.15 | 0.47 |

## Held-out evaluation (orchestrator only, D11)

The held-out period starts after the dev cutoff; the full research directory's insider data ends 2026-03-31 (DATA.md), so the window is `2024-07-01..2026-03-31`:

```
LOWFREQ_RESEARCH_CONFIG=~/.local/share/mmr/research_full/trader_research.yaml \
  uv run python -m trader.research.lowfreq evaluate insider/ins_net_buy_ratio --fixed-params '{}' --start 2024-07-01 --end 2026-03-31
```

Source of every number: `results/ins_net_buy_ratio.json` keys `oos`, `oos_benchmarks.eq_ew_pit`, `active`, `dsr`, `full_period.report_cell`, `stopped_names`, `gates`; other candidates from their own `results/<id>.json`; ledger rows from `trials.jsonl`; K and market-cap percentiles from `results/_calibration_counts.json`.
