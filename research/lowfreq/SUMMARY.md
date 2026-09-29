# Low-frequency research program — summary (2026-09-29)

Program `20260928-185549`: a reusable honest-evaluation harness
(`trader/research/lowfreq`, C01) and two pre-registered candidate families run
through it — cross-asset ETF allocation (`xasset`, C02) and long-only equity
tilts on the point-in-time universe with as-of fundamentals (`equity`, C03).
Protocol D01–D07 (`research/lowfreq/README.md`): rolling walk-forward on the
dev period, fills at t+1 open, fixed costs, a decisive benchmark through the
identical path, Newey-West t on active return, DSR deflated by the family's
distinct-entry trial count, 3× cost stress, drawdown and per-year gates.
**All data ends 2024-06-28. Nothing after 2024-06-30 was seen by any agent;
the held-out evaluation from 2024-07-01 will be run by the orchestrator** with
the commands below. Every number here is read from the named results JSON and
is reproducible with the named command.

## Champions

| family | champion | verdict | final parameter cell | trials N (distinct entries) | ledger rows | OOS Sharpe vs decisive | active/yr, t(NW) | DSR |
|---|---|---|---|---|---|---|---|---|
| xasset | `xa_vol_target` (H5, volatility-managed SPY/BIL) | **FAIL** | `{"TARGET_VOL": 0.15}` (modal over 12 folds; pre-registered default 0.12) | 7 | 18 (16 candidate + 2 benchmark) | 0.897 vs 0.950 (60/40) | +1.22 %, 0.77 | 0.04 |
| equity | `s2p_top_quintile` (H1, top sales-to-price quintile) | **PROMISING** | `{}` (single registered cell: QUANTILE 0.2, MIN_NAMES 30, monthly, equal weight) | 20 | 42 (all candidate) | 0.878 vs 0.564 (covered-EW) | +12.0 %, 2.01 | 0.64 |

Sources: `research/lowfreq/xasset/results/xa_vol_target.json`,
`research/lowfreq/equity/results/s2p_top_quintile.json`; full tables in each
family's `REPORT.md`. Reproduction on this tree (2026-09-29, `dc1d60c`):
`uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target` →
FAIL and `... evaluate equity/s2p_top_quintile` → PROMISING, matching the
verdicts and every deterministic field in the results JSONs.

**No family produced a PASS.**

### xasset — null

Seven entries covering five mechanisms (time-series trend ×3, cross-sectional
momentum, risk parity, volatility targeting, distribution-yield carry), 24 grid
cells, 12 OOS folds 2012-06..2024-06 (3,015 days). Every candidate fails gate
(a): none reaches 60/40's OOS Sharpe of 0.950, and only the champion has a
positive active return at all (+1.22 %/yr, t 0.77, 69 % positive years, max
DD −21.3 % vs −18.6 %). Its book averages 85 % SPY, so the extra CAGR is the
equity premium it carried; de-risking arrived after the falls (2020 active
−18.6 %). The trend and risk-balancing books sat 49–72 % in bonds/BIL and lost
6–8 pp/yr to 60/40; the trend filter on 60/40 itself lost 2.95 %/yr (t −2.01).
PBO for the three 4-cell grids: 0.34 / 0.73 / 0.51.

### equity — PROMISING, not PASS

Twenty single-cell entries (seven hypotheses, thirteen decomposition
controls), 4 OOS folds 2019-08..2023-07 (1,005 days), decisive benchmark
equal weight of the same covered names. The champion clears every gate but
(c): OOS CAGR 22.9 % vs 10.5 %, active +12.0 %/yr in all five counted years,
+11.5 % at 3× cost, max DD −36.8 % vs −32.7 %, DSR 0.64 at N = 20 (needs
0.90). Decomposition: frozen-revenue control 0.88 / +11.9 %, size alone 0.76 /
+8.7 %, revenue alone 0.53 / −0.4 % — the signal is the level of sales
relative to price, not revenue updates and not only size. Capacity: turnover
9.0/yr one-way, median 65 names, median holding $ADV $263 M. D07
terminal-loss stress (all stopped names marked to zero, both books): active
+13.8 %/yr, t 2.25 — holds. Eight of twenty entries PROMISING, twelve FAIL;
momentum, low volatility and gross profitability do not beat covered-EW.
Caveat stated by C03: t ≈ 2 on four years of monthly returns is fragile;
every OOS drawdown includes March 2020.

## Held-out evaluation (orchestrator, 2024-07-01 onward)

Fixed-cell mode (D02): one registered cell, no selection, warm-up loaded
before `--start` (500 days ETF, 400 days equity). The CLI requires `--end`
(exit 2 without it); use the last held-out session available.

```
uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target --fixed-params '{"TARGET_VOL": 0.15}' --start 2024-07-01 --end <YYYY-MM-DD>
uv run python -m trader.research.lowfreq evaluate equity/s2p_top_quintile --fixed-params '{}' --start 2024-07-01 --end <YYYY-MM-DD>
```

The databases named by `LOWFREQ_RESEARCH_CONFIG` must carry `etf_daily_bars`,
`pit_daily_bars` and `fundamentals` through `--end`. Results land in
`results/<id>__fixed_2024-07-01_<end>.json`; each call appends one ledger row
with the same entry hash (N unchanged). Both command shapes were exercised on
this tree over one dev-period year (`--start 2023-07-03 --end 2024-06-28`,
ledger rows xasset 18 and equity 42): they run to completion; their one-year
verdicts (xasset PROMISING, equity FAIL) are inside the dev period and carry
no evidential weight. Priors for the held-out: xasset is expected to track
SPY-with-cash and lose to 60/40 on Sharpe; equity's PROMISING rests on t ≈ 2.

## Bugs fixed (C01; C02 and C03 found none)

* `trader/simulation/backtester.py` `run_panel` — BUY fills could take cash
  below zero (implicit, cost-free leverage when a rebalance sized buys off
  holdings it could not sell). D06: sells settle first, buys scale pro rata to
  available cash in whole shares; `panel_scaled_buy_fills` /
  `panel_scaled_rebalances` diagnostics. Tests: `tests/test_panel_cash.py`
  (8 tests, e.g. `test_a_gapping_open_scales_the_buy_to_the_cash_available_and_is_counted`,
  `test_cash_rebuilt_from_the_fill_log_never_goes_negative_on_a_long_only_book`).
* `scripts/pit_panel.load_pit_panel` prices only on membership days — every
  name leaving the top-500 became unsellable (covered-EW benchmark: 511 frozen
  holdings worth 102 % of equity, cash −$3.2 M on 99 % of fill dates). Fixed
  in the harness loader (prices kept for every name ever a member,
  `fields['member']`, opens refused outside membership) plus D07 stopped-name
  exits at the last stored print (`panel_terminal_prints`,
  `panel_forced_exits`). Tests:
  `tests/research/test_lowfreq_known_answer.py::test_covered_benchmark_sells_leavers_and_reports_cash_and_frozen_diagnostics`,
  `::test_buying_a_name_outside_pit_membership_is_refused`,
  `tests/research/test_lowfreq_round4.py::test_executor_sells_a_stopped_name_at_its_last_print_and_never_after`,
  `::test_a_name_missing_at_a_window_boundary_that_prints_again_is_not_a_stopped_name`,
  `::test_equity_loader_never_fabricates_a_price_and_the_harness_exits_at_the_last_print`.
  Consequence: the covered-EW full-period Sharpe fell 0.88 → 0.59; every
  `run_panel` figure dated before 2026-09-29 (including the 2026-08-17 size /
  sales-to-price books, Sharpe 1.26 vs 1.04) is pre-fix.

## Integration changes (this round)

Re-running a champion (acceptance criterion 3) appends a ledger row and
rewrites `results/<id>.json` with new run provenance, which two xasset
artefact tests and the equity artefact checker rejected. Fixed without
changing any result: `tests/research/test_xasset_report.py` now requires the
report's ledger rows as a verbatim prefix and accepts later rows only if they
repeat an earlier ok row's (id, entry hash) and reproduce the results verdict
(fixed-cell rows carry their own); `xasset/make_report.py check_passes`
compares deterministic fields, verdict and DSR instead of bytes (pinned by
`test_check_passes_accepts_a_reproduction_rewrite_but_flags_drift`);
`equity/check_artifacts.py` excludes reproductions from the 40-row budget and
accepts an unarchived row whose `results_path` file embeds it. Equity rows
41–42 are archived (`archive_trials.py`); `check_artifacts.py` and
`provenance.py` report ALL OK / 0 mismatches. Ledger rows added this round:
xasset 17 (walk-forward reproduction, FAIL), 18 (fixed-cell check); equity 41
(reproduction, PROMISING), 42 (fixed-cell check); reference 44–45 (two gate
runs of `etf_6040`, both PASS).

## Where this leaves the program

Two information sets the program had not exploited were searched under
pre-registration and trial counting: neither yields a PASS on the dev period.
The cross-asset family is a clean null. The sales-to-price tilt is the one
construction that beats its own universe after honest execution (D06/D07),
and it is PROMISING, not proven — the held-out period decides. No further
search is warranted before that verdict; if it holds, the next step is
operational (a monthly ~65-name book on MMR's execution path), not
statistical.
