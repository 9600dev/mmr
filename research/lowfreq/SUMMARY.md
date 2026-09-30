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

| family | champion | verdict | final parameter cell | trials N (distinct entries) | OOS Sharpe vs decisive | active/yr, t(NW) | DSR |
|---|---|---|---|---|---|---|---|
| xasset | `xa_vol_target` (H5, volatility-managed SPY/BIL) | **FAIL** | `{"TARGET_VOL": 0.15}` (modal over 12 folds; pre-registered default 0.12) | 7 | 0.897 vs 0.950 (60/40) | +1.22 %, 0.77 | 0.04 |
| equity | `s2p_top_quintile` (H1, top sales-to-price quintile) | **PROMISING** | `{}` (single registered cell: QUANTILE 0.2, MIN_NAMES 30, monthly, equal weight) | 20 | 0.878 vs 0.564 (covered-EW) | +12.0 %, 2.01 | 0.64 |

Sources: `research/lowfreq/xasset/results/xa_vol_target.json`,
`research/lowfreq/equity/results/s2p_top_quintile.json`; full tables in each
family's `REPORT.md`. Reproduction on this tree (2026-09-29, `dc1d60c`):
`uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target` →
FAIL and `... evaluate equity/s2p_top_quintile` → PROMISING, matching the
verdicts and every deterministic field in the results JSONs; the round-1
critic reproduced both independently (ledger rows xasset 19, equity 43).

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
before `--start` (500 days ETF, 400 days equity). The end dates are the last
sessions in the orchestrator's held-out databases: 2026-09-28 for the ETF
panel, 2026-07-28 for the PIT universe and fundamentals. Primary commands,
one per champion:

```
uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target --fixed-params '{"TARGET_VOL": 0.15}' --start 2024-07-01 --end 2026-09-28
uv run python -m trader.research.lowfreq evaluate equity/s2p_top_quintile --fixed-params '{}' --start 2024-07-01 --end 2026-07-28
```

Secondary, xasset only — the pre-registered default cell, chosen by the
walk-forward in 2 of 12 folds; reported beside the primary, never in its
place:

```
uv run python -m trader.research.lowfreq evaluate xasset/xa_vol_target --fixed-params '{"TARGET_VOL": 0.12}' --start 2024-07-01 --end 2026-09-28
```

Results land in `results/<id>__fixed_2024-07-01_<end>.json`; each call appends
one ledger row with the same entry hash (N unchanged). Both command shapes
were exercised on this tree over one dev-period year (`--start 2023-07-03
--end 2024-06-28`): they run to completion; their one-year verdicts (xasset
PROMISING, equity FAIL) are inside the dev period and carry no evidential
weight. Priors for the held-out: xasset is expected to track SPY-with-cash
and lose to 60/40 on Sharpe; equity's PROMISING rests on t ≈ 2.

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
(a fixed-cell row must instead point at its persisted
`results/<id>__fixed_*.json`, which must embed that row); `xasset/make_report.py check_passes`
compares deterministic fields, verdict and DSR instead of bytes (pinned by
`test_check_passes_accepts_a_reproduction_rewrite_but_flags_drift`);
`equity/check_artifacts.py` excludes reproductions from the 40-row budget and
accepts an unarchived row whose `results_path` file embeds it. Post-selection ledger rows are reproductions, identified
here by kind rather than by a running count (gate, review and held-out runs
keep appending): xasset 17 and equity 41 (2026-09-29 10:04 / 10:06 UTC) are
the integration re-runs; xasset 18 and equity 42 the fixed-cell command
checks; xasset 19 and equity 43 the round-1 critic's re-runs; reference rows
from 44 on are `etf_6040` gate runs (all PASS). Every equity ok row has an
archived artifact under `results/trials/` (`archive_trials.py`, re-run after
each reproduction); `check_artifacts.py` and `provenance.py` report ALL OK /
0 mismatches.

## Held-out result (orchestrator, 2026-09-29)

| champion | held-out window | OOS Sharpe | benchmark | active/yr | t(NW) | verdict |
|---|---|---|---|---|---|---|
| equity `s2p_top_quintile` | 2024-07-01..2026-07-28 | 1.30 | 1.17 covered-EW | +0.15 % | 0.03 | FAIL |
| xasset `xa_vol_target` 0.15 | 2024-07-01..2026-09-28 | 0.99 | 1.12 60/40, 1.40 EW | +0.85 % | 0.16 | FAIL |
| xasset `xa_vol_target` 0.12 (secondary) | 2024-07-01..2026-09-28 | 0.99 | same | −0.81 % | −0.17 | FAIL |

The dev-period edge of the sales-to-price tilt (+12.0 %/yr) did not appear
held out (+0.15 %/yr). Details in `docs/RESEARCH_LOG.md` (2026-09-29 held-out entry).

## Insider family (C04, program 2, 2026-09-29) — null

Eight pre-registered rules on SEC Form 4 officer/director open-market
purchases (count baseline, non-routine buyers, cluster, conviction, officer,
director, net purchase ratio, value / market cap) as 2 %-per-name overlays on
the monthly equal-weight PIT basket (D11), each with a market-cap-matched
control holding the same sleeve in neighbouring non-qualifiers; D14 event
timing; 16 trials, two passes (final DSR deflated by N = 16).

| book | OOS Sharpe | bench Sharpe | active/yr | t(NW) | verdict |
|---|---|---|---|---|---|
| `ins_purchase_count` (C0) | 0.40 | 0.49 | −0.65 % | −0.25 | FAIL |
| `ins_net_buy_ratio` (best rule, champion by rule 4) | 0.51 | 0.49 | +1.75 % | 0.56 | FAIL |
| `ins_size_matched_count` (C0's size control) | 0.68 | 0.49 | +4.69 % | 2.39 | PROMISING |

No rule beats its own size control (gaps of 2–8 %/yr): what insiders buy has a
favourable size / operating-company profile, and the buying subtracts from it.
Champion `ins_net_buy_ratio`, verdict FAIL — the null. Details, per-year
returns, 3x costs, D07 stress and the orchestrator's held-out command:
`research/lowfreq/insider/REPORT.md`.

## Where this leaves the program

Two information sets the program had not exploited were searched under
pre-registration and trial counting: neither yields a PASS on the dev period.
The cross-asset family is a clean null. The sales-to-price tilt is the one
construction that beats its own universe after honest execution (D06/D07),
and it is PROMISING, not proven — the held-out period decides. No further
search is warranted before that verdict; if it holds, the next step is
operational (a monthly ~65-name book on MMR's execution path), not
statistical.
