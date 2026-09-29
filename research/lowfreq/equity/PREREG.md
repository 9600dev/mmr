# Pre-registration — low-frequency EQUITY family (C03, decision D05)

Written 2026-09-29 BEFORE any candidate row exists in `trials.jsonl`
(the harness ledger; the orchestrator's checkpoint commit of this file
precedes the first `evaluate` call by construction). Nothing below is
edited after results are seen; corrections go in `REPORT.md`.

## Common design (fixed for every hypothesis, not fitted)

* Universe: point-in-time top-500 by dollar volume (`scripts/pit_panel.py`),
  delisted names included; fundamentals strictly as-of their EDGAR
  acceptance instant (`scripts/fundamental_signals.build(pit=True)`), reached
  only through the C01 loader (`trader.research.lowfreq.load_equity_panel`).
* Construction: on the first NYSE session of each month rank the COVERED set
  (member, priced, signal computable) on one signal and hold the top **20 %**
  (`QUANTILE = 0.2`) equal-weighted; flat elsewhere. Long-only, gross = 1.0
  when invested, never fewer than 30 names (`MIN_NAMES = 30`; if fewer than
  30 names are covered the book holds rather than rebalances). Monthly
  because the 2026-08-17 survivor was monthly and D05 allows nothing finer
  than weekly; a quintile because that is what the survivor used.
* Every candidate publishes its coverage frame; the DECISIVE BENCHMARK is
  `eq_ew_covered` (monthly equal weight of that same covered set), run
  through the identical `Backtester.run_panel` path and cost model
  (5 bps + $0.005/share; 3x for the stress). Walk-forward per D05: train
  1095 days / test 365 days rolling over 2016-08-01..2024-06-28, 400 calendar
  days of warm-up loaded before every window (the momentum and volatility
  signals need ~273 sessions of history).
* **No parameter grids.** Every entry is a single pre-registered cell
  (`params: {}`), so no cell is ever selected on a train window, gate (g) is
  n/a, and the trial count is exactly the number of registered candidates.
  The one free choice in each hypothesis (the scale) is stated below and is
  not varied.
* Implementation: `strategies/lowfreq_equity_base.py::CoveredTilt`; one module
  per candidate; tests in `tests/research/test_equity_*.py`.

## Hypotheses (registered in `candidates.yaml` in this order)

| id | H | signal (higher = held) | economic rationale | expected sign |
|---|---|---|---|---|
| `s2p_top_quintile` | H1 | as-of TTM revenue / (close x as-of diluted shares) | The program's one survivor (2026-08-17, PRE-D06/D07). Value: sales are the least manipulable fundamental; cheap-on-sales names carry a discount that mean-reverts. Must be DECOMPOSED (D1, D2) before belief. | + active |
| `size_smallcap` | D1 (decomposition, denominator) | -(close x diluted shares) | The size effect inside a liquid universe; the 2026-08-17 decomposition found 1/marketcap alone at IC 0.0443, t(NW) 3.34. If H1 does not beat this, H1 is size. | + active |
| `s2p_frozen_revenue` | D2 (decomposition, numerator control) | first-observed revenue in the window / market cap | Same knowledge mask as H1 with the revenue information removed. If it matches H1, the numerator added nothing. A control, never a champion. | ~ H1 |
| `pead_earnings` | H2 | (as-of TTM net income - the TTM it replaced) / as-of total assets, carried to the next filing | Post-earnings-announcement drift (Ball & Brown 1968; Bernard & Thomas 1989): TTM_t - TTM_prev = Q_t - Q_{t-4}, a seasonal random-walk surprise measured from the acceptance instant; under-reaction to news that arrived <= a quarter ago. Scaled by assets because earnings can be negative. | + active |
| `pead_revenue` | H3 | (as-of TTM revenue - the TTM it replaced) / prior TTM revenue | The same drift on the top line, which is less noisy and less managed than earnings; revenue surprises are known to drift independently of earnings surprises (Jegadeesh & Livnat 2006). | + active |
| `quality_value_blend` | H4 | mean of percentile ranks of gross profit / assets and sales-to-price | Novy-Marx (2013): profitability is "the other side of value"; blending removes the cheap-because-failing names from a value book. | + active, > H1 |
| `value_momentum_filter` | H5 | 1 + momentum rank for top-40 % sales-to-price names, momentum rank otherwise; 12-1 momentum = close[-21]/close[-252] - 1 | Value tells you what is cheap, momentum tells you when the market has started to agree (Asness, Moskowitz & Pedersen 2013). Held slice = the momentum-better half of the cheapest 40 %. The only price signal that survived the 2026-07-28 IC scan is 12-1 momentum; here it filters, it does not select. | + active, > H1 |
| `low_volatility` | H6 | -(126-session std of daily returns, >= 100 obs) | Low-volatility anomaly (Ang et al. 2006; Frazzini & Pedersen 2014): leverage-constrained investors overpay for volatile names; within an already-liquid set the effect should show as a drawdown advantage, so gate (d) is where it is expected to help. | + active, better max DD |
| `low_accruals` | H7 | -(TTM net income - TTM cash from operations) / total assets | Sloan (1996): earnings not backed by cash reverse; scaled by assets so it is not a size proxy. | + active |

Distinct information sets: H1/D1/D2 = level of sales vs price; H2/H3 = the
CHANGE in fundamentals at the filing instant (not used anywhere in the
program before); H4 = profitability; H5 = price momentum as a filter; H6 =
price volatility; H7 = accrual composition. D1/D2 are controls for H1.

## Champion-selection rule (written before results)

1. Eligible: candidate-role entries above except the control `s2p_frozen_revenue`.
2. Decomposition guard: `s2p_top_quintile` is eligible only if its OOS active
   annualised return exceeds BOTH `size_smallcap`'s and `s2p_frozen_revenue`'s;
   otherwise the sales numerator added nothing and `size_smallcap` stands in
   for it. `quality_value_blend` and `value_momentum_filter` are eligible
   only if they exceed `s2p_top_quintile`'s OOS active return (else the
   blend/filter added nothing over plain value).
3. Rank eligible candidates by harness verdict (PASS > PROMISING > FAIL),
   then by OOS active-return Newey-West t (higher first), then by lower
   full-period turnover/yr. The top-ranked entry is the family champion.
4. The champion's verdict is reported exactly as its `results/<id>.json`
   says. If no eligible candidate reaches PASS, REPORT.md states the null.

## Stopping rule

Stop after these nine evaluations (plus any error re-runs, which are
ledgered) unless the champion PASSes earlier. No new hypothesis is added
after seeing results without a dated addendum below this line explaining
what was seen first; any such addition is a new trial and counts.

## Stress (D07)

Every candidate's `stopped_names` block is reported; the champion's forced
exits are re-marked to zero (terminal-loss stress) in REPORT.md before it is
believed.
