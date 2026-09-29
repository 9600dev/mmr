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

---

## Addendum R2 — 2026-09-29, written BEFORE the first `evaluate` call

State when written: the round-1 checkpoint (commit `92653bf`) holds the
registration above and NO `trials.jsonl` / `results/` — no result of any
kind existed. The changes below answer the round-1 review; each makes the
test stricter or the rule more conservative, none could be tuned to a
result. The ledger rows of every run carry `git_sha = 92653bf`, i.e. they
descend from that checkpoint.

1. **H6 low volatility — implementation aligned with the registered rule.**
   The round-1 file also required an as-of `shares_diluted` (via market cap)
   for coverage, which the rule above never said; that condition is removed.
   Coverage for H6 is member ∧ priced ∧ 126-session volatility computable, so
   candidate and decisive benchmark share exactly that universe.
2. **Decomposition controls for every ratio candidate** (all
   `role: candidate`, so they COUNT in the DSR trial count; none can be
   champion):

   | id | control for | signal |
   |---|---|---|
   | `size_small_assets` (D3) | `pead_earnings`, `quality_value_blend`, `low_accruals` (asset denominators) | −as-of total assets |
   | `size_small_revenue` (D4) | `pead_revenue` (prior-revenue denominator) | −as-of TTM revenue |
   | `quality_value_frozen` (D5) | `quality_value_blend` (both numerators) | rank(first GP / assets) + rank(first revenue / mcap) |
   | `accruals_frozen` (D6) | `low_accruals` (numerator) | −first (NI − CFO) / assets |

   Change-based signals (H2, H3) have no meaningful frozen-numerator variant
   (a frozen change is a constant, i.e. the denominator control itself), so
   their denominator control is the whole decomposition.
3. **Eligibility guards (extend rule 2).** A candidate is eligible only if
   its OOS active annualised return exceeds that of EVERY control listed:
   `pead_earnings` → `size_small_assets`; `pead_revenue` → `size_small_revenue`;
   `quality_value_blend` → `s2p_top_quintile`, `size_smallcap`,
   `size_small_assets`, `quality_value_frozen`; `low_accruals` →
   `size_small_assets`, `accruals_frozen`; `value_momentum_filter` →
   `s2p_top_quintile`, `size_smallcap` (its value half is a ratio);
   `low_volatility` → no control (price-only, not a ratio). Only
   `s2p_top_quintile` is REPLACED by its denominator (`size_smallcap`) when it
   fails its guard; every other guarded candidate is simply ineligible.
   `size_smallcap`, `size_small_assets`, `size_small_revenue` are pure size
   tilts: eligible as the "size effect" hypothesis only if no guarded
   hypothesis is eligible, and then only `size_smallcap` (registered as H-D1).
4. **Trial-count protocol for DSR.** The harness deflates by the distinct
   candidate hashes present in the ledger at run time, so a single pass
   would give the first run N = 1 and the last N = 13. Protocol: **pass 1**
   evaluates all 13 candidates (rows kept, their N is partial by
   construction); **pass 2** re-evaluates all 13 against the complete ledger,
   so every FINAL `results/<id>.json` reports N = 13 with the same
   trial-Sharpe set. Re-running an identical entry hash adds no trial. Every
   ledger row (both passes, any error re-run) is listed in REPORT.md. Any
   run whose code or registry entry changes after pass 1 is a new hash and
   a new trial.
5. **D07 terminal-loss stress — procedure** (`stress_d07.py`, run on the
   champion after pass 2): re-run the champion's OOS fold windows (the fold
   table in its results JSON) and the decisive benchmark through the same
   engine path; a forced exit is a SELL fill dated at the name's terminal
   print (`engine.terminal_prints`); the stress removes the proceeds of every
   such fill from equity from that day on (the name marked to ZERO), (i) for
   the candidate alone and (ii) for candidate and benchmark alike. The event
   list (date, ticker, quantity, price, value) and the stressed OOS Sharpe /
   active return are stored in `results/<id>__d07_stress.json`. The
   champion is believed only if the fair (ii) active return stays > 0.
6. **PEAD semantics pinned.** `latest_change` detects a change in the as-of
   TTM value, not a filing event: a filing that leaves the TTM numerically
   unchanged (Q_t = Q_{t−4} to the dollar) is not a surprise and the previous
   one carries. Rare for TTM sums; documented, tested, not varied.
7. Shared files (`docs/RESEARCH_LOG.md`, `research/lowfreq/SUMMARY.md`) are
   not edited by this component; findings live in `REPORT.md`.

---

## Addendum R3 — 2026-09-29, written AFTER pass-1 results were seen, BEFORE any R3 run

State when written: checkpoint `f41bc9a` holds the 13 registered entries, the
R2 addendum, the 13 pass-1 ledger rows (N = 1..6, partial by construction)
and their results files. Pass 2 has not run. This addendum answers the
round-2 review. Nothing here can raise a candidate's chance of PASS: it adds
controls (raising N), adds guards, and preserves artifacts.

### Provenance (honest statement of a structural limit)

Every ledger row records the git SHA of the checkpoint the working tree was
based on, not of a commit containing the exact files that ran: pass-1 rows
say `92653bf`, but ran from `92653bf` + the then-uncommitted R2 changes,
which were committed unchanged in `f41bc9a`. Pass-2 rows carry `f41bc9a`,
which contains the exact entries and sources of the 13 entries. The R3
controls below run before their commit (the round-3 checkpoint). What IS
immutable and machine-checkable: every row carries `entry_sha256` (canonical
JSON of its registry entry) and `strategy_sha256` (its module source), so
`uv run python research/lowfreq/equity/provenance.py` recomputes both from
the committed files for every row and fails on any mismatch — the committed
registry and sources are provably the ones that ran. `PROVENANCE.json`
records sha256 of `PREREG.md` and `candidates.yaml` at the instant before the
first R3 run, checked by the same script. The claim "PREREG existed before
the first candidate row" rests on `92653bf` for the nine original hypotheses
and the champion rule; the R2/R3 additions rest on this hash chain.

### Expected sign and rationale for the R2 controls (D3–D6)

| id | expected active sign | rationale |
|---|---|---|
| `size_small_assets` | + (a size effect inside the liquid universe, as D1; weaker than D1 since assets ≠ price) | pure denominator of every asset-scaled ratio here |
| `size_small_revenue` | + or ~0 (small-revenue names are small caps and growth names alike) | pure denominator of the revenue-surprise ratio |
| `quality_value_frozen` | ≈ `quality_value_blend` (the blend's fundamentals update slowly) | the blend with its information updates removed |
| `accruals_frozen` | ≈ `low_accruals` if the accrual information is inert; below it if accruals matter | the accrual level with its updates removed |

### R3 controls — each remaining ratio component ALONE (standing rule)

| id | control for | signal | expected active sign |
|---|---|---|---|
| `pead_earnings_raw` (D7) | `pead_earnings` numerator | raw $ change in TTM net income | ~0 / − : a large-cap tilt inside the universe |
| `pead_revenue_raw` (D8) | `pead_revenue` numerator | raw $ change in TTM revenue | ~0 / − (large-cap tilt) |
| `accruals_raw` (D9) | `low_accruals` numerator | −raw $ (NI − CFO) | ~0 (large negative-accrual $ are large firms) |
| `gross_profitability` (D10) | `quality_value_blend` component | GP / assets | + (Novy-Marx), below the blend if value adds |
| `gross_profit_raw` (D11) | `gross_profitability` numerator | raw $ gross profit | ~0 / − (large-cap tilt) |
| `momentum_12_1` (D12) | `value_momentum_filter` component | 12-1 price momentum | ≤ 0 vs covered-EW (2026-07-30) |
| `revenue_raw` (D13) | `s2p_top_quintile` numerator | raw $ TTM revenue | ~0 / − (large-cap tilt) |

All seven are `role: candidate` (they count in N), single-cell, never
champions. Guards (rule 2 + R2 item 3) now read: a candidate is eligible only
if its OOS active return exceeds EVERY listed control —
`s2p_top_quintile`: size_smallcap, s2p_frozen_revenue, revenue_raw;
`pead_earnings`: size_small_assets, pead_earnings_raw;
`pead_revenue`: size_small_revenue, pead_revenue_raw;
`low_accruals`: size_small_assets, accruals_frozen, accruals_raw;
`quality_value_blend`: s2p_top_quintile, size_smallcap, size_small_assets,
quality_value_frozen, gross_profitability, gross_profit_raw;
`value_momentum_filter`: s2p_top_quintile, size_smallcap, momentum_12_1;
`low_volatility`: none (price-only, not a ratio).

### Protocol additions

* Pass 1 for the seven R3 controls, then **pass 2 for all 20** against the
  complete ledger (R2 item 4), so every final `results/<id>.json` carries
  N = 20 and the same trial-Sharpe set. Total candidate-role rows after
  pass 2: 13 + 7 + 20 = 40 (budget 40; any error re-run would exceed it and
  is reported as such).
* Every `ok` ledger row keeps an immutable artifact
  `results/trials/<id>__<timestamp>.json` (`archive_trials.py`, run before
  and after pass 2); REPORT.md cites the artifact for every row.
* `stress_d07.py` identifies a forced exit as a SELL dated at the name's
  terminal print AND filled at that print's close (regular fills are at the
  open); it refuses to write unless its event count and value reconcile
  exactly with `Backtester`'s own `panel_forced_exits` /
  `panel_forced_exit_value` diagnostics for every window.
* `check_artifacts.py` asserts before REPORT.md is believed: every candidate
  has a final JSON at the common N, every ok row has an artifact, the
  champion equals `make_report.choose_champion`, the report's verdict equals
  the champion JSON's, the report is under 3000 words.
