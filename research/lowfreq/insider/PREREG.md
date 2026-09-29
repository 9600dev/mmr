# Pre-registration — low-frequency INSIDER family (C04; design record D11, D14)

Written 2026-09-29 BEFORE any candidate row exists in `trials.jsonl`. The
orchestrator's checkpoint commit of this file precedes the first `evaluate`
call of this family by construction: no candidate-role evaluation is run
until this file is in git history. Nothing below is edited after results are
seen; corrections and post-hoc additions go in dated addenda under
"Addenda" (each counts as a trial) or in `REPORT.md`.

What was looked at before writing this: C03's `DATA.md` (event counts,
coverage per member-month, spot checks), the harness code, and the dev
insider database's ROW COUNTS (P/S rows, share of joint filings, share of
ten-percent-owner-only rows). No return, price series or backtest of any
insider rule was computed before this file was written. A development
run of the code on a SYNTHETIC panel (the unit tests) is the only execution
that preceded it.

## Common design (fixed for every hypothesis, not fitted)

* **Data.** Universe: point-in-time top-500 by dollar volume
  (`load_equity_panel`, delisted names included, D11). Events: C03's
  `trader.data.insider.load_insider_events` (effective Form 4 filings, D14
  amendment rule), codes P (open-market purchase) and S (open-market sale),
  from 2014-01-01 (the data's start; the routine classification needs
  history before the panel).
* **Timing (D14).** An event enters a strategy's daily frames on the first
  panel session whose date is >= the loader's `usable_from` (the first
  session strictly after the filing date), never earlier; the harness fills
  at the NEXT session's open. `insider_signals.prepare_events` re-asserts
  `usable_from > filing_date` on every row and refuses otherwise.
* **Insider scope.** Officers or directors only. Rows whose only
  relationship is ten-percent owner (funds, holding companies; 37% of P
  rows in the dev DB) are excluded from every signal. Joint filings (44% of
  P rows carry `n_owners > 1`) weight dollar values by `1/n_owners` and count
  as ONE buyer (the filing's lowest `owner_cik`). Rows with a missing or
  mis-keyed price (> $100,000/share or > $10 B) contribute zero dollar value.
* **Construction — the D11 overlay.** On the first NYSE session of each
  month the book is the equal-weight basket of the COVERED universe
  (member AND priced; coverage = membership, published to the benchmark)
  plus a fixed overlay of **`W_NAME` = 2 %** of the book on each covered name
  that QUALIFIES under the hypothesis's rule on the decision bar. The overlay
  is capped at **`SLEEVE_MAX` = 60 %** of the book (more than 30 qualifiers
  share 60 % equally). Weights: `(1 - S)/N` on every covered name plus `S/K`
  on each of the `K` qualifiers, `S = min(0.60, 0.02 K)`; gross = 1.0,
  long-only, N >= 30 always (D11's "fewer than 30 signal names" clause is
  met: the unfilled weight is the benchmark basket). Fewer than
  **5 qualifiers** => the book is the plain basket that month. Monthly
  because the benchmark is monthly and the insider-return literature forms
  monthly portfolios; the active return is then exactly the sleeve's excess
  over equal weight net of the overlay's own costs.
* **Windows.** A purchase stays "recent" for **63 sessions** (~3 months,
  `LOOKBACK`), the horizon over which the literature finds the drift; the
  net-purchase ratio uses 126 sessions because it nets two flows. The
  dollar floor is **$25,000** of 1/n_owners-weighted value, enough to exclude
  token purchases without excluding a director's ordinary buy.
* **Decisive benchmark.** `eq_ew_pit` = `EqEwCovered` on the published
  coverage (monthly equal weight of the PIT members), identical path and
  costs (5 bps + $0.005/share; 3x stress). Walk-forward per D11: train
  1095 days / test 365 days rolling over 2016-08-01..2024-06-28, 400 calendar
  days of warm-up loaded before every window.
* **No parameter grids.** Every entry is one pre-registered cell
  (`params: {}`), so no cell is ever selected on a train window, gate (g) is
  n/a, and the trial count is exactly the number of registered candidates.
  Reason: with ~5 OOS years the family has no power to spare for selection,
  and each grid cell multiplies the lookahead-probe cost. The free choices
  (63 sessions, $25k, 2 %, 60 %, 5, thresholds below) are stated here and
  not varied.
* **Implementation.** `strategies/lowfreq_insider_base.py::InsiderOverlay`
  (overlay arithmetic, coverage), `trader/research/lowfreq/insider_signals.py`
  (event provider, daily frames, routine classification, distinct-buyer
  counts; all backward-looking), one module per candidate in
  `strategies/lowfreq_insider_*.py`, tests in `tests/research/test_insider_*.py`.

## Hypotheses (registered in `candidates.yaml` in this order)

| id | H | exact rule (qualify on the decision bar) | parameters (single cell) | economic rationale | expected sign |
|---|---|---|---|---|---|
| `ins_any_purchase` | C0 (baseline / purchase-count confounder) | trailing-63-session sum of officer/director P value >= $25k | LOOKBACK 63, MIN_VALUE 25k | Insiders buy with their own money only when they expect a rise (Seyhun 1986; Lakonishok & Lee 2001). The plainest form of the information; every refinement must beat it or the refinement added nothing. | + active |
| `ins_opportunistic` | H1 | as C0 counting only purchases by insiders NOT routine for that issuer; routine = same (owner, issuer) bought in the same calendar month in each of the 3 prior years (by trade month; prior years only) | LOOKBACK 63, MIN_VALUE 25k, ROUTINE_YEARS 3 | Cohen, Malloy & Pomorski (2012): routine (calendar-driven) trades carry no information; removing them roughly doubles the predictive content. | + active, > C0 |
| `ins_cluster` | H2 | >= 2 distinct officer/director buyers (joint filing = one) within 63 sessions AND C0's value floor | BUYER_WINDOW 63, MIN_BUYERS 2, MIN_VALUE 25k | Independent insiders agreeing is stronger evidence than one insider's possibly liquidity-driven trade (Alldredge & Blank 2019). Threshold 2 not 3 because a top-500 universe sees few clusters. | + active, > C0 |
| `ins_conviction` | H3 | trailing-63-session value of purchases with shares / shares_after >= 0.10 (the trade was >= 10 % of the post-trade holding) >= $25k | LOOKBACK 63, CONVICTION_MIN 0.10, MIN_VALUE 25k | The cost of being wrong scales with the trade relative to the insider's wealth in the firm; holding-relative size measures conviction where dollar size measures wealth. | + active, > C0 |
| `ins_officer` | H4a | trailing-63-session value of purchases by OFFICERS (officer-directors included) >= $25k | LOOKBACK 63, MIN_VALUE 25k | Officers hold the day-to-day private information (Seyhun 1986 hierarchy). | + active, > H4b |
| `ins_director` | H4b (contrast) | trailing-63-session value of purchases by directors who are NOT officers >= $25k | LOOKBACK 63, MIN_VALUE 25k | Directors trade on less; if H4b matches H4a the split carries nothing. Eligible on its own merits. | + active, < H4a |
| `ins_net_buy_ratio` | H5 | over 126 sessions NPR = (P$ - S$)/(P$ + S$) >= 0.5 AND P$ >= $25k | LOOKBACK 126, NPR_MIN 0.5, MIN_VALUE 25k | Lakonishok & Lee (2001): net buying predicts returns; sales alone are noise (scheduled, option-related), so netting removes names insiders are also selling. | + active |
| `ins_size_matched_any` | D1 (size control, never a champion) | C0's qualifiers each replaced by the covered non-qualifier nearest in trailing-63-session median dollar-volume rank (ties by ticker, each match used once) | ADV_WINDOW 63 | Insider purchases cluster in smaller, cheaper names; the control earns whatever the SIZE of the bought names earns without the buying. | ~ 0; if ~ C0, C0 is size |

Distinct information sets: C0 = the fact of buying; H1 = the buyer's
history; H2 = the number of buyers; H3 = the trade relative to the buyer's
stake; H4 = the buyer's role; H5 = purchases net of sales. D1 is a control.

## Decomposition against confounders (pre-declared)

* **Purchase count alone.** C0 IS the purchase-count signal. A refined rule
  (H1–H5) is credited only if its OOS active annualised return exceeds C0's.
* **Size.** C0 is credited only if its OOS active return exceeds D1's. If a
  refined rule becomes champion, ONE dated addendum trial
  `ins_size_matched_<champion>` (the champion's rule with the same
  ADV-rank replacement) is registered and evaluated; it is pre-declared
  here so it is not post-hoc, and it counts in the trial count. The
  champion is credited only if it exceeds that control too.
* Also reported (diagnostics, not gates): median dollar-volume rank of the
  sleeve names vs the universe; the sleeve's median K per rebalance; the
  share of months the book was the plain basket.

## Champion-selection rule (written before results)

1. Eligible: candidate-role entries above except the control `ins_size_matched_any`.
2. Guards: a refined rule (`ins_opportunistic`, `ins_cluster`, `ins_conviction`,
   `ins_officer`, `ins_director`, `ins_net_buy_ratio`) is eligible only if
   its OOS active annualised return exceeds `ins_any_purchase`'s;
   `ins_any_purchase` is eligible only if its OOS active return exceeds
   `ins_size_matched_any`'s.
3. Rank eligible candidates by harness verdict (PASS > PROMISING > FAIL),
   then by OOS active-return Newey-West t (higher first), then by lower
   full-period turnover/yr. The top-ranked entry is the family champion.
4. If the guards leave no one eligible, the champion is the non-control
   candidate with the highest OOS active Newey-West t, and REPORT.md states
   which guard it failed (the null: the insider rules added nothing over the
   purchase count / over size).
5. The champion's verdict is reported exactly as its `results/<id>.json`
   says. If it is not PASS, REPORT.md states the null.
6. The held-out period (after 2024-06-30) is evaluated by the orchestrator
   only, with the fixed-cell command REPORT.md will name (D11).

## Stopping rule

Stop after these eight evaluations, plus the one pre-declared size-matched
champion control and any error re-runs (all ledgered), unless the champion
PASSes earlier. Trial count then <= 9 of the D11 budget of 30. No new
hypothesis is added after seeing results without a dated addendum below
explaining what was seen first; any such addition is a new trial and counts.

## Stress and diagnostics reported for the champion

D07 stopped-name block and a terminal-loss stress (forced exits re-marked to
zero), 3x costs (harness gate f), turnover and capacity (median $ADV of
holdings), per-year active returns, DSR with the family trial count.

## Addenda

(none yet)
