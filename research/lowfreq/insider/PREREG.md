# Pre-registration — low-frequency INSIDER family (C04; design record D11, D14)

Written 2026-09-29 BEFORE any candidate row exists in `trials.jsonl`. A first
draft was checkpointed in commit `38954c8` (round 1); this FINAL version is
the one in the orchestrator's checkpoint commit of round 2 (the first commit
after `38954c8` containing the section "Evaluation protocol" below). No
`evaluate` call of this family is made before that commit exists, so every
ledger row's `git_sha` is that commit or a descendant of it. Nothing below is
edited after results are seen; corrections and post-hoc additions go in dated
addenda under "Addenda" (each counts as a trial) or in `REPORT.md`.

What was looked at before writing this: C03's `DATA.md`, the harness code,
the dev insider database's ROW COUNTS, and the qualifier COUNTS of every rule
on the real dev panel (`calibrate_counts.py`, table below - how many names
qualify per rebalance and where they sit in the market-cap distribution).
No return, equity curve, benchmark comparison or backtest of any insider rule
was computed before this file was finalised. The unit tests run on a
synthetic panel only.

## Common design (fixed for every hypothesis, not fitted)

* **Data.** Universe: point-in-time top-500 by dollar volume
  (`load_equity_panel`, delisted names included, D11). Events: C03's
  `trader.data.insider.load_insider_events` (effective Form 4 filings, D14
  amendment rule), codes P (open-market purchase) and S (open-market sale),
  from 2014-01-01 (the data's start; the routine classification needs
  history before the panel).
* **Timing (D14).** An event enters a strategy's daily frames on the first
  panel session whose date is >= the loader's `usable_from` (the first
  session strictly after the filing date), never earlier; the harness marks
  at that close and fills at the NEXT session's open. Every derived quantity
  at session t reads only events usable at or before t: the routine
  classification of a purchase consults only prior purchases whose own
  `usable_from` is at or before the classified purchase's `usable_from` (a
  late or backdated filing cannot make an earlier purchase routine - the
  round-1 leak, now pinned by
  `test_a_late_filed_prior_purchase_cannot_make_an_earlier_purchase_routine`).
  `insider_signals.prepare_events` re-asserts `usable_from > filing_date`.
* **Insider scope.** Officers or directors only. Rows whose only
  relationship is ten-percent owner (37 % of P rows in the dev DB) are
  excluded from every signal. Joint filings (44 % of P rows) count as ONE
  filing and ONE buyer (the filing's lowest `owner_cik`); dollar values are
  weighted `1/n_owners`; a missing or mis-keyed price (> $100,000/share or
  > $10 B) contributes zero dollars but the filing still counts.
* **Construction - the D11 overlay.** On the first NYSE session of each
  month the book is the equal-weight basket of the COVERED universe
  (member AND priced; coverage = membership, published to the benchmark)
  plus a fixed overlay of **`W_NAME` = 2 %** on each covered name that
  QUALIFIES under the hypothesis's rule on the decision bar. The overlay is
  capped at **`SLEEVE_MAX` = 60 %** of the book (more than 30 qualifiers
  share 60 % equally). Weights: `(1 - S)/N` on every covered name plus
  `S/K` on each of the `K` qualifiers, `S = min(0.60, 0.02 K)`; gross = 1.0,
  long-only, N >= 30 always (D11's "fewer than 30 signal names" clause is
  met: the unfilled weight is the benchmark basket). Fewer than
  **5 qualifiers** => the plain basket that month. Monthly because the
  benchmark is monthly and the insider-return literature forms monthly
  portfolios; the active return is then exactly the sleeve's excess over
  equal weight net of the overlay's own costs.
* **Rules are COUNTS.** The baseline C0 qualifies a name on >= 1 purchase
  FILING usable in the trailing **63 sessions** (~3 months, the horizon of
  the documented drift). Every refined rule is the same count after ONE
  filter, so "refined minus C0" is exactly what the filter contributed. Only
  H5 (which nets two flows) and H6 (dollars relative to firm size) use
  dollar values. No dollar floor anywhere (a floor would confound count with
  value).
* **Decisive benchmark.** `eq_ew_pit` = `EqEwCovered` on the published
  coverage (monthly equal weight of the PIT members), identical path and
  costs (5 bps + $0.005/share; 3x for the stress). Walk-forward per D11:
  train 1095 days / test 365 days rolling over 2016-08-01..2024-06-28, 400
  calendar days of warm-up loaded before every window.
* **No parameter grids.** Every entry is one pre-registered cell
  (`params: {}`): no cell is selected on a train window, gate (g) is n/a,
  and the trial count is exactly the number of registered candidate-role
  entries (16). The free choices (63 / 126 sessions, 2 %, 60 %, 5, 2 buyers,
  10 % holding, NPR 0.5, 0.2 bp) are stated here and not varied.
* **Implementation.** `strategies/lowfreq_insider_base.py` (overlay,
  count rule, market cap, size-matched control),
  `trader/research/lowfreq/insider_signals.py` (event provider, daily
  frames, routine flags, distinct buyers; all backward-looking), one module
  per rule in `strategies/lowfreq_insider_*.py`, the eight controls in
  `strategies/lowfreq_insider_size_matched.py`, tests in
  `tests/research/test_insider_*.py`.

## Hypotheses (registered in `candidates.yaml` in this order)

| id | H | exact rule (qualify on the decision bar) | fixed parameters | economic rationale | expected sign |
|---|---|---|---|---|---|
| `ins_purchase_count` | C0 (baseline; the purchase-count confounder) | >= 1 officer/director open-market purchase filing usable in the trailing 63 sessions | LOOKBACK 63, MIN_COUNT 1 | Insiders buy with their own money only when they expect a rise (Seyhun 1986; Lakonishok & Lee 2001). The fact of buying alone; every refinement must beat it or the refinement added nothing. | + active |
| `ins_opportunistic` | H1 | C0 counting only filings by insiders NOT routine for that issuer; routine = same (owner, issuer) bought in the same calendar month (trade month) in each of the 3 prior years, priors already usable | ROUTINE_YEARS 3 | Cohen, Malloy & Pomorski (2012): calendar-driven trades carry no information. Calibration shows routine PURCHASES are rare here (H1's qualifier count equals C0's at the median), so H1 is expected to be nearly indistinguishable from C0. | + active, >= C0 |
| `ins_cluster` | H2 | >= 2 distinct officer/director buyers (joint filing = one) whose purchases became usable within 63 sessions | BUYER_WINDOW 63, MIN_BUYERS 2 | Independent insiders agreeing is stronger evidence than one insider's possibly liquidity-driven trade (Alldredge & Blank 2019). Threshold 2 because a top-500 universe sees few clusters. | + active, > C0 |
| `ins_conviction` | H3 | >= 1 purchase filing with shares / shares_after >= 0.10 (the trade was >= 10 % of the post-trade holding) in 63 sessions | CONVICTION_MIN 0.10 | The cost of being wrong scales with the trade relative to the insider's wealth in the firm; holding-relative size measures conviction where dollar size measures wealth. | + active, > C0 |
| `ins_officer` | H4a | >= 1 purchase filing by an OFFICER (officer-directors included) in 63 sessions | - | Officers hold the day-to-day private information (Seyhun 1986 hierarchy). | + active, > H4b |
| `ins_director` | H4b (contrast) | >= 1 purchase filing by a director who is NOT an officer in 63 sessions | - | Directors trade on less; if H4b matches H4a the split carries nothing. Eligible on its own merits. | + active, < H4a |
| `ins_net_buy_ratio` | H5 | >= 1 purchase filing in 126 sessions AND NPR = (P$ - S$)/(P$ + S$) >= 0.5 over those sessions | LOOKBACK 126, NPR_MIN 0.5 | Lakonishok & Lee (2001): net buying predicts returns; sales alone are noise, so netting removes names insiders are also selling. | + active, > C0 |
| `ins_value_to_mcap` | H6 | trailing-63-session purchase value / market cap (close x as-of diluted shares) >= 0.2 bp; no market cap => cannot qualify | VALUE_TO_MCAP_MIN 2e-5 | A purchase that is large relative to the firm moves the insider's wealth materially into it. The floor is the calibration MEDIAN of the ratio among purchasers (1.7e-5), so about half of C0's qualifiers qualify; at 1 bp only 8 names/month qualified. | + active, > C0 |
| `ins_size_matched_<rule>` (8 entries) | D1-D8 (size controls, never champions) | the mirrored rule's qualifiers each replaced by the covered NON-qualifier nearest in MARKET-CAP rank (ties by ticker, each match used once); qualifiers without a market cap are skipped | - | Insider purchases cluster in smaller names; each control earns what the SIZE of its rule's names earns without the buying. | ~ 0; if ~ its rule, the rule is size |

Distinct information sets: C0 = the fact of buying; H1 = the buyer's
history; H2 = the number of buyers; H3 = the trade relative to the buyer's
stake; H4 = the buyer's role; H5 = purchases net of sales; H6 = dollars
relative to firm size. D1-D8 are controls.

## Calibration (counts only, real dev panel, `results/_calibration_counts.json`)

94 monthly rebalances 2016-09..2024-06, median 494.5 covered names; market
cap is known for 58 % of covered name-days (names with EDGAR fundamentals).
K = qualifiers per rebalance; mcap pct = median market-cap percentile of the
qualifiers within the covered set (0.5 = no size tilt).

| rule | K p25 / median / p75 | months with K < 5 | mcap pct |
|---|---|---|---|
| `ins_purchase_count` | 44 / 56 / 65 | 1 | 0.50 |
| `ins_opportunistic` | 44 / 56 / 65 | 1 | 0.50 |
| `ins_cluster` | 11 / 14 / 19 | 3 | 0.42 |
| `ins_conviction` | 29 / 37 / 46 | 1 | 0.51 |
| `ins_officer` | 12 / 19 / 23 | 2 | 0.44 |
| `ins_director` | 35 / 44 / 51 | 1 | 0.51 |
| `ins_net_buy_ratio` | 19 / 24 / 28 | 1 | 0.44 |
| `ins_value_to_mcap` at 1 bp (rejected floor) | 5 / 8 / 13 | 16 | 0.17 |
| `ins_value_to_mcap` at 0.2 bp (registered) | 13 / 17 / 23 | 1 | 0.26 |

Value/market-cap ratio among purchasers: q25 3.8e-6, median 1.7e-5, q75
9.8e-5, q90 1.3e-3 (n = 6,048 name-rebalances) - hence the 2e-5 floor.
Rules other than H6 therefore hold the 60 % sleeve most months (K > 30) or
about 0.02 K otherwise.

## Decomposition against confounders (pre-declared, all evaluated)

* **Purchase count alone.** C0 IS the purchase-count signal. A refined rule
  is credited only if its OOS active annualised return exceeds C0's.
* **Size.** Every rule has its own pre-registered size control (D1-D8),
  evaluated with the rules. A rule is credited only if its OOS active return
  exceeds its control's. No addendum is needed, so eligibility is fixed by
  the registry alone.
* Also reported (diagnostics, not gates): the sleeve's median K per
  rebalance, months on the plain basket, the qualifiers' market-cap
  percentile.

## Evaluation protocol (order-independent verdicts)

The harness deflates DSR by the trials ledgered when a run starts, so a
first pass gives early runs a smaller trial count than late ones. Therefore:
**pass 1** evaluates all 16 candidates in registry order (walk-forward,
`uv run python -m trader.research.lowfreq evaluate insider/<id>`); **pass 2**
re-runs every candidate once all 16 are ledgered, so every final
`results/<id>.json` carries the full trial count N = 16 in its DSR and
verdict. The ledger keeps both passes (append-only; the second pass adds no
new distinct entry hash). REPORT.md quotes pass-2 files only, and the
verdict it reports for the champion is the one in the champion's pass-2 JSON.
Error re-runs are ledgered and do not add hashes.

## Champion-selection rule (written before results)

1. Eligible: the eight rules (C0, H1-H6); controls never.
2. Guards, from pass-2 OOS active annualised returns: a refined rule
   (H1-H6) is eligible only if it exceeds BOTH `ins_purchase_count` AND its
   own size control; `ins_purchase_count` is eligible only if it exceeds
   `ins_size_matched_count`.
3. Rank eligible rules by harness verdict (PASS > PROMISING > FAIL), then
   by OOS active-return Newey-West t (higher first), then by lower
   full-period turnover/yr. The top-ranked entry is the family champion.
4. If the guards leave no rule eligible, the champion is the rule with the
   highest OOS active Newey-West t, and REPORT.md states which guard it
   failed (the null: the insider rules added nothing over the purchase
   count and/or over size).
5. The champion's verdict is reported exactly as its pass-2
   `results/<id>.json` says. If it is not PASS, REPORT.md states the null.
6. The held-out period (after 2024-06-30) is evaluated by the orchestrator
   only, with the fixed-cell command REPORT.md will name (D11).

## Stopping rule

Stop after the 16 registered evaluations (two passes each) plus any error
re-runs, unless the champion PASSes earlier. Trial count 16 of the D11
budget of 30. No new hypothesis is added after seeing results without a
dated addendum below explaining what was seen first; any such addition is a
new trial and counts.

## Stress and diagnostics reported for the champion

D07 stopped-name block and a terminal-loss stress (forced exits re-marked to
zero), 3x costs (harness gate f), turnover and capacity (median $ADV of
holdings), per-year active returns, DSR with the family trial count.

## Addenda

(none yet)
