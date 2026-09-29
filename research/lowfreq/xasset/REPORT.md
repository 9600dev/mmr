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

Pass 1: the seven `evaluate` calls in registry order. Snapshots
`results/snapshots/<id>__pass1.json`. Pass 2: the same seven calls with
unchanged entries (same hashes, N stays 7), so every `results/<id>.json`
carries DSR at the final trial count; snapshots `<id>__pass2.json`. The
champion rule was applied to pass-2 files only. The two benchmark rows at
the top of the ledger are the family's own benchmark evaluations (kept
deliberately: they document the benchmark numbers under this family's
500-day warm-up; benchmark rows never count as trials).
