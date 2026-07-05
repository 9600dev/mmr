# MMR Operational State

Living snapshot of the **deployed/running** state (config, strategies, data, infra)
and the reasoning behind it. Distinct from `AUDIT_ROADMAP.md` (code backlog).
Update the date + relevant sections when the running config changes.

**Last updated: 2026-07-05 (Sun) — paper trading, account `DUM422056`.**

---

## Live strategy config (armed for Monday 2026-07-06 US open)

**5 strategies armed** (RUNNING + `auto_execute=True`), all individually
backtested-positive over 1yr of 1-min data. D2 persists enabled-state across
restarts.

| Strategy | Sym | conId | Class | 1yr return | PF | Notes |
|---|---|---|---|---|---|---|
| orb_googl | GOOGL | 208813719 | OpeningRangeBreakout | +13.9% | 2.45 | strongest |
| orb_pltr | PLTR | 444857009 | OpeningRangeBreakout | +7.5% | 1.44 | |
| orb_wds | WDS | 564155292 | OpeningRangeBreakout | +7.1% | 1.68 | ASX (Sydney session) |
| vwap_reclaim_cat | CAT | 5437 | VwapReclaim | +4.7% | 1.44 | 55% win, −2.3% dd |
| orb_bhp | BHP | 4036812 | OpeningRangeBreakout | +4.7% | 1.26 | ASX (Sydney session) |

**ASX strategies** carry `params: {SESSION_TZ: Australia/Sydney, RTH_OPEN_MIN: 600,
RTH_CLOSE_MIN: 960}`. US strategies use the class defaults (09:30 ET). The live
runtime reads session config from `self.params` (see [[live-bar-strategies-fix]]).

**Disabled** (validated marginal/losing — do NOT re-arm without new evidence):
orb_cba, orb_rio, orb_fmg, orb_csl (marginal ASX), orb_gld (losing US −1.1% PF 0.81),
orb_xlk (US +3.3% but −27% max drawdown — dropped as too volatile).

Risk guardrails on the whole book: sizing ~$1,888/trade (FX-correct, ATR-adj),
max 20 positions, 10%/$25k per position, **$2,000 daily-loss limit (shared)**.
PLS position (3800sh, AUD) has a protective SELL STP @ 4.75.

---

## Backtest findings (2026-07-04/05, 1yr 1-min)

- **ORB on ASX is marginal universe-wide.** Top-20 leaderboard: 11/20 positive,
  median PF 0.89, negative median per-trade expectancy. Only BHP (PF 1.26) and
  WDS (PF 1.68) have a genuine per-name edge. **Param sweep** (RANGE_MINUTES ×
  VOLUME_MULT) confirmed the DEFAULTS (30 / 1.5) are already the best combo and
  still only marginal (medPF 1.075, mean return −0.58%) — **tuning does not
  rescue ORB on ASX**. Treat ORB-ASX as a per-name edge (BHP, WDS), not a
  universe strategy.
- **US ORB:** GOOGL strong (PF 2.45), PLTR/XLK positive, GLD losing.
- **VWAP:** works on CAT (PF 1.44, +18.9bps); loses on every other US name tried
  → correctly deployed only on CAT.
- **Metric caveat:** `expectancy_bps` repeatedly disagrees with return/PF (e.g.
  positive return + negative expectancy on fixed sizing) — treat return + PF as
  the reliable metrics until the expectancy_bps calc is reconciled. (Worth an
  AUDIT_ROADMAP item.)
- **Not done:** statistical-confidence tests (PSR/t-test/bootstrap) — the script
  hung on MC/bootstrap over large trade sets after ~3 of 6 survivors. Rerun with
  iteration caps + per-strategy timeouts if wanted. No results saved; nothing
  relies on partial numbers.

---

## Data coverage

- **ASX 1-min: all 53 universe symbols, a full year** (2025-07-06 → 2026-07-02).
  ~5.1M ASX 1-min bars. Source: IB (only ASX intraday source).
- **US 1-min:** deep history for the deployed names (GLD/GOOGL/XLK/PLTR/CAT).
- History DB (`mmr_history.duckdb`) ~23.5M 1-min bars total; ~1.7 GB.
- ASX daily was tz-migrated to Sydney-midnight keys + deduped (Cluster B).

---

## Infrastructure

- **DB backups:** `mmr data backup` (clean DuckDB snapshot via in-mem ATTACH +
  COPY FROM DATABASE — safe on a live DB). Nightly pycron `db_backup` @ 22:17
  (keep 7) to the host-mounted `~/.local/share/mmr/backups/`. `docker.sh -B` uses
  the no-downtime path; `docker.sh up` seeds an empty volume from `backups/latest`.
  Milestone backup `before_monday` on host.
- **Restart policy:** `unless-stopped` (live + compose) — survives host reboot /
  Docker restart. Changed from `on-failure` on 2026-07-04.
- **Container** must stay running for market opens + nightly backups + armed
  strategies. Paper mode; live creds preserved as .env comments (see
  [[live_account_setup]]).

---

## Monday 2026-07-06 plan

- US opens 06:30 PDT / 09:30 ET. ASX reopens Sun ~17:00 PDT (Mon 10:00 Sydney).
- 5 strategies auto-execute from the open (live-watching if the container stays
  up). Best-effort cron for a monitoring session; reliable path = prompt for a
  live watch before the open.
- Monitor script: `scratchpad/monitor.sh` (market-aware; may be gone if the
  scratchpad is cleared — regenerate if needed).
- The live path was just rebuilt this session (on_prices + tick→bar resampling +
  priming); Monday is the first from-the-open clean test.

---

## Open items / follow-ups (offline)

- Cluster G (AUDIT_ROADMAP): G1 mass-enable RPC timeout, G2 IB farm-status log noise.
- `expectancy_bps` metric inconsistency (see above).
- Statistical-confidence script needs timeouts/caps before rerun.
- ORB-ASX: consider a proper train/test split before trusting BHP/WDS edges live.
