# MMR Operational State

Living snapshot of the **deployed/running** state (config, strategies, data, infra)
and the reasoning behind it. Distinct from `AUDIT_ROADMAP.md` (code backlog).
Update the date + relevant sections when the running config changes.

**Last updated: 2026-07-05 (Sun, midday) — paper trading, account `DUM422056`.**

---

## Live strategy config (armed for Monday 2026-07-06 US open)

> **⚠ STACK IS DOWN + AUTO-EXECUTION IS NOW REAL (2026-07-05 ~23:30 PDT).**
> The missing signal→order path (G6, found earlier tonight when orb_wds's
> 17:39 BUY went nowhere) was **built and paper-validated live** the same
> night: `trader/strategy/auto_executor.py`, routed through the proposal
> pipeline, long-only semantics matching the backtester, time exits honored,
> DuckDB-persisted attribution, 42 tests. See AUDIT_ROADMAP G6 for the
> validation evidence. The docker stack was then wound down for morning
> review (`./docker.sh -d`).
>
> **Restarting the stack now means the 5 armed strategies actually TRADE**
> (paper account) from the next session open — that was never true before.
> Bring it back with `./docker.sh -u`; disarm instantly by exporting
> `MMR_AUTO_EXECUTE_DISABLED=1` into the strategy_service environment or
> `mmr strategies disable <name>`. The PLS position + its GTC stop @4.75
> rest server-side at IB and are unaffected by the stack being down.

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

### Re-validation at deployed configs (2026-07-17, 365d 1-min, statistical confidence NOW COMPUTED)

The armed roster was re-validated at each strategy's EXACT deployed params
(the July-4/5 provenance had drifted: GOOGL was deployed at a never-tested
combo; CAT/WDS validations were never persisted). `source_run_id` + verdicts
now live in the strategies' YAML entries. Bar = PSR ≥ 0.95, positive trade-CI,
PF ≥ 1.5:

| Strategy | Run | Ret | PF | PSR | p | Verdict |
|---|---|---|---|---|---|---|
| orb_pltr | 2901 | +13.1% | 2.63 | 0.957 | 0.009 | **clears** — only clean pass |
| orb_wds | 2898 | +9.4% | 1.89 | 0.900 | 0.002 | near-pass (CI [+15,+66] positive; PSR just under; streak 14>MC95 9) |
| vwap_reclaim_cat | 2900 | +3.9% | 1.35 | 0.893 | 0.087 | marginal (CI touches zero) |
| orb_googl @45/1.3 | 2899 | +4.8% | 1.34 | 0.738 | 0.203 | **FAILS at deployed params** — validated combo is 30/1.5 (run 2666: +14.3%, PSR 0.986, p=0.001); consider redeploying at 30/1.5 |
| orb_bhp | 2896 | +4.7% | 1.26 | 0.757 | 0.289 | **fails** — CI crosses zero, streak 17>MC95 10; weakest armed edge, watch live-vs-backtest closely |

Also: the 6 killed strategies now carry `auto_execute: false` + kill-verdict
descriptions in the YAML (they were one mass-enable away before), and
`paper_only: true` was extended to orb_bhp/orb_wds (was unset — the ASX pair
would have been first to trade real money without the per-strategy gate).

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
  Milestone backup `before_monday` on host; fresh pre-Monday snapshot
  `2026-07-05T17-25-00_clean`.
- **pycron actually runs now (fixed 2026-07-05):** `start_mmr.sh` launches the
  services directly and **never started pycron**, so the `db_backup` and
  `data_refresh_us/asx` cron entries silently never fired (zero nightly backups
  since the job was added). Fix: `start_mmr.sh` now also launches a cron-only
  pycron (`python3 -m pycron.pycron --config ~/.config/mmr/pycron.yaml -s
  db_backup -s data_refresh_us -s data_refresh_asx`, log:
  `~/.local/share/mmr/logs/pycron_cron.log`, status: `curl :8081` in-container).
  A detached instance was also started in the running container so tonight's
  jobs fire without a restart. Port 8081 bind doubles as a double-start guard.
- **IB Gateway watchdog (added 2026-07-05):** the gateway's nightly 23:59
  auto-restart hung on 2026-07-04 (stuck login dialog; API port never opened;
  **10.5 h outage**, invisible to `mmr status` — see AUDIT_ROADMAP G3). Host-side
  launchd agent `com.mmr.ib-gateway-watchdog` now runs
  `scripts/ib_gateway_watchdog.sh` every 5 min: if host port 7497 is closed for
  3 consecutive checks (~15 min) while the container is running, it
  `docker restart`s the gateway (30-min cooldown; never touches a deliberately
  stopped container). Log: `~/.local/share/mmr/logs/ib_gateway_watchdog.log`.
  Restart path validated end-to-end on install.
- **Restart policy:** `unless-stopped` (live + compose) — survives host reboot /
  Docker restart. Changed from `on-failure` on 2026-07-04.
- **Container** must stay running for market opens + nightly backups + armed
  strategies. Paper mode; live creds preserved as .env comments (see
  [[live_account_setup]]).
- **Host port map (correction):** paper API = host **7497** (container 4004),
  live = host 7496 (container 4003), VNC = 5901. The "4001/4002" in older docs
  was stale.

---

## Monday 2026-07-06 plan

- US opens 06:30 PDT / 09:30 ET. ASX reopens Sun ~17:00 PDT (Mon 10:00 Sydney).
- 5 strategies auto-execute from the open (live-watching if the container stays
  up). Best-effort cron for a monitoring session; reliable path = prompt for a
  live watch before the open.
- Monitoring is now a committed artifact (2026-07-16): `scripts/monitor_trading.sh`,
  `scripts/monitor_health.sh`, `scripts/last_pulse.sh`, with the escalation
  policy + triage order in `docs/MONITORING.md`. Post-restart assertions:
  `mmr verify` (non-zero exit on FAIL). The old `scratchpad/monitor.sh`
  pattern is retired.
- The live path was just rebuilt this session (on_prices + tick→bar resampling +
  priming); Monday is the first from-the-open clean test.
- **2026-07-05 incident + recovery:** gateway had been dead since Sat 23:59
  (hung auto-restart). Restarted ~10:18 Sun; trader reconnected, account values
  streaming again, all 5 armed strategies re-subscribed, PLS stop confirmed
  live at IB. Watchdog now guards tonight's 23:59 auto-restart (which happens
  ~7 h *after* ASX's Sunday-evening open and ~6.5 h before US open — if it
  hangs again, the watchdog bounces it by ~00:15).
- **2026-07-05 ASX-open validation:** ticks flowed to all ASX strategies from
  the Sunday 17:00 PDT open, tick→bar resampling + priming worked, and orb_wds
  emitted a correctly-timed BUY signal at 17:39 PDT. No trade resulted — which
  exposed the missing auto-execute path. **G6 was built and paper-validated
  live the same night** (see warning at top + AUDIT_ROADMAP G6). The stack was
  wound down afterwards; nothing trades until it is restarted.
- **Monday morning checklist (after review):** `./docker.sh -u` → wait for
  gateway login (~2 min) → `mmr status` (expect account values streaming) →
  `mmr strategies` (expect 5 × state 3). US open 06:30 PDT: GOOGL/PLTR/CAT
  strategies will trade their first live signals. Watch
  `strategy_service` logs for `auto-executor:` lines; first-session items to
  eyeball: VwapReclaim's EOD flatten (close_by_time time-exit — unit-tested,
  not yet seen live) and proposal audit rows (`mmr proposals --all`).
- Overnight schedule (PDT): 17:00 ASX open → 22:17 db_backup → 23:30
  data_refresh_asx → 23:59 gateway auto-restart (watchdog armed).

---

## Open items / follow-ups (offline)

- Cluster G (AUDIT_ROADMAP): **G6 signals never execute — THE blocker for live
  trading (see top of file)**, G1 mass-enable RPC timeout, G2 IB farm-status log
  noise, **G3 status lies while IB socket is dead (it hid a 10.5 h outage)**,
  G4 status open_orders counts updates not orders, G5 ib_async decoder KeyError
  noise.
- `expectancy_bps` metric inconsistency (see above).
- Statistical-confidence script needs timeouts/caps before rerun.
- ORB-ASX: consider a proper train/test split before trusting BHP/WDS edges live.
