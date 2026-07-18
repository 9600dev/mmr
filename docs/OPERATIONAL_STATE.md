# MMR Operational State

Living snapshot of the **deployed/running** state (config, strategies, data, infra)
and the reasoning behind it. Distinct from `AUDIT_ROADMAP.md` (code backlog).
Update the date + relevant sections when the running config changes.

**Last updated: 2026-07-17 (Fri evening) — paper trading, account `DUM422056`. STACK IS UP (healthy) and trading autonomously.**

---

## Live strategy config

> **✅ LIVE AUTO-EXECUTION ERA STARTED 2026-07-17.** First auto-executed
> trade: orb_pltr BUY 3 × PLTR @ $132.83, 09:28 PDT — signal→sized
> proposal→risk gate→fill in 1.3s, strategy-attributed in the event store
> (`mmr strategies pnl`). The no-pyramiding rail refused a second signal at
> 12:56. Position open into the weekend (ORB exits on its SELL signal; no
> time exit). Kill options: `mmr strategies disable <name>` (per-strategy,
> live RPC) or `MMR_AUTO_EXECUTE_DISABLED=1` (env — service restart to
> apply). Real money additionally requires the **double-arm**:
> `trading_mode: live` AND `MMR_AUTO_EXECUTE_LIVE=1` (strict '1'; deployed
> 2026-07-17, closes never gated). The old PLS position is GONE (flat before
> the 2026-07-16 redeploy — likely the GTC stop filled while the stack was
> down; not investigated further).
>
> Monitoring/self-heal layer (2026-07-16/17): 30s pulse lines from both
> services, `mmr verify` / cron-driven `mmr preflight`, committed monitor
> scripts + docs/MONITORING.md, container healthchecks, supervised service
> restarts, stale-bar gate, per-strategy PnL ledger.

**5 strategies armed** (RUNNING + `auto_execute=True`). D2 persists
enabled-state across restarts. **The July-5 numbers in this table were
superseded by the 2026-07-17 re-validation at deployed configs** (see the
re-validation section below — source_run_id + verdicts also live in each
strategy's YAML entry):

| Strategy | Sym | conId | Class | Re-validated (run) | Confidence verdict |
|---|---|---|---|---|---|
| orb_pltr | PLTR | 444857009 | OpeningRangeBreakout | +13.1% PF 2.63 (2901) | **clears the bar** — only clean pass |
| orb_wds | WDS | 564155292 | OpeningRangeBreakout | +9.4% PF 1.89 (2898) | near-pass |
| vwap_reclaim_cat | CAT | 5437 | VwapReclaim | +3.9% PF 1.35 (2900) | marginal |
| orb_googl | GOOGL | 208813719 | OpeningRangeBreakout | +4.8% PF 1.34 (2899) | **fails at deployed 45/1.3** — validated combo is 30/1.5 (run 2666); redeploy decision pending |
| orb_bhp | BHP | 4036812 | OpeningRangeBreakout | +4.7% PF 1.26 (2896) | fails — watch live-vs-backtest |

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
  `~/.local/share/mmr/logs/pycron_cron.log`, status: `curl :7425` — LAN-visible since 2026-07-17).
  A detached instance was also started in the running container so tonight's
  jobs fire without a restart. The pycron web-port bind doubles as a double-start guard (7425 since 2026-07-17; was 8081).
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

## Weekend state (2026-07-17 Fri evening) + Monday 2026-07-20 plan

- **Stack is UP, healthy, and autonomous over the weekend.** Both containers
  `(healthy)`; `mmr verify --expect-running 5` all-PASS post-deploy. Nightly
  cadence runs itself: db_backup 22:17 → data_refresh (US 17:30 wk / ASX
  23:30) → gateway auto-restart 23:59 (watchdog armed; the 07-16 and 07-17
  restarts recovered in 47s/17s, observed end-to-end via the pulse layer) →
  `preflight_us` 06:00 wk / `preflight_asx` 16:30 Sun-Thu writing
  `PREFLIGHT OK|FAIL` to `logs/preflight.log`.
- **First live-trading day (Fri 2026-07-17) recap:** orb_pltr traded (see top
  block); GOOGL/CAT produced no signals (correct-quiet, provable via pulse
  `ticks_60s`/`bar_age_s`); two mid-day IBKR upstream flaps self-healed
  (100s/23s, data maintained throughout). **PLTR position (3 sh) is open into
  the weekend** — exits on the strategy's SELL signal (no time exit).
- **ASX Monday open (Sun 17:00 PDT / Mon 10:00 Sydney)**: BHP + WDS trade
  their first live signals. `preflight_asx` fires 16:30 PDT Sunday; check
  `preflight.log` or run `mmr verify` before the open if watching live.
  Monitors: `scripts/monitor_trading.sh` + `monitor_health.sh` (re-arm after
  any container recreate), `scripts/last_pulse.sh` on demand,
  `mmr strategies pnl` for the ledger.
- **Pending operator decisions:**
  - orb_googl: redeploy at the validated 30/1.5 (run 2666 clears the bar;
    deployed 45/1.3 fails it). One YAML edit; reconcile picks it up in ≤30s
    (params are read at load — restart to be sure).
  - orb_bhp: statistically unestablished edge — keep watching live-vs-backtest
    via `strategies pnl`, or demote.

---

## Open items / follow-ups (offline)

- Cluster G (AUDIT_ROADMAP): ~~G6~~ DONE (live-validated 2026-07-17 — first
  auto-executed trade), ~~G2~~ FIXED (farm codes at INFO), G3 detection layer
  shipped (ping_ib + pulses + verify) but the status-flag truthfulness fix
  itself remains open, G1 mass-enable RPC timeout, G4 open_orders counts
  updates not orders, G5 decoder noise, **G7 (new 2026-07-17): pytest touches
  the live stack when it's up**.
- Backtest persist can still fail silently under concurrent live-service +
  backfill load (`run_id: None`, reproduced 2026-07-17 despite the 32-attempt
  retry) — check `run_id` in every `--json backtest` response.
- Host-side CLI **data/backtest commands hit a different (stale) DuckDB** than
  the stack — the live DBs are in the `mmr_db_data` volume. Always
  `docker exec` for data work; RPC-backed commands are unaffected.
- `expectancy_bps` metric inconsistency (see above).
- ORB-ASX: consider a proper train/test split before trusting BHP/WDS edges
  live (BHP's confidence stats already argue for caution).
