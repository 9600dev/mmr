# MONITORING.md — the live-session operating loop

How an operator (human or Claude) watches MMR trade, what to escalate, and
how to triage. This is the committed replacement for the ad-hoc
`scratchpad/monitor.sh` pattern — the monitors, noise list, and escalation
policy live in the repo, not in a clearable scratchpad. (Pattern borrowed
from horserank's DESIGN.md §7, which caught two real bugs in its first five
live days.)

## Quick start

```bash
# after ./docker.sh -u or start_mmr.sh — assert the stack can actually trade:
mmr verify                       # non-zero exit on FAIL; --json for scripts

# then keep these open for the session (host or container — same paths):
./scripts/monitor_trading.sh     # signals + executions + strategy crashes
./scripts/monitor_health.sh      # IB connectivity + reconnects + errors

# on-demand health read (the monitors exclude the heartbeat on purpose):
./scripts/last_pulse.sh          # non-zero exit if a pulse is stale/missing
```

Container recreation (`./docker.sh -d && -u`, `-g`) kills exec-based monitor
sessions — **re-arm both monitors immediately after**, then run `mmr verify`.

## Log topology (which file carries what)

Files live in `~/.local/share/mmr/logs/` (bind-mounted — identical from host
and container) and are **per-process session-stamped**:
`strategy_service_<YYYY-MM-DD_HH-MM-SS>.log`. The monitor scripts always
follow the newest session file and switch automatically after a restart.

| Signal | File |
|---|---|
| BUY/SELL signals, strategy crashes, reconcile errors | `strategy_service_<ts>.log` |
| `auto-executor:` OPENING/OPENED/CLOSING/CLOSED/CLOSE FAILED/refused | `strategy_service_<ts>.log` (named `auto_executor` logger) |
| strategy pulse (`pulse strategies=…`) | `strategy_service_<ts>.log` |
| IB connectivity (1100/1102/2110/2157), farm status, reconnects | `trader_service_<ts>.log` (incl. the `ibreactivex` logger) |
| trader pulse (`pulse ib_connected=…`) | `trader_service_<ts>.log` |
| all ERROR+ from any named logger | `errors.log` (plus the service file) |
| ib_async internals (incl. G5's harmless `KeyError: 81` decoder noise) | console + `/tmp/debug.log` only |

## The pulse (why silence is a signal)

A healthy pipeline is **silent at INFO between signals** — which means a
dead pipeline is silent too. The 10.5h gateway outage of 2026-07-05
(AUDIT_ROADMAP G3) produced *no* error lines; the failure signature was
absence. The pulses make liveness positively visible:

- `trader_service` every 30s: `pulse ib_connected=True ib_upstream=True open_orders=0`
- `strategy_service` every 30s (reconcile tick):
  `pulse strategies=5/5 ticks_60s=[208813719:42,…] bar_age_s=[208813719:75,…] auto_exec_open=1`

Reads:
- `ticks_60s` **all zero while a traded market is open** → the feed is dead
  (gateway hang, dropped subscription, pubsub break) even if every flag
  still says connected. This is THE line that would have caught G3.
- `bar_age_s` ≳ 2–3× the strategy's bar size during market hours → bars are
  not forming / not dispatching.
- **No pulse line for >2 intervals** → the service's loop is wedged
  (`last_pulse.sh` exits non-zero on this).

## Escalation policy

**Stay silent on** (known, routine, or self-healing):
- Farm-status transitions during the nightly ~23:59 gateway auto-restart
  (`ibrx farm status:` INFO lines, code 2103/2105/2119 — G2) when they
  resolve within a few minutes.
- Single reconcile-RPC timeouts while trader_service restarts.
- `auto-executor: … skip` decisions (cooldown, no-pyramiding, SELL-while-flat,
  already-executed-bar) — these are correct behavior, logged for audit.
- G5's `KeyError: 81` ib_async decoder traceback after a reconnect (console
  noise only; root-caused, harmless).

**Escalate immediately on**:
- `auto-executor: CLOSE FAILED … position remains OPEN` — an unmanaged
  position; verify against IB (`mmr orders` / `mmr portfolio`) before anything else.
- `auto-executor: OPEN failed` / `open_failed` — a signal that should have
  traded didn't; check the proposal trail (`mmr proposals --all`).
- Any `refused` conId round-trip line — a strategy tried to trade a stale
  identifier.
- `ticks_60s` all zero (or pulses absent) during market hours, persisting
  past one reconcile cycle.
- `ib_upstream=False` outside the nightly restart window, or `mmr verify`'s
  `ib_socket` check failing (live round-trip — trust it over the flags).
- `raised on_prices … disabling it` — a strategy crashed and was disabled;
  it will NOT re-enable itself.
- Watchdog restarts (`~/.local/share/mmr/logs/ib_gateway_watchdog.log`)
  more than once per day.

## Triage order (before concluding anything)

1. **Read the actual traceback** in the session log (not just the monitor line).
2. **Check ground truth in DuckDB**: `auto_exec_positions` / `auto_exec_bar_log`
   (executor's view), `mmr proposals --all` (proposal trail),
   `mmr strategies signals <name>` (event store).
3. **Verify against IB** when money is in question: `mmr orders`, `mmr trades`,
   `mmr portfolio` — the broker is the source of truth, not our tables.
4. Only then restart things. After any restart: re-arm monitors, `mmr verify`.

## Sharp edges

- `docker.sh -s` hot-sync copies code but **running services keep executing
  what they imported at launch** — restart services for a fix to take effect,
  and remember a container recreation reverts anything not rebuilt into the
  image (`docker.sh -b`).
- The CLI suppresses logging by default; a service started before a config
  change keeps its old logging routing until restarted.
- Deploy windows: don't `./docker.sh -d` with an auto-exec position open or
  an order working — check `mmr orders` and the `auto_exec_open=` pulse field
  first.
