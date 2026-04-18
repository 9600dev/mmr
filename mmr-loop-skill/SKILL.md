---
name: mmr-loop-skill
description: Autonomous trading loop for the MMR platform. Continuously monitors portfolio, scans for opportunities, creates trade proposals, and manages risk. Requires the 'mmr' skill to be loaded first. Use when the user asks to start monitoring, trading autonomously, or running a trading loop.
metadata:
  author: mmr
  version: "1.0"
  dependencies: mmr-skill
---

# MMR Trading Loop

This skill turns the MMR trading platform into an autonomous trading agent. It implements a phased state machine that continuously monitors your portfolio, scans for opportunities, and creates trade proposals for your review.

**Important**: This skill requires the `mmr` skill to be loaded first. Load both:
```
await load_skill("mmr", "all")
await load_skill("mmr-loop", "all")
```

## How It Works

The loop runs a repeating cycle with four phases:

```
PRE-FLIGHT → MONITOR → ANALYZE → PROPOSE → DIGEST → (sleep) → PRE-FLIGHT → ...
```

**PRE-FLIGHT**: Checks `status()` for trader_service connectivity and IB Gateway upstream connection. If IB Gateway can't reach IBKR servers, the cycle skips directly to DIGEST — no wasted API calls that would timeout.

**MONITOR**: Quick health check. Calls `portfolio_snapshot()` and `portfolio_diff()` (~500 tokens). If nothing moved (all positions unchanged), skips directly to DIGEST — no wasted API calls or context.

**ANALYZE**: Only runs when MONITOR finds something interesting (positions moved, market open, new cycle). Runs `portfolio_risk()` for warnings, scans with `ideas()` using rotating presets (momentum → mean-reversion → breakout → volatile → gap-down), checks news for held positions.

**PROPOSE**: Creates trade proposals via `propose()` with auto-sizing (ATR-adjusted), group tagging, confidence scores, and reasoning. Never auto-executes — proposals wait for user approval.

**DIGEST**: Writes cycle summary to memory (persists across compactions), compacts context with `compact("drop-helpers-results")`, sleeps until next cycle.

## Starting the Loop

To start the trading loop, register hooks and initialize state:

```python
result = await load_skill("mmr", "all")
result = await load_skill("mmr-loop", "all")

# Initialize the loop engine
await TradingLoop.start()
```

The loop will run until you say "stop" or call `await TradingLoop.stop()`.

## User Interaction

While the loop is running, you can interrupt at any time:
- **"stop"** or **"pause"** — stops the loop
- **"approve 42"** — approve a pending proposal
- **"reject 42"** — reject a proposal
- **"status"** — get current loop state and cycle count
- **"skip"** — skip to next cycle immediately
- Any other question — the loop pauses, answers, then resumes

## Configuration

The loop reads configuration from the `TradingLoop.config` dict. Override before starting:

```python
TradingLoop.config["scan_interval_seconds"] = 300  # 5 minutes between cycles
TradingLoop.config["scan_presets"] = ["momentum", "mean-reversion"]
TradingLoop.config["max_proposals_per_cycle"] = 1
TradingLoop.config["auto_approve"] = False  # NEVER set to True without understanding the risks
```

See [references/LOOP_CONFIG.md](references/LOOP_CONFIG.md) for full configuration reference.

## Safety Boundaries

1. **Proposals, not trades**: The loop creates proposals but NEVER auto-executes. You approve or reject.
2. **Position limits**: Respects `max_positions` from position_sizing.yaml. Stops proposing at the limit.
3. **Group budgets**: Checks group allocation budgets before proposing. Over-budget = warning, not block.
4. **Risk gate**: All approved trades still pass through the risk gate (max leverage, daily loss limit, etc.).
5. **Context management**: Aggressive compaction keeps context at ~12K tokens. Can run indefinitely.

## What Gets Written to Memory

Each cycle writes a summary entry:
```
Key: trading_loop_cycle_{N}
Summary: "Cycle N — Portfolio $67K (+$20 daily). Scanned momentum. Created 1 proposal (BHP BUY $3,600). HHI 0.064, no warnings."
```

You can review cycle history anytime:
```python
keys = read_memory_keys()
for k in keys:
    if k["key"].startswith("trading_loop_cycle_"):
        print(f'{k["key"]}: {k["summary"]}')
```

## Position Tracking

The loop has a built-in position monitor that checks tracked positions between cycles. When a position hits its stop-loss or take-profit threshold, the LLM is alerted. Between alerts, monitoring happens silently in the hook — no LLM turns are wasted.

```python
# Track positions after entering trades
await TradingLoop.track_position("BHP", "LONG", entry=50.15, qty=350, stop_pct=-1.5, add_pct=2.0)
await TradingLoop.track_position("PLS", "SHORT", entry=4.58, qty=3800)

# Or use module-level convenience functions
await track_position("BHP", "LONG", 50.15, 350)

# Check what's being tracked
status = await TradingLoop.status()
# status["tracked_symbols"] → ["BHP", "PLS"]

# Stop tracking
await TradingLoop.untrack_position("BHP")
await TradingLoop.untrack_all()  # clear all
```

The monitor uses `snapshots_batch()` for efficiency (~4s for all symbols vs ~4s per symbol). Configure timing with `monitor_interval_seconds` (default 180s). Default thresholds: `monitor_stop_pct=-1.5`, `monitor_add_pct=2.0`.

## Quick Reference

| Function | Description |
|----------|-------------|
| `await TradingLoop.start()` | Start the loop (registers hooks) |
| `await TradingLoop.stop()` | Stop the loop (unregisters hooks) |
| `await TradingLoop.status()` | Get running state, cycle count, tracked positions |
| `await TradingLoop.track_position(sym, side, entry, qty, ...)` | Add position to monitor watchlist |
| `await TradingLoop.untrack_position(sym)` | Remove from watchlist |
| `await TradingLoop.untrack_all()` | Clear all tracked positions |
| `await start_trading_loop(**overrides)` | Start with config overrides in one call |
| `await stop_trading_loop()` | Alias for `TradingLoop.stop()` |
| `TradingLoop.config["key"] = value` | Change config before or during loop |

## Architecture Notes

- Hook sleeps internally via `asyncio.sleep()` — no wasted LLM turns between cycles
- Timer-based gating: pure elapsed time, no iteration-modulo tricks
- Position monitor runs between full cycles, only alerts the LLM when thresholds are breached
- Uses `snapshots_batch()` for efficient multi-symbol price checks (~4s total)
- Uses `delegate_task()` for parallel data gathering (portfolio + risk + scan simultaneously)
- Context stays at ~12K tokens after compaction across unlimited cycles
- Rotating scan presets ensure different patterns are checked each cycle
