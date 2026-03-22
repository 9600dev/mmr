"""MMR Trading Loop Engine — autonomous trading state machine for LLMVM.

This module provides the TradingLoop class which registers hooks into the
LLMVM runtime to create a continuous MONITOR → ANALYZE → PROPOSE → DIGEST
trading cycle.

Requires the 'mmr' skill to be loaded first (provides MMRHelpers).
"""

import asyncio
import json
import time
from datetime import datetime
from llmvm_lite.llm import User
from llmvm_lite.llmvm_runtime import HookResult


# ---------------------------------------------------------------------------
# Default configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    # Timing
    "scan_interval_seconds": 120,       # seconds between cycles
    "monitor_interval_seconds": 180,    # seconds between position monitor checks

    # Scanning
    "scan_presets": ["momentum", "mean-reversion", "breakout", "volatile", "gap-down"],
    "scan_num": 10,                     # ideas per scan
    "max_proposals_per_cycle": 2,       # max proposals created per cycle

    # Thresholds for triggering analysis
    "position_move_pct": 0.015,         # 1.5% move triggers deeper analysis
    "daily_pnl_alert_pct": 0.02,        # 2% daily portfolio loss triggers alert

    # Risk
    "risk_hhi_warning": 0.15,           # HHI above this triggers warning
    "auto_approve": False,              # NEVER auto-approve by default

    # Context management
    "compact_after_cycle": True,        # compact context after each DIGEST phase

    # Location for IB-path scanning (empty = use Massive/US default)
    "location": "",                     # e.g. "STK.AU.ASX" for ASX stocks
    "tickers": [],                      # explicit tickers for IB path scanning
    "exchange": "",                     # exchange hint for proposals
    "currency": "",                     # currency hint for proposals

    # Position monitor defaults
    "monitor_stop_pct": -1.5,           # default stop-loss % for tracked positions
    "monitor_add_pct": 2.0,             # default add-to-position % threshold
}


class TradingLoop:
    """Autonomous trading loop state machine.

    Usage in LLMVM:
        await load_skill("mmr-skill", "all")
        await load_skill("mmr-loop-skill", "all")
        await TradingLoop.start()
    """

    # Class-level state (persists across helpers blocks)
    config = dict(DEFAULT_CONFIG)
    _state = {
        "running": False,
        "cycle": 0,
        "phase": "IDLE",
        "preset_index": 0,
        "last_cycle_time": 0,
        "last_monitor_time": 0,
        "last_summary": "",
        "proposals_this_cycle": 0,
        "tracked_positions": {},  # symbol -> {"side": "LONG", "entry": 50.15, "qty": 100, "stop_pct": -1.5, "add_pct": 2.0}
    }

    @classmethod
    async def start(cls):
        """Start the trading loop by registering hooks."""
        if cls._state["running"]:
            print("Trading loop is already running.")
            return

        cls._state["running"] = True
        cls._state["cycle"] = 0
        cls._state["phase"] = "WAITING"
        cls._state["last_cycle_time"] = 0  # trigger immediately
        cls._state["last_monitor_time"] = 0
        cls._state["preset_index"] = 0

        # Single hook on both triggers. After an on_complete hook fires,
        # llmvm's hook_context.trigger stays as "on_complete" for subsequent
        # iterations (never reset to "pre_llm_call"). Register on both to
        # ensure the hook fires regardless.
        register_hook(
            "mmr_trading_loop",
            cls._loop_hook,
            "MMR trading loop — schedules cycles, sleeps between them",
            triggers={"pre_llm_call", "on_complete"},
        )

        print(f"Trading loop started. Scan interval: {cls.config['scan_interval_seconds']}s, "
              f"Monitor interval: {cls.config['monitor_interval_seconds']}s")
        print(f"Presets: {', '.join(cls.config['scan_presets'])}")
        if cls._state["tracked_positions"]:
            print(f"Monitoring {len(cls._state['tracked_positions'])} position(s): "
                  f"{', '.join(cls._state['tracked_positions'].keys())}")
        print("Say 'stop' to end the loop. Proposals require your approval.")

    @classmethod
    async def stop(cls):
        """Stop the trading loop and unregister hooks."""
        cls._state["running"] = False
        cls._state["phase"] = "STOPPED"
        try:
            unregister_hook("mmr_trading_loop")
        except Exception:
            pass
        print(f"Trading loop stopped after {cls._state['cycle']} cycles.")

    @classmethod
    async def status(cls):
        """Return current loop status."""
        return {
            "running": cls._state["running"],
            "cycle": cls._state["cycle"],
            "phase": cls._state["phase"],
            "next_preset": cls.config["scan_presets"][
                cls._state["preset_index"] % len(cls.config["scan_presets"])
            ] if cls.config["scan_presets"] else "none",
            "scan_interval": cls.config["scan_interval_seconds"],
            "monitor_interval": cls.config["monitor_interval_seconds"],
            "tracked_positions": len(cls._state["tracked_positions"]),
            "tracked_symbols": list(cls._state["tracked_positions"].keys()),
            "last_summary": cls._state["last_summary"],
        }

    @classmethod
    async def _loop_hook(cls, ctx):
        """Unified hook: if it's cycle time, inject the cycle prompt.
        Between cycles, check if position monitoring is due.
        If neither, actually sleep in the hook (no LLM turn wasted)."""
        if not cls._state["running"]:
            return HookResult()

        now = time.time()
        cycle_elapsed = now - cls._state["last_cycle_time"]
        cycle_interval = cls.config["scan_interval_seconds"]

        monitor_elapsed = now - cls._state["last_monitor_time"]
        monitor_interval = cls.config["monitor_interval_seconds"]

        # Priority 1: Full cycle is due
        if cycle_elapsed >= cycle_interval:
            cls._state["cycle"] += 1
            cls._state["last_cycle_time"] = time.time()
            cls._state["last_monitor_time"] = time.time()  # monitor runs as part of cycle
            cls._state["proposals_this_cycle"] = 0
            cycle_num = cls._state["cycle"]

            preset = cls.config["scan_presets"][
                cls._state["preset_index"] % len(cls.config["scan_presets"])
            ] if cls.config["scan_presets"] else "momentum"
            cls._state["preset_index"] += 1

            prompt = cls._build_cycle_prompt(cycle_num, preset)

            return HookResult(
                inject=[User(prompt)],
                continue_loop=True,
            )

        # Priority 2: Position monitor check is due (between cycles)
        if cls._state["tracked_positions"] and monitor_elapsed >= monitor_interval:
            cls._state["last_monitor_time"] = time.time()
            alerts = await cls._check_tracked_positions()
            if alerts:
                alert_text = "\n".join(alerts)
                return HookResult(
                    inject=[User(
                        f"[Position Monitor] {len(alerts)} alert(s) detected:\n\n"
                        f"{alert_text}\n\n"
                        f"Review these alerts and take action if needed. "
                        f"Use `await MMRHelpers.propose(...)` to create proposals. "
                        f"Be concise — save context."
                    )],
                    continue_loop=True,
                )
            # No alerts — fall through to sleep

        # Not time for anything — actually sleep in the hook instead of
        # wasting an LLM turn with a "run this sleep code" prompt.
        next_cycle_in = cycle_interval - cycle_elapsed
        next_monitor_in = monitor_interval - monitor_elapsed if cls._state["tracked_positions"] else next_cycle_in
        next_event_in = min(next_cycle_in, next_monitor_in)
        sleep_time = min(next_event_in, 30)  # sleep up to 30s at a time
        await asyncio.sleep(sleep_time)
        return HookResult(continue_loop=True)

    @classmethod
    def _build_cycle_prompt(cls, cycle_num, preset):
        """Build the instruction prompt for a cycle."""
        loc = cls.config.get("location", "")
        tickers = cls.config.get("tickers", [])
        exchange = cls.config.get("exchange", "")
        currency = cls.config.get("currency", "")
        max_proposals = cls.config.get("max_proposals_per_cycle", 2)
        move_threshold = cls.config.get("position_move_pct", 0.015)

        prompt = f"""[Trading Loop] Cycle {cycle_num} — {datetime.now().strftime('%H:%M:%S')}

Run the trading cycle. Follow these phases IN ORDER. Be concise — save context.

## PHASE 1: MONITOR
```python
snap = await MMRHelpers.portfolio_snapshot()
diff = await MMRHelpers.portfolio_diff()
hours = await MMRHelpers.market_hours()
print(json.dumps({{"pnl": snap["data"]["daily_pnl"], "positions": snap["data"]["position_count"], "top_mover": snap["data"]["movers"][0] if snap["data"]["movers"] else None}}, indent=2))
print(f"Changes: {{len(diff['data']['changed'])}} moved, {{len(diff['data']['new'])}} new, {{len(diff['data']['removed'])}} removed, {{diff['data']['unchanged_count']}} flat")
print(hours)
```

Note: The current date/time is {datetime.now().strftime('%Y-%m-%d %H:%M')} LOCAL. Australia (ASX) is ~14-17 hours AHEAD of US Pacific — if it's Sunday evening in the US, it's Monday in Australia and ASX may be OPEN. Check the market_hours output for actual status.

If ALL positions are unchanged AND no relevant markets are open, skip to PHASE 4 (DIGEST) with "No action — markets closed."

If any position moved >{move_threshold:.1%}, note it for deeper analysis.

## PHASE 2: ANALYZE
Run these in parallel with `asyncio.gather()` or `delegate_task()`:
1. `await MMRHelpers.portfolio_risk()` — check warnings, HHI, group budgets
2. `await MMRHelpers.session_status()` — check remaining_positions capacity"""

        if loc and tickers:
            tickers_str = json.dumps(tickers)
            prompt += f"""
3. `await MMRHelpers.ideas("{preset}", location="{loc}", tickers={tickers_str}, num={cls.config['scan_num']})` — scan for ideas"""
        elif tickers:
            tickers_str = json.dumps(tickers)
            prompt += f"""
3. `await MMRHelpers.ideas("{preset}", tickers={tickers_str}, num={cls.config['scan_num']})` — scan for ideas"""
        else:
            prompt += f"""
3. `await MMRHelpers.ideas("{preset}", num={cls.config['scan_num']})` — scan for ideas"""

        prompt += f"""

Analyze results:
- Flag positions that moved significantly
- Note any risk warnings (concentration, group over-budget, correlated clusters)
- Identify actionable ideas from the scan (score > 6, not already held)
- If remaining_positions = 0, do NOT propose new positions

## PHASE 3: PROPOSE (max {max_proposals} this cycle)
For each actionable idea, create a proposal:
```python
result = await MMRHelpers.propose(symbol, "BUY", confidence=X,
    reasoning="...", group="...", source="llm\""""

        if exchange:
            prompt += f',\n    exchange="{exchange}"'
        if currency:
            prompt += f',\n    currency="{currency}"'

        prompt += """)
# result["data"]["proposal_id"] and result["data"]["sizing_result"]["reasoning"]
```

Set confidence based on signal strength: scan score 8+/10 → 0.8, 6-8 → 0.6, <6 → skip.
Include the scan preset and key indicators in reasoning.
Tag with appropriate group if one exists.

Skip PROPOSE entirely if: no actionable ideas, at position limit, or risk warnings suggest reducing exposure.
"""

        prompt += f"""## PHASE 4: DIGEST
Write a ONE-LINE summary, then compact:
```python
_cycle = {cycle_num}
_preset = "{preset}"
summary = f"Cycle {{_cycle}} — [key finding]. [action taken or 'no action']. [risk status]."
write_memory(f"trading_loop_cycle_{{_cycle}}", summary,
    metadata={{"type": "cycle", "cycle": _cycle, "preset": _preset}})
print(summary)
await compact("drop-helpers-results")
```

CRITICAL: Do NOT emit long analysis text. Keep output minimal. The summary in memory is your record."""

        return prompt

    # -------------------------------------------------------------------
    # Position tracking
    # -------------------------------------------------------------------

    @classmethod
    async def track_position(cls, symbol, side, entry, qty, stop_pct=None, add_pct=None):
        """Add a position to the monitor watchlist.

        :param symbol: Ticker symbol (e.g. "AAPL")
        :param side: "LONG" or "SHORT"
        :param entry: Entry price
        :param qty: Position quantity
        :param stop_pct: Stop-loss threshold as negative % (default from config)
        :param add_pct: Add-to-position threshold as positive % (default from config)
        """
        if stop_pct is None:
            stop_pct = cls.config.get("monitor_stop_pct", -1.5)
        if add_pct is None:
            add_pct = cls.config.get("monitor_add_pct", 2.0)
        cls._state["tracked_positions"][symbol] = {
            "side": side.upper(),
            "entry": float(entry),
            "qty": int(qty),
            "stop_pct": float(stop_pct),
            "add_pct": float(add_pct),
        }
        print(f"[Position Monitor] Tracking {symbol} {side} {qty}@{entry} "
              f"(stop: {stop_pct}%, add: {add_pct}%)")

    @classmethod
    async def untrack_position(cls, symbol):
        """Remove a position from the monitor watchlist."""
        if symbol in cls._state["tracked_positions"]:
            del cls._state["tracked_positions"][symbol]
            print(f"[Position Monitor] Untracked {symbol}")
        else:
            print(f"[Position Monitor] {symbol} was not being tracked")

    @classmethod
    async def untrack_all(cls):
        """Clear all tracked positions."""
        count = len(cls._state["tracked_positions"])
        cls._state["tracked_positions"] = {}
        print(f"[Position Monitor] Cleared {count} tracked position(s)")

    @classmethod
    async def _check_tracked_positions(cls):
        """Check tracked positions against current prices, return alert strings.

        Uses batch snapshot for efficiency (~4s total vs ~4s per symbol).
        Only returns alerts when action thresholds are breached.
        """
        alerts = []
        exchange = cls.config.get("exchange", "")
        currency = cls.config.get("currency", "")
        symbols = list(cls._state["tracked_positions"].keys())

        if not symbols:
            return alerts

        # Batch snapshot — one call for all tracked symbols
        try:
            result = await MMRHelpers.snapshots_batch(
                symbols, exchange=exchange, currency=currency,
            )
            snapshots = result.get("data", []) if isinstance(result, dict) else []
        except Exception:
            # Fall back to individual snapshots if batch fails
            snapshots = []
            for sym in symbols:
                try:
                    snap = await MMRHelpers.snapshot(sym, exchange=exchange, currency=currency)
                    if isinstance(snap, dict) and "data" in snap:
                        snapshots.append(snap["data"])
                except Exception:
                    pass

        # Build symbol → price lookup
        price_map = {}
        for snap in snapshots:
            sym = snap.get("symbol", "")
            price = snap.get("last") or snap.get("close") or snap.get("bid")
            if sym and price and price == price:  # NaN check
                price_map[sym] = float(price)

        for symbol, pos in cls._state["tracked_positions"].items():
            last_price = price_map.get(symbol)
            if last_price is None:
                continue

            entry = pos["entry"]
            side = pos["side"]

            if side == "LONG":
                pnl_pct = ((last_price - entry) / entry) * 100
            else:  # SHORT
                pnl_pct = ((entry - last_price) / entry) * 100

            # Check stop-loss threshold (stop_pct is negative, e.g. -1.5)
            if pnl_pct <= pos["stop_pct"]:
                alerts.append(
                    f"STOP-LOSS {symbol}: {side} {pos['qty']}@{entry:.2f}, "
                    f"now {last_price:.2f} ({pnl_pct:+.2f}%). "
                    f"Threshold: {pos['stop_pct']}%. Consider closing."
                )
            # Check take-profit / add threshold
            elif pnl_pct >= pos["add_pct"]:
                alerts.append(
                    f"TARGET-HIT {symbol}: {side} {pos['qty']}@{entry:.2f}, "
                    f"now {last_price:.2f} ({pnl_pct:+.2f}%). "
                    f"Threshold: +{pos['add_pct']}%. Consider taking profit or adding."
                )

        return alerts


# ---------------------------------------------------------------------------
# Convenience functions (available in helpers blocks)
# ---------------------------------------------------------------------------

async def start_trading_loop(**overrides):
    """Start the trading loop with optional config overrides.

    Example:
        await start_trading_loop(scan_interval_seconds=300, location="STK.AU.ASX")
    """
    for k, v in overrides.items():
        if k in TradingLoop.config:
            TradingLoop.config[k] = v
    await TradingLoop.start()


async def stop_trading_loop():
    """Stop the trading loop."""
    await TradingLoop.stop()


async def trading_loop_status():
    """Get current loop status."""
    return await TradingLoop.status()


async def track_position(symbol, side, entry, qty, stop_pct=None, add_pct=None):
    """Add a position to the monitor watchlist.

    Example:
        await track_position("AAPL", "LONG", 185.50, 100, stop_pct=-2.0, add_pct=3.0)
    """
    await TradingLoop.track_position(symbol, side, entry, qty, stop_pct, add_pct)


async def untrack_position(symbol):
    """Remove a position from the monitor watchlist."""
    await TradingLoop.untrack_position(symbol)


async def untrack_all():
    """Clear all tracked positions."""
    await TradingLoop.untrack_all()
