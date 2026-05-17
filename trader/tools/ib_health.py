"""
IB Gateway data-farm health probe.

Connects briefly to IB Gateway, hooks ``IB.errorEvent``, triggers a no-op
to wake the data subsystem, and listens for farm-status codes for a few
seconds. Reports the *direct* health of the IB market-data subsystem —
something a snapshot timeout can only hint at.

Codes we care about (per IB API documentation)::

    2103   Market data farm connection is broken: <farm>      (FAIL)
    2104   Market data farm connection is OK: <farm>          (OK)
    2105   HMDS data farm connection is broken: <farm>        (FAIL — historical)
    2106   HMDS data farm connection is OK: <farm>            (OK — historical)
    2107   HMDS data farm connection is inactive...           (LAZY-OK)
    2108   Market data farm connection is inactive...         (LAZY-OK)
    2110   Connectivity between TWS and server is broken      (FAIL — total outage)
    2119   Market data farm is connecting: <farm>             (TRANSIENT)
    2158   Sec-def data farm connection is OK: <farm>         (OK)
    162    "Trading TWS session is connected from a different IP address"
                                                              (FAIL — session conflict)
    10168  Delayed data not available, subscription required (FAIL — sub gate)
    10186  Requested market data requires additional subscription
                                                              (FAIL — sub gate)

Run as a CLI:

    uv run python -m trader.tools.ib_health \
        --host 127.0.0.1 --port 7497 --timeout 12

Exit code 0 if at least one US data farm reported OK or LAZY-OK; 1 if the
farms are broken or a session-conflict signal was seen; 2 if the connection
itself failed.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from dataclasses import dataclass, field
from typing import Any

try:
    from ib_async import IB
except ImportError as e:  # pragma: no cover
    print(f"ib_health: ib_async not installed: {e}", file=sys.stderr)
    sys.exit(2)


# Farm-name extractor. IB encodes the farm name at the very end of the
# error string, separated by either ":" (most farm-status codes — 2103/
# 2104/2105/2106/2119) or "." (the lazy variants — 2107/2108 use a period:
# "...available upon demand.afarm"). Multi-farm messages are comma-joined
# inside the trailing token (e.g. "usfarm,usfuture"). The character class
# explicitly excludes spaces / dashes so we don't accidentally swallow
# the prose half of the message.
_FARM_RE = re.compile(r"[:.]\s*([a-zA-Z0-9_,]+)\s*$")


@dataclass
class FarmEvent:
    code: int
    msg: str
    farm: str | None = None
    raw: str = ""


@dataclass
class HealthReport:
    """
    Structured outcome of a farm-health probe.

    ``ok`` is True when *no* hard-fail was seen AND at least one US-flavour
    farm reported OK / LAZY-OK. We treat LAZY-OK (2107/2108) as healthy
    because that's IB's normal "farm is up but idle, will activate on
    first request" state, not a failure mode.
    """
    connected: bool = False
    ok: bool = False
    duration_sec: float = 0.0

    # Per-farm last-seen state — keyed by farm name (e.g. "usfarm", "ushmds",
    # "afarm"), value is one of: "ok", "broken", "connecting", "lazy".
    farms: dict[str, str] = field(default_factory=dict)

    # Specific failure flags.
    session_conflict: bool = False
    subscription_gate: bool = False
    upstream_broken: bool = False

    # All raw events (debugging / logging). Capped at 50 to avoid runaway logs.
    events: list[dict[str, Any]] = field(default_factory=list)

    # Connection failure diagnostic, if any.
    connect_error: str | None = None

    # Human-readable next steps.
    diagnosis: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "ok": self.ok,
            "duration_sec": round(self.duration_sec, 2),
            "farms": dict(self.farms),
            "session_conflict": self.session_conflict,
            "subscription_gate": self.subscription_gate,
            "upstream_broken": self.upstream_broken,
            "connect_error": self.connect_error,
            "diagnosis": list(self.diagnosis),
            "events": list(self.events),
        }


def _classify(code: int, msg: str) -> tuple[str | None, str | None]:
    """
    Classify an IB error code into (farm_name | None, status | None).
    Status is one of: "ok", "broken", "connecting", "lazy". None means
    the code isn't a farm-status code — caller should special-case it.
    """
    m = _FARM_RE.search(msg)
    farm = m.group(1) if m else None
    if code == 2104:
        return farm, "ok"
    if code == 2106:
        return farm, "ok"
    if code == 2158:
        return farm, "ok"
    if code == 2103:
        return farm, "broken"
    if code == 2105:
        return farm, "broken"
    if code == 2119:
        return farm, "connecting"
    if code == 2107 or code == 2108:
        return farm, "lazy"
    return None, None


async def check_data_farms(
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int | None = None,
    timeout: float = 12.0,
    nudge: bool = True,
) -> HealthReport:
    """
    Connect briefly to IB Gateway and probe the market-data subsystem.

    :param host: Gateway host. Pre-resolve to an IP if you've hit DNS
        flakiness with podman/aardvark — see start_mmr.sh's IB_HOST_FOR_PY.
    :param port: 7497 (paper) or 7496 (live) by convention.
    :param client_id: ID for this probe connection. If None, picks a random
        ID in [3000, 6000) to avoid clashing with trader_service's IDs.
    :param timeout: Total budget in seconds. We listen for farm-status
        events for this long. 8-15 s is the right ballpark.
    :param nudge: If True (default), call ``reqCurrentTime()`` after
        connect to wake up the data subsystem. Some Gateway versions
        won't emit 2103/2104 until something prods the data path.

    :returns: A populated ``HealthReport``.
    """
    import random
    import time

    if client_id is None:
        client_id = random.randint(3000, 5999)

    report = HealthReport()
    ib = IB()

    # Hook the error stream BEFORE connect — IB sometimes emits 2103/2105
    # immediately on the post-login handshake before any explicit request,
    # and we want to catch those too.
    def _on_error(reqId, code, msg, contract):
        # Cap event list to 50 to avoid log spam in degraded scenarios.
        if len(report.events) < 50:
            report.events.append({
                "reqId": reqId, "code": code, "msg": msg,
                "contract": str(contract) if contract else None,
            })
        farm, status = _classify(code, msg)
        if farm and status:
            # If a farm flips broken→ok within the window, last-write-wins
            # (we want the latest known state, not the first).
            report.farms[farm] = status
            return
        if code == 162 and "different IP address" in msg.lower():
            report.session_conflict = True
            return
        if code in (10168, 10186, 354) or "subscription" in msg.lower():
            report.subscription_gate = True
            return
        if code == 2110:
            report.upstream_broken = True
            return

    # ib_async exposes errorEvent as a Signal-like object (+= to subscribe).
    ib.errorEvent += _on_error

    t0 = time.monotonic()
    try:
        # Use a short connect timeout (separate from the listen budget).
        await ib.connectAsync(host, port, clientId=client_id, timeout=8, readonly=True)
    except Exception as e:
        report.connect_error = f"{type(e).__name__}: {e}"
        report.duration_sec = time.monotonic() - t0
        report.diagnosis.append(
            f"Could not connect to IB Gateway at {host}:{port}. "
            "Check that the gateway container is running (./docker.sh -i) "
            "and the API port is open (port 7497 paper / 7496 live)."
        )
        return report

    report.connected = True

    # Optionally nudge the data path. ``reqCurrentTime`` is the cheapest
    # request that touches the server-side session and tends to flush any
    # pending farm-status messages.
    if nudge:
        try:
            ib.reqCurrentTime()
        except Exception:
            pass  # best-effort

    # Listen for the budget.
    try:
        # We could short-circuit on first definitive signal, but listening
        # the full budget gives a richer picture (e.g. usfarm OK + ushmds
        # broken — we want both, not just the first one).
        await asyncio.sleep(timeout)
    finally:
        report.duration_sec = time.monotonic() - t0
        try:
            ib.disconnect()
        except Exception:
            pass
        try:
            ib.errorEvent -= _on_error
        except Exception:
            pass

    # Decide overall health.
    healthy_farms = [f for f, s in report.farms.items() if s in ("ok", "lazy")]
    broken_farms = [f for f, s in report.farms.items() if s == "broken"]
    connecting = [f for f, s in report.farms.items() if s == "connecting"]

    # Heuristic for "ok":
    #   * no upstream-broken signal
    #   * no session conflict
    #   * at least one healthy farm
    #   * AND no farm is currently broken
    report.ok = (
        not report.upstream_broken
        and not report.session_conflict
        and len(healthy_farms) > 0
        and len(broken_farms) == 0
    )

    # Generate diagnosis. Order matters — we lead with the most likely
    # blocking issue.
    if report.session_conflict:
        report.diagnosis.append(
            "SESSION CONFLICT (error 162): another IB login is bumping this Gateway "
            "off the data farms. Common culprits: IBKR Mobile on your phone (log "
            "out, don't just close), TWS desktop running on another machine, "
            "Web Trader / Client Portal in a browser tab, or a leftover IB Gateway "
            "container. Find and kill them, then restart the Gateway "
            "(`./docker.sh -r`)."
        )
    if broken_farms:
        report.diagnosis.append(
            f"BROKEN data farms: {', '.join(broken_farms)}. The Gateway logged in "
            "but its data subsystem cannot reach IB's farms. Most often this is "
            "downstream of a session conflict (see above), but it can also be "
            "transient on IB's side — try `./docker.sh -r` after a few minutes."
        )
    if connecting and not broken_farms and not healthy_farms:
        report.diagnosis.append(
            f"Farms still CONNECTING after {timeout:.0f}s: {', '.join(connecting)}. "
            "Re-run this probe in 30-60 s — IBC sometimes takes a while to finish "
            "post-login handshakes, especially right after a clean Gateway boot."
        )
    if report.subscription_gate:
        report.diagnosis.append(
            "SUBSCRIPTION GATE seen (codes 354/10168/10186): the contract probed "
            "needs a market-data subscription you don't have on the live account "
            "shared with this paper account. Confirm in Client Portal that "
            "the relevant subscription line (e.g. 'NASDAQ TotalView', 'ASX Total') "
            "is active on the *live* account whose data is shared with paper."
        )
    if report.upstream_broken:
        report.diagnosis.append(
            "UPSTREAM BROKEN (error 2110): the Gateway lost connectivity to IB's "
            "servers entirely. Check internet, then restart Gateway."
        )
    if report.ok and not report.diagnosis:
        report.diagnosis.append(
            f"All probed farms healthy ({', '.join(sorted(healthy_farms))}). "
            "Snapshots / history should work."
        )

    return report


def _format_pretty(report: HealthReport) -> str:
    """Render a HealthReport as human-readable text for terminal output."""
    lines = []
    if report.connected:
        lines.append(f"  connected:    yes (probe lasted {report.duration_sec:.1f}s)")
    else:
        lines.append(f"  connected:    NO  ({report.connect_error})")
        lines.append("")
        lines.extend("  " + d for d in report.diagnosis)
        return "\n".join(lines)

    if report.farms:
        lines.append("  farms seen:")
        for farm in sorted(report.farms):
            status = report.farms[farm]
            symbol = {"ok": "✓", "lazy": "○", "connecting": "…", "broken": "✗"}.get(
                status, "?"
            )
            lines.append(f"    {symbol} {farm:<14} {status}")
    else:
        lines.append("  farms seen:   (none reported in window)")

    flags = []
    if report.session_conflict:
        flags.append("session_conflict")
    if report.subscription_gate:
        flags.append("subscription_gate")
    if report.upstream_broken:
        flags.append("upstream_broken")
    if flags:
        lines.append(f"  red flags:    {', '.join(flags)}")

    overall = "OK" if report.ok else "DEGRADED"
    lines.append(f"  overall:      {overall}")

    if report.diagnosis:
        lines.append("")
        lines.append("  diagnosis:")
        for d in report.diagnosis:
            # Wrap each diagnosis blob at ~78 chars.
            for chunk in _wrap_indent(d, prefix="    "):
                lines.append(chunk)

    return "\n".join(lines)


def _wrap_indent(text: str, prefix: str = "  ", width: int = 78) -> list[str]:
    """Cheap word-wrap with a fixed prefix per line."""
    import textwrap
    return textwrap.wrap(
        text,
        width=width,
        initial_indent=prefix,
        subsequent_indent=prefix,
        break_long_words=False,
        break_on_hyphens=False,
    ) or [prefix.rstrip()]


def _main_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.split("\n\n")[0] if __doc__ else None,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1",
                        help="IB Gateway host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=7497,
                        help="IB Gateway API port (default: 7497 / paper)")
    parser.add_argument("--client-id", type=int, default=None,
                        help="Client ID for this probe (default: random 3000-5999)")
    parser.add_argument("--timeout", type=float, default=12.0,
                        help="Listen window in seconds (default: 12)")
    parser.add_argument("--no-nudge", action="store_true",
                        help="Skip the reqCurrentTime nudge")
    parser.add_argument("--json", action="store_true",
                        help="Emit JSON instead of pretty text")
    args = parser.parse_args(argv)

    report = asyncio.run(check_data_farms(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        timeout=args.timeout,
        nudge=not args.no_nudge,
    ))

    if args.json:
        print(json.dumps(report.as_dict(), indent=2, default=str))
    else:
        print(_format_pretty(report))

    if not report.connected:
        return 2
    if not report.ok:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_main_cli())
