"""One parameter-construction contract for research and deployed strategies."""

from typing import Any, Callable, Dict, Optional
from trader.trading.strategy import Strategy


def _validate_session_tz(raw: Any, key: str) -> str:
    """``SESSION_TZ`` must name a real IANA zone. The runtime converts live
    UTC bars into this zone before comparing ``close_by_time`` (see
    ``strategy_runtime._session_bar_ts``), and that conversion falls back to
    the RAW bar on any error — so a typo here would not fail loudly, it would
    fire a 15:45 ET flatten at 15:45 UTC. Refuse it at construction instead.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"param {key!r} expects an IANA timezone name; got {raw!r}")
    name = raw.strip()
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as ex:
        raise ValueError(f"param {key!r} is not a valid IANA timezone: {raw!r} ({ex})") from ex
    return name


# Upper-case parameters the RUNTIME reads from ``context.params`` as generic
# settings, independent of the strategy class. They are accepted for every
# strategy, validated by the mapped function, stored in ``context.params`` and
# NOT set on the instance unless the class itself declares the attribute.
#
#   SESSION_TZ  — the venue's session timezone used for live time-of-day exits
#                 (``close_by_time``) and recorded with each managed holding.
#                 Default when absent: America/New_York.
#
# Every other upper-case key that the class does not declare is still refused
# as a typo (``EMAPERIOD`` must not silently no-op).
RUNTIME_OWNED_PARAMS: Dict[str, Callable[[Any, str], Any]] = {
    'SESSION_TZ': _validate_session_tz,
}


def _coerce_param(raw: Any, current: Any, key: str) -> Any:
    """Coerce a raw param override (usually a CLI string) to the type of
    the existing class attribute. Bools get string-aware handling because
    ``bool("False")`` is ``True``.

    Rejects malformed inputs with a clear ``ValueError`` naming the key
    rather than failing deep inside the strategy's indicator math.
    """
    # Already native type — fast path.
    if type(current) is type(raw) or raw is None:
        return raw
    if isinstance(current, bool):
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            low = raw.strip().lower()
            if low in ('true', 'yes', '1'):
                return True
            if low in ('false', 'no', '0'):
                return False
            raise ValueError(
                f"param {key!r} expects a boolean; got {raw!r}"
            )
        return bool(raw)
    if isinstance(current, int):
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"param {key!r} expects int; got {raw!r}"
            )
    if isinstance(current, float):
        try:
            return float(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"param {key!r} expects float; got {raw!r}"
            )
    if isinstance(current, str):
        return str(raw)
    # For anything else (lists, dicts) pass through as-is; JSON sweep path
    # will have already parsed them.
    return raw


def _coerce_loose(raw: Any) -> Any:
    """Best-effort numeric coercion for lower-case ``self.params`` keys,
    where we have no type info. Tries int, then float, else returns the
    value unchanged. JSON-parsed inputs (already typed) pass straight through.
    """
    if not isinstance(raw, str):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        pass
    try:
        return float(raw)
    except (TypeError, ValueError):
        pass
    low = raw.strip().lower()
    if low in ('true', 'yes'):
        return True
    if low in ('false', 'no'):
        return False
    return raw


def apply_param_overrides(
    instance: Strategy,
    params: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Override strategy parameters. Call **after** ``install(context)``
    so the strategy has a writable ``params`` dict on its context.

    Two tunable idioms are handled:

    1. **Class attribute** (``EMA_PERIOD = 20``) — read as
       ``self.EMA_PERIOD``. Python MRO means
       ``setattr(instance, name, value)`` shadows the class attribute
       without mutating the class itself, so parallel sweeps don't
       collide on each other.
    2. **params dict** (``self.params.get('roc_period', 10)``) — older
       strategies that read from the runtime-provided params mapping.
       We write to ``instance._context.params`` directly since the
       ``Strategy.params`` property is read-only.

    Upper-case keys that aren't recognised class attributes raise
    ``ValueError`` (typos like ``EMAPERIOD`` would otherwise silently
    no-op) — EXCEPT the runtime-owned set in ``RUNTIME_OWNED_PARAMS``
    (currently ``SESSION_TZ``), which the live runtime reads from
    ``context.params`` for every strategy regardless of class. Those are
    validated (``SESSION_TZ`` must be a real IANA zone), stored in the
    context's params dict, and set on the instance ONLY when the class
    declares the attribute itself. Before this carve-out a class without
    ``SESSION_TZ = ...`` failed construction in the callback worker on
    ``params: {SESSION_TZ: Australia/Sydney}`` — the deployed ASX
    strategies survived only because their class happens to declare it.
    Lower-case keys are free-form — they always land in the context's
    params dict.

    Returns the effective overrides (post-coercion) for the caller to
    persist to ``BacktestRecord.params``.
    """
    if not params:
        return {}

    context = getattr(instance, '_context', None)

    def store(key: str, value: Any) -> None:
        if context is not None:
            context.params[key] = value
        else:
            # No context yet — stash on the instance so callers
            # that apply overrides pre-install (e.g. tests) don't
            # silently drop them. Strategy.__init__ seeds an empty
            # dict on _pending_params that install() can pick up.
            if not hasattr(instance, '_pending_params'):
                instance._pending_params = {}
            instance._pending_params[key] = value

    applied: Dict[str, Any] = {}
    for key, raw in params.items():
        is_class_attr = (
            hasattr(type(instance), key)
            and key.isupper()
        )
        runtime_validator = RUNTIME_OWNED_PARAMS.get(key)
        if is_class_attr:
            current = getattr(instance, key)
            coerced = _coerce_param(raw, current, key)
            if runtime_validator is not None:
                # The class declares it, but the runtime still reads it
                # generically — the same validation applies on both paths.
                coerced = runtime_validator(coerced, key)
            setattr(instance, key, coerced)
            store(key, coerced)
            applied[key] = coerced
        elif runtime_validator is not None:
            # Runtime-owned: accepted for any class, never setattr'd on an
            # instance whose class does not declare it.
            coerced = runtime_validator(raw, key)
            store(key, coerced)
            applied[key] = coerced
        elif key.isupper():
            raise ValueError(
                f"strategy {type(instance).__name__!r} has no "
                f"parameter {key!r}; known class-level tunables: "
                f"{sorted(k for k in vars(type(instance)) if k.isupper())}; "
                f"runtime-owned settings: {sorted(RUNTIME_OWNED_PARAMS)}"
            )
        else:
            coerced = _coerce_loose(raw)
            store(key, coerced)
            applied[key] = coerced
    return applied

