"""One parameter-construction contract for research and deployed strategies."""

from typing import Any, Dict, Optional
from trader.trading.strategy import Strategy


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
    no-op). Lower-case keys are free-form — they always land in the
    context's params dict.

    Returns the effective overrides (post-coercion) for the caller to
    persist to ``BacktestRecord.params``.
    """
    if not params:
        return {}

    context = getattr(instance, '_context', None)

    applied: Dict[str, Any] = {}
    for key, raw in params.items():
        is_class_attr = (
            hasattr(type(instance), key)
            and key.isupper()
        )
        if is_class_attr:
            current = getattr(instance, key)
            coerced = _coerce_param(raw, current, key)
            setattr(instance, key, coerced)
            if context is not None:
                context.params[key] = coerced
            applied[key] = coerced
        elif key.isupper():
            raise ValueError(
                f"strategy {type(instance).__name__!r} has no "
                f"parameter {key!r}; known class-level tunables: "
                f"{sorted(k for k in vars(type(instance)) if k.isupper())}"
            )
        else:
            coerced = _coerce_loose(raw)
            if context is not None:
                context.params[key] = coerced
            else:
                # No context yet — stash on the instance so callers
                # that apply overrides pre-install (e.g. tests) don't
                # silently drop them. Strategy.__init__ seeds an empty
                # dict on _pending_params that install() can pick up.
                if not hasattr(instance, '_pending_params'):
                    instance._pending_params = {}
                instance._pending_params[key] = coerced
            applied[key] = coerced
    return applied

