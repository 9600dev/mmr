"""Target-weights file (design decision D08): load and validate ``mmr.targets.v1``.

The file is the interface between a low-frequency strategy and the rebalancer.
Everything here is pure: no network, no database, no clock (staleness is judged
in :mod:`trader.rebalance.plan`, where ``today`` can be injected).

Validation is fail-closed and names the offending field: a malformed file is a
refusal, never a best-effort parse — wrong targets are worse than no targets.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

SCHEMA = 'mmr.targets.v1'

# D08: sleeve = lowercase letters, digits, underscore.
_SLEEVE_RE = re.compile(r'^[a-z][a-z0-9_]*$')
# D08: US common-stock symbols as traded today (BRK.B, BF-B style suffixes allowed).
_TICKER_RE = re.compile(r'^[A-Z][A-Z0-9]{0,5}([.\-][A-Z0-9]{1,3})?$')
_SHA256_RE = re.compile(r'^[0-9a-f]{64}$')
# Floating-point slack on the weight sum: 1.0000000001 is "1.0", 1.0001 is not.
WEIGHT_SUM_TOLERANCE = 1e-9


class TargetsError(ValueError):
    """A targets file that must not be acted on. The message names the field."""


@dataclass(frozen=True)
class StrategyRef:
    family: str
    id: str
    module: str
    class_name: str
    params: Mapping[str, Any]
    source_sha256: str


@dataclass(frozen=True)
class Targets:
    """A validated ``mmr.targets.v1`` file."""

    sleeve: str
    as_of: dt.date
    generated_at: dt.datetime
    strategy: StrategyRef
    capital_usd: float
    prices_as_of: dt.date
    weights: Mapping[str, float]
    notes: str = ''
    path: str = ''
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    @property
    def group(self) -> str:
        """The position group that owns this sleeve's holdings (D09)."""
        return sleeve_group(self.sleeve)

    @property
    def source(self) -> str:
        """The proposal ``source`` stamp for this sleeve/as_of batch."""
        return batch_source(self.sleeve, self.as_of)

    @property
    def weight_sum(self) -> float:
        return float(sum(self.weights.values()))


def sleeve_group(sleeve: str) -> str:
    return f'sleeve_{sleeve}'


def batch_source(sleeve: str, as_of: dt.date) -> str:
    return f'rebalance:{sleeve}:{as_of.isoformat()}'


def _require(doc: Mapping[str, Any], key: str, kind: type | tuple[type, ...], where: str) -> Any:
    if key not in doc:
        raise TargetsError(f'{where}: missing required field {key!r}')
    value = doc[key]
    if isinstance(value, bool) and kind is not bool:
        raise TargetsError(f'{where}: field {key!r} must be {_kind_name(kind)}, got bool')
    if not isinstance(value, kind):
        raise TargetsError(f'{where}: field {key!r} must be {_kind_name(kind)}, '
                           f'got {type(value).__name__}')
    return value


def _kind_name(kind: type | tuple[type, ...]) -> str:
    if isinstance(kind, tuple):
        return ' or '.join(k.__name__ for k in kind)
    return kind.__name__


def _parse_date(value: Any, key: str, where: str) -> dt.date:
    if not isinstance(value, str):
        raise TargetsError(f'{where}: field {key!r} must be a YYYY-MM-DD string')
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise TargetsError(f'{where}: field {key!r} is not a YYYY-MM-DD date: {value!r} ({exc})') from exc


def _parse_generated_at(value: Any, where: str) -> dt.datetime:
    if not isinstance(value, str):
        raise TargetsError(f'{where}: field \'generated_at\' must be an ISO-8601 UTC string')
    text = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError as exc:
        raise TargetsError(f'{where}: field \'generated_at\' is not ISO-8601: {value!r} ({exc})') from exc
    if parsed.tzinfo is None:
        raise TargetsError(f'{where}: field \'generated_at\' must carry a UTC offset: {value!r}')
    return parsed.astimezone(dt.timezone.utc)


def _finite_positive(value: Any, key: str, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TargetsError(f'{where}: field {key!r} must be a number, got {type(value).__name__}')
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise TargetsError(f'{where}: field {key!r} must be a finite number > 0, got {value!r}')
    return number


def validate_targets(doc: Any, *, path: str = '') -> Targets:
    """Validate a parsed JSON document against D08 and return :class:`Targets`.

    Refuses (``TargetsError``): wrong schema tag, bad sleeve name, malformed
    dates, a strategy block missing its provenance, non-positive capital, an
    empty or non-long-only weight map, any weight outside (0, 1], a weight sum
    above 1.0, a ticker that is not a US common-stock symbol.
    """
    where = path or '<targets>'
    if not isinstance(doc, Mapping):
        raise TargetsError(f'{where}: top level must be a JSON object, got {type(doc).__name__}')

    schema = _require(doc, 'schema', str, where)
    if schema != SCHEMA:
        raise TargetsError(f'{where}: schema must be {SCHEMA!r}, got {schema!r}')

    sleeve = _require(doc, 'sleeve', str, where)
    if not _SLEEVE_RE.match(sleeve):
        raise TargetsError(f'{where}: sleeve {sleeve!r} must match {_SLEEVE_RE.pattern} '
                           f'(lowercase letters, digits, underscore)')

    as_of = _parse_date(_require(doc, 'as_of', str, where), 'as_of', where)
    generated_at = _parse_generated_at(_require(doc, 'generated_at', str, where), where)
    prices_as_of = _parse_date(_require(doc, 'prices_as_of', str, where), 'prices_as_of', where)
    if prices_as_of > as_of:
        raise TargetsError(f'{where}: prices_as_of {prices_as_of} is after as_of {as_of} — '
                           f'a decision cannot use prices from after its session')

    strat = _require(doc, 'strategy', Mapping, where)
    swhere = f'{where}: strategy'
    params = _require(strat, 'params', Mapping, swhere)
    sha = _require(strat, 'source_sha256', str, swhere)
    if not _SHA256_RE.match(sha):
        raise TargetsError(f'{swhere}: source_sha256 must be 64 lowercase hex chars, got {sha!r}')
    strategy = StrategyRef(
        family=_require(strat, 'family', str, swhere),
        id=_require(strat, 'id', str, swhere),
        module=_require(strat, 'module', str, swhere),
        class_name=_require(strat, 'class', str, swhere),
        params=dict(params),
        source_sha256=sha,
    )
    for key in ('family', 'id', 'module', 'class'):
        if not strat[key].strip():
            raise TargetsError(f'{swhere}: field {key!r} must not be empty')

    capital = _finite_positive(_require(doc, 'capital_usd', (int, float), where), 'capital_usd', where)

    weights_raw = _require(doc, 'weights', Mapping, where)
    if not weights_raw:
        raise TargetsError(f'{where}: weights must name at least one ticker')
    weights: dict[str, float] = {}
    for ticker, weight in weights_raw.items():
        if not isinstance(ticker, str) or not _TICKER_RE.match(ticker):
            raise TargetsError(f'{where}: weights key {ticker!r} is not a US common-stock symbol '
                               f'(expected e.g. AAPL, BRK.B)')
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise TargetsError(f'{where}: weight for {ticker} must be a number, got {type(weight).__name__}')
        w = float(weight)
        if not math.isfinite(w):
            raise TargetsError(f'{where}: weight for {ticker} must be finite, got {weight!r}')
        if w <= 0:
            raise TargetsError(f'{where}: weight for {ticker} is {w!r} — targets are long-only, '
                               f'every weight must be in (0, 1]')
        if w > 1.0:
            raise TargetsError(f'{where}: weight for {ticker} is {w!r} > 1.0')
        weights[ticker] = w
    total = sum(weights.values())
    if total > 1.0 + WEIGHT_SUM_TOLERANCE:
        raise TargetsError(f'{where}: weights sum to {total:.6f} > 1.0 (no leverage; the '
                           f'remainder of 1.0 is cash)')

    notes = doc.get('notes', '')
    if not isinstance(notes, str):
        raise TargetsError(f'{where}: field \'notes\' must be a string')

    return Targets(
        sleeve=sleeve, as_of=as_of, generated_at=generated_at, strategy=strategy,
        capital_usd=capital, prices_as_of=prices_as_of, weights=weights, notes=notes,
        path=path, raw=dict(doc),
    )


def load_targets(path: str | Path) -> Targets:
    """Read and validate a targets file. Any problem raises ``TargetsError``."""
    p = Path(path).expanduser()
    if not p.is_file():
        raise TargetsError(f'{p}: targets file not found')
    try:
        text = p.read_text(encoding='utf-8')
    except OSError as exc:
        raise TargetsError(f'{p}: cannot read targets file: {exc}') from exc
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise TargetsError(f'{p}: not valid JSON: {exc}') from exc
    return validate_targets(doc, path=str(p))


def default_targets_dir() -> Path:
    """Where targets live (D08): ``~/.local/share/mmr/data/targets`` unless ``MMR_TARGETS_DIR`` is set."""
    import os
    override = os.environ.get('MMR_TARGETS_DIR')
    if override:
        return Path(override).expanduser()
    return Path.home() / '.local' / 'share' / 'mmr' / 'data' / 'targets'


def latest_targets_path(sleeve: str, targets_dir: str | Path | None = None) -> Path | None:
    """The newest ``<dir>/<sleeve>/<as_of>.json`` by ``as_of`` in the file name, or None."""
    if not _SLEEVE_RE.match(sleeve):
        raise TargetsError(f'sleeve {sleeve!r} must match {_SLEEVE_RE.pattern}')
    base = Path(targets_dir).expanduser() if targets_dir else default_targets_dir()
    folder = base / sleeve
    if not folder.is_dir():
        return None
    dated: list[tuple[dt.date, Path]] = []
    for candidate in folder.glob('*.json'):
        try:
            dated.append((dt.date.fromisoformat(candidate.stem), candidate))
        except ValueError:
            continue  # not an <as_of>.json file; ignore
    if not dated:
        return None
    return max(dated)[1]
