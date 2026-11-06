"""Instrument-class filters for scanner / idea output.

Two questions the scanners have to answer before showing a row:

1. ``mmr scan`` — *is this a common-share-like listing, or a right / warrant /
   unit?* (:func:`scan_exclusion_reason`). IB's scanner reports a right such
   as ``NMZ RT`` as ``secType STK`` like any share, so secType alone cannot
   tell them apart. The authoritative field is ``ContractDetails.stockType``
   ('COMMON', 'ETF', 'RIGHT', 'UNIT', 'PREFERRED', 'ADR', 'CLOSED-END FUND',
   ...), which trader_service now attaches to every scanner row. Verified
   against the live account 2026-11-05: ``NMZ RT`` -> RIGHT, ``AIIA U`` /
   ``ALFUU`` -> UNIT, ``ARCLR`` -> RIGHT, ``BRK B`` -> COMMON,
   ``JPM PRC`` -> PREFERRED.
2. ``mmr ideas`` — *is this an exchange-traded product, and is it leveraged
   or inverse?* (:func:`classify_etp` / :func:`apply_etp_filter`).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

# ---------------------------------------------------------------------------
# mmr scan: rights / warrants / units
# ---------------------------------------------------------------------------

#: IB ContractDetails.stockType values that are NOT common-share-like and are
#: excluded from ``mmr scan`` by default. Deliberately narrow: preferreds,
#: ADRs, ETFs and closed-end funds are real tradable listings the user may be
#: scanning for (``scan --instrument ETF``), so they stay.
NON_COMMON_STOCK_TYPES = frozenset({'RIGHT', 'WARRANT', 'UNIT'})

#: IB secTypes that are warrants regardless of stockType.
NON_COMMON_SEC_TYPES = frozenset({'WAR', 'IOPT'})

#: Fallback ONLY when stockType is unavailable (an older trader_service that
#: does not attach it, or a failed contract-details lookup). IB writes the
#: special-security suffix after a SPACE on NYSE/AMEX listings: 'NMZ RT'
#: (right), 'XYZ WS' / 'XYZ WS A' (warrant), 'AIIA U' / 'XYZ UN' (unit).
#: Not matched, on purpose:
#:   * a dotted '.U' — on TSX/TSXV that is the US-dollar line of an ordinary
#:     share, not a unit;
#:   * a bare 5th letter (Nasdaq 'ALFUU', 'ARCLR', 'XYZW') — 'GOOGL'-style
#:     5-letter commons exist, so the letter alone is a guess. stockType
#:     classifies those correctly; without it they are kept and counted as
#:     unclassified instead of dropped on a heuristic;
#:   * other space suffixes ('BRK B' class shares, 'JPM PRC' preferreds).
_IB_SPECIAL_SUFFIX = re.compile(r' (RT|WS(?: [A-Z])?|U|UN)$')


def scan_exclusion_reason(row: Mapping[str, Any]) -> Optional[str]:
    """Why an IB scanner row should be hidden from ``mmr scan``, or None.

    ``row`` is a ``scanner_data`` dict (symbol, secType, localSymbol,
    stockType, ...). stockType, when present, is authoritative in BOTH
    directions: 'COMMON' keeps 'BRK B' even though it has a space suffix.
    """
    sec_type = str(row.get('secType') or '').upper()
    if sec_type in NON_COMMON_SEC_TYPES:
        return f'secType {sec_type}'
    stock_type = str(row.get('stockType') or '').strip().upper()
    if stock_type:
        if stock_type in NON_COMMON_STOCK_TYPES:
            return f'stockType {stock_type}'
        return None
    for field in ('localSymbol', 'symbol'):
        value = str(row.get(field) or '')
        m = _IB_SPECIAL_SUFFIX.search(value)
        if m:
            return f'{field} suffix {m.group(1)!r} (stockType unavailable)'
    return None


def is_unclassified(row: Mapping[str, Any]) -> bool:
    """True when the row carries no stockType, i.e. only the conservative
    symbol fallback could be applied to it."""
    return not str(row.get('stockType') or '').strip()


# ---------------------------------------------------------------------------
# mmr ideas: exchange-traded products (ETF / ETN / ETV ...)
# ---------------------------------------------------------------------------
#
# 2026-11-05 (a crash day) the momentum BUY list led with UVXY and UVIX —
# 1.5x / 2x long-VIX-futures products. They "gap up with relative volume"
# exactly when equities fall, and decay by construction, so they are noise in
# an equity momentum list. ETP-ness comes from the data source's own
# instrument metadata (never from the ticker spelling):
#
#   * TwelveData — membership in GET /etfs?country=United States (one call,
#     ~11.5k US ETPs incl. ETNs such as VXX and ETVs such as GLD; verified
#     2026-11-05), cached in-process;
#   * Massive    — ticker details ``type`` in {ETF, ETN, ETV, ETS};
#   * IB         — ContractDetails.stockType in {ETF, ETN, ETP, ETC}.
#
# Whether an ETP is LEVERAGED / INVERSE is not a metadata field anywhere, so
# it is read from the product name (``2x``, ``-1x``, ``UltraPro``,
# ``UltraShort``, ``Inverse``, ``Bear`` ...) — but only for instruments the
# metadata already says are ETPs: "Ultra Clean Holdings" (UCTT) is a common
# stock. KNOWN_LEVERAGED_INVERSE is a curated SECONDARY safety net applied
# regardless of metadata (it also covers the case where the metadata call
# failed); it is not the primary mechanism and does not need to be complete.

ETP_MODES = ('leveraged', 'all', 'none')

#: Massive ``type`` codes that are exchange-traded products.
MASSIVE_ETP_TYPES = frozenset({'ETF', 'ETN', 'ETV', 'ETS'})
#: IB ContractDetails.stockType values that are exchange-traded products.
IB_ETP_STOCK_TYPES = frozenset({'ETF', 'ETN', 'ETP', 'ETC'})

#: Curated safety net (US listings, leveraged or inverse). Secondary to the
#: metadata + name check above — see the module comment.
KNOWN_LEVERAGED_INVERSE = frozenset({
    # VIX futures
    'UVXY', 'UVIX', 'SVIX', 'SVXY',
    # broad index
    'TQQQ', 'SQQQ', 'QLD', 'QID', 'PSQ', 'SPXL', 'SPXS', 'SPXU', 'UPRO', 'SSO',
    'SDS', 'SH', 'UDOW', 'SDOW', 'DDM', 'DXD', 'DOG', 'TNA', 'TZA', 'UWM', 'TWM',
    'RWM', 'URTY', 'SRTY',
    # sector / rates / commodities / crypto
    'SOXL', 'SOXS', 'USD', 'SSG', 'TECL', 'TECS', 'ROM', 'REW', 'FAS', 'FAZ',
    'UYG', 'SKF', 'DPST', 'LABU', 'LABD', 'CURE', 'NAIL', 'WEBL', 'WEBS',
    'NUGT', 'DUST', 'JNUG', 'JDST', 'ERX', 'ERY', 'GUSH', 'DRIP', 'YINN', 'YANG',
    'TMF', 'TMV', 'TBT', 'TBF', 'UCO', 'SCO', 'BOIL', 'KOLD', 'AGQ', 'ZSL',
    'UGL', 'GLL', 'BITX', 'BITU', 'SBIT', 'BITI', 'ETHT', 'ETHD',
    # single-stock
    'TSLL', 'TSLQ', 'TSLS', 'TSLZ', 'NVDL', 'NVDU', 'NVDD', 'NVDQ', 'NVDS',
    'MSTU', 'MSTX', 'MSTZ', 'CONL', 'AMDL', 'AAPU', 'AAPD', 'AMZU', 'AMZD',
    'GGLL', 'METU', 'MSFU',
})

# Multiplier tokens: '2x', '3X', '-1x', '1.5X', '2XL' (T-REX), '2 x'.
_MULTIPLIER = re.compile(r'(?<![\w.])(-?)(\d+(?:\.\d+)?)\s?x(?:l|s)?\b', re.IGNORECASE)
# Word markers of leverage / inversion. The 'Ultra' family is matched ONLY
# for ProShares (Ultra = 2x, UltraPro = 3x, UltraShort / UltraPro Short =
# -2x / -3x): elsewhere 'Ultra' / 'Ultrashort' is a bond-duration or
# marketing word — 'JPMorgan Ultra-Short Income', 'DoubleLine Ultrashort
# Income', 'Innovator Ultra Buffer', 'YieldMax Ultra Option Income' (checked
# against TwelveData's full US ETF list, 2026-11-05). Likewise 'ProShares
# Short X' is the ProShares -1x line while 'Short-Term' is a maturity and
# 'Long Online/Short Stores' (CLIX) is a long/short pair, not an inverse.
_LEVERAGE_WORDS = re.compile(
    r'\bproshares\b.*?\bultra(?:pro|short)?\b'
    r'|\bproshares\b.*?(?<!/)\bshort\b(?![-\s]*term)'
    r'|\binverse\b'
    r'|\bleveraged\b'
    r'|\bleverage\s+shares\b'
    r'|\bbear\b',
    re.IGNORECASE,
)


def leveraged_inverse_marker(name: Optional[str]) -> Optional[str]:
    """The token in an ETP *name* that marks it leveraged or inverse, or None.

    Only meaningful for instruments already known to be ETPs. A ``1x`` long
    multiplier is not leverage; a negative one is inversion."""
    if not name:
        return None
    for m in _MULTIPLIER.finditer(name):
        try:
            mult = float(m.group(2))
        except ValueError:
            continue
        if m.group(1) == '-' or mult > 1.0:
            return m.group(0)
    w = _LEVERAGE_WORDS.search(name)
    return w.group(0) if w else None


def classify_etp(ticker: str, meta: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Classify one ticker from source metadata.

    ``meta`` is ``{'is_etp': bool | None, 'type': str | None,
    'name': str | None, 'source': str}`` (None/absent = no metadata).

    Returns ``{'is_etp': bool | None, 'leveraged_inverse': bool,
    'reason': str}``. ``is_etp`` None means "unknown" (metadata missing);
    the curated list can still mark such a ticker leveraged/inverse.
    """
    meta = meta or {}
    is_etp = meta.get('is_etp')
    src = meta.get('source') or 'metadata'
    label = f"{src}{' ' + str(meta['type']) if meta.get('type') else ''}"
    curated = (ticker or '').upper() in KNOWN_LEVERAGED_INVERSE
    if is_etp:
        marker = leveraged_inverse_marker(meta.get('name'))
        if marker:
            return {'is_etp': True, 'leveraged_inverse': True,
                    'reason': f'leveraged/inverse ETP ({label}; name {meta.get("name")!r} '
                              f'has {marker!r})'}
        if curated:
            return {'is_etp': True, 'leveraged_inverse': True,
                    'reason': f'leveraged/inverse ETP ({label}; curated list)'}
        return {'is_etp': True, 'leveraged_inverse': False, 'reason': f'ETP ({label})'}
    if curated:
        # Curated tickers are ETPs by construction; flag even when metadata
        # disagrees or is missing — the net exists for exactly that case.
        return {'is_etp': True, 'leveraged_inverse': True,
                'reason': 'leveraged/inverse ETP (curated list'
                          + ('; no source metadata)' if is_etp is None else
                             f'; {label} did not list it as an ETP)')}
    return {'is_etp': is_etp, 'leveraged_inverse': False, 'reason': ''}


def apply_etp_filter(
    candidates: List[Dict[str, Any]],
    meta_by_ticker: Mapping[str, Mapping[str, Any]],
    mode: str = 'leveraged',
    source: str = '',
    metadata_error: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Drop ETP candidates per ``mode`` and report what was dropped and why.

    ``mode``: 'leveraged' (default) drops leveraged/inverse ETPs; 'all' drops
    every ETP; 'none' keeps everything (the summary still says so).

    Returns ``(kept, summary)`` where summary is
    ``{'mode', 'source', 'excluded': [{'ticker', 'reason'}], 'unknown': int,
    'metadata_error': str | None}``. ``unknown`` counts kept candidates with
    no ETP metadata (only the curated list could judge them).
    """
    if mode not in ETP_MODES:
        raise ValueError(f'etp mode must be one of {ETP_MODES}, got {mode!r}')
    summary: Dict[str, Any] = {'mode': mode, 'source': source, 'excluded': [],
                               'unknown': 0, 'metadata_error': metadata_error}
    if mode == 'none':
        return list(candidates), summary
    kept = []
    for c in candidates:
        ticker = str(c.get('ticker') or '')
        cls = classify_etp(ticker, meta_by_ticker.get(ticker))
        drop = cls['leveraged_inverse'] if mode == 'leveraged' else bool(cls['is_etp'])
        if drop:
            summary['excluded'].append({'ticker': ticker, 'reason': cls['reason']})
            continue
        if cls['is_etp'] is None:
            summary['unknown'] += 1
        kept.append(c)
    return kept, summary
