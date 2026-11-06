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
"""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Optional

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
