"""The vendor's statement endpoints (``/stocks/financials/v1``), read
point-in-time.

The endpoint the fundamentals store was built from (``/vX/reference/
financials``) was retired; it answered 404 from some time before 2026-11-15
and the ingest swallowed the error, so nothing said the store had stopped.
Its replacement is three endpoints - income statements, balance sheets, cash
flow statements - one row per (ticker, period). Two things about it decide how
a row may be used, both measured against the live API on 2026-11-15:

IT SERVES THE LATEST VERSION OF A PERIOD, NOT THE ORIGINAL
    Each row carries the ``filing_date`` of the filing its numbers were taken
    from, and for an old period that is a LATER filing: AAPL's quarter ended
    2020-06-27 comes back dated 2021-07-28, the 10-Q a year on, where it is the
    comparative column. Comparatives are recast (discontinued operations,
    spin-offs: ABT's 2012 revenue reads 5.6B, not the 10.8B it reported), so a
    backfill from here is not what was known at the time. A row is therefore
    used only when its ``filing_date`` is the day the filing we are dating it
    to was accepted, give or take ``MAX_FILING_GAP_DAYS``; anything else is
    refused, and counted, not stored.

ITS SHARE COUNTS ARE ON TODAY'S SPLIT BASIS
    AAPL 2020-06-27 reads 17.4B diluted shares (4.35B as reported, times the
    4:1 of August 2020). A row fetched promptly has no later split yet, so
    the number is the reported one; either way the basis is the FETCH day, and
    ``scripts/fundamental_signals.prepare_rows`` puts a ``source ==
    'financials_v1'`` row on the price basis from ``fetched_at``, where a
    legacy row is on the basis of its filing day.

The field mapping was chosen by measurement, not by name: over 150 names and
5,200 quarterly rows present in both, the share of rows where the stored
(legacy) value equals the new field to 0.1% is shown beside each entry. A
legacy ``net_income`` is the consolidated figure (94%), not the one
attributable to common shareholders (58%); ``stockholders_equity`` is total
equity (97%), not equity attributable to the parent (69%).

Pure functions of their arguments.
"""

from __future__ import annotations

import datetime as dt
import math

from typing import Any, Dict, Mapping, Optional, Tuple

SOURCE = 'financials_v1'

# A filing accepted late in the evening can carry the next day's date at the
# vendor, and a weekend can sit between the two; a comparative column is a
# quarter or a year away. Five days separates them with room to spare.
MAX_FILING_GAP_DAYS = 5

# stored column -> (statement, vendor field).           legacy == new (10-Q rows)
FIELDS: Dict[str, Tuple[str, str]] = {
    'revenue': ('income', 'revenue'),                                         # 81%
    'net_income': ('income', 'consolidated_net_income_loss'),                 # 94%
    'gross_profit': ('income', 'gross_profit'),                               # 78%
    'operating_income': ('income', 'operating_income'),                       # 66%
    'total_assets': ('balance', 'total_assets'),                              # 99%
    'total_liabilities': ('balance', 'total_liabilities'),                    # 88%
    'stockholders_equity': ('balance', 'total_equity'),                       # 97%
    'cash_from_operations': ('cashflow', 'net_cash_from_operating_activities'),   # 87%
    'shares_diluted': ('income', 'diluted_shares_outstanding'),               # 88%
}

# The vendor writes 0.0 for a line the filer did not report (AAPL's goodwill,
# its depreciation line). For these columns a zero is that, never a fact:
# nobody has zero diluted shares or zero total assets.
ZERO_IS_MISSING = frozenset({'shares_diluted', 'total_assets'})

STATEMENTS = ('income', 'balance', 'cashflow')

# Why a (ticker, period) yields no row.
NO_STATEMENT = 'no income statement'
OTHER_FILING = 'income statement is from another filing'


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _day(value: Any) -> Optional[dt.date]:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    try:
        return dt.date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def filed_with(row: Optional[Mapping[str, Any]], accepted_on: dt.date,
               max_gap_days: int = MAX_FILING_GAP_DAYS) -> bool:
    """Were this row's numbers taken from the filing accepted on
    ``accepted_on``? True only when the row states a ``filing_date`` within
    ``max_gap_days`` of it. No row, or no readable date, is False: a number
    whose filing cannot be named cannot be dated."""
    if not row:
        return False
    filed = _day(row.get('filing_date'))
    return filed is not None and abs((filed - accepted_on).days) <= max_gap_days


def statement_values(statements: Mapping[str, Optional[Mapping[str, Any]]],
                     accepted_on: dt.date,
                     max_gap_days: int = MAX_FILING_GAP_DAYS) -> Tuple[Optional[Dict[str, Optional[float]]], str]:
    """``(values, reason)`` for one (ticker, period): the stored columns read
    from that period's three statement rows, or ``(None, why)``.

    ``statements`` maps ``'income'`` / ``'balance'`` / ``'cashflow'`` to the
    vendor's row for the period (None when it has none).

    The INCOME statement decides whether there is a row at all - it carries
    revenue and the share count, which is what the store exists for: absent
    (``NO_STATEMENT``) or taken from another filing (``OTHER_FILING``) and
    nothing is stored. A balance
    sheet or cash flow statement that is absent or from another filing leaves
    its columns NULL: unknown is storable, a recast number dated to the
    original filing is not.
    """
    income = statements.get('income')
    if not income:
        return None, NO_STATEMENT
    if not filed_with(income, accepted_on, max_gap_days):
        return None, OTHER_FILING
    usable = {'income': income}
    for name in ('balance', 'cashflow'):
        row = statements.get(name)
        usable[name] = row if filed_with(row, accepted_on, max_gap_days) else None      # type: ignore[assignment]
    values: Dict[str, Optional[float]] = {}
    for column, (statement, field) in FIELDS.items():
        row = usable.get(statement)
        number = _number(row.get(field)) if row else None
        if number is not None and column in ZERO_IS_MISSING and number <= 0:
            number = None
        values[column] = number
    return values, ''
