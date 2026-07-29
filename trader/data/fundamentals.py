"""Point-in-time fundamentals: what was KNOWN, not what was true.

WHY THIS FILE IS MOSTLY ABOUT DATES
    Fundamental data has two timestamps and using the wrong one manufactures an
    enormous fake edge. A quarter ENDS on one date and is FILED on another, and
    the gap is 18 to 60 days (median 34, measured across 499 records). Ranking
    companies by an earnings figure on its period-end date means trading on
    results that were not public for another month - the classic fundamentals
    backtest bug, and one that produces a beautiful equity curve.

    So every record here is keyed by when it became KNOWABLE, never by the
    period it describes. `as_of` reads take only what had been accepted by
    EDGAR strictly before the moment asked about.

WHY THE ACCEPTANCE TIMESTAMP MATTERS, NOT JUST THE DATE
    A 10-Q accepted at 06:01 ET is tradeable at that day's close. One accepted
    at 16:45 is not. Polygon exposes a filing DATE only, which forces a
    conservative next-day assumption and costs a day of signal on every name.
    sec-api.io carries the full timestamp, so the rule can be exact instead of
    defensive.

WHY RESTATEMENTS ARE KEPT, NOT OVERWRITTEN
    When a company restates Q2 in November, a database that replaces the
    original silently rewrites history: a backtest then trades July using
    numbers that did not exist until November. Amendments are stored as
    additional rows with their own acceptance time, and an as-of read
    naturally sees only the version that existed then. This is the same
    principle as the bar quarantine - keep the evidence, and let the query
    decide what is visible.
"""

from __future__ import annotations

import datetime as dt
import math

from typing import Any, Dict, List, Mapping, NamedTuple, Optional, Sequence

import deal


class Filing(NamedTuple):
    """One filing, identified by when it became public."""
    conid: int
    cik: str
    ticker: str
    form_type: str                 # 10-Q, 10-K, 10-Q/A ...
    accession_no: str              # unique per filing; the natural key
    accepted_at: dt.datetime       # tz-aware; when EDGAR accepted it
    period_end: Optional[dt.date]  # what the numbers describe
    is_amendment: bool


@deal.pure
@deal.ensure(
    lambda _: _.result is None or _.result >= 0,
    message='a reporting lag cannot be negative')
def reporting_lag_days(period_end: Optional[dt.date],
                       accepted_at: Optional[dt.datetime]) -> Optional[int]:
    """Days between the end of the period and the filing becoming public.

    Returned for auditing rather than used for correction. A fixed offset
    cannot substitute for the real acceptance time: the lag ranges 18 to 60
    days in practice, so any constant is wrong by weeks in both directions,
    and wrong in the optimistic direction half the time.

    None when either end is missing, and NEGATIVE lags return None rather than
    a negative number - a filing accepted before the period it reports on is a
    data defect, not a fast filer.
    """
    if period_end is None or accepted_at is None:
        return None
    days = (accepted_at.date() - period_end).days
    return days if days >= 0 else None


@deal.pure
def is_knowable_at(accepted_at: Optional[dt.datetime],
                   as_of: dt.datetime) -> bool:
    """Was this filing public at ``as_of``?

    STRICTLY before, not before-or-equal. A filing accepted at exactly the
    instant you are asking about was not available to act on at that instant,
    and the boundary is where an off-by-one would hide - it fires on precisely
    the days that matter most, when news lands.

    A filing with no acceptance time is NOT knowable at any moment. It cannot
    be placed in time, and admitting it "because it is probably old enough"
    would let unplaceable records into exactly the periods where they do the
    most damage.
    """
    if accepted_at is None:
        return False
    if accepted_at.tzinfo is None or as_of.tzinfo is None:
        # Comparing a naive to an aware datetime raises; refusing is the safe
        # direction, since the alternative is guessing a timezone and being
        # wrong by up to a day around the close.
        return False
    return accepted_at < as_of


@deal.pure
def latest_known(
    filings: Sequence[Filing],
    as_of: dt.datetime,
) -> Dict[int, Filing]:
    """Most recent filing per instrument that was public at ``as_of``.

    Later ACCEPTANCE wins, not later period. That is what makes restatements
    behave correctly without special-casing them: an amendment filed in
    November is simply a filing whose acceptance time is November, invisible
    to any as-of read before then, and preferred afterwards.
    """
    best: Dict[int, Filing] = {}
    for f in filings:
        if not is_knowable_at(f.accepted_at, as_of):
            continue
        prior = best.get(f.conid)
        if prior is None or f.accepted_at > prior.accepted_at:
            best[f.conid] = f
    return best


@deal.pure
def safe_ratio(numerator: Optional[float],
               denominator: Optional[float]) -> Optional[float]:
    """Fundamental ratios, with the divide-by-zero cases that actually occur.

    Loss-making companies have negative earnings, newly-listed ones report
    zero book value, and both are common enough in a 500-name universe that
    the degenerate cases are the rule rather than the exception. Returning
    None keeps them out of a ranking; returning 0.0 or infinity would place
    them at one end of it, which is a view nobody expressed.
    """
    if numerator is None or denominator is None:
        return None
    if not (math.isfinite(numerator) and math.isfinite(denominator)):
        return None
    if denominator == 0:
        return None
    out = numerator / denominator
    return out if math.isfinite(out) else None


# Line items worth extracting, by XBRL tag. Deliberately a short list: each one
# has to survive being missing, renamed, or reported under a different tag by
# different filers, and a wide schema multiplies that problem without adding
# signal. US-GAAP tags vary between companies, so several alternatives per
# concept are tried in order.
CONCEPTS: Dict[str, List[str]] = {
    'revenue': ['RevenueFromContractWithCustomerExcludingAssessedTax',
                'Revenues', 'SalesRevenueNet'],
    'net_income': ['NetIncomeLoss', 'ProfitLoss'],
    'gross_profit': ['GrossProfit'],
    'operating_income': ['OperatingIncomeLoss'],
    'total_assets': ['Assets'],
    'total_liabilities': ['Liabilities'],
    'stockholders_equity': ['StockholdersEquity',
                            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
    'cash_from_operations': ['NetCashProvidedByUsedInOperatingActivities'],
    'capex': ['PaymentsToAcquirePropertyPlantAndEquipment'],
    'shares_diluted': ['WeightedAverageNumberOfDilutedSharesOutstanding'],
}


# Polygon's own field names, which are NOT XBRL tags. Kept separate from
# CONCEPTS rather than merged: one maps a vendor's schema, the other maps the
# filings themselves, and collapsing them would mean a vendor rename silently
# changing what an XBRL extraction returns.
POLYGON_FIELDS: Dict[str, List[tuple]] = {
    'revenue': [('income_statement', 'revenues')],
    'net_income': [('income_statement', 'net_income_loss')],
    'gross_profit': [('income_statement', 'gross_profit')],
    'operating_income': [('income_statement', 'operating_income_loss')],
    'total_assets': [('balance_sheet', 'assets')],
    'total_liabilities': [('balance_sheet', 'liabilities')],
    'stockholders_equity': [('balance_sheet', 'equity'),
                            ('balance_sheet', 'equity_attributable_to_parent')],
    'cash_from_operations': [('cash_flow_statement', 'net_cash_flow_from_operating_activities')],
    'shares_diluted': [('income_statement', 'diluted_average_shares')],
}


def extract_polygon(financials: Any) -> Dict[str, Optional[float]]:
    """Pull the same concepts out of a Polygon financials object.

    Separate from `extract_concepts` because the two schemas share nothing but
    intent: Polygon exposes snake_case fields on nested objects, filings expose
    XBRL tags in flat dicts. An adapter that tried to serve both would silently
    return None for everything the moment either side changed - which is
    exactly what happened on the first attempt here, producing 204 rows of
    correct timestamps attached to entirely empty values.
    """
    out: Dict[str, Optional[float]] = {}
    for name, paths in POLYGON_FIELDS.items():
        value = None
        for section, field in paths:
            sec = getattr(financials, section, None)
            if sec is None:
                continue
            item = getattr(sec, field, None)
            raw = getattr(item, 'value', None)
            if raw is None:
                continue
            try:
                candidate = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(candidate):
                value = candidate
                break
        out[name] = value
    return out


def extract_concepts(statements: Mapping[str, Any]) -> Dict[str, Optional[float]]:
    """Pull the concepts above out of an xbrl-to-json payload.

    Takes the FIRST period in each series, which the converter orders most
    recent first. Filers disagree on tag names, so alternatives are tried in
    order and the first present wins; a concept no filer reported comes back
    None rather than 0.0, because "did not report" and "reported zero" are
    different facts and only one of them belongs in a ranking.
    """
    flat: Dict[str, Any] = {}
    for section in statements.values():
        if isinstance(section, dict):
            flat.update(section)

    out: Dict[str, Optional[float]] = {}
    for name, tags in CONCEPTS.items():
        value = None
        for tag in tags:
            series = flat.get(tag)
            if isinstance(series, list) and series:
                raw = series[0].get('value')
                try:
                    candidate = float(raw)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(candidate):
                    value = candidate
                    break
        out[name] = value
    return out
