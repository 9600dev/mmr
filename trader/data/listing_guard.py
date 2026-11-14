"""Which listing a market-data source actually serves.

TwelveData and Massive are asked for a bare ticker and answer with the US
listing. Writing those bars under the conId of a listing somewhere else labels
one instrument's prices as another's: found 2026-11-14, 38 of the 514 members
of the nightly refresh universe were foreign listings (ORCL on the Mexican
exchange, CCL = Coles Group on ASX, SU = Schneider Electric in Paris, MRK =
Merck KGaA in Frankfurt), each receiving the US ticker's bars every night.
conId-or-refuse applies to the DATA a conId is given, not only to resolution.

One rule, used by every download path (`data download` / `data refresh` in the
CLI and the data service behind `mmr history`), so they cannot disagree.
"""

from __future__ import annotations

# Venues whose listing a US vendor serves when asked for a bare ticker.
US_LISTING_VENUES = frozenset({
    'NYSE', 'NASDAQ', 'ARCA', 'AMEX', 'BATS', 'IEX', 'ISLAND', 'PINK', 'NYSENAT',
    'NASDAQ.NMS', 'PSE', 'CBOE',
})
US_TICKER_SOURCES = frozenset({'twelvedata', 'massive'})


def is_us_listing(security) -> bool:
    currency = str(getattr(security, 'currency', '') or '').upper()
    venues = {str(getattr(security, name, '') or '').upper()
              for name in ('primaryExchange', 'exchange')}
    return currency == 'USD' and bool(venues & US_LISTING_VENUES)


def listing_source_mismatch(security, source: str) -> str:
    """Why this instrument must not be filled from ``source``, or ''."""
    if source not in US_TICKER_SOURCES or is_us_listing(security):
        return ''
    venue = getattr(security, 'primaryExchange', '') or getattr(security, 'exchange', '')
    return (f'{source} serves the US listing of {getattr(security, "symbol", "?")!r}, but conId '
            f'{getattr(security, "conId", "?")} is its {venue or "?"}/'
            f'{getattr(security, "currency", "?")} listing. Storing US bars under that conId '
            f'would mislabel them. Fix the universe entry (re-add the US listing), or fetch '
            f'this listing with --source ib.')
