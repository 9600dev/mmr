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

import re

# IB's spelling of a share class (or a unit): the root, ONE space, one letter.
_IB_ONE_LETTER_SUFFIX = re.compile(r'([A-Z]+) ([A-Z])')


def vendor_ticker(symbol: str) -> str:
    """The spelling TwelveData and Massive use for an IB symbol.

    IB writes a share class after a space — ``BRK B``, ``BF B`` — and both
    vendors write it after a dot (``BRK.B``). Universes hold IB's spelling,
    and it used to be sent verbatim: Massive answers a ticker it does not know
    with an empty result and TwelveData with "not found", so a class-share
    series (BRK B is a top-ten S&P 500 name) was never filled and nothing said
    why. Call this wherever a symbol becomes a vendor request.

    Exact, like every other identifier here:

    * no whitespace in it: returned as given. That covers plain tickers, a
      symbol that already has its dot, and vendor-native forms that are not
      IB's at all (``KKRpD``, ``EUR/USD``, ``C:EURUSD``, ``O:AAPL...``);
    * ``ROOT X`` — capital letters, one space, one capital letter: ``ROOT.X``.
      IB writes a unit the same way (``AIIA U``) and so do the vendors
      (``AIIA.U``);
    * anything else containing whitespace: ``ValueError``. IB's other space
      suffixes — preferreds (``JPM PRC``), warrants (``XYZ WS``), rights
      (``NMZ RT``) — are not spelled with a dot at the vendors and not spelled
      alike by the two of them (a preferred is ``JPMpC``), so nothing is
      guessed, and a string with a space in it is not sent: it cannot be a
      vendor ticker.

    Whitespace around the symbol is dropped. Case is never changed (a vendor
    preferred is marked by a lowercase ``p``).
    """
    if not isinstance(symbol, str):
        raise ValueError(f'{symbol!r} is not a ticker (a ticker is a str; a conId is never one)')
    ticker = symbol.strip()
    if ticker and not any(ch.isspace() for ch in ticker):
        return ticker
    match = _IB_ONE_LETTER_SUFFIX.fullmatch(ticker)
    if match:
        return f'{match.group(1)}.{match.group(2)}'
    raise ValueError(
        f'{symbol!r} has no spelling at a US market-data vendor that MMR knows: only IB\'s '
        f'class-share form "ROOT X" is translated (BRK B -> BRK.B). Preferreds, warrants and '
        f'rights are spelled differently by each vendor; fetch this one with --source ib.')


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
