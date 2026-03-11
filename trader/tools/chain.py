"""Options chain analysis — probability distributions from market-implied volatility.

Data sourced from Massive.com API. Keeps the binary options pricing math
(d2, binary_call, binary_put, monte_carlo_binary, implied_constant_helper)
data-source agnostic.
"""

import datetime as dt
import logging
import numpy as np
import pandas as pd

from scipy.stats import norm
from typing import Any, Dict, List, Optional, Tuple
from uniplot.uniplot import plot


def monte_carlo_binary(S, K, T, r, sigma, Q,
                       type_='call', Ndraws=10_000_000, seed=0):
    np.random.seed(seed)
    dS = np.random.normal((r - sigma**2 / 2) * T, sigma * np.sqrt(T), size=Ndraws)
    ST = S * np.exp(dS)
    if type_ == 'call':
        return len(ST[ST > K]) / Ndraws * Q * np.exp(-r * T)
    elif type_ == 'put':
        return len(ST[ST < K]) / Ndraws * Q * np.exp(-r * T)
    else:
        raise ValueError('Type must be put or call')


def d2(S, K, T, r, sigma):
    return (np.log(S / K) + (r - sigma**2 / 2) * T) / (sigma * np.sqrt(T))


def binary_call(S, K, T, r, sigma, Q=1):
    N = norm.cdf
    return np.exp(-r * T) * N(d2(S, K, T, r, sigma)) * Q


def binary_put(S, K, T, r, sigma, Q=1):
    N = norm.cdf
    return np.exp(-r * T) * N(-d2(S, K, T, r, sigma)) * Q


def vol_by_strike(polymdl, K):
    return np.poly1d(polymdl)(K)


def new_K(chain: pd.DataFrame):
    newK = np.arange(1.0, chain.K.iloc[-1], 0.1)
    return newK


def _get_massive_client(api_key: str = ''):
    """Get a Massive REST client, reading api_key from config if not provided."""
    if not api_key:
        from trader.container import Container
        cfg = Container.instance().config()
        api_key = cfg.get('massive_api_key', '')
    if not api_key:
        raise ValueError("massive_api_key not configured in trader.yaml")
    from massive import RESTClient
    return RESTClient(api_key=api_key)


def get_option_dates(symbol: str, api_key: str = '') -> List[str]:
    """Get unique expiration dates for a symbol via Massive API."""
    logging.info('getting option dates for symbol %s', symbol)
    client = _get_massive_client(api_key)
    dates_seen = set()
    for contract in client.list_options_contracts(
        underlying_ticker=symbol,
        expired=False,
        limit=1000,
        sort='expiration_date',
        order='asc',
    ):
        if contract.expiration_date and contract.expiration_date not in dates_seen:
            dates_seen.add(contract.expiration_date)
    return sorted(dates_seen)


def get_chains(symbol: str, date: str, api_key: str = '') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get call and put chain DataFrames with IV, T, S, K columns via Massive API."""
    logging.info('getting option chain data for %s expiring %s', symbol, date)
    client = _get_massive_client(api_key)

    rows = []
    for snap in client.list_snapshot_options_chain(
        underlying_asset=symbol,
        params={'expiration_date': date},
    ):
        details = snap.details
        if not details or not details.strike_price:
            continue

        iv = snap.implied_volatility or 0.0
        strike = details.strike_price
        contract_type = (details.contract_type or '').lower()

        bid = 0.0
        ask = 0.0
        if snap.last_quote:
            bid = snap.last_quote.bid or 0.0
            ask = snap.last_quote.ask or 0.0

        underlying_price = 0.0
        if snap.underlying_asset:
            underlying_price = snap.underlying_asset.price or 0.0

        rows.append({
            'type': contract_type,
            'strike': strike,
            'IV': iv,
            'bid': bid,
            'ask': ask,
            'underlying_price': underlying_price,
            'T': (dt.datetime.strptime(date, '%Y-%m-%d') - dt.datetime.now()).days / 255.0,
            'S': underlying_price,
            'K': strike,
        })

    if not rows:
        empty = pd.DataFrame(columns=['type', 'strike', 'IV', 'bid', 'ask',
                                       'underlying_price', 'T', 'S', 'K'])
        return (empty, empty.copy())

    df = pd.DataFrame(rows)
    calls = df[df['type'] == 'call'].sort_values('K').reset_index(drop=True)
    puts = df[df['type'] == 'put'].sort_values('K').reset_index(drop=True)
    return (calls, puts)


def get_call_chain(symbol: str, date: str, api_key: str = '') -> pd.DataFrame:
    return get_chains(symbol, date, api_key)[0]


def get_put_chain(symbol: str, date: str, api_key: str = '') -> pd.DataFrame:
    return get_chains(symbol, date, api_key)[1]


def implied_constant_helper(chain: pd.DataFrame, risk_free_rate: float = 0.001):
    df = chain
    S = df.S.iloc[0]
    T = df['T'].iloc[0]

    r = risk_free_rate

    logging.info('calculating implied and constant distributions')
    vols = chain.IV.values
    Ks = chain.K.values
    poly = np.polyfit(Ks, vols, 5)
    newK = new_K(chain)
    newVols = np.poly1d(poly)(newK)

    binaries = binary_put(S, newK, T, r, newVols)

    PsT = []
    for i in range(1, len(binaries)):
        p = binaries[i] - binaries[i - 1]
        PsT.append(p)

    constant_vol = vol_by_strike(poly, S)
    binaries_const = binary_put(S, newK, T, r, constant_vol)

    const_p = []
    for i in range(1, len(binaries_const)):
        p = binaries_const[i] - binaries_const[i - 1]
        const_p.append(p)

    logging.debug('finished calculating')
    return {
        'x': newK,
        'market_implied': PsT,
        'constant': const_p,
    }


def plot_market_implied_vs_constant_console(x, market_implied, constant, title):
    plot(
        [x[1:], x[1:]],
        [constant, market_implied],
        lines=True,
        color=True,
        interactive=True,
        height=55,
        width=100,
        legend_labels=['constant', 'market_implied'],
        title=title
    )


def implied_constant(symbol: str, date: str, risk_free_rate: float = 0.001,
                     api_key: str = '') -> Dict[str, Any]:
    calls, puts = get_chains(symbol, date, api_key)
    if calls.empty:
        raise ValueError(f'No call chain data for {symbol} expiring {date}')
    return implied_constant_helper(calls, risk_free_rate)


def plot_chain(
    symbol: str,
    list_dates: bool,
    date: str,
    risk_free_rate: float = 0.001,
    api_key: str = '',
):
    if list_dates:
        dates = get_option_dates(symbol, api_key)
        for d in dates:
            option_date = dt.datetime.strptime(d, '%Y-%m-%d')
            print('{} [{}]   {} days from today'.format(symbol, d, (option_date - dt.datetime.now()).days))
        return

    if not date:
        dates = get_option_dates(symbol, api_key)
        if not dates:
            print(f'No option dates found for {symbol}')
            return
        date = dates[0]

    data = implied_constant(symbol, date, risk_free_rate, api_key)
    plot_market_implied_vs_constant_console(
        data['x'], data['market_implied'],
        data['constant'],
        '{} for {}, constant vs market implied'.format(symbol, date)
    )


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print('Usage: python -m trader.tools.chain SYMBOL [--date YYYY-MM-DD] [--list-dates] [--risk-free-rate 0.05]')
        sys.exit(1)

    symbol = sys.argv[1]
    list_dates = '--list-dates' in sys.argv
    date = ''
    rfr = 0.05
    for i, arg in enumerate(sys.argv):
        if arg == '--date' and i + 1 < len(sys.argv):
            date = sys.argv[i + 1]
        if arg == '--risk-free-rate' and i + 1 < len(sys.argv):
            rfr = float(sys.argv[i + 1])

    plot_chain(symbol, list_dates, date, rfr)
