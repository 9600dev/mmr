import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
import datetime as dt
import numpy as np
import click
from scipy.stats import norm
from typing import List, cast, Tuple, Any, Dict
import logging
import coloredlogs
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


def get_option_dates(symbol) -> List[str]:
    logging.info('getting option dates for symbol {}'.format(symbol))
    return cast(List[str], yf.Ticker(symbol).options)


def get_chains(symbol: str, date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    def augment(bid: float, df: pd.DataFrame) -> pd.DataFrame:
        df['IV'] = df.impliedVolatility
        df['T'] = (dt.datetime.strptime(date, '%Y-%m-%d') - dt.datetime.now()).days / 255.0
        df['S'] = bid
        df['K'] = df.strike
        return df

    logging.info('getting option chain data for {}'.format(symbol))
    ticker = yf.Ticker(symbol)  # type: ignore
    bid = ticker.get_info()['bid']  # type: ignore
    ask = ticker.get_info()['ask']  # type: ignore

    calls = augment(bid, pd.DataFrame(ticker.option_chain(date=date).calls))
    puts = augment(bid, pd.DataFrame(ticker.option_chain(date=date).puts))

    return (calls, puts)


def get_call_chain(symbol: str, date: str) -> pd.DataFrame:
    return get_chains(symbol, date)[0]


def get_put_chain(symbol: str, date: str) -> pd.DataFrame:
    return get_chains(symbol, date)[1]


def vol_by_strike(polymdl, K):
    return np.poly1d(polymdl)(K)


def new_K(chain: pd.DataFrame):
    # newK = np.arange(chain.K.iloc[0], chain.K.iloc[-1], 0.0001) # new higher resolution strikes
    # newK = np.arange(1.0, chain.K.iloc[-1], 0.0001)
    newK = np.arange(1.0, chain.K.iloc[-1], 0.1)
    return newK


def plot_implied_vol(chain: pd.DataFrame):
    vols = chain.IV.values  # vol
    Ks = chain.K.values  # strikes

    newK = new_K(chain)

    poly = np.polyfit(Ks, vols, 5)  # create implied vol fit
    newVols = np.poly1d(poly)(newK)  # fit model to new higher resolution strikes

    plt.plot(newK, newVols)
    plt.title('Implied Volatility Function')
    plt.xlabel('$K$')
    plt.ylabel('Implied Vol')
    plt.show()


def plot_expiration_cdf(chain: pd.DataFrame, risk_free_rate: float = 0.001):
    df = chain
    S = df.S[0]  # extract S_0
    T = df['T'][0]  # extract T

    r = risk_free_rate

    vols = chain.IV.values  # volatility values
    Ks = chain.K.values  # strikes
    poly = np.polyfit(Ks, vols, 5)  # create implied vol fit
    newK = new_K(chain)
    newVols = np.poly1d(poly)(newK)  # fit model to new higher resolution strikes

    binaries = binary_put(S, newK, T, r, newVols)  # calculating the binaries

    plt.plot(newK, binaries, label='cdf')
    plt.axvline(S, color='black', linestyle='--', label='$S_0$')
    plt.xlabel('$S_T$')
    plt.ylabel('cdf')
    plt.title('Cdf of option chain')
    plt.legend()
    plt.show()


def implied_constant_helper(chain: pd.DataFrame, risk_free_rate: float = 0.001):
    df = chain
    S = df.S[0]
    T = df['T'][0]

    r = risk_free_rate

    logging.info('calculating implied and constant distributions')
    vols = chain.IV.values  # volatility values
    Ks = chain.K.values  # strikes
    poly = np.polyfit(Ks, vols, 5)  # create implied vol fit
    newK = new_K(chain)
    newVols = np.poly1d(poly)(newK)  # fit model to new higher resolution strikes

    binaries = binary_put(S, newK, T, r, newVols)

    PsT = []  # market implied probabilities

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


def plot_market_implied_vs_constant_helper(x, market_implied, constant):
    plt.plot(x[1:], market_implied, color='red', label='Market Implied')
    plt.plot(x[1:], constant, color='black', label='Constant Vol')
    plt.ylabel('$\mathbb{P}$')
    plt.xlabel('$S_T$')
    plt.legend()
    plt.title('Market implied vs Constant')
    plt.show()


def plot_market_implied_vs_constant_console(x, market_implied, constant, title):
    plot(
        [x[1:], x[1:]],
        [constant, market_implied],
        lines=True,
        color=True,
        interactive=True,
        height=55,
        width=100,
        legend_labels=['constant, market_implied'],
        title=title
    )


def implied_constant(symbol: str, date: str, risk_free_rate: float = 0.001) -> Dict[str, Any]:
    calls, puts = get_chains(symbol, date)
    return implied_constant_helper(calls, risk_free_rate)


def plot_implied_constant(symbol, date: str, risk_free_rate: float = 0.001, console = False):
    data = implied_constant(symbol, date, risk_free_rate)
    if console:
        plot_market_implied_vs_constant_console(
            data['x'], data['market_implied'],
            data['constant'],
            '{} for {}, constant vs market implied'.format(symbol, date)
        )
    else:
        plot_market_implied_vs_constant_helper(data['x'], data['market_implied'], data['constant'])


@click.command()
@click.option('--symbol', required=True, help='ticker symbol e.g. FB')
@click.option('--list_dates', required=False, is_flag=True, default=False, help='get the list of expirary dates')
@click.option('--date', required=False, help='option expiry date, format YYYY-MM-DD')
@click.option('--console', required=False, default=False, is_flag=True, help='print histogram to console [default: false]')
@click.option('--risk_free_rate', required=False, default=0.001, help='risk free rate [default 0.001]')
def main(
    symbol: str,
    list_dates: bool,
    date: str,
    console: bool,
    risk_free_rate: float,
):
    if list_dates:
        dates = get_option_dates(symbol)
        for d in dates:
            option_date = dt.datetime.strptime(d, '%Y-%m-%d')
            print('{} [{}]   {} days from today'.format(symbol, d, (option_date - dt.datetime.now()).days))
        return

    if not date:
        dates = get_option_dates(symbol)
        date = dates[0]

    option_date = dt.datetime.strptime(date, '%Y-%m-%d')
    plot_implied_constant(symbol, date, risk_free_rate, console)


if __name__ == '__main__':
    coloredlogs.install('INFO')
    main()