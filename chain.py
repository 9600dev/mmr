import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
import datetime as dt
import numpy as np
from scipy.stats import norm
from typing import List, cast, Tuple, Any, Dict


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
    return (np.log(S/K) + (r - sigma**2 / 2) * T) / (sigma * np.sqrt(T))


def binary_call(S, K, T, r, sigma, Q=1):
    N = norm.cdf
    return np.exp(-r*T) * N(d2(S, K, T, r, sigma)) * Q


def binary_put(S, K, T, r, sigma, Q=1):
    N = norm.cdf
    return np.exp(-r*T)* N(-d2(S, K, T, r, sigma)) * Q


def get_option_dates(symbol) -> List[str]:
    return cast(List[str], yf.Ticker(symbol).options)


def get_chains(symbol: str, date: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    def augment(bid: float, df: pd.DataFrame) -> pd.DataFrame:
        df['IV'] = df.impliedVolatility
        df['T'] = (dt.datetime.strptime(date, '%Y-%m-%d') - dt.datetime.now()).days / 255.0
        df['S'] = bid
        df['K'] = df.strike
        return df

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
    newK = np.arange(1.0, chain.K.iloc[-1], 0.0001)
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



def implied_constant(symbol: str, date: str, risk_free_rate: float = 0.001) -> Dict[str, Any]:
    calls, puts = get_chains(symbol, date)
    return implied_constant_helper(calls, risk_free_rate)


def plot_implied_constant(symbol, date: str, risk_free_rate: float = 0.001):
    data = implied_constant(symbol, date, risk_free_rate)
    plot_market_implied_vs_constant_helper(data['x'], data['market_implied'], data['constant'])

