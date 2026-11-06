"""Correlation clustering against a real (temp) DuckDB: the I/O path.

``PortfolioRiskAnalyzer._compute_correlations`` resolves symbol -> conId from
the universe store and reads ~60 daily closes per position from the history
DB. These tests pin WHAT it reads (1-day bars or NULL bar_size only, last 90
days, conId key with ticker fallback, symbols without data skipped) and the
clusters that come out, so the batched single-query read that replaced the
per-symbol N+1 reads (dashboard: 15 positions -> 1.6s) cannot change results.
"""
import datetime as dt

import numpy as np
import pandas as pd
import pytest

from trader.data.data_access import SecurityDefinition
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import Universe, UniverseAccessor
from trader.trading.portfolio_risk import PortfolioRiskAnalyzer

SYMBOLS = ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF']
CONIDS = {'AAA': 1001, 'BBB': 1002, 'CCC': 1003, 'DDD': 1004, 'FFF': 1006}  # EEE: ticker-keyed


def _daily_index(n=80):
    end = pd.Timestamp.now(tz='UTC').normalize() - pd.Timedelta(days=1)
    return pd.bdate_range(end=end, periods=n, tz='UTC', name='date')


def _bars(close, idx, bar_size='1 day'):
    close = np.asarray(close, dtype=float)
    return pd.DataFrame({
        'open': close, 'high': close + 0.5, 'low': close - 0.5, 'close': close,
        'volume': 1000.0, 'bar_size': bar_size,
    }, index=idx)


def _sd(symbol, conid):
    return SecurityDefinition(
        symbol=symbol, exchange='SMART', conId=conid, secType='STK',
        primaryExchange='NASDAQ', currency='USD', tradingClass=symbol,
        includeExpired=False, secIdType='', secId='', description='', minTick=0.01,
        orderTypes='', validExchanges='', priceMagnifier=1, longName='', category='',
        subcategory='', tradingHours='', timeZoneId='', liquidHours='', stockType='',
        minSize=1.0, sizeIncrement=1.0, suggestedSizeIncrement=1.0, bondType='',
        couponType='', callable=False, putable=False, coupon=0.0, convertable=False,
        maturity='', issueDate='', nextOptionDate='', nextOptionPartial=False,
        nextOptionType='', marketRuleIds='',
    )


@pytest.fixture
def risk_dbs(tmp_path):
    """(trading_db, history_db, expected_closes) with a known correlation structure."""
    trading_db = str(tmp_path / 'trading.duckdb')
    history_db = str(tmp_path / 'history.duckdb')
    ua = UniverseAccessor(trading_db, '')
    ua.update(Universe('portfolio', [_sd(s, c) for s, c in CONIDS.items()]))

    rng = np.random.default_rng(5)
    idx = _daily_index()
    ra = rng.normal(0, 0.02, len(idx))
    rc = rng.normal(0, 0.02, len(idx))
    closes = {
        'AAA': 100 * np.cumprod(1 + ra),
        'BBB': 50 * np.cumprod(1 + ra + rng.normal(0, 0.003, len(idx))),   # ~AAA
        'CCC': 80 * np.cumprod(1 + rc),
        'EEE': 20 * np.cumprod(1 + rc + rng.normal(0, 0.003, len(idx))),   # ~CCC
        'FFF': 30 * np.cumprod(1 + rng.normal(0, 0.02, len(idx))),          # independent
    }
    store = DuckDBDataStore(history_db)
    store.write('1001', _bars(closes['AAA'], idx))
    store.write('1002', _bars(closes['BBB'], idx))
    store.write('1003', _bars(closes['CCC'], idx, bar_size=None))   # NULL bar_size counts
    store.write('EEE', _bars(closes['EEE'], idx))                    # no conId -> ticker key
    store.write('1006', _bars(closes['FFF'], idx))
    # Noise that must NOT be read: intraday bars for AAA inside the window,
    # and ancient daily bars (outside the 90-day lookback) for BBB.
    intraday = pd.date_range(idx[-3], periods=50, freq='1min', tz='UTC', name='date')
    store.write('1001', _bars(np.full(len(intraday), 1e6), intraday, bar_size='1 min'))
    ancient = pd.bdate_range(end=idx[0] - pd.Timedelta(days=200), periods=30, tz='UTC',
                             name='date')
    store.write('1002', _bars(np.full(len(ancient), 1e-3), ancient))
    # DDD is in the universe but has no history at all.
    return trading_db, history_db, closes, idx


def _positions():
    vals = {'AAA': 20_000, 'BBB': 15_000, 'CCC': 12_000, 'DDD': 8_000, 'EEE': 9_000, 'FFF': 5_000}
    return [{'symbol': s, 'marketValue': vals[s]} for s in SYMBOLS]


def _expected_clusters(closes, idx):
    """Independent re-derivation of the clustering contract with pandas."""
    start = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=90)
    series = {}
    for s in ['AAA', 'BBB', 'CCC', 'EEE', 'FFF']:
        ser = pd.Series(closes[s], index=idx)
        series[s] = ser[ser.index >= start].tail(60)
    rets = pd.DataFrame(series).dropna().pct_change().dropna()
    corr = rets.corr()
    return {
        ('AAA', 'BBB'): round(float(corr.loc['AAA', 'BBB']), 3),
        ('CCC', 'EEE'): round(float(corr.loc['CCC', 'EEE']), 3),
    }


def test_correlation_clusters_from_real_duckdb(risk_dbs):
    trading_db, history_db, closes, idx = risk_dbs
    report = PortfolioRiskAnalyzer(trading_db, history_db).analyze(_positions(), 200_000.0)
    assert report.data_coverage == 5            # DDD has no data
    got = {tuple(c['symbols']): c['avg_corr'] for c in report.correlation_clusters}
    assert got == _expected_clusters(closes, idx)
    by_syms = {tuple(c['symbols']): c for c in report.correlation_clusters}
    assert by_syms[('AAA', 'BBB')]['combined_weight_pct'] == round((20_000 + 15_000) / 200_000, 4)


def test_read_many_matches_per_symbol_read(risk_dbs):
    _, history_db, _, _ = risk_dbs
    store = DuckDBDataStore(history_db)
    start = dt.datetime.now() - dt.timedelta(days=90)
    keys = ['1001', '1003', 'EEE', '1004', 'nope', '1001']
    batch = store.read_many(keys, start=start, bar_size='1 day')
    assert set(batch) == {'1001', '1003', 'EEE'}     # no-data keys are absent
    for k in ('1001', '1003', 'EEE'):
        single = store.read(k, start=start, bar_size='1 day')
        # Values, columns, index and order are identical. Dtypes may differ
        # where a column is all-NULL for one symbol or NULL-representation (NaN vs
        # None) (DuckDB types the batch result set as a whole) — documented on read_many.
        b = batch[k]
        assert list(b.columns) == list(single.columns)
        pd.testing.assert_index_equal(b.index, single.index)
        for col in single.columns:
            assert (b[col].isna().to_numpy() == single[col].isna().to_numpy()).all(), col
            mask = single[col].notna().to_numpy()
            assert b[col].to_numpy()[mask].tolist() == single[col].to_numpy()[mask].tolist(), col
        pd.testing.assert_series_equal(b['close'], single['close'])


def test_read_many_empty_inputs(risk_dbs):
    _, history_db, _, _ = risk_dbs
    store = DuckDBDataStore(history_db)
    assert store.read_many([]) == {}
    assert store.read_many(['nope']) == {}


# ---------------------------------------------------------------------------
# Opt-in TTL cache (web dashboard) + query count
# ---------------------------------------------------------------------------

class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _count_reads(monkeypatch):
    calls = {'read_many': 0, 'read': 0, 'keys': []}
    real_many, real_read = DuckDBDataStore.read_many, DuckDBDataStore.read

    def read_many(self, symbols, *a, **kw):
        calls['read_many'] += 1
        calls['keys'].append(list(symbols))
        return real_many(self, symbols, *a, **kw)

    def read(self, *a, **kw):
        calls['read'] += 1
        return real_read(self, *a, **kw)

    monkeypatch.setattr(DuckDBDataStore, 'read_many', read_many)
    monkeypatch.setattr(DuckDBDataStore, 'read', read)
    return calls


def test_history_read_is_one_batched_query_not_n_plus_1(risk_dbs, monkeypatch):
    trading_db, history_db, _, _ = risk_dbs
    calls = _count_reads(monkeypatch)
    PortfolioRiskAnalyzer(trading_db, history_db).analyze(_positions(), 200_000.0)
    assert calls['read'] == 0
    assert calls['read_many'] == 1
    assert sorted(calls['keys'][0]) == sorted(['1001', '1002', '1003', '1004', 'EEE', '1006'])


def test_cached_report_identical_and_skips_db_within_ttl(risk_dbs, monkeypatch):
    from trader.trading.portfolio_risk import CorrelationHistoryCache
    trading_db, history_db, _, _ = risk_dbs
    uncached = PortfolioRiskAnalyzer(trading_db, history_db).analyze(_positions(), 200_000.0)

    clock = _Clock()
    cache = CorrelationHistoryCache(ttl_seconds=300, clock=clock)
    calls = _count_reads(monkeypatch)
    first = PortfolioRiskAnalyzer(trading_db, history_db, history_cache=cache).analyze(
        _positions(), 200_000.0)
    assert first.to_dict() == uncached.to_dict()
    assert calls['read_many'] == 1

    # Within TTL: no history read at all, identical report; live inputs
    # (net liq) still flow through.
    clock.t += 299
    second = PortfolioRiskAnalyzer(trading_db, history_db, history_cache=cache).analyze(
        _positions(), 200_000.0)
    assert calls['read_many'] == 1
    assert second.to_dict() == uncached.to_dict()
    halved = PortfolioRiskAnalyzer(trading_db, history_db, history_cache=cache).analyze(
        _positions(), 100_000.0)
    assert halved.gross_exposure_pct == pytest.approx(2 * uncached.gross_exposure_pct, abs=1e-4)
    assert calls['read_many'] == 1

    # Past TTL: re-read.
    clock.t += 2
    PortfolioRiskAnalyzer(trading_db, history_db, history_cache=cache).analyze(
        _positions(), 200_000.0)
    assert calls['read_many'] == 2


def test_cache_reads_only_new_symbols(risk_dbs, monkeypatch):
    from trader.trading.portfolio_risk import CorrelationHistoryCache
    trading_db, history_db, _, _ = risk_dbs
    cache = CorrelationHistoryCache(ttl_seconds=300, clock=_Clock())
    calls = _count_reads(monkeypatch)
    few = [p for p in _positions() if p['symbol'] in ('AAA', 'BBB')]
    PortfolioRiskAnalyzer(trading_db, history_db, history_cache=cache).analyze(few, 200_000.0)
    PortfolioRiskAnalyzer(trading_db, history_db, history_cache=cache).analyze(_positions(), 200_000.0)
    assert calls['read_many'] == 2
    assert sorted(calls['keys'][1]) == sorted(['1003', '1004', 'EEE', '1006'])


def test_cache_rejects_bad_ttl():
    from trader.trading.portfolio_risk import CorrelationHistoryCache
    for bad in (-1, float('nan'), float('inf')):
        with pytest.raises(ValueError):
            CorrelationHistoryCache(ttl_seconds=bad)
