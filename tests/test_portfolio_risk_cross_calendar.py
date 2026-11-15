"""Correlation clustering when positions trade on different exchange calendars.

AUDIT_2026-11-14 B5.8. Daily bars are keyed at the EXCHANGE's local midnight
(US -> 04:00/05:00 UTC, ASX -> 13:00/14:00 UTC the day before). The clustering
built one frame from every position's closes and dropped each row with a gap,
so a single ASX name next to US names left no row at all: no returns, no
clusters, for ANY pair - including the two US names.

Now: closes are aligned on the SESSION DATE, and each pair is correlated over
the sessions both traded, with a stated minimum overlap.
"""
import numpy as np
import pandas as pd
import pytest

from trader.data.data_access import SecurityDefinition
from trader.data.duckdb_store import DuckDBDataStore
from trader.data.universe import Universe, UniverseAccessor
from trader.trading.portfolio_risk import (
    MIN_CORRELATION_RETURNS,
    PortfolioRiskAnalyzer,
    pairwise_return_correlation,
    session_dates,
)

NY = 'America/New_York'
SYDNEY = 'Australia/Sydney'
CONIDS = {'USA': 2001, 'USB': 2002, 'ASX1': 3001}


def _sd(symbol, conid, currency='USD'):
    return SecurityDefinition(
        symbol=symbol, exchange='SMART', conId=conid, secType='STK',
        primaryExchange='', currency=currency, tradingClass=symbol,
        includeExpired=False, secIdType='', secId='', description='', minTick=0.01,
        orderTypes='', validExchanges='', priceMagnifier=1, longName='', category='',
        subcategory='', tradingHours='', timeZoneId='', liquidHours='', stockType='',
        minSize=1.0, sizeIncrement=1.0, suggestedSizeIncrement=1.0, bondType='',
        couponType='', callable=False, putable=False, coupon=0.0, convertable=False,
        maturity='', issueDate='', nextOptionDate='', nextOptionPartial=False,
        nextOptionType='', marketRuleIds='',
    )


def _session_days(n=70):
    """The last ``n`` weekdays ending yesterday, as naive calendar dates."""
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=1)
    return pd.bdate_range(end=end, periods=n)


def _bars(closes, days, tz):
    """Daily bars stamped at ``tz`` midnight of each session day (store convention)."""
    idx = pd.DatetimeIndex(days).tz_localize(tz).tz_convert('UTC')
    idx.name = 'date'
    close = np.asarray(closes, dtype=float)
    return pd.DataFrame({
        'open': close, 'high': close + 0.5, 'low': close - 0.5, 'close': close,
        'volume': 1000.0, 'bar_size': '1 day',
    }, index=idx)


def _prices(start, returns):
    return start * np.cumprod(1 + np.asarray(returns))


@pytest.fixture
def dbs(tmp_path):
    trading_db = str(tmp_path / 'trading.duckdb')
    history_db = str(tmp_path / 'history.duckdb')
    UniverseAccessor(trading_db, '').update(Universe('portfolio', [
        _sd('USA', 2001), _sd('USB', 2002), _sd('ASX1', 3001, 'AUD')]))
    return trading_db, history_db, DuckDBDataStore(history_db)


def _positions():
    return [{'symbol': 'USA', 'marketValue': 20_000}, {'symbol': 'USB', 'marketValue': 15_000},
            {'symbol': 'ASX1', 'marketValue': 10_000}]


def _clusters(trading_db, history_db):
    report = PortfolioRiskAnalyzer(trading_db, history_db).analyze(_positions(), 200_000.0)
    return report, {tuple(c['symbols']): c for c in report.correlation_clusters}


def test_an_asx_holding_does_not_switch_off_clustering_of_the_us_names(dbs):
    trading_db, history_db, store = dbs
    days = _session_days()
    rng = np.random.default_rng(11)
    common = rng.normal(0, 0.02, len(days))
    store.write('2001', _bars(_prices(100, common), days, NY))
    store.write('2002', _bars(_prices(50, common + rng.normal(0, 0.003, len(days))), days, NY))
    store.write('3001', _bars(_prices(30, rng.normal(0, 0.02, len(days))), days, SYDNEY))

    report, clusters = _clusters(trading_db, history_db)
    assert report.data_coverage == 3
    assert set(clusters) == {('USA', 'USB')}                  # was: no clusters at all
    assert clusters[('USA', 'USB')]['avg_corr'] > 0.9


def test_a_cross_calendar_pair_is_correlated_on_the_sessions_both_traded(dbs):
    trading_db, history_db, store = dbs
    days = _session_days()
    rng = np.random.default_rng(12)
    common = rng.normal(0, 0.02, len(days))
    store.write('2001', _bars(_prices(100, common), days, NY))
    store.write('2002', _bars(_prices(50, rng.normal(0, 0.02, len(days))), days, NY))
    # The ASX name moves with USA on the same session dates. It also has its
    # own holidays: three sessions the US traded and it did not.
    asx_days = days.delete([20, 21, 40])
    asx_prices = _prices(30, common + rng.normal(0, 0.003, len(days)))
    store.write('3001', _bars(np.delete(asx_prices, [20, 21, 40]), asx_days, SYDNEY))

    _, clusters = _clusters(trading_db, history_db)
    assert set(clusters) == {('USA', 'ASX1')}
    assert clusters[('USA', 'ASX1')]['avg_corr'] > 0.9
    assert clusters[('USA', 'ASX1')]['combined_weight_pct'] == round(30_000 / 200_000, 4)


def test_a_pair_with_too_few_common_sessions_is_not_clustered(dbs):
    trading_db, history_db, store = dbs
    days = _session_days()
    rng = np.random.default_rng(13)
    common = rng.normal(0, 0.02, len(days))
    store.write('2001', _bars(_prices(100, common), days, NY))
    store.write('2002', _bars(_prices(50, common + rng.normal(0, 0.003, len(days))), days, NY))
    # Perfectly aligned with USA, but only MIN_CORRELATION_RETURNS sessions of
    # it exist: one return short of the minimum overlap.
    recent = days[-MIN_CORRELATION_RETURNS:]
    store.write('3001', _bars(_prices(100, common)[-MIN_CORRELATION_RETURNS:], recent, SYDNEY))

    _, clusters = _clusters(trading_db, history_db)
    assert set(clusters) == {('USA', 'USB')}       # ASX1 left out; the US pair unaffected


# ---------------------------------------------------------------------------
# The two pure pieces
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('tz,day', [
    (NY, '2026-01-15'),             # EST: 05:00 UTC the same day
    (NY, '2026-07-15'),             # EDT: 04:00 UTC the same day
    (SYDNEY, '2026-01-15'),         # AEDT: 13:00 UTC the day BEFORE
    (SYDNEY, '2026-07-15'),         # AEST: 14:00 UTC the day before
    ('Asia/Tokyo', '2026-03-02'),
    ('Asia/Hong_Kong', '2026-03-02'),
    ('Europe/London', '2026-07-15'),
    ('Europe/Berlin', '2026-01-15'),
    ('America/Toronto', '2026-07-15'),
    ('America/Chicago', '2026-01-15'),
    ('Pacific/Auckland', '2026-01-15'),   # NZDT, UTC+13: 11:00 UTC the day before
    ('Pacific/Auckland', '2026-07-15'),   # NZST, UTC+12
    ('Pacific/Honolulu', '2026-07-15'),   # UTC-10
    ('UTC', '2026-07-15'),                # legacy UTC-midnight rows
])
def test_session_date_of_a_bar_stamped_at_exchange_midnight(tz, day):
    stamp = pd.DatetimeIndex([pd.Timestamp(day, tz=tz)]).tz_convert('UTC')
    assert list(session_dates(stamp)) == [pd.Timestamp(day)]


def test_pairwise_correlation_uses_returns_between_common_sessions():
    days = pd.bdate_range('2026-06-01', periods=40)
    rng = np.random.default_rng(3)
    a = _prices(100, rng.normal(0, 0.02, 40))
    b = a * 2.0                                   # the same returns, by construction
    frame = pd.DataFrame({'A': a, 'B': b}, index=days)
    frame.loc[days[10], 'B'] = np.nan             # B's holiday
    frame.loc[days[25], 'A'] = np.nan             # A's holiday
    corr = pairwise_return_correlation(frame, MIN_CORRELATION_RETURNS)
    # Returns are taken between sessions BOTH traded, so both sides span the
    # same interval across each holiday and the pair is still exactly 1.
    assert corr.loc['A', 'B'] == pytest.approx(1.0)
    assert corr.loc['B', 'A'] == pytest.approx(1.0)


def test_pairwise_correlation_is_nan_below_the_minimum_overlap():
    days = pd.bdate_range('2026-06-01', periods=MIN_CORRELATION_RETURNS + 1)
    rng = np.random.default_rng(4)
    a = _prices(100, rng.normal(0, 0.02, len(days)))
    frame = pd.DataFrame({'A': a, 'B': a * 2.0}, index=days)
    assert pairwise_return_correlation(frame, MIN_CORRELATION_RETURNS).loc['A', 'B'] == pytest.approx(1.0)
    short = frame.iloc[1:]                         # one return fewer than required
    assert np.isnan(pairwise_return_correlation(short, MIN_CORRELATION_RETURNS).loc['A', 'B'])


def test_pairwise_correlation_matches_the_joint_frame_when_calendars_agree():
    days = pd.bdate_range('2026-06-01', periods=60)
    rng = np.random.default_rng(5)
    frame = pd.DataFrame({name: _prices(100, rng.normal(0, 0.02, 60)) for name in 'ABCD'},
                         index=days)
    expected = frame.pct_change().dropna().corr()
    got = pairwise_return_correlation(frame, MIN_CORRELATION_RETURNS)
    for i in 'ABCD':
        for j in 'ABCD':
            if i != j:
                assert got.loc[i, j] == pytest.approx(expected.loc[i, j], abs=1e-12)
