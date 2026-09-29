"""D03: loaders read only the LOWFREQ_RESEARCH_CONFIG databases, read-only."""
from __future__ import annotations

import os

import duckdb
import numpy as np
import pandas as pd
import pytest

from trader.research.lowfreq import config as cfg
from trader.research.lowfreq import load_etf_panel
from research.conftest import TICKERS


def test_etf_panel_is_total_return_and_nan_before_first_bar(research_env):
    p = load_etf_panel(None, '2015-01-01', '2017-12-31')
    assert list(p.close.columns) == sorted(TICKERS)
    assert p.open.shape == p.close.shape and p.close.index.equals(p.open.index)
    # GLD is listed 200 sessions late: NaN before its first bar, finite after.
    gld = p.close['GLD']
    assert gld.iloc[:200].isna().all() and gld.iloc[200:].notna().all()
    # total-return, not raw close: the tr series starts at 100 by construction
    assert p.close['SPY'].iloc[0] == pytest.approx(100.0)
    # dividends make tr_close drift away from close for SPY
    con = duckdb.connect(str(research_env['hist']), read_only=True)
    raw = con.execute("select close, tr_close from etf_daily_bars where ticker='SPY' order by day").fetchdf()
    con.close()
    assert not np.allclose(raw['close'] / raw['close'].iloc[0], raw['tr_close'] / raw['tr_close'].iloc[0])
    assert np.allclose(p.close['SPY'].to_numpy(), raw['tr_close'].to_numpy())


def test_etf_panel_subset_and_missing_ticker(research_env):
    p = load_etf_panel(['SPY', 'IEF'], '2015-01-01', '2016-01-01')
    assert list(p.close.columns) == ['SPY', 'IEF']
    with pytest.raises(ValueError, match='no rows for'):
        load_etf_panel(['SPY', 'NOPE'], '2015-01-01', '2016-01-01')


def test_loader_reads_only_the_configured_db_read_only(research_env, monkeypatch):
    opened = []
    real = duckdb.connect

    def spy(path, *a, **kw):
        opened.append((os.path.realpath(str(path)), kw.get('read_only')))
        return real(path, *a, **kw)
    monkeypatch.setattr(cfg.duckdb, 'connect', spy)
    load_etf_panel(None, '2015-01-01', '2016-01-01')
    assert opened, 'no database was opened'
    assert {p for p, _ in opened} == {os.path.realpath(str(research_env['hist']))}
    assert all(ro is True for _, ro in opened)
    # and the file was not modified
    before = research_env['hist'].stat().st_mtime_ns
    load_etf_panel(None, '2015-01-01', '2016-01-01')
    assert research_env['hist'].stat().st_mtime_ns == before


def test_missing_config_fails_loudly(research_env, monkeypatch):
    monkeypatch.setenv('LOWFREQ_RESEARCH_CONFIG', str(research_env['tmp'] / 'absent.yaml'))
    with pytest.raises(FileNotFoundError):
        load_etf_panel(None, '2015-01-01', '2016-01-01')


def test_read_only_store_wrapper_forces_read_only(research_env):
    from trader.data import duckdb_store
    from trader.data.duckdb_store import DuckDBConnection
    db = DuckDBConnection(str(research_env['hist']))
    with cfg.read_only_duckdb_store():
        assert db.execute('select count(*) from etf_daily_bars', fetch='one')[0] > 0
        with pytest.raises(Exception):
            db.execute("insert into etf_daily_bars values ('2030-01-01','X',1,1,1,1,1,0,1,1,1)")
    assert duckdb_store.duckdb.connect is duckdb.connect


def test_bound_container_points_at_research_config(research_env):
    from trader.container import Container
    with cfg.bound_container():
        c = Container.instance().config()
        assert os.path.realpath(os.path.expanduser(c['history_duckdb_path'])) == os.path.realpath(str(research_env['hist']))
    assert os.environ.get('TRADER_CONFIG') != str(research_env['config'])


def _spy_connects(monkeypatch):
    from trader.data import duckdb_store
    opened = []
    real = duckdb.connect

    def spy(path, *a, **kw):
        opened.append((os.path.realpath(str(path)), kw.get('read_only')))
        return real(path, *a, **kw)
    monkeypatch.setattr(duckdb_store.duckdb, 'connect', spy)
    monkeypatch.setattr(cfg.duckdb, 'connect', spy)
    return opened


def test_equity_panel_wraps_pit_and_fundamentals_as_of_and_read_only(research_env, monkeypatch):
    from trader.research.lowfreq import load_equity_panel
    from research.conftest import PIT_TICKERS
    opened = _spy_connects(monkeypatch)
    p = load_equity_panel('2016-08-01', '2018-03-01')
    # every connection on the way: one of the two research DBs, read-only
    assert opened
    assert {path for path, _ in opened} <= {os.path.realpath(str(research_env['hist'])),
                                            os.path.realpath(str(research_env['main']))}
    assert all(ro is True for _, ro in opened)
    # membership is lagged a month: the panel starts in September, holds all three names
    assert sorted(p.close.columns) == sorted(PIT_TICKERS)
    assert p.close.index[0] >= pd.Timestamp('2016-09-01')
    assert p.open.shape == p.close.shape and p.close.notna().all().all()
    assert p.universe == 'equity'
    # as-of fundamentals: shares_diluted is NaN until the first ACCEPTANCE instant, then the filed value
    shares = p.fields['shares_diluted']['AAA']
    first_accept = pd.Timestamp('2016-03-31') + pd.Timedelta(days=35)     # 2016-05-05 21:30 UTC
    assert shares.loc[shares.index > first_accept + pd.Timedelta(days=1)].iloc[0] == pytest.approx(1e8)
    # sales_to_price needs four accepted quarters (TTM): NaN before the 4th acceptance, finite after,
    # and equal to revenue_ttm / (close * shares) on a sample day
    s2p = p.signals['sales_to_price']['AAA']
    fourth_accept = pd.Timestamp('2016-12-31') + pd.Timedelta(days=35)   # 2017-02-04
    assert s2p.loc[s2p.index < fourth_accept].isna().all()
    after = s2p.loc[s2p.index > fourth_accept + pd.Timedelta(days=2)]
    assert after.notna().all()
    day = after.index[10]
    expect = p.fields['revenue_ttm']['AAA'].loc[day] / (p.close['AAA'].loc[day] * shares.loc[day])
    assert s2p.loc[day] == pytest.approx(expect)
    # capacity input present and aligned
    assert p.fields['dollar_volume'].shape == p.close.shape
    assert p.fields['dollar_volume']['CCC'].iloc[-1] == pytest.approx(p.close['CCC'].iloc[-1] * 3e6)
    # CCC leaves the top 500 in August 2017: its PRICES persist (so it can be sold) while
    # the member mask turns False; AAA/BBB are members throughout
    m = p.fields['member']
    assert m.shape == p.close.shape and m.dtypes.eq(bool).all()
    assert m['CCC'].loc['2016-10-01':'2017-06-30'].all()
    assert not m['CCC'].loc['2017-09-01':].any()
    assert m[['AAA', 'BBB']].all().all()
    assert p.close['CCC'].loc['2017-09-01':].notna().all()


def test_equity_panel_is_loud_on_an_empty_pit_table(research_env):
    from trader.research.lowfreq import load_equity_panel
    con = duckdb.connect(str(research_env['hist']))
    con.execute('delete from pit_daily_bars')
    con.close()
    with pytest.raises(Exception):
        load_equity_panel('2016-08-01', '2024-06-28')
