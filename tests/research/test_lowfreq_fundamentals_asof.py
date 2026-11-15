"""Fundamentals are usable from the first session whose 16:00 America/New_York
close is AFTER the EDGAR acceptance instant - daylight-saving aware. A fixed
21:00 UTC close treated a filing accepted at 20:30 UTC on a summer day as known
at that day's close (16:30 EDT: it was not), one session early."""
from __future__ import annotations

import datetime as dt
import importlib
import sys

import duckdb
import pandas as pd
import pytest

from trader.research.lowfreq import config as cfg


def _fs():
    scripts = str(cfg.repo_root() / 'scripts')
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    return importlib.import_module('fundamental_signals')


def test_session_close_is_16_00_new_york_in_summer_and_winter():
    close = _fs().session_close_utc(pd.DatetimeIndex(['2017-07-14', '2017-12-14', '2017-03-10', '2017-03-13']))
    assert [str(t) for t in close] == ['2017-07-14 20:00:00+00:00', '2017-12-14 21:00:00+00:00',
                                       '2017-03-10 21:00:00+00:00', '2017-03-13 20:00:00+00:00']  # DST began 2017-03-12


def _add_bbb_filing(path, accepted_utc: str, shares: float, period_end: str):
    acc = pd.Timestamp(accepted_utc).to_pydatetime().replace(tzinfo=dt.timezone.utc)
    con = duckdb.connect(str(path))
    con.execute('INSERT INTO fundamentals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                ('1001', 'BBB', pd.Timestamp(period_end).date(), acc, '10-Q', f'acc-dst-{accepted_utc}',
                 2e9, 2e8, 8e8, 3e8, 6e9, 3e9, 3e9, 2.4e8, shares, acc))
    con.close()


def test_as_of_uses_the_dst_aware_close(research_env):
    from trader.research.lowfreq import load_equity_panel
    # The share counts are markers for WHICH filing is in force. They step by
    # 10%: a count that jumps 3.5x between filings is what a unit error looks
    # like, and the loader (rightly) holds such a figure back
    # (share_basis.plausible_share_counts) - which is not what is under test.
    _add_bbb_filing(research_env['main'], '2017-07-14 20:30:00', 2.2e8, '2017-06-30')   # 16:30 EDT: after close
    _add_bbb_filing(research_env['main'], '2017-12-14 20:30:00', 2.4e8, '2017-09-30')   # 15:30 EST: before close
    _add_bbb_filing(research_env['main'], '2018-01-18 21:00:00', 2.6e8, '2017-12-31')   # exactly AT the EST close
    shares = load_equity_panel('2016-08-01', '2018-03-01').fields['shares_diluted']['BBB']
    assert shares.loc['2017-07-14'] == pytest.approx(2e8), 'accepted after the 20:00 UTC summer close: not yet known'
    assert shares.loc['2017-07-17'] == pytest.approx(2.2e8)
    assert shares.loc['2017-12-13'] == pytest.approx(2e8)
    assert shares.loc['2017-12-14'] == pytest.approx(2.4e8), 'accepted 15:30 EST, before the winter close: known same day'
    assert shares.loc['2018-01-18'] == pytest.approx(2.4e8), 'accepted at the close instant is not strictly before it'
    assert shares.loc['2018-01-19'] == pytest.approx(2.6e8)


def test_session_close_honours_early_closes():
    close = _fs().session_close_utc(pd.DatetimeIndex(['2017-11-24', '2017-07-03', '2017-11-27', '2019-12-24']))
    assert [str(t) for t in close] == ['2017-11-24 18:00:00+00:00',   # day after Thanksgiving: 13:00 EST
                                       '2017-07-03 17:00:00+00:00',   # 13:00 EDT
                                       '2017-11-27 21:00:00+00:00',
                                       '2019-12-24 18:00:00+00:00']


def test_as_of_uses_the_early_close_on_a_half_day(research_env):
    """2017-11-24 closed at 13:00 EST (18:00 UTC): a filing accepted 19:30 UTC is
    after the real close - not known that session, known the next one."""
    from trader.research.lowfreq import load_equity_panel
    _add_bbb_filing(research_env['main'], '2017-11-24 19:30:00', 6e8, '2017-09-30')
    shares = load_equity_panel('2016-08-01', '2018-03-01').fields['shares_diluted']['BBB']
    assert shares.loc['2017-11-24'] == pytest.approx(2e8), 'accepted after the 18:00 UTC early close'
    assert shares.loc['2017-11-27'] == pytest.approx(6e8)
