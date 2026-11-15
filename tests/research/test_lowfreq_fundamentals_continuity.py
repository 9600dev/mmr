"""What the research panel says about a market cap it has no business
knowing (docs/RESEARCH_LOG.md, 2026-11-15 addendum).

Two rules, end to end through ``load_equity_panel``:

* a filing is the latest word for at most ``MAX_DAYS_BETWEEN_FILINGS``. B was
  Barnes Group until January 2025 and has been Barrick since May; Barnes's
  last 10-Q priced Barrick for as long as nothing replaced it.
* a market cap smaller than two typical days of the name's own trading is
  contradicted by the tape (TEVA 2018: a "count" of 2,000,000).
"""
from __future__ import annotations

import duckdb
import pandas as pd
import pytest


def _panel(start='2016-08-01', end='2018-03-01'):
    from trader.research.lowfreq import load_equity_panel
    return load_equity_panel(start, end)


def test_a_filer_that_goes_quiet_has_no_fundamentals_two_hundred_days_on(research_env):
    con = duckdb.connect(str(research_env['main']))
    con.execute("DELETE FROM fundamentals WHERE ticker = 'AAA' AND period_end > DATE '2016-12-31'")
    con.close()
    # AAA's last filing: period 2016-12-31, accepted 2017-02-04 21:30 UTC. 200 days on is 2017-08-23 21:30 UTC.
    fields = _panel().fields
    shares, revenue = fields['shares_diluted']['AAA'], fields['revenue_ttm']['AAA']
    assert shares.loc['2017-08-23'] == pytest.approx(1e8)          # that day's close (20:00 UTC) is before it
    assert pd.isna(shares.loc['2017-08-24'])
    assert pd.notna(revenue.loc['2017-08-23']) and pd.isna(revenue.loc['2017-08-24'])   # the whole filing, not only the count
    assert shares.loc['2017-08-24':].isna().all()
    # ...and a name that kept filing is untouched
    assert fields['shares_diluted']['BBB'].loc['2017-08-24':].notna().all()


def test_the_limit_is_a_named_correction(research_env, monkeypatch):
    con = duckdb.connect(str(research_env['main']))
    con.execute("DELETE FROM fundamentals WHERE ticker = 'AAA' AND period_end > DATE '2016-12-31'")
    con.close()
    monkeypatch.setenv('MMR_FUNDAMENTALS_CORRECTIONS', 'notices,annual_shares,split_basis,units,periods,turnover')
    shares = _panel().fields['shares_diluted']['AAA']
    assert shares.loc['2017-08-24':].notna().all()                 # as it used to be: the last filing, for ever


def test_a_market_cap_the_tape_contradicts_is_unknown(research_env):
    """BBB: 2e8 shares at about $100 is a $20B company. From June 2017 it
    "trades" $200B a day."""
    con = duckdb.connect(str(research_env['hist']))
    con.execute("UPDATE pit_daily_bars SET dollar_volume = dollar_volume * 1000 "
                "WHERE ticker = 'BBB' AND day >= DATE '2017-06-01'")
    con.close()
    fields = _panel().fields
    shares = fields['shares_diluted']['BBB']
    assert shares.loc[:'2017-06-14'].dropna().size > 100           # the typical day is still an ordinary one
    assert shares.loc['2017-06-01':'2017-06-14'].notna().all()     # ten frantic sessions of twenty-one: not typical yet
    assert shares.loc['2017-06-15':].isna().all()                  # the eleventh makes the median
    assert fields['shares_diluted']['AAA'].loc['2017-06-15':].notna().all()


def test_the_tape_check_is_a_named_correction(research_env, monkeypatch):
    con = duckdb.connect(str(research_env['hist']))
    con.execute("UPDATE pit_daily_bars SET dollar_volume = dollar_volume * 1000 "
                "WHERE ticker = 'BBB' AND day >= DATE '2017-06-01'")
    con.close()
    monkeypatch.setenv('MMR_FUNDAMENTALS_CORRECTIONS', 'notices,annual_shares,split_basis,units,periods,continuity')
    assert _panel().fields['shares_diluted']['BBB'].loc['2017-06-15':].notna().all()


def test_a_series_has_no_count_until_its_second_filing(research_env):
    """The first figure of a series is as likely to be wrong as any (TEVA:
    2,000,000; XOM: 4,000,000), and nothing says so until the next one."""
    con = duckdb.connect(str(research_env['main']))
    con.execute("DELETE FROM fundamentals WHERE ticker = 'AAA' AND period_end < DATE '2016-12-31'")
    con.close()
    # AAA's filings now start with period 2016-12-31 (accepted Saturday 2017-02-04) and 2017-03-31
    # (accepted Friday 2017-05-05 at 21:30 UTC, after the close).
    shares = _panel().fields['shares_diluted']['AAA']
    assert shares.loc[:'2017-05-05'].isna().all()
    assert shares.loc['2017-05-08'] == pytest.approx(1e8)
    assert shares.loc['2017-05-08':].notna().all()
