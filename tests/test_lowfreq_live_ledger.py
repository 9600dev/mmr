"""Paper ledger (C02 criterion 4) on a temporary DuckDB with in-memory bars."""
from __future__ import annotations

import datetime as dt
import json

import numpy as np
import pandas as pd
import pytest

from trader.lowfreq_live import ledger as L
from trader.lowfreq_live import targets as T


def _doc(sleeve, as_of, weights, capital=10_000.0):
    return {'schema': 'mmr.targets.v1', 'sleeve': sleeve, 'as_of': as_of, 'generated_at': '2026-09-01T21:00:00+00:00',
            'strategy': {'family': 'equity', 'id': 's2p_top_quintile', 'module': 'm', 'class': 'C', 'params': {},
                         'source_sha256': 'a' * 64},
            'capital_usd': capital, 'prices_as_of': as_of, 'weights': weights, 'notes': ''}


def _write(tdir, sleeve, as_of, weights, bench):
    res = T.TargetsResult(document=_doc(sleeve, as_of, weights), benchmarks={'eq_ew_covered': bench})
    T.write_targets(res, targets_dir=str(tdir))


@pytest.fixture
def world(tmp_path):
    days = pd.bdate_range('2026-09-01', periods=8)          # 09-01 .. 09-10
    closes = pd.DataFrame({'AAA': [10, 10, 11, 12, 12, 13, 13, 14.0],
                           'BBB': [20, 20, 20, 21, 22, 22, np.nan, 24.0]}, index=days)
    opens = closes.shift(1).fillna(closes.iloc[0])
    bars = L.FrameBars(opens, closes)
    tdir = tmp_path / 'targets'
    _write(tdir, 'paper_s2p', '2026-09-01', {'AAA': 0.5}, {'AAA': 0.5, 'BBB': 0.5})
    store = L.LedgerStore(str(tmp_path / 'ledger.duckdb'))
    return {'bars': bars, 'tdir': tdir, 'store': store, 'days': days, 'closes': closes, 'opens': opens}


def test_track_appends_one_row_per_session_and_is_append_only(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    # the sleeve bought 500 AAA at the t+1 open (10.0) with $1 commission
    store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 2), 'AAA', 'BUY', 500, 10.0, 1.0))
    res = L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert res['appended'] == 4 and res['disagreements'] == []
    led = store.ledger('paper_s2p')
    assert len(led) == 4
    r1 = led.loc[pd.Timestamp('2026-09-01')]
    assert r1['sleeve_value'] == pytest.approx(10_000.0) and r1['n_holdings'] == 0       # decision day: cash only
    r2 = led.loc[pd.Timestamp('2026-09-02')]
    assert r2['cash'] == pytest.approx(10_000 - 5_000 - 1) and r2['holdings_value'] == pytest.approx(500 * 10.0)
    r4 = led.loc[pd.Timestamp('2026-09-04')]
    assert r4['sleeve_value'] == pytest.approx(4_999 + 500 * 12.0)
    # virtual benchmark: 50/50 filled at the 09-02 opens (10, 20) with 5 bps + $0.005/share;
    # fully invested, so the D06 guard scales the buys pro rata to the cash (cash ends at 0)
    slip = 5 / 10_000
    full_cost = 0.5 * 10_000 * (1 + slip) * 2 + 0.005 * (500 + 250)
    scale = 10_000 / full_cost
    q_a = 500 * scale
    q_b = 250 * scale
    cash = 0.0
    assert r2['benchmark_cash'] == pytest.approx(cash, abs=1e-6)
    assert r2['benchmark_value'] == pytest.approx(cash + q_a * 10.0 + q_b * 20.0)
    assert r4['benchmark_value'] == pytest.approx(cash + q_a * 12.0 + q_b * 21.0)
    assert r1['benchmark_value'] == pytest.approx(10_000.0)

    # idempotent: a second track through the same date appends nothing
    res2 = L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert res2['appended'] == 0 and res2['disagreements'] == []
    # extending appends only the new sessions; the stored rows are untouched
    before = store.ledger('paper_s2p')
    res3 = L.track('paper_s2p', store, bars, targets_dir=str(tdir))
    assert res3['appended'] == 4
    after = store.ledger('paper_s2p')
    pd.testing.assert_frame_equal(after.loc[before.index], before)
    # BBB has no print on 09-09: carried at its last close and counted
    r7 = after.loc[pd.Timestamp('2026-09-09')]
    assert r7['benchmark_n_unpriced'] == 1
    assert r7['benchmark_value'] == pytest.approx(cash + q_a * 13.0 + q_b * 22.0)


def test_track_reports_disagreement_instead_of_rewriting(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 3))
    stored = store.ledger('paper_s2p')
    # a fill recorded LATE for a past session changes the replay of stored rows
    store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 2), 'AAA', 'BUY', 100, 10.0))
    res = L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert res['appended'] == 1
    assert any(d['column'] == 'sleeve_value' for d in res['disagreements'])
    pd.testing.assert_frame_equal(store.ledger('paper_s2p').loc[stored.index], stored)


def test_second_rebalance_and_report(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _write(tdir, 'paper_s2p', '2026-09-04', {'BBB': 0.5}, {'BBB': 1.0})
    store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 2), 'AAA', 'BUY', 500, 10.0))
    store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 8), 'AAA', 'SELL', 500, 12.0))
    store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 8), 'BBB', 'BUY', 200, 22.0))
    L.track('paper_s2p', store, bars, targets_dir=str(tdir))
    led = store.ledger('paper_s2p')
    last = led.iloc[-1]
    assert last['n_holdings'] == 1 and last['holdings_value'] == pytest.approx(200 * 24.0)
    assert last['cash'] == pytest.approx(10_000 - 5_000 + 6_000 - 4_400)
    # the benchmark moved to 100% BBB at the 09-08 open (22.0): AAA sold, BBB bought
    r8 = led.loc[pd.Timestamp('2026-09-08')]
    assert r8['benchmark_n_holdings'] == 1
    rep = L.report('paper_s2p', store)
    assert rep['sessions'] == 8 and rep['capital_usd'] == 10_000.0
    assert rep['sleeve_return'] == pytest.approx(last['sleeve_value'] / 10_000 - 1)
    assert rep['active_return'] == pytest.approx(rep['sleeve_return'] - rep['benchmark_return'])
    assert np.isfinite(rep['tracking_error_annualised']) and rep['tracking_error_annualised'] >= 0


def test_record_fill_validation_and_cli_roundtrip(world, tmp_path):
    from trader.lowfreq_live.__main__ import main
    store = world['store']
    with pytest.raises(ValueError):
        store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 2), 'AAA', 'HOLD', 1, 1.0))
    with pytest.raises(ValueError):
        store.record_fill('paper_s2p', L.Fill(dt.date(2026, 9, 2), 'AAA', 'BUY', 0, 1.0))
    ledger_db = str(tmp_path / 'ledger.duckdb')
    hist = tmp_path / 'hist.duckdb'
    import duckdb
    con = duckdb.connect(str(hist))
    con.execute('CREATE TABLE pit_daily_bars(day DATE, ticker VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, '
                'close DOUBLE, volume DOUBLE, dollar_volume DOUBLE, dv_rank INTEGER, PRIMARY KEY(day, ticker))')
    for ts in world['days']:
        for t in ('AAA', 'BBB'):
            c = world['closes'].at[ts, t]
            if np.isfinite(c):
                con.execute('INSERT INTO pit_daily_bars VALUES (?,?,?,?,?,?,?,?,?)',
                            [ts.date(), t, world['opens'].at[ts, t], c, c, c, 1e6, 1e7, 1])
    con.close()
    common = ['--ledger-db', ledger_db, '--bars-db', str(hist), '--targets-dir', str(world['tdir'])]
    assert main(['record-fill', '--sleeve', 'paper_s2p', '--date', '2026-09-02', '--ticker', 'aaa', '--side', 'buy',
                 '--quantity', '500', '--price', '10', *common]) == 0
    assert main(['track', '--sleeve', 'paper_s2p', *common]) == 0
    assert main(['--json', 'report', '--sleeve', 'paper_s2p', *common]) == 0
    assert main(['fills', '--sleeve', 'paper_s2p', *common]) == 0
    assert len(L.LedgerStore(ledger_db).ledger('paper_s2p')) == 8
