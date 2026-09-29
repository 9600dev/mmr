"""Paper ledger (C02 criterion 4) on a temporary DuckDB with in-memory bars.

Known answers below are derived by hand from the harness executor's rules
(``Backtester.run_panel``): whole shares truncated off the DECISION close,
fills at the next open with 5 bps slippage + $0.005/share, D06 buys scaled pro
rata to cash and floored, D07 exits at the terminal print, carry-forward marks.
A fractional / next-open re-implementation would fail them."""
from __future__ import annotations

import dataclasses
import datetime as dt
import math

import duckdb
import numpy as np
import pandas as pd
import pytest

from trader.lowfreq_live import ledger as L
from trader.lowfreq_live import targets as T
from trader.research.lowfreq import engine
from trader.research.lowfreq.costs import base_costs

SLIP = 5 / 10_000
COMM = 0.005


def _doc(sleeve, as_of, weights, capital=10_000.0):
    return {'schema': 'mmr.targets.v1', 'sleeve': sleeve, 'as_of': as_of, 'generated_at': '2026-09-01T21:00:00+00:00',
            'strategy': {'family': 'equity', 'id': 's2p_top_quintile', 'module': 'm', 'class': 'C', 'params': {},
                         'source_sha256': 'a' * 64},
            'capital_usd': capital, 'prices_as_of': as_of, 'weights': weights, 'notes': ''}


def _write(tdir, sleeve, as_of, weights, bench):
    res = T.TargetsResult(document=_doc(sleeve, as_of, weights), benchmarks={'eq_ew_covered': bench})
    T.write_targets(res, targets_dir=str(tdir))


def _fill(store, sleeve, date, ticker, qty, price, comm=0.0, exec_id=None):
    store.db.execute('INSERT INTO sleeve_fills VALUES (?,?,?,?,?,?,?,?,?, now())',
                     [sleeve, ticker, date, qty, price, comm, exec_id or f'{ticker}-{date}-{qty}', 'o1', 1], fetch='none')


def _cost(delta, px):
    return delta * px * (1 + SLIP) + delta * COMM


@pytest.fixture
def world(tmp_path):
    days = pd.bdate_range('2026-09-01', periods=8)          # 09-01 .. 09-10
    closes = pd.DataFrame({'AAA': [10, 10, 11, 12, 12, 13, 13, 14.0],
                           'BBB': [20, 20, 20, 21, 22, 22, np.nan, 24.0]}, index=days)
    opens = closes.shift(1).fillna(closes.iloc[0])
    tdir = tmp_path / 'targets'
    _write(tdir, 'paper_s2p', '2026-09-01', {'AAA': 0.5}, {'AAA': 0.5, 'BBB': 0.5})
    store = L.LedgerStore(str(tmp_path / 'ledger.duckdb'))
    store.db.execute(L.SLEEVE_FILLS_DDL, fetch='none')       # C01 creates it in production (D13)
    return {'bars': L.FrameBars(opens, closes), 'tdir': tdir, 'store': store, 'days': days,
            'closes': closes, 'opens': opens, 'tmp': tmp_path}


def test_benchmark_follows_run_panel_rules_and_ledger_is_append_only(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0, 1.0)
    res = L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 4),
                  broker_positions={'AAA': 500})
    assert res['appended'] == 4 and res['disagreements'] == [] and res['reconciled'] is True
    led = store.ledger('paper_s2p')
    r1 = led.loc[pd.Timestamp('2026-09-01')]
    assert r1['sleeve_value'] == pytest.approx(10_000.0) and r1['benchmark_value'] == pytest.approx(10_000.0)
    r2 = led.loc[pd.Timestamp('2026-09-02')]
    assert r2['cash'] == pytest.approx(10_000 - 5_000 - 1) and r2['holdings_value'] == pytest.approx(5_000.0)
    # benchmark: whole shares off the 09-01 close (500 AAA, 250 BBB); the full cost
    # 10,008.75 exceeds the cash so D06 scales by 10000/10008.75 and FLOORS: 499 and 249
    scale = 10_000 / (_cost(500, 10.0) + _cost(250, 20.0))
    qa, qb = math.floor(500 * scale), math.floor(250 * scale)
    assert (qa, qb) == (499, 249)
    cash = 10_000 - _cost(qa, 10.0) - _cost(qb, 20.0)
    assert r2['benchmark_cash'] == pytest.approx(cash)
    assert r2['benchmark_value'] == pytest.approx(cash + qa * 10 + qb * 20)
    assert r2['benchmark_n_holdings'] == 2
    r4 = led.loc[pd.Timestamp('2026-09-04')]
    assert r4['benchmark_value'] == pytest.approx(cash + qa * 12 + qb * 21)
    assert r4['sleeve_value'] == pytest.approx(4_999 + 500 * 12.0)

    # idempotent: nothing appended, nothing disagrees
    res2 = L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 4),
                   broker_positions={'AAA': 500})
    assert res2['appended'] == 0 and res2['disagreements'] == []
    before = store.ledger('paper_s2p')
    res3 = L.track('paper_s2p', store, bars, targets_dir=str(tdir), broker_positions={'AAA': 500})
    assert res3['appended'] == 4
    after = store.ledger('paper_s2p')
    pd.testing.assert_frame_equal(after.loc[before.index], before)
    r7 = after.loc[pd.Timestamp('2026-09-09')]               # BBB has no print: carried at 22, counted
    assert r7['benchmark_n_unpriced'] == 1
    assert r7['benchmark_value'] == pytest.approx(cash + qa * 13 + qb * 22)


def test_opening_gap_sizes_off_decision_close_and_fills_at_open(world):
    store, tdir = world['store'], world['tdir']
    opens = world['opens'].copy()
    opens.at[pd.Timestamp('2026-09-02'), 'AAA'] = 11.0        # gaps up at the fill open
    bars = L.FrameBars(opens, world['closes'])
    L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 2), broker_positions={})
    r2 = store.ledger('paper_s2p').loc[pd.Timestamp('2026-09-02')]
    scale = 10_000 / (_cost(500, 11.0) + _cost(250, 20.0))    # 500/250 sized off the 10/20 decision closes
    qa, qb = math.floor(500 * scale), math.floor(250 * scale)
    assert (qa, qb) == (475, 237)
    cash = 10_000 - _cost(qa, 11.0) - _cost(qb, 20.0)
    assert r2['benchmark_cash'] == pytest.approx(cash)
    assert r2['benchmark_value'] == pytest.approx(cash + qa * 10.0 + qb * 20.0)


def test_second_rebalance_and_report(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _write(tdir, 'paper_s2p', '2026-09-04', {'BBB': 0.5}, {'BBB': 1.0})
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    _fill(store, 'paper_s2p', dt.date(2026, 9, 8), 'AAA', -500, 12.0)
    _fill(store, 'paper_s2p', dt.date(2026, 9, 8), 'BBB', 200, 22.0)
    L.track('paper_s2p', store, bars, targets_dir=str(tdir), broker_positions={'BBB': 200})
    led = store.ledger('paper_s2p')
    last = led.iloc[-1]
    assert last['n_holdings'] == 1 and last['holdings_value'] == pytest.approx(200 * 24.0)
    assert last['cash'] == pytest.approx(10_000 - 5_000 + 6_000 - 4_400)
    # benchmark at the 09-04 decision: equity 11238.275 -> 535 BBB target; on 09-08 sell 499 AAA @ 12
    # then buy 286 BBB @ 21, scaled by D06 to 285 (cost 6010.43 > cash 6003.79)
    cash0 = 10_000 - _cost(499, 10.0) - _cost(249, 20.0)
    equity_dec = cash0 + 499 * 12 + 249 * 21
    target_b = math.trunc(equity_dec / 21)
    assert target_b == 535
    cash1 = cash0 + 499 * 12 * (1 - SLIP) - 499 * COMM
    delta = target_b - 249
    scale = min(1.0, cash1 / _cost(delta, 21.0))
    bought = math.floor(delta * scale)
    assert bought == 285
    cash2 = cash1 - _cost(bought, 21.0)
    r8 = led.loc[pd.Timestamp('2026-09-08')]
    assert r8['benchmark_n_holdings'] == 1 and r8['benchmark_cash'] == pytest.approx(cash2)
    assert r8['benchmark_value'] == pytest.approx(cash2 + (249 + bought) * 22.0)
    rep = L.report('paper_s2p', store)
    assert rep['sessions'] == 8 and rep['capital_usd'] == 10_000.0
    assert rep['sleeve_return'] == pytest.approx(last['sleeve_value'] / 10_000 - 1)
    assert rep['active_return'] == pytest.approx(rep['sleeve_return'] - rep['benchmark_return'])
    assert np.isfinite(rep['tracking_error_annualised']) and rep['tracking_error_annualised'] >= 0


def test_stopped_name_exits_at_terminal_print(world):
    store, tdir = world['store'], world['tdir']
    days = world['days']
    closes = world['closes'].copy()
    closes['CCC'] = [30, 30, 33, np.nan, np.nan, np.nan, np.nan, np.nan]     # last print 09-03, silent 5 sessions
    opens = closes.shift(1).fillna(closes.iloc[0])
    bars = L.FrameBars(opens, closes)
    _write(tdir, 'stopped', '2026-09-01', {'AAA': 0.5}, {'AAA': 0.5, 'CCC': 0.5})
    panel = L.PricePanel(open=opens, close=closes)
    assert L.live_terminal_prints(panel) == {'CCC': pd.Timestamp('2026-09-03')}
    L.track('stopped', store, bars, targets_dir=str(tdir), broker_positions={})
    led = store.ledger('stopped')
    r3 = led.loc[pd.Timestamp('2026-09-03')]
    assert r3['benchmark_n_holdings'] == 1                     # CCC sold at its last print (D07)
    scale = 10_000 / (_cost(500, 10.0) + _cost(166, 30.0))     # trunc(5000/30) = 166 CCC
    qa, qc = math.floor(500 * scale), math.floor(166 * scale)
    cash = 10_000 - _cost(qa, 10.0) - _cost(qc, 30.0) + qc * 33 * (1 - SLIP) - qc * COMM
    assert r3['benchmark_cash'] == pytest.approx(cash)
    last = led.iloc[-1]
    assert last['benchmark_n_holdings'] == 1 and last['benchmark_n_unpriced'] == 0
    assert last['benchmark_value'] == pytest.approx(cash + qa * 14.0)


def test_backtest_config_matches_engine_config():
    costs = base_costs('equity')
    term = {'AAA': pd.Timestamp('2026-09-03')}
    ours = L.backtest_config(dt.date(2026, 9, 1), dt.date(2026, 9, 11), 25_000.0, costs, term)
    theirs = engine._config(dt.date(2026, 9, 1), dt.date(2026, 9, 11), costs, term)
    assert ours == dataclasses.replace(theirs, initial_capital=25_000.0)


def test_reconciliation_fails_closed_and_late_fill_is_a_disagreement(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    with pytest.raises(L.ReconciliationError):
        L.track('paper_s2p', store, bars, targets_dir=str(tdir), broker_positions={'AAA': 499})
    with pytest.raises(L.ReconciliationError):
        L.track('paper_s2p', store, bars, targets_dir=str(tdir), broker_positions={})
    assert len(store.ledger('paper_s2p')) == 0                # refused: nothing appended
    L.reconcile_holdings({'AAA': 500}, {'AAA': 500.0, 'ZZZ': 3})
    with pytest.raises(L.ReconciliationError):
        L.reconcile_holdings({'AAA': -1}, {'AAA': 0})
    L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 3), broker_positions={'AAA': 500})
    stored = store.ledger('paper_s2p')
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 100, 10.0, exec_id='late')   # journaled late
    res = L.track('paper_s2p', store, bars, targets_dir=str(tdir), through=dt.date(2026, 9, 4),
                  broker_positions={'AAA': 600})
    assert res['appended'] == 1 and any(d['column'] == 'sleeve_value' for d in res['disagreements'])
    pd.testing.assert_frame_equal(store.ledger('paper_s2p').loc[stored.index], stored)


def test_missing_fills_table_refuses(tmp_path):
    store = L.LedgerStore(str(tmp_path / 'x.duckdb'))
    with pytest.raises(RuntimeError, match='sleeve_fills'):
        store.fills('paper_s2p')


def test_cli_roundtrip_with_fake_broker(world, monkeypatch):
    from trader.lowfreq_live import __main__ as cli
    store = world['store']
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    hist = world['tmp'] / 'hist.duckdb'
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
    common = ['--ledger-db', str(world['tmp'] / 'ledger.duckdb'), '--bars-db', str(hist),
              '--targets-dir', str(world['tdir'])]
    monkeypatch.setattr(cli, '_ib_positions', lambda: {'AAA': 400.0})
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 2          # IB shows fewer: refused
    assert len(store.ledger('paper_s2p')) == 0

    def boom():
        raise ConnectionError('trader_service down')
    monkeypatch.setattr(cli, '_ib_positions', boom)
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 2          # cannot read IB: refused
    monkeypatch.setattr(cli, '_ib_positions', lambda: {'AAA': 500.0})
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 0
    assert cli.main(['--json', 'report', '--sleeve', 'paper_s2p', *common]) == 0
    assert cli.main(['fills', '--sleeve', 'paper_s2p', *common]) == 0
    assert len(store.ledger('paper_s2p')) == 8
