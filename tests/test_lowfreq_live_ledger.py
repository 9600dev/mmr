"""Paper ledger (C02 criterion 4) on a temporary DuckDB with in-memory bars.

Known answers below are derived by hand from the harness executor's rules
(``Backtester.run_panel``): whole shares truncated off the DECISION close,
fills at the next open with 5 bps slippage + $0.005/share, D06 buys scaled pro
rata to cash and floored, D07 exits at the terminal print, carry-forward marks.
A fractional / next-open re-implementation would fail them."""
from __future__ import annotations

import dataclasses
import datetime as dt
import json
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
    res = L.track('paper_s2p', store, bars, {'AAA': 500}, targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert res['appended'] == 4 and res['reconciled'] is True
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
    res2 = L.track('paper_s2p', store, bars, {'AAA': 500}, targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert res2['appended'] == 0
    before = store.ledger('paper_s2p')
    res3 = L.track('paper_s2p', store, bars, {'AAA': 500}, targets_dir=str(tdir))
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
    L.track('paper_s2p', store, bars, {}, targets_dir=str(tdir), through=dt.date(2026, 9, 2))
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
    L.track('paper_s2p', store, bars, {'BBB': 200}, targets_dir=str(tdir))
    led = store.ledger('paper_s2p')
    last = led.iloc[-1]
    assert last['n_holdings'] == 1 and last['holdings_value'] == pytest.approx(200 * 24.0)
    assert last['cash'] == pytest.approx(10_000 - 5_000 + 6_000 - 4_400)
    # benchmark at the 09-04 decision: equity 11238.275 -> 535 BBB target; at the 09-07 open sell 499 AAA @ 12
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


def test_stopped_name_exits_when_silence_is_recognised_without_revising_history(world):
    """D07 live rule: CCC's last print is 09-03; it is silent on 09-04, 09-07, 09-08,
    09-09 and 09-10 (five sessions), so the exit is booked ON 09-10 at the carried
    09-03 price. Rows stored before recognition carry the name and are never
    replayed differently (a daily track appends exactly one row)."""
    store, tdir = world['store'], world['tdir']
    closes = world['closes'].copy()
    closes['CCC'] = [30, 30, 33, np.nan, np.nan, np.nan, np.nan, np.nan]
    opens = closes.shift(1).fillna(closes.iloc[0])
    bars = L.FrameBars(opens, closes)
    _write(tdir, 'stopped', '2026-09-01', {'AAA': 0.5}, {'AAA': 0.5, 'CCC': 0.5})
    panel = L.PricePanel(open=opens, close=closes)
    assert L.silence_exits(panel) == {'CCC': [pd.Timestamp('2026-09-10')]}
    assert L.silence_exits(panel.sliced('2026-09-01', '2026-09-10')) == {}         # not yet recognised on 09-09
    qa, qc = 500, 166                                        # trunc(5000/10), trunc(5000/30); no D06 scaling needed
    cash0 = 10_000 - _cost(qa, 10.0) - _cost(qc, 30.0)
    assert cash0 > 0
    # a daily operator: track through 09-09, then through 09-10
    L.track('stopped', store, bars, {}, targets_dir=str(tdir), through=dt.date(2026, 9, 9))
    before = store.ledger('stopped')
    r9 = before.loc[pd.Timestamp('2026-09-09')]
    assert r9['benchmark_n_holdings'] == 2 and r9['benchmark_n_unpriced'] == 1
    assert r9['benchmark_value'] == pytest.approx(cash0 + qa * 13.0 + qc * 33.0)   # CCC carried at its last print
    res = L.track('stopped', store, bars, {}, targets_dir=str(tdir))
    assert res['appended'] == 1
    after = store.ledger('stopped')
    pd.testing.assert_frame_equal(after.loc[before.index], before)              # nothing revised
    r10 = after.loc[pd.Timestamp('2026-09-10')]
    cash1 = cash0 + qc * 33.0 * (1 - SLIP) - qc * COMM
    assert r10['benchmark_n_holdings'] == 1 and r10['benchmark_n_unpriced'] == 0
    assert r10['benchmark_cash'] == pytest.approx(cash1)
    assert r10['benchmark_value'] == pytest.approx(cash1 + qa * 14.0)


def test_replay_is_prefix_stable(world):
    """The replay through a later session reproduces every earlier session exactly."""
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _write(tdir, 'paper_s2p', '2026-09-04', {'BBB': 0.5}, {'BBB': 1.0})
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    inputs = L.load_inputs('paper_s2p', store, str(tdir))
    short = L.replay(inputs, bars, dt.date(2026, 9, 7))
    full = L.replay(inputs, bars, dt.date(2026, 9, 10))
    pd.testing.assert_frame_equal(full.loc[short.index], short)


def test_backtest_config_matches_engine_config():
    costs = base_costs('equity')
    term = {'AAA': pd.Timestamp('2026-09-03')}
    ours = L.backtest_config(dt.date(2026, 9, 1), dt.date(2026, 9, 11), 25_000.0, costs, term)
    theirs = engine._config(dt.date(2026, 9, 1), dt.date(2026, 9, 11), costs, term)
    assert ours == dataclasses.replace(theirs, initial_capital=25_000.0)


def test_reconciliation_is_mandatory_and_fails_closed(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    with pytest.raises(L.ReconciliationError):
        L.track('paper_s2p', store, bars, {'AAA': 499}, targets_dir=str(tdir))
    with pytest.raises(L.ReconciliationError):
        L.track('paper_s2p', store, bars, {}, targets_dir=str(tdir))
    with pytest.raises(L.ReconciliationError):
        L.track('paper_s2p', store, bars, None, targets_dir=str(tdir))       # type: ignore[arg-type]
    assert len(store.ledger('paper_s2p')) == 0                # refused: nothing appended
    L.reconcile_holdings({'AAA': 500}, {'AAA': 500.0, 'ZZZ': 3})
    with pytest.raises(L.ReconciliationError):
        L.reconcile_holdings({'AAA': -1}, {'AAA': 0})


def test_disagreement_refuses_everything_and_rebuild_is_attested(world):
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    L.track('paper_s2p', store, bars, {'AAA': 500}, targets_dir=str(tdir), through=dt.date(2026, 9, 3))
    stored = store.ledger('paper_s2p')
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 100, 10.0, exec_id='late')   # reconciled late
    with pytest.raises(L.LedgerDisagreement) as ei:
        L.track('paper_s2p', store, bars, {'AAA': 600}, targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert {d['column'] for d in ei.value.disagreements} >= {'sleeve_value', 'cash'}
    pd.testing.assert_frame_equal(store.ledger('paper_s2p'), stored)   # NOTHING appended, nothing changed
    with pytest.raises(RuntimeError, match='reason'):
        L.rebuild('paper_s2p', store, bars, {'AAA': 600}, '', targets_dir=str(tdir))
    res = L.rebuild('paper_s2p', store, bars, {'AAA': 600}, 'late fill AAA-late reconciled 09-05',
                    targets_dir=str(tdir), through=dt.date(2026, 9, 4))
    assert res['rows_dropped'] == 3 and res['rows_written'] == 4 and res['disagreements_resolved']
    led = store.ledger('paper_s2p')
    assert len(led) == 4 and led.loc[pd.Timestamp('2026-09-02'), 'cash'] == pytest.approx(10_000 - 6_000)
    audit = store.rebuilds('paper_s2p')
    assert len(audit) == 1 and audit[0]['reason'].startswith('late fill') and audit[0]['rows_dropped'] == 3
    res2 = L.track('paper_s2p', store, bars, {'AAA': 600}, targets_dir=str(tdir))
    assert res2['appended'] == 4
    assert L.report('paper_s2p', store)['rebuilds'] == audit


def test_sidecar_must_match_its_d08_file(world):
    store, tdir = world['store'], world['tdir']
    side = T.benchmark_sidecar_path(tdir / 'paper_s2p' / '2026-09-01.json')
    doc = json.loads(side.read_text())
    doc['capital_usd'] = 999.0
    side.write_text(json.dumps(doc))
    with pytest.raises(RuntimeError, match='capital_usd'):
        L.load_inputs('paper_s2p', store, str(tdir))


def test_missing_fills_table_refuses(tmp_path):
    store = L.LedgerStore(str(tmp_path / 'x.duckdb'))
    with pytest.raises(RuntimeError, match='sleeve_fills'):
        store.fills('paper_s2p')


def test_cli_roundtrip_with_fake_broker(world, monkeypatch, capsys):
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
    with pytest.raises(SystemExit):                           # the bypass no longer exists (D13)
        cli.main(['track', '--sleeve', 'paper_s2p', '--no-reconcile', *common])
    monkeypatch.setattr(cli, '_ib_positions', lambda: {'AAA': 400.0})
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 2          # IB shows fewer: refused
    assert len(store.ledger('paper_s2p')) == 0

    def boom():
        raise ConnectionError('trader_service down')
    monkeypatch.setattr(cli, '_ib_positions', boom)
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 2          # cannot read IB: refused
    monkeypatch.setattr(cli, '_ib_positions', lambda: {'AAA': 500.0})
    assert cli.main(['track', '--sleeve', 'paper_s2p', '--through', '2026-09-03', *common]) == 0
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 100, 10.0, exec_id='late')
    monkeypatch.setattr(cli, '_ib_positions', lambda: {'AAA': 600.0})
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 2          # disagreement: refused
    assert len(store.ledger('paper_s2p')) == 3
    assert cli.main(['rebuild', '--sleeve', 'paper_s2p', '--reason', 'late fill', *common]) == 2   # no --attest
    assert cli.main(['rebuild', '--sleeve', 'paper_s2p', '--attest', *common]) == 2             # no reason
    assert cli.main(['rebuild', '--sleeve', 'paper_s2p', '--reason', 'late fill', '--attest', *common]) == 0
    assert len(store.ledger('paper_s2p')) == 8
    assert cli.main(['track', '--sleeve', 'paper_s2p', *common]) == 0
    assert cli.main(['--json', 'report', '--sleeve', 'paper_s2p', *common]) == 0
    capsys.readouterr()
    assert cli.main(['report', '--sleeve', 'paper_s2p', *common]) == 0
    text = capsys.readouterr().out
    assert 'rebuilds (1' in text and 'late fill' in text                     # the text report lists rebuilds
    assert cli.main(['fills', '--sleeve', 'paper_s2p', *common]) == 0


@pytest.fixture
def episodes(tmp_path):
    """24 sessions (09-01 .. 10-02). CCC prints 09-01..09-03, is silent five
    sessions (recognised 09-10), prints again 09-11..09-15, then stops for good
    (recognised 09-22). DDD prints 09-01, is silent five sessions BEFORE the
    benchmark ever holds it (recognised 09-08 while unheld), prints
    09-09..09-15, then stops for good (recognised 09-22). AAA is flat at 10."""
    days = pd.bdate_range('2026-09-01', periods=24)
    nan = np.nan
    closes = pd.DataFrame({'AAA': [10.0] * 24,
                           'CCC': [30, 30, 33] + [nan] * 5 + [30, 30, 30] + [nan] * 13,
                           'DDD': [40] + [nan] * 5 + [40, 40, 40, 40, 44] + [nan] * 13}, index=days)
    opens = closes.shift(1).fillna(closes.iloc[0])
    tdir = tmp_path / 'targets'
    _write(tdir, 'multi', '2026-09-01', {'AAA': 0.5}, {'AAA': 0.5, 'CCC': 0.5})
    _write(tdir, 'multi', '2026-09-14', {'AAA': 0.5}, {'AAA': 0.4, 'CCC': 0.3, 'DDD': 0.3})
    store = L.LedgerStore(str(tmp_path / 'ledger.duckdb'))
    store.db.execute(L.SLEEVE_FILLS_DDL, fetch='none')
    return {'bars': L.FrameBars(opens, closes), 'tdir': tdir, 'store': store, 'opens': opens, 'closes': closes}


def _log(res):
    return [(str(pd.Timestamp(t.timestamp).date()), str(t.action).rsplit('.', 1)[-1], t.conid, t.quantity)
            for t in res.trades]


def test_every_recognised_silence_is_handed_to_the_executor(episodes):
    panel = L.PricePanel(open=episodes['opens'], close=episodes['closes'])
    assert L.silence_exits(panel) == {'CCC': [pd.Timestamp('2026-09-10'), pd.Timestamp('2026-09-22')],
                                      'DDD': [pd.Timestamp('2026-09-08'), pd.Timestamp('2026-09-22')]}
    # prefix-stable: a shorter panel has the same earlier entries and nothing later
    assert L.silence_exits(panel.sliced('2026-09-01', '2026-09-22')) == {
        'CCC': [pd.Timestamp('2026-09-10')], 'DDD': [pd.Timestamp('2026-09-08')]}
    # a name whose silence began before the window is never recognised (never printed here)
    assert L.silence_exits(panel.sliced('2026-09-16', '2026-10-03')) == {}


def test_stop_reenter_stop_exits_twice_and_an_unheld_gap_changes_nothing(episodes):
    """CCC is exited at BOTH recognitions (it re-entered through the 09-14
    rebalance); DDD's first recognition (09-08) fires nothing because the
    benchmark does not hold it, and its second (09-22) sells it. Under the
    first-gap-only rule both re-entered names were carried forever. Known
    answers derived by hand from run_panel's rules."""
    store, bars, tdir = episodes['store'], episodes['bars'], episodes['tdir']
    inputs = L.load_inputs('multi', store, str(tdir))
    res, _, _ = L.run_benchmark(inputs, bars, dt.date(2026, 10, 2))
    assert _log(res) == [('2026-09-02', 'BUY', 'AAA', 500.0), ('2026-09-02', 'BUY', 'CCC', 166.0),
                         ('2026-09-10', 'SELL', 'CCC', 166.0),                    # first stop, recognised
                         ('2026-09-15', 'SELL', 'AAA', 81.0), ('2026-09-15', 'BUY', 'CCC', 104.0),
                         ('2026-09-15', 'BUY', 'DDD', 78.0),                     # re-entry / first entry
                         ('2026-09-22', 'SELL', 'CCC', 104.0), ('2026-09-22', 'SELL', 'DDD', 78.0)]   # second stops
    assert res.diagnostics['panel_forced_exits'] == 3
    # 09-02: 500 AAA + 166 CCC off the 09-01 closes, no D06 scaling
    cash0 = 10_000 - _cost(500, 10.0) - _cost(166, 30.0)
    # 09-10: CCC sold at its carried 09-03 print (33)
    cash1 = cash0 + 166 * 33.0 * (1 - SLIP) - 166 * COMM
    # 09-14 decision off equity cash1 + 500 x 10 -> 419 AAA / 104 CCC / 78 DDD, filled at the 09-15 opens
    eq = cash1 + 500 * 10.0
    assert (math.trunc(0.4 * eq / 10), math.trunc(0.3 * eq / 30), math.trunc(0.3 * eq / 40)) == (419, 104, 78)
    cash2 = cash1 + 81 * 10.0 * (1 - SLIP) - 81 * COMM - _cost(104, 30.0) - _cost(78, 40.0)
    # 09-22: both names sold at their carried 09-15 prints (30 and 44)
    cash3 = cash2 + 104 * 30.0 * (1 - SLIP) - 104 * COMM + 78 * 44.0 * (1 - SLIP) - 78 * COMM
    out = L.track('multi', store, bars, {}, targets_dir=str(tdir))
    assert out['benchmark_forced_exits'] == 3
    led = store.ledger('multi')
    r21 = led.loc[pd.Timestamp('2026-09-21')]
    assert r21['benchmark_n_holdings'] == 3 and r21['benchmark_n_unpriced'] == 2
    assert r21['benchmark_value'] == pytest.approx(cash2 + 419 * 10 + 104 * 30 + 78 * 44)
    r22 = led.loc[pd.Timestamp('2026-09-22')]
    assert r22['benchmark_n_holdings'] == 1 and r22['benchmark_n_unpriced'] == 0
    assert r22['benchmark_cash'] == pytest.approx(cash3)
    assert r22['benchmark_value'] == pytest.approx(cash3 + 419 * 10)
    assert led.iloc[-1]['benchmark_n_holdings'] == 1 and led.iloc[-1]['benchmark_n_unpriced'] == 0


def test_second_exit_appears_in_a_new_row_only(episodes):
    """A daily operator tracking through 09-21 and then 09-22: stored rows are
    identical afterwards, the second episode's exits are booked in the one
    appended row, and the running count is reported."""
    store, bars, tdir = episodes['store'], episodes['bars'], episodes['tdir']
    r1 = L.track('multi', store, bars, {}, targets_dir=str(tdir), through=dt.date(2026, 9, 21))
    assert r1['benchmark_forced_exits'] == 1
    before = store.ledger('multi')
    r2 = L.track('multi', store, bars, {}, targets_dir=str(tdir), through=dt.date(2026, 9, 22))
    assert r2['appended'] == 1 and r2['benchmark_forced_exits'] == 3
    after = store.ledger('multi')
    pd.testing.assert_frame_equal(after.loc[before.index], before)
    assert after.iloc[-1]['benchmark_n_holdings'] == 1


def test_rebuild_is_one_transaction(world):
    """A failure in the LAST statement (the audit row) leaves the ledger exactly
    as it was: no half-written history, no rewrite without its audit entry."""
    store, bars, tdir = world['store'], world['bars'], world['tdir']
    _fill(store, 'paper_s2p', dt.date(2026, 9, 2), 'AAA', 500, 10.0)
    L.track('paper_s2p', store, bars, {'AAA': 500}, targets_dir=str(tdir), through=dt.date(2026, 9, 3))
    stored = store.ledger('paper_s2p')
    inputs = L.load_inputs('paper_s2p', store, str(tdir))
    frame = L.replay(inputs, bars, dt.date(2026, 9, 10))
    rows = [dict(r, session=pd.Timestamp(ts).date()) for ts, r in frame.iterrows()]
    with pytest.raises(Exception):
        store.rebuild_atomic('paper_s2p', rows, None, [])          # type: ignore[arg-type]  reason is NOT NULL
    pd.testing.assert_frame_equal(store.ledger('paper_s2p'), stored)
    assert store.rebuilds('paper_s2p') == []
    dropped, written = store.rebuild_atomic('paper_s2p', rows, 'ok', [])
    assert (dropped, written) == (3, 8) and len(store.ledger('paper_s2p')) == 8
