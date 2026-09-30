"""research/lowfreq/insider/make_report.py and d07_stress.py (C04; D07, D11):
every number REPORT.md quotes comes from a JSON artifact that agrees with the
ledger row it claims to be; the D07 terminal-loss stress is pathwise,
identifies forced exits by the close-priced fill on the terminal print, and
its unstressed numbers must equal the champion's harness file. Synthetic
inputs for the kernels; the real ledger/artifacts for the consistency pins."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[2]
INSIDER = REPO / 'research' / 'lowfreq' / 'insider'


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, INSIDER / filename)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


MR = _load('insider_make_report', 'make_report.py')
ST = _load('insider_d07_stress', 'd07_stress.py')


# ------------------------------------------------------------- stress kernel
def _equity():
    idx = pd.bdate_range('2020-01-01', periods=8)
    return pd.Series([100.0, 101.0, 103.0, 102.0, 104.0, 105.0, 103.0, 106.0], index=idx)


def test_stressed_returns_without_forced_exits_are_the_plain_returns():
    eq = _equity()
    out = ST.stressed_returns(eq, pd.Series(dtype=float))
    pd.testing.assert_series_equal(out, eq.pct_change().iloc[1:], check_names=False)


def test_stressed_returns_remove_the_proceeds_on_their_day_and_scale_the_path_after_it():
    eq = _equity()
    day = eq.index[3]
    p = pd.Series({day: 10.0})
    out = ST.stressed_returns(eq, p)
    plain = eq.pct_change().iloc[1:]
    # identical before the exit day
    pd.testing.assert_series_equal(out.loc[:eq.index[2]], plain.loc[:eq.index[2]], check_names=False)
    # the stressed path is the plain path scaled by (E_t - P_t)/E_t from the exit day on
    stressed_path = (1 + out).cumprod() * eq.iloc[0]
    factor = (eq.loc[day] - 10.0) / eq.loc[day]
    np.testing.assert_allclose(stressed_path.loc[day:], eq.loc[day:] * factor)


def test_stressed_returns_refuse_negative_or_over_large_proceeds():
    eq = _equity()
    with pytest.raises(ValueError):
        ST.stressed_returns(eq, pd.Series({eq.index[2]: -1.0}))
    with pytest.raises(ValueError):
        ST.stressed_returns(eq, pd.Series({eq.index[2]: eq.iloc[2] + 1.0}))


def _trade(ts, conid, action, qty, price, commission=0.5):
    return SimpleNamespace(timestamp=ts, conid=conid, action=action, quantity=qty, price=price, commission=commission)


def test_forced_exit_is_the_close_priced_sell_booked_after_the_bars_scheduled_fills():
    idx = pd.bdate_range('2021-03-01', periods=3)
    t_term = idx[1]
    # A: open 12 / close 9 on its terminal print; C: open == close == 7 (price cannot tell the fills apart)
    closes = pd.DataFrame({'A': [11.0, 9.0, np.nan], 'B': [5.0, 5.0, 5.0], 'C': [7.5, 7.0, np.nan]}, index=idx)
    slip = 5 / 10_000.0
    terminal = {'A': t_term, 'C': t_term}
    trades = [                                                          # booking order, as the backtester writes it
        _trade(t_term, 'A', 'Action.SELL', 30.0, 12.0 * (1 - slip)),   # scheduled fill at the OPEN: not forced
        _trade(t_term, 'C', 'Action.SELL', 40.0, 7.0 * (1 - slip)),    # scheduled full close, open == close ...
        _trade(t_term, 'B', 'Action.SELL', 10.0, 5.0 * (1 - slip)),    # ... followed by other scheduled fills
        _trade(t_term, 'A', 'Action.BUY', 1.0, 12.0 * (1 + slip)),     # a buy is never an exit
        _trade(t_term, 'A', 'Action.SELL', 70.0, 9.0 * (1 - slip)),    # the D07 exit at the CLOSE, booked last
    ]
    out = ST.forced_exits_from_trades(trades, terminal, closes, slip)
    assert list(out['ticker']) == ['A'] and out.iloc[0]['quantity'] == 70.0
    assert out.iloc[0]['net_proceeds'] == pytest.approx(70.0 * 9.0 * (1 - slip) - 0.5)
    # a forced exit booked before a scheduled fill of the same bar is impossible; such a SELL is scheduled
    assert len(ST.forced_exits_from_trades(trades[:4], terminal, closes, slip)) == 0
    # two forced exits on one bar (both after every scheduled fill) are both found, last SELL per name
    two = trades + [_trade(t_term, 'C', 'Action.SELL', 5.0, 7.0 * (1 - slip))]
    out2 = ST.forced_exits_from_trades(two, terminal, closes, slip)
    assert list(out2['ticker']) == ['A', 'C'] and list(out2['quantity']) == [70.0, 5.0]


def test_collapse_flags_mark_a_name_whose_terminal_close_is_far_below_its_trailing_high():
    idx = pd.bdate_range('2020-01-01', periods=30)
    x = np.linspace(100.0, 30.0, 30)          # falls to 30 % of its high: a collapse
    y = np.full(30, 95.0); y[5] = 100.0      # ends at 95 % of its high: not one
    closes = pd.DataFrame({'X': x, 'Y': y}, index=idx)
    exits = pd.DataFrame([{'date': str(idx[-1].date()), 'ticker': 'X'}, {'date': str(idx[-1].date()), 'ticker': 'Y'},
                          {'date': str(idx[-1].date()), 'ticker': 'Z'}])   # Z: no history -> not a collapse
    flags = ST.collapse_flags(closes, exits)
    assert list(flags) == [True, False, False]


# ----------------------------------------------------------- report artifacts
def _row(cid, ts, n, metrics, verdict='FAIL', status='ok'):
    return {'id': cid, 'timestamp': ts, 'n_trials': n, 'metrics': metrics, 'verdict': verdict, 'status': status,
            'mode': 'walk_forward', 'role': 'candidate', 'entry_sha256': 'h_' + cid}


def _artifact(cid, ts, n, metrics, verdict='FAIL'):
    return {'id': cid, 'verdict': verdict, 'ledger_metrics': dict(metrics),
            'ledger_row': {'id': cid, 'timestamp': ts, 'n_trials': n}, 'dsr': {'n_trials': n}}


METRICS = {'oos_sharpe': 0.5, 'active_ann_return': 0.01, 'active_t_nw': 0.6, 'active_dsr': 0.05}


def test_artifact_for_row_accepts_a_matching_file_and_refuses_a_contradicting_one(tmp_path):
    row = _row('ins_x', '2026-09-30T00:00:00+00:00', 16, METRICS)
    (tmp_path / '001_ins_x.json').write_text(json.dumps(_artifact('ins_x', row['timestamp'], 16, METRICS)))
    assert MR.artifact_for_row(1, row, tmp_path)['ledger_row']['n_trials'] == 16
    assert MR.artifact_for_row(2, row, tmp_path) is None                    # nothing retained for row 2
    bad_ts = dict(row, timestamp='2026-09-30T01:00:00+00:00')
    with pytest.raises(MR.ReportError):
        MR.artifact_for_row(1, bad_ts, tmp_path)
    bad_metric = dict(row, metrics=dict(METRICS, active_dsr=0.9))
    with pytest.raises(MR.ReportError):
        MR.artifact_for_row(1, bad_metric, tmp_path)
    bad_verdict = dict(row, verdict='PASS')
    with pytest.raises(MR.ReportError):
        MR.artifact_for_row(1, bad_verdict, tmp_path)


def test_ledger_table_quotes_numbers_only_for_rows_with_a_retained_artifact_but_every_verdict():
    rows = [_row('ins_x', 't1', 1, METRICS, verdict='PROMISING'), _row('ins_x', 't2', 16, METRICS)]
    artifacts = {1: None, 2: _artifact('ins_x', 't2', 16, METRICS)}
    table = MR.ledger_table(rows, artifacts, MR.final_row_index(rows))
    unretained, retained = table[2], table[3]
    # no artifact: the ledger's verdict is shown, every numeric column is blank
    assert 'not retained' in unretained and '| - | PROMISING | - | - | - | - |' in unretained
    assert '0.50' not in unretained and '0.05' not in unretained
    assert '| final | 16 | FAIL | 0.50 | +1.00% | 0.60 | 0.05 |' in retained


def test_results_must_be_the_artifact_of_the_latest_ok_ledger_row():
    rows = [_row('ins_x', 't1', 1, METRICS), _row('ins_x', 't2', 16, METRICS)]
    MR.check_results_are_final({'ins_x': {'ledger_row': {'timestamp': 't2'}}}, rows)
    with pytest.raises(MR.ReportError):
        MR.check_results_are_final({'ins_x': {'ledger_row': {'timestamp': 't1'}}}, rows)


def test_a_held_out_fixed_row_never_becomes_the_final_dev_row():
    # Regression (2026-09-30): the orchestrator's held-out run appended a
    # mode=fixed row for the champion; final_row_index took it as the id's
    # final row, which has no retained trials/ artifact, and the real-artifact
    # check failed. Held-out rows are ledger entries, not dev trials.
    fixed = dict(_row('ins_x', 't3', 16, METRICS), mode='fixed', start='2024-07-01', end='2026-03-31')
    rows = [_row('ins_x', 't1', 1, METRICS), _row('ins_x', 't2', 16, METRICS), fixed]
    assert MR.final_row_index(rows) == {'ins_x': 2}
    MR.check_results_are_final({'ins_x': {'ledger_row': {'timestamp': 't2'}}}, rows)
    lines = MR.held_out_lines(rows)
    assert len(lines) == 3 and lines[2].startswith('| 3 | `ins_x` | 2024-07-01..2026-03-31 |')
    assert MR.held_out_lines(rows[:2]) == ['Not yet run.']


def _stress(cand_sharpe=0.4, forced=3):
    book = lambda: {'id': 'ins_x', 'summary': {'unstressed': {'sharpe': cand_sharpe, 'cagr': 0.08, 'max_drawdown': -0.3}},
                    'forced_exits': forced, 'forced_exit_value_fraction': 0.5, 'reconciliation': {'reconciled': True}}
    return {'candidate': book(), 'benchmark': book()}


def _champion_file():
    cell = {'sharpe': 0.4, 'cagr': 0.08, 'max_drawdown': -0.3, 'forced_exits': 3, 'forced_exit_value_fraction': 0.5}
    return {'full_period': {'report_cell': dict(cell), 'benchmarks': {MR.BENCH: dict(cell)}}}


def test_stress_artifact_must_reproduce_the_champion_files_unstressed_numbers():
    MR.check_stress_matches(_stress(), 'ins_x', _champion_file())
    with pytest.raises(MR.ReportError):
        MR.check_stress_matches(_stress(cand_sharpe=0.41), 'ins_x', _champion_file())
    with pytest.raises(MR.ReportError):
        MR.check_stress_matches(_stress(forced=4), 'ins_x', _champion_file())
    with pytest.raises(MR.ReportError):
        MR.check_stress_matches(_stress(), 'ins_other', _champion_file())


# ------------------------------------------------------- the real artifacts
def test_every_retained_artifact_agrees_with_its_ledger_row_and_final_files_are_final():
    rows = [json.loads(l) for l in (INSIDER / 'trials.jsonl').read_text().splitlines() if l.strip()]
    artifacts = {i: MR.artifact_for_row(i, r) for i, r in enumerate(rows, 1)}   # raises on any contradiction
    retained = [i for i, a in artifacts.items() if a is not None]
    assert len(retained) >= 16, 'at least one full pass must be retained per trial'
    final = MR.final_row_index(rows)
    assert all(artifacts[i] is not None for i in final.values()), 'the final row of every id must be retained'
    ids = [e['id'] for e in MR.yaml.safe_load((INSIDER / 'candidates.yaml').read_text())['candidates']
           if e['role'] == 'candidate']
    results = {cid: json.loads((INSIDER / 'results' / f'{cid}.json').read_text()) for cid in ids}
    MR.check_results_are_final(results, rows)
    n = MR.distinct_trials(rows)
    assert n <= 30 and all(results[cid]['dsr']['n_trials'] == n for cid in ids)


def test_report_champion_verdict_equals_its_results_json_and_stress_is_reconciled():
    ids = [e['id'] for e in MR.yaml.safe_load((INSIDER / 'candidates.yaml').read_text())['candidates']
           if e['role'] == 'candidate']
    results = {cid: json.loads((INSIDER / 'results' / f'{cid}.json').read_text()) for cid in ids}
    champ = MR.champion(results)['champion']
    report = (INSIDER / 'REPORT.md').read_text()
    assert f'**Champion: `{champ}`' in report
    assert f'Harness verdict: {results[champ]["verdict"]}.' in report
    stress = json.loads((INSIDER / 'results' / '_d07_stress.json').read_text())
    MR.check_stress_matches(stress, champ, results[champ])
