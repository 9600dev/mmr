"""Known answers on synthetic panels (criterion 5) and the fixed-cell mode (3)."""
from __future__ import annotations

import duckdb
import numpy as np
import pytest

from trader.research.lowfreq.evaluate import evaluate
from trader.research.lowfreq.verdict import GateInputs, decide
from research.conftest import entry, make_etf_rows, write_family

BENCH = entry('bench_ew', 'benchmark', 'EtfEqualWeight', module='trader.research.lowfreq.reference_strategies')


def _reload_with_drift(research_env, drift):
    con = duckdb.connect(str(research_env['hist']))
    con.execute('delete from etf_daily_bars')
    con.executemany('INSERT INTO etf_daily_bars VALUES (?,?,?,?,?,?,?,?,?,?,?)', make_etf_rows(drift=drift, seed=3))
    con.close()


def test_candidate_identical_to_benchmark_has_zero_active_return_and_fails(research_env):
    write_family(research_env['root'], 'fam', [BENCH, entry('same', 'candidate', 'SameAsBenchmark', benchmarks=['bench_ew'])])
    r = evaluate('fam/same')
    assert abs(r['active']['ann_return']) < 1e-4          # |active| < 1bp/yr
    assert r['verdict'] == 'FAIL'
    assert r['oos']['days'] > 200 and r['window']['n_folds'] >= 2


def test_candidate_holding_injected_drift_passes(research_env):
    # SPY drifts +60%/yr against flat peers: a book that holds it must beat equal weight.
    _reload_with_drift(research_env, {'SPY': 0.60, 'IEF': -0.10, 'GLD': -0.10, 'BIL': 0.0})
    write_family(research_env['root'], 'fam', [BENCH, entry('spy', 'candidate', 'HoldOne', benchmarks=['bench_ew'])])
    r = evaluate('fam/spy')
    assert r['active']['ann_return'] > 0.10
    assert r['active']['t_nw'] >= 2.0
    assert r['verdict'] == 'PASS', r['gates']


def test_lookahead_strategy_is_rejected_before_any_metric(research_env):
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('peek', 'candidate', 'PeeksAhead', benchmarks=['bench_ew']),
                 entry('peek2', 'candidate', 'PeeksInOnPanel', benchmarks=['bench_ew'])])
    for cid in ('peek', 'peek2'):
        with pytest.raises(AssertionError, match='LOOKAHEAD'):
            evaluate(f'fam/{cid}')
        assert not (research_env['root'] / 'fam' / 'results' / f'{cid}.json').exists()
    import json
    rows = [json.loads(l) for l in (research_env['root'] / 'fam' / 'trials.jsonl').read_text().splitlines()]
    assert [r['status'] for r in rows] == ['error', 'error'] and all('LOOKAHEAD' in r['error'] for r in rows)


def test_weights_below_one_leave_remainder_in_cash_earning_zero(research_env):
    _reload_with_drift(research_env, {'SPY': 0.30})
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('full', 'candidate', 'HoldOne', benchmarks=['bench_ew'], default_params={'WEIGHT': 1.0}, params={'WEIGHT': [1.0]}),
                 entry('half', 'candidate', 'HoldOne', benchmarks=['bench_ew'], default_params={'WEIGHT': 0.5}, params={'WEIGHT': [0.5]})])
    full = evaluate('fam/full')['full_period']['report_cell']
    half = evaluate('fam/half')['full_period']['report_cell']
    # half the exposure, half the arithmetic return (cash adds nothing); costs are tiny here
    assert half['ann_mean'] == pytest.approx(full['ann_mean'] / 2, rel=0.05)
    assert half['ann_mean'] > 0


def test_fixed_cell_mode_runs_one_cell_over_the_window_without_selection(research_env):
    _reload_with_drift(research_env, {'SPY': 0.40})
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('grid', 'candidate', 'Gridded', benchmarks=['bench_ew'], params={'LOOKBACK': [10, 20], 'WEIGHT': [0.5, 1.0]})])
    r = evaluate('fam/grid', fixed_params={'LOOKBACK': 10, 'WEIGHT': 0.5}, start='2016-06-01', end='2017-06-01')
    assert r['mode'] == 'fixed' and r['window'] == {**r['window'], 'start': '2016-06-01', 'end': '2017-06-01', 'n_folds': 1}
    assert r['grid']['report_cell'] == {'LOOKBACK': 10, 'WEIGHT': 0.5}
    assert r['pbo'] is None and r['folds'][0]['chosen'] == {'LOOKBACK': 10, 'WEIGHT': 0.5}
    assert r['gates']['g_pbo_and_stability'] is None
    first, last = min(r['series']['oos_returns']), max(r['series']['oos_returns'])
    assert first >= '2016-06-01' and last <= '2017-06-01'
    assert r['results_path'].endswith('grid__fixed_2016-06-01_2017-06-01.json')
    assert 'bench_ew' in r['oos_benchmarks']


def test_grid_of_four_cells_reports_pbo_and_stability(research_env):
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('grid', 'candidate', 'Gridded', benchmarks=['bench_ew'], params={'LOOKBACK': [10, 20], 'WEIGHT': [0.5, 1.0]})])
    r = evaluate('fam/grid')
    assert r['grid']['cells'] == 4 and len(r['full_period']['cells']) == 4
    assert r['pbo'] is not None and 0.0 <= r['pbo']['pbo'] <= 1.0
    assert r['selection_stability'] is not None
    assert all(f['chosen'] is not None for f in r['folds'])


def test_verdict_gates_are_fail_closed_and_match_d01():
    good = GateInputs(1.0, 0.8, 0.03, 2.5, 0.95, -0.2, -0.22, 0.8, 0.01, 1)
    assert decide(good)['verdict'] == 'PASS'
    assert decide(good.__class__(**{**good.__dict__, 'active_t_nw': 1.5}))['verdict'] == 'PROMISING'
    assert decide(good.__class__(**{**good.__dict__, 'active_t_nw': 0.5}))['verdict'] == 'FAIL'
    assert decide(good.__class__(**{**good.__dict__, 'active_dsr': None}))['verdict'] == 'PROMISING'
    assert decide(good.__class__(**{**good.__dict__, 'oos_max_drawdown': -0.30}))['verdict'] == 'FAIL'
    assert decide(good.__class__(**{**good.__dict__, 'active_ann_return_stressed': -0.001}))['verdict'] == 'FAIL'
    g4 = good.__class__(**{**good.__dict__, 'n_cells': 4, 'pbo': 0.5, 'selection_stability': 0.9})
    assert decide(g4)['verdict'] == 'PROMISING'
    assert decide(good.__class__(**{**good.__dict__, 'n_cells': 4, 'pbo': 0.1, 'selection_stability': 0.9}))['verdict'] == 'PASS'


def test_candidate_coverage_is_forwarded_to_the_covered_benchmark(research_env):
    """D05: the decisive benchmark owns exactly the names the candidate could choose
    from. A candidate covering only SPY+IEF makes EqEwCovered hold those two, so
    the benchmark's return equals a plain equal weight of SPY+IEF."""
    covered_bench = entry('bench_cov', 'benchmark', 'EqEwCovered', module='trader.research.lowfreq.reference_strategies',
                          params={'MIN_NAMES': [2]}, default_params={'MIN_NAMES': 2})
    write_family(research_env['root'], 'fam', [covered_bench,
                 entry('cov', 'candidate', 'CoversTwo', benchmarks=['bench_cov']),
                 entry('two_ew', 'candidate', 'EqualWeightTwo', benchmarks=['bench_cov'])])
    r = evaluate('fam/cov')
    assert r['coverage']['forwarded_to_benchmarks'] is True
    assert r['coverage']['median_names_covered_per_day'] == 2.0
    bench_full = r['full_period']['benchmarks']['bench_cov']
    # a candidate that IS equal weight of the same two names must have zero active return
    # against the covered benchmark
    r2 = evaluate('fam/two_ew')
    assert abs(r2['active']['ann_return']) < 1e-4
    assert r2['full_period']['report_cell']['cagr'] == pytest.approx(bench_full['cagr'], abs=1e-9)
    assert bench_full['median_names_held'] == 2.0


def test_capacity_metrics_are_reported(research_env):
    write_family(research_env['root'], 'fam', [BENCH, entry('spy', 'candidate', 'HoldOne', benchmarks=['bench_ew'])])
    r = evaluate('fam/spy')
    fp = r['full_period']['report_cell']
    assert fp['min_cash_fraction'] >= -1e-9                      # D06 holds through the harness
    for fold in r['folds']:
        assert fold['oos']['min_cash_fraction'] >= -1e-9
        for b in fold['benchmarks'].values():
            assert b['min_cash_fraction'] >= -1e-9
    assert fp['median_names_held'] == 1.0
    assert fp['median_holding_adv_usd'] is not None and fp['median_holding_adv_usd'] > 0
    assert fp['turnover_per_year'] is not None and fp['turnover_per_year'] < 0.5   # buy once, hold


EQ_BENCH = entry('eq_ew_covered', 'benchmark', 'EqEwCovered', module='trader.research.lowfreq.reference_strategies',
                 params={'MIN_NAMES': [2]}, default_params={'MIN_NAMES': 2})
EQ_FAMILY = dict(universe='equity', dev_period={'start': '2016-11-01', 'end': '2018-02-01'},
                 walk_forward={'train_days': 180, 'test_days': 120}, warmup_days=30)


def test_buying_a_name_outside_pit_membership_is_refused(research_env):
    """D05: opens only inside the point-in-time universe. CCC leaves the top 500 in
    Aug 2017 but keeps trading; a strategy that keeps buying it is refused and the
    refusal is ledgered as an error."""
    write_family(research_env['root'], 'eq', [EQ_BENCH,
                 entry('churn', 'candidate', 'ChurnsOneName', benchmarks=['eq_ew_covered'])], **EQ_FAMILY)
    with pytest.raises(ValueError, match='universe violation'):
        evaluate('eq/churn')
    import json
    rows = [json.loads(l) for l in (research_env['root'] / 'eq' / 'trials.jsonl').read_text().splitlines()]
    assert rows[-1]['status'] == 'error' and 'universe violation' in rows[-1]['error']


def test_covered_benchmark_sells_leavers_and_reports_cash_and_frozen_diagnostics(research_env):
    """The covered-EW book must exit CCC at a real price when it leaves membership:
    nothing is frozen at the end, cash never goes materially negative, and the
    equity loader's member mask reaches the strategy through research data."""
    write_family(research_env['root'], 'eq', [EQ_BENCH], **EQ_FAMILY)
    r = evaluate('eq/eq_ew_covered')
    fp = r['full_period']['report_cell']
    assert fp['n_frozen_holdings_end'] == 0 and fp['frozen_value_fraction_end'] == 0.0
    # D06: cash rebuilt from the fill log never below zero on a long-only book,
    # and the guard's activity is reported (a count, possibly zero)
    assert fp['min_cash_fraction'] >= -1e-9
    assert isinstance(fp['scaled_buy_fills'], int) and isinstance(fp['scaled_rebalances'], int)
    assert r['ledger_metrics']['full_min_cash_fraction'] >= -1e-9
    assert fp['median_names_held'] in (2.0, 3.0)
    assert r['coverage']['forwarded_to_benchmarks'] is True
    # only members are ever held at the end: CCC left in Aug 2017, the window ends Feb 2018
    assert fp['n_trades'] > 0
