"""Round-4 regressions: sampled-prefix lookahead with fresh state (honest
stateful strategies pass, suppressed/omitted-key leaks fail, no short-panel
bypass), stopped names exit at their last stored print (D07, no fabricated
opens), coverage derived per window with candidate BUYs validated against it,
and ledger/path hardening (safe segments, corrupt ledger, mixed-key grids)."""
from __future__ import annotations

import datetime as dt
import json

import duckdb
import numpy as np
import pandas as pd
import pytest

from trader.research.lowfreq import engine, load_etf_panel
from trader.research.lowfreq.evaluate import evaluate
from trader.research.lowfreq.registry import RegistryError
from research.conftest import PIT_TICKERS, entry, write_family

BENCH = entry('bench_ew', 'benchmark', 'EtfEqualWeight', module='trader.research.lowfreq.reference_strategies')


def _ledger(root, fam='fam'):
    p = root / fam / 'trials.jsonl'
    return [json.loads(l) for l in p.read_text().splitlines() if l.strip()] if p.exists() else []


# ---------------------------------------------------------------- lookahead

def test_honest_stateful_strategies_pass_the_lookahead_check_and_evaluate(research_env):
    """The executor shares ONE mutable state across sequential on_panel calls;
    a strategy that counts its rebalances in it is honest and must not be
    refused (the checker replays each fresh prefix sequentially)."""
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('alt', 'candidate', 'StatefulAlternator', benchmarks=['bench_ew']),
                 entry('churn', 'candidate', 'ChurnsOneName', benchmarks=['bench_ew'], TICKER='SPY')])
    r = evaluate('fam/alt')
    assert r['verdict'] in ('PASS', 'PROMISING', 'FAIL') and r['full_period']['report_cell']['n_trades'] > 2
    panel = load_etf_panel(None, '2015-01-01', '2016-12-31')
    engine.lookahead_check(entry('churn', 'candidate', 'ChurnsOneName'), {}, panel)


def test_a_state_that_suppresses_its_instruction_on_full_data_is_rejected(research_env):
    """Full run: None everywhere (a next bar always exists); honest prefix:
    emits at its last bar. The old checker only recomputed where the full run
    had an instruction, so this passed."""
    write_family(research_env['root'], 'fam', [BENCH, entry('sup', 'candidate', 'SuppressesOnFullData', benchmarks=['bench_ew'])])
    with pytest.raises(AssertionError, match='LOOKAHEAD'):
        evaluate('fam/sup')
    assert not (research_env['root'] / 'fam' / 'results').exists()
    assert _ledger(research_env['root'])[-1]['status'] == 'error'


def test_an_omitted_key_is_not_an_explicit_zero(research_env):
    """The executor holds an omitted name and flattens an explicit 0.0; a
    strategy whose only leak is WHICH of the two it does must be caught."""
    panel = load_etf_panel(None, '2015-01-01', '2016-12-31')
    with pytest.raises(AssertionError, match='LOOKAHEAD'):
        engine.lookahead_check(entry('omit', 'candidate', 'OmitsKeyWhenPeeking'), {}, panel)
    assert engine._weights_equal({'SPY': 1.0}, {'SPY': 1.0, 'IEF': 0.0}) is False
    assert engine._weights_equal({}, None) is False
    assert engine._weights_equal({'SPY': 1.0, 'IEF': 0.0}, {'IEF': 0.0, 'SPY': 1.0}) is True


def test_short_panels_are_checked_not_bypassed(research_env):
    panel = load_etf_panel(None, '2015-01-01', '2015-01-05')      # 3 bars
    assert len(panel.close) == 3
    with pytest.raises(AssertionError, match='LOOKAHEAD'):
        engine.lookahead_check(entry('peek', 'candidate', 'PeeksInOnPanel'), {}, panel)
    engine.lookahead_check(entry('spy', 'candidate', 'HoldOne'), {}, panel)      # honest: passes


def test_sampled_prefixes_cover_boundaries_and_consecutive_runs():
    n = 4300
    picks = engine.sampled_prefixes(n)
    assert picks[:3] == [0, 1, 2] and picks[-3:] == [n - 4, n - 3, n - 2]
    runs, cur = [], [picks[3]]
    for a, b in zip(picks[3:], picks[4:]):
        if b == a + 1:
            cur.append(b)
        else:
            runs.append(cur); cur = [b]
    runs.append(cur)
    long_runs = [r for r in runs if len(r) >= engine.PROBE_WINDOW_LEN]
    assert len(long_runs) == engine.PROBE_WINDOWS
    assert engine.sampled_prefixes(2) == [0] and engine.sampled_prefixes(1) == []
    assert engine.sampled_prefixes(10) == list(range(9))


def test_prefix_replay_builds_fresh_state_from_the_truncated_panel(research_env, monkeypatch):
    """Every sampled prefix gets its own precompute on the truncated panel and a
    sequential replay from index 0 - never a truncated copy of an already
    replayed state."""
    from trader.research.lowfreq.reference_strategies import EtfEqualWeight
    panel = load_etf_panel(None, '2015-01-01', '2016-12-31')
    lengths, calls = [], []
    orig_pre, orig_on = EtfEqualWeight.precompute_panel, EtfEqualWeight.on_panel

    def pre(self, wide):
        lengths.append(len(wide)); return orig_pre(self, wide)

    def on(self, wide, state, i):
        calls.append((len(wide), i)); return orig_on(self, wide, state, i)
    monkeypatch.setattr(EtfEqualWeight, 'precompute_panel', pre)
    monkeypatch.setattr(EtfEqualWeight, 'on_panel', on)
    e = entry('ew', 'benchmark', 'EtfEqualWeight', module='trader.research.lowfreq.reference_strategies')
    engine.lookahead_check(e, {}, panel, prefixes=[0, 5, 40])
    n = len(panel.close)
    assert {l for l in lengths if l in (1, 6, 41)} == {1, 6, 41}             # a fresh precompute per prefix
    for L in (1, 6, 41):
        assert [i for l, i in calls if l == L] == list(range(L))              # sequential replay from 0
    assert [i for l, i in calls if l == n] == list(range(n - 1))              # the reference run


# ---------------------------------------------------------------- D07 stopped names

def _net(trades):
    qty = {}
    for t in trades:
        qty[t.conid] = qty.get(t.conid, 0.0) + (t.quantity if str(t.action).endswith('BUY') else -t.quantity)
    return qty


def _stopped_frames(n=60, stop_at=30):
    days = pd.bdate_range('2020-01-01', periods=n)
    a = pd.DataFrame({'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.0, 'volume': 1e6}, index=days)
    b = a.copy()
    b.iloc[stop_at:] = np.nan                       # B's last print is bar stop_at-1
    b = b.dropna()
    return {'A': a, 'B': b}, days


def test_executor_sells_a_stopped_name_at_its_last_print_and_never_after(tmp_path):
    """No open exists after B's last print, so nothing can fill there; with the
    D07 flag the executor sells B at its last stored close (costs charged) and
    counts it. Without the flag B stays in the book at a stale mark forever."""
    from trader.objects import BarSize
    from trader.simulation.backtester import BacktestConfig, Backtester
    from trader.trading.strategy import Strategy
    frames, days = _stopped_frames()

    class HoldBoth(Strategy):
        def precompute_panel(self, panel):
            return {}

        def on_panel(self, panel, state, index):
            return {'A': 0.5, 'B': 0.5} if index == 0 else None

    def run(flag):
        cfg = BacktestConfig(start_date=dt.datetime(2020, 1, 1), end_date=dt.datetime(2020, 4, 1),
                             initial_capital=1e6, bar_size=BarSize.Days1, slippage_bps=10.0,
                             commission_per_share=0.005, panel_exit_at_last_print=flag)
        return Backtester(engine._NoStorage(), cfg).run_panel(HoldBoth(), ['A', 'B'], frames=frames)

    on = run(True)
    sells = [t for t in on.trades if str(t.action).endswith('SELL')]
    assert len(sells) == 1 and sells[0].conid == 'B' and pd.Timestamp(sells[0].timestamp) == days[29]
    assert sells[0].price == pytest.approx(100.0 * (1 - 0.001))
    assert _net(on.trades)['B'] == pytest.approx(0.0) and _net(on.trades)['A'] > 0
    assert on.diagnostics['panel_forced_exits'] == 1
    assert on.diagnostics['panel_forced_exit_value'] == pytest.approx(sells[0].quantity * 100.0)
    assert all(pd.Timestamp(t.timestamp) <= days[29] for t in on.trades if t.conid == 'B')
    off = run(False)
    assert not [t for t in off.trades if str(t.action).endswith('SELL')]
    assert _net(off.trades)['B'] > 0 and off.diagnostics['panel_forced_exits'] == 0


def test_a_name_alive_at_the_window_end_is_not_force_exited():
    from trader.objects import BarSize
    from trader.simulation.backtester import BacktestConfig, Backtester
    from trader.trading.strategy import Strategy
    frames, _ = _stopped_frames(n=60, stop_at=60)        # nothing stops

    class HoldBoth(Strategy):
        def precompute_panel(self, panel):
            return {}

        def on_panel(self, panel, state, index):
            return {'A': 0.5, 'B': 0.5} if index == 0 else None
    cfg = BacktestConfig(start_date=dt.datetime(2020, 1, 1), end_date=dt.datetime(2020, 4, 1), initial_capital=1e6,
                         bar_size=BarSize.Days1, panel_exit_at_last_print=True)
    res = Backtester(engine._NoStorage(), cfg).run_panel(HoldBoth(), ['A', 'B'], frames=frames)
    assert res.diagnostics['panel_forced_exits'] == 0
    assert _net(res.trades)['A'] > 0 and _net(res.trades)['B'] > 0


EQ_BENCH = entry('eq_ew_covered', 'benchmark', 'EqEwCovered', module='trader.research.lowfreq.reference_strategies',
                 params={'MIN_NAMES': [2]}, default_params={'MIN_NAMES': 2})
EQ_FAMILY = dict(universe='equity', dev_period={'start': '2016-11-01', 'end': '2018-02-01'},
                 walk_forward={'train_days': 180, 'test_days': 120}, warmup_days=30)


def test_equity_loader_never_fabricates_a_price_and_the_harness_exits_at_the_last_print(research_env):
    """CCC's rows are deleted from 2017-05-01: its open/close are NaN from
    then on (no forward-fill), the covered-EW book that held it is sold at its
    last stored print, the exit is counted, and no CCC fill exists after it."""
    from trader.research.lowfreq import load_equity_panel
    con = duckdb.connect(str(research_env['hist']))
    # CCC is still a member (and held by the covered-EW book) in April 2017
    con.execute("DELETE FROM pit_daily_bars WHERE ticker = 'CCC' AND day >= DATE '2017-05-01'")
    con.close()
    panel = load_equity_panel('2016-11-01', '2018-02-01', top_n=500)
    last = panel.close['CCC'].last_valid_index()
    assert last is not None and last < pd.Timestamp('2017-05-01')
    assert panel.close['CCC'].loc[panel.close.index > last].isna().all()
    assert panel.open['CCC'].loc[panel.open.index > last].isna().all()
    write_family(research_env['root'], 'eq', [EQ_BENCH], **EQ_FAMILY)
    r = evaluate('eq/eq_ew_covered')
    sn = r['stopped_names']
    assert sn['full_period_forced_exits'] == 1 and sn['full_period_forced_exit_value_fraction'] > 0
    fp = r['full_period']['report_cell']
    assert fp['forced_exits'] == 1 and fp['n_frozen_holdings_end'] == 0
    assert fp['min_cash_fraction'] >= -1e-9


# ---------------------------------------------------------------- D05 coverage per window

def test_benchmark_receives_coverage_derived_from_each_executed_window(research_env, monkeypatch):
    """The frame installed on the covered benchmark must be indexed on the
    window's own bars (not the unsliced full-period frame), for every fold and
    for the full-period run."""
    from trader.research.lowfreq.reference_strategies import EqEwCovered
    seen = []
    orig = EqEwCovered.set_coverage

    def spy(self, frame):
        seen.append(frame); return orig(self, frame)
    monkeypatch.setattr(EqEwCovered, 'set_coverage', spy)
    write_family(research_env['root'], 'fam', [
        entry('bench_cov', 'benchmark', 'EqEwCovered', module='trader.research.lowfreq.reference_strategies',
              params={'MIN_NAMES': [2]}, default_params={'MIN_NAMES': 2}),
        entry('two', 'candidate', 'CoversTwo', benchmarks=['bench_cov'])])
    r = evaluate('fam/two')
    assert r['coverage']['per_window'] is True and r['coverage']['candidate_buys_validated'] is True
    assert seen, 'the benchmark never received a coverage frame'
    starts = sorted({str(f.index[0].date()) for f in seen})
    assert len(starts) >= 2, f'every window must hand over its own frame, saw starts {starts}'
    full_start = pd.Timestamp(r['window']['start']) - pd.Timedelta(days=r['window']['warmup_days'])
    for f in seen:
        assert f.index[0] >= full_start and f.index[-1] <= pd.Timestamp(r['window']['end'])
        assert set(f.columns[f.any()]) <= {'SPY', 'IEF'}


def test_a_candidate_buying_outside_its_own_published_coverage_is_refused(research_env):
    """CoversByParam with NAMES=2 covers SPY+IEF; forcing it to hold GLD is a
    coverage violation - the benchmark would own weak names while the
    candidate traded different ones."""
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('outside', 'candidate', 'CoversTwo', benchmarks=['bench_ew'], TICKER='GLD')])
    with pytest.raises(ValueError, match='coverage violation'):
        evaluate('fam/outside')
    assert _ledger(research_env['root'])[-1]['status'] == 'error'


# ---------------------------------------------------------------- D02 paths + ledger exactly-once

@pytest.mark.parametrize('target', ['../fam/x', 'fam/../x', 'Fam/x', 'fam/x.json', 'fam/x/y', '/fam/x', 'fam/', 'fam/x y'])
def test_unsafe_targets_are_refused_at_the_root_ledger_before_any_family_access(research_env, target):
    write_family(research_env['root'], 'fam', [BENCH, entry('spy', 'candidate', 'HoldOne', benchmarks=['bench_ew'])])
    before = sorted(p.relative_to(research_env['root']) for p in research_env['root'].rglob('*'))
    with pytest.raises(ValueError, match='must be <family>/<candidate_id>'):
        evaluate(target)
    rows = [json.loads(l) for l in (research_env['root'] / 'trials.jsonl').read_text().splitlines()]
    assert rows[-1]['status'] == 'refused' and rows[-1]['id'] == target and rows[-1]['family'] is None
    after = sorted(p.relative_to(research_env['root']) for p in research_env['root'].rglob('*'))
    from pathlib import Path
    assert set(after) - set(before) == {Path('trials.jsonl')}


def test_registry_refuses_ids_that_are_not_safe_segments(research_env):
    write_family(research_env['root'], 'fam', [BENCH, entry('../escape', 'candidate', 'HoldOne', benchmarks=['bench_ew'])])
    with pytest.raises(RegistryError, match='must match'):
        evaluate('fam/spy')
    assert _ledger(research_env['root'])[-1]['status'] == 'refused'


def test_a_mixed_key_grid_is_refused_with_a_ledger_row(research_env):
    write_family(research_env['root'], 'fam', [BENCH,
                 entry('mixed', 'candidate', 'Gridded', benchmarks=['bench_ew'], params={1: [1, 2], 'LOOKBACK': [20]})])
    with pytest.raises(RegistryError, match='keys must be strings'):
        evaluate('fam/mixed')
    assert _ledger(research_env['root'])[-1]['status'] == 'refused'


def test_a_corrupt_ledger_line_is_reported_and_an_error_row_is_still_appended(research_env):
    write_family(research_env['root'], 'fam', [BENCH, entry('spy', 'candidate', 'HoldOne', benchmarks=['bench_ew'])])
    p = research_env['root'] / 'fam' / 'trials.jsonl'
    p.write_text('{"not": "closed"\n')
    with pytest.raises(ValueError, match='corrupt ledger line'):
        evaluate('fam/spy')
    lines = p.read_text().splitlines()
    assert len(lines) == 2
    row = json.loads(lines[1])
    assert row['status'] == 'error' and 'corrupt ledger line' in row['error'] and row['id'] == 'spy'
    assert not (research_env['root'] / 'fam' / 'results').exists()
