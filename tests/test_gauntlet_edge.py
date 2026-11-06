"""Gauntlet statistical-edge honesty (2026-11-05 smoke test).

`orb_googl` held a gauntlet PASS while its DSR was 0.093 deflated over 875
trials: S4 (PSR) and S5 (DSR) are record-only by default, so the PASS line was
indistinguishable from a PASS with a real edge. The verdict semantics are
pinned by tests/invariants/test_gauntlet_verdict.py and are NOT changed here;
these tests pin that the result now says `edge_credible: false` (JSON + Rich)
and that deploy / enable / `mmr strategies` / the runtime arm path surface it.
"""

import argparse
import datetime as dt
import io
import json
import logging as stdlib_logging
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
import yaml
from rich.console import Console

import trader.mmr_cli as cli
from trader.data.backtest_store import BacktestRecord, BacktestStore, compute_strategy_hash
from trader.data.gauntlet_store import (ADVISORY_MIN_DSR, ADVISORY_MIN_PSR,
                                        GauntletRecord, GauntletStore,
                                        assess_statistical_edge)
from test_gauntlet import CLEAN_ONPRICES, _write


# ---------------------------------------------------------------------------
# Pure assessment
# ---------------------------------------------------------------------------

def _checks(psr=None, dsr=None, n_trials=None, min_psr=None, min_dsr=None):
    s4 = {'status': 'pass', 'min_psr': min_psr}
    if psr is not None:
        s4['psr'] = psr
    s5 = {'status': 'pass', 'min_dsr': min_dsr}
    if dsr is not None:
        s5['dsr'] = dsr
    if n_trials is not None:
        s5['n_trials'] = n_trials
    return {'s1_imports': {'status': 'pass'}, 's2_lookahead': {'status': 'pass'},
            's3_battery': {'status': 'pass'}, 's4_psr': s4, 's5_dsr': s5}


class TestAssessStatisticalEdge:
    def test_orb_googl_incident_is_not_credible(self):
        """The exact field case: high PSR, DSR 0.093 over 875 trials."""
        edge = assess_statistical_edge(_checks(psr=0.99, dsr=0.093, n_trials=875))
        assert edge['edge_credible'] is False
        text = ' '.join(edge['warnings'])
        assert 's5_dsr' in text and '0.0930' in text and '875 trials' in text
        assert 'record-only' in text
        # PSR was above the bar: it must not be flagged.
        assert not any(w.startswith('s4_psr') for w in edge['warnings'])

    def test_both_above_bar_is_credible(self):
        edge = assess_statistical_edge(_checks(psr=0.99, dsr=0.97, n_trials=10))
        assert edge == {'edge_credible': True, 'warnings': []}

    def test_dsr_exactly_at_bar_is_credible(self):
        edge = assess_statistical_edge(_checks(psr=ADVISORY_MIN_PSR, dsr=ADVISORY_MIN_DSR))
        assert edge['edge_credible'] is True

    def test_low_psr_alone_is_not_credible(self):
        edge = assess_statistical_edge(_checks(psr=0.40, dsr=None))
        assert edge['edge_credible'] is False
        assert edge['warnings'][0].startswith('s4_psr')

    def test_high_psr_without_dsr_is_unknown_not_credible(self):
        """PSR never rejected anything across thirteen sweeps; on its own it
        is not evidence of an edge."""
        edge = assess_statistical_edge(_checks(psr=0.999, dsr=None))
        assert edge['edge_credible'] is None
        assert edge['warnings'] and 'not evaluated' in edge['warnings'][0]

    def test_nothing_computed_is_unknown(self):
        edge = assess_statistical_edge({'s1_imports': {'status': 'pass'}})
        assert edge['edge_credible'] is None
        assert edge['warnings']

    def test_nan_is_unknown_not_a_pass(self):
        edge = assess_statistical_edge(_checks(psr=float('nan'), dsr=float('nan')))
        assert edge['edge_credible'] is None

    def test_gated_stage_uses_its_own_gate(self):
        # --min-dsr 0.5 was set and met: judged against the gate the
        # operator chose, not the advisory bar.
        assert assess_statistical_edge(
            _checks(psr=0.99, dsr=0.6, min_dsr=0.5))['edge_credible'] is True
        edge = assess_statistical_edge(_checks(psr=0.99, dsr=0.6, min_dsr=0.7))
        assert edge['edge_credible'] is False
        assert 'gated (--min-dsr 0.7)' in edge['warnings'][0]


# ---------------------------------------------------------------------------
# _gauntlet_run end to end (verdict unchanged, edge reported)
# ---------------------------------------------------------------------------

def _seed_family(duckdb_path: str, code_hash: str, values, n_trials: int) -> None:
    """One run of THIS hash with a curve, plus a trial family for DSR."""
    start = dt.datetime(2024, 1, 1)
    curve = [{'timestamp': (start + dt.timedelta(days=i)).isoformat(), 'value': v}
             for i, v in enumerate(values)]
    store = BacktestStore(duckdb_path)
    for i in range(n_trials - 1):
        store.add(BacktestRecord(
            strategy_path='strategies/clean_op.py', class_name='CleanOnPrices',
            conids=[1], universe='', start_date=start, end_date=start,
            bar_size='1 day', initial_capital=100000.0, fill_policy='next_open',
            slippage_bps=0.0, commission_per_share=0.0,
            code_hash=f'other{i}', sharpe_ratio=0.5 + 0.1 * (i % 15)))
    store.add(BacktestRecord(
        strategy_path='strategies/clean_op.py', class_name='CleanOnPrices',
        conids=[1], universe='', start_date=start,
        end_date=start + dt.timedelta(days=len(values)), bar_size='1 day',
        initial_capital=100000.0, fill_policy='next_open',
        slippage_bps=0.0, commission_per_share=0.0,
        code_hash=code_hash, sharpe_ratio=0.3,
        equity_curve_json=json.dumps(curve)))


def _weak_curve(n=120):
    # Small positive drift + noise: PSR reasonably high, DSR crushed by the
    # trial family.
    import numpy as np
    rng = np.random.default_rng(7)
    rets = 0.0003 + rng.normal(0, 0.01, n)
    vals = [100000.0]
    for r in rets:
        vals.append(vals[-1] * (1 + r))
    return vals


class TestGauntletRunEdge:
    def test_pass_verdict_unchanged_but_edge_not_credible(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        _seed_family(tmp_duckdb_path, compute_strategy_hash(str(f)), _weak_curve(), 40)
        result = cli._gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path)
        assert result['checks']['s5_dsr'].get('dsr') is not None
        assert result['checks']['s5_dsr']['dsr'] < ADVISORY_MIN_DSR
        # Record-only: status and verdict exactly as before.
        assert result['checks']['s5_dsr']['status'] == 'pass'
        assert result['verdict'] == 'PASS'
        assert result['edge_credible'] is False
        assert any(w.startswith('s5_dsr') for w in result['warnings'])

    def test_no_backtest_edge_is_unknown(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        result = cli._gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path)
        assert result['verdict'] == 'PASS'
        assert result['edge_credible'] is None
        assert result['warnings']

    def test_gated_dsr_still_fails_verdict(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        _seed_family(tmp_duckdb_path, compute_strategy_hash(str(f)), _weak_curve(), 40)
        result = cli._gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path, min_dsr=0.95)
        assert result['checks']['s5_dsr']['status'] == 'fail'
        assert result['verdict'] == 'FAIL'
        assert result['edge_credible'] is False


# ---------------------------------------------------------------------------
# CLI surfaces
# ---------------------------------------------------------------------------

@pytest.fixture
def gauntlet_db(monkeypatch, tmp_duckdb_path):
    monkeypatch.setattr(cli, '_gauntlet_db_path', lambda: tmp_duckdb_path)
    return tmp_duckdb_path


@pytest.fixture
def rich_out(monkeypatch):
    buf = io.StringIO()
    monkeypatch.setattr(cli, 'console', Console(file=buf, width=250, force_terminal=False))
    return buf


@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    return tmp_path


def _gauntlet_ns(f):
    return argparse.Namespace(module_path=str(f), class_name='CleanOnPrices',
                              min_psr=None, min_dsr=None, name=None)


class TestGauntletHandlerOutput:
    def test_json_carries_edge_credible_false(self, tmp_path, gauntlet_db,
                                              monkeypatch, capsys):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        _seed_family(gauntlet_db, compute_strategy_hash(str(f)), _weak_curve(), 40)
        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_strategies_gauntlet(_gauntlet_ns(f))
        payload = json.loads(capsys.readouterr().out)
        assert payload['data']['verdict'] == 'PASS'
        assert payload['data']['edge_credible'] is False
        assert any('s5_dsr' in w for w in payload['data']['warnings'])
        # Persisted alongside the record for later audit.
        rec = GauntletStore(gauntlet_db).latest_for_hash(compute_strategy_hash(str(f)))
        assert 's5_dsr' in rec.notes

    def test_rich_shows_yellow_warning(self, tmp_path, gauntlet_db, rich_out):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        _seed_family(gauntlet_db, compute_strategy_hash(str(f)), _weak_curve(), 40)
        cli._handle_strategies_gauntlet(_gauntlet_ns(f))
        out = rich_out.getvalue()
        assert 'edge_credible: false' in out
        assert 'pass (record-only, below bar)' in out
        assert 'safety stages only' in out
        assert 'may now be' in out          # still a PASS — gating unchanged

    def test_rich_credible_edge_has_no_warning(self, tmp_path, gauntlet_db,
                                               rich_out, monkeypatch):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        monkeypatch.setattr(cli, '_gauntlet_check_dsr', lambda *a, **k: {
            'status': 'pass', 'min_dsr': None, 'dsr': 0.99, 'n_trials': 3})
        monkeypatch.setattr(cli, '_gauntlet_check_psr', lambda *a, **k: {
            'status': 'pass', 'min_psr': None, 'psr': 0.99})
        cli._handle_strategies_gauntlet(_gauntlet_ns(f))
        out = rich_out.getvalue()
        assert 'edge_credible' not in out
        assert 'Gauntlet PASS' in out and 'safety stages only' not in out


def _record_weak_pass(duckdb_path, f):
    code_hash = compute_strategy_hash(str(f))
    GauntletStore(duckdb_path).record(GauntletRecord(
        strategy_name='CleanOnPrices', module_path=str(f), class_name='CleanOnPrices',
        code_hash=code_hash, verdict='PASS',
        checks=_checks(psr=0.99, dsr=0.093, n_trials=875)))
    return code_hash


def _write_cfg(home: Path, name: str, module: str, auto_execute=True):
    d = home / '.config' / 'mmr'
    d.mkdir(parents=True, exist_ok=True)
    (d / 'strategy_runtime.yaml').write_text(yaml.safe_dump({'strategies': [{
        'name': name, 'module': module, 'class_name': 'CleanOnPrices',
        'bar_size': '1 min', 'conids': [1], 'auto_execute': auto_execute}]}))


class TestDeployEnableListSurfaceEdge:
    def test_enable_warns(self, tmp_home, gauntlet_db, tmp_path, rich_out):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _write_cfg(tmp_home, 'clean_strat', str(f))
        _record_weak_pass(gauntlet_db, f)
        mmr = MagicMock()
        cli._handle_strategies(mmr, argparse.Namespace(strat_action='enable',
                                                       name='clean_strat'))
        mmr.enable_strategy.assert_called_once_with('clean_strat')   # not blocked
        out = rich_out.getvalue()
        assert 'NO statistically credible edge' in out
        assert 'edge_credible: false' in out

    def test_enable_json_message_carries_warning(self, tmp_home, gauntlet_db,
                                                 tmp_path, monkeypatch, capsys,
                                                 rich_out):
        # rich_out stands in for dispatch()'s --json routing of `console` to
        # stderr: stdout must stay pure JSON.
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _write_cfg(tmp_home, 'clean_strat', str(f))
        _record_weak_pass(gauntlet_db, f)
        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_strategies(MagicMock(), argparse.Namespace(
            strat_action='enable', name='clean_strat'))
        lines = [json.loads(x) for x in capsys.readouterr().out.splitlines() if x.strip()]
        assert lines[-1]['success'] is True
        assert 'edge_credible: false' in lines[-1]['message']

    def test_deploy_warns_but_deploys(self, tmp_home, gauntlet_db, tmp_path,
                                      rich_out, monkeypatch):
        from test_gauntlet import _deploy_ns, _read_deployed
        monkeypatch.setattr(cli, '_ack_strategy_config', lambda *a, **k: True)
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _record_weak_pass(gauntlet_db, f)
        cli._handle_strategy_deploy(_deploy_ns(
            name='clean_strat', module=str(f), class_name='CleanOnPrices',
            auto_execute=True))
        assert [s['name'] for s in _read_deployed(tmp_home)['strategies']] == ['clean_strat']
        out = rich_out.getvalue()
        assert 'NO statistically credible edge' in out
        assert 'Deployed strategy' in out

    def test_strategies_list_json_and_rich(self, tmp_home, gauntlet_db, tmp_path,
                                           rich_out, monkeypatch, capsys):
        capsys.readouterr()
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _write_cfg(tmp_home, 'clean_strat', str(f))
        _record_weak_pass(gauntlet_db, f)
        mmr = MagicMock()
        mmr.strategies.return_value = pd.DataFrame([
            {'name': 'clean_strat', 'state': 'RUNNING', 'bar_size': '1 min',
             'conids': [1], 'hist_days_prior': 5, 'auto_execute': True},
            {'name': 'not_in_yaml', 'state': 'RUNNING', 'bar_size': '1 min',
             'conids': [2], 'hist_days_prior': 5, 'auto_execute': False},
        ])
        cli._handle_strategies_list(mmr)
        out = rich_out.getvalue()
        assert 'NOT credible' in out
        assert 'WARNING: clean_strat' in out

        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_strategies_list(mmr)
        rows = json.loads(capsys.readouterr().out)['data']
        by = {r['name']: r for r in rows}
        assert by['clean_strat']['edge_credible'] is False
        assert any('s5_dsr' in w for w in by['clean_strat']['edge_warnings'])
        assert by['not_in_yaml']['edge_credible'] is None
        assert by['not_in_yaml']['edge_warnings'] == []


    def test_config_fallback_surfaces_edge(self, tmp_home, gauntlet_db, tmp_path,
                                           rich_out, monkeypatch, capsys):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _write_cfg(tmp_home, 'clean_strat', str(f))
        _record_weak_pass(gauntlet_db, f)
        mmr = MagicMock()
        mmr.strategies.side_effect = ConnectionError('refused')
        cli._handle_strategies_list(mmr)
        assert 'WARNING: clean_strat' in rich_out.getvalue()

        capsys.readouterr()
        monkeypatch.setattr(cli, '_json_mode', True)
        cli._handle_strategies_list(mmr)
        out = capsys.readouterr().out.splitlines()
        rows = json.loads(out[-1])['data']
        assert rows[0]['edge_credible'] is False


class TestRuntimeArmPathLogsEdge:
    def test_warns_on_armed_pass_without_edge(self, tmp_path, tmp_duckdb_path, caplog):
        from trader.strategy.strategy_runtime import StrategyRuntime
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        code_hash = _record_weak_pass(tmp_duckdb_path, f)
        store = GauntletStore(tmp_duckdb_path)
        with caplog.at_level(stdlib_logging.WARNING):
            StrategyRuntime._warn_gauntlet_edge('clean_strat', store, code_hash,
                                                'CleanOnPrices')
        assert 'NO statistically credible edge' in caplog.text

    def test_never_raises(self, caplog):
        from trader.strategy.strategy_runtime import StrategyRuntime
        broken = MagicMock()
        broken.latest_pass_for_hash.side_effect = RuntimeError('db gone')
        with caplog.at_level(stdlib_logging.WARNING):
            StrategyRuntime._warn_gauntlet_edge('x', broken, 'h', 'C')
        assert 'could not assess gauntlet edge' in caplog.text
