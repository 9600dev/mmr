"""Tests for the "no hash, no live" strategy gauntlet.

Covers: GauntletStore CRUD + hash keying, the S1 import-allowlist scan,
end-to-end gauntlet runs (clean pass, NaN-crash S3 fail, lookahead S2 fail,
denied-import S1 fail), deploy/enable refusal without a PASS record (and
after editing the file — hash mismatch), the StrategyRuntime warn-vs-enforce
arm gate, and the two adjacent deploy fixes (safe_load, nonexistent-module /
missing-class refusal).
"""

import argparse
import datetime as dt
import json
import logging as stdlib_logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from trader.data.backtest_store import BacktestRecord, BacktestStore, compute_strategy_hash
from trader.data.gauntlet_store import GauntletRecord, GauntletStore
from trader.mmr_cli import (
    _gauntlet_enable_refusal,
    _gauntlet_pass_refusal,
    _gauntlet_run,
    _gauntlet_scan_imports,
    _handle_strategies,
    _handle_strategies_gauntlet,
    _handle_strategy_deploy,
)
from trader.strategy.strategy_runtime import StrategyRuntime


# ---------------------------------------------------------------------------
# Strategy source bodies
# ---------------------------------------------------------------------------

CLEAN_ONPRICES = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class CleanOnPrices(Strategy):
    def on_prices(self, prices):
        if prices.empty:
            return None
        closes = prices['close'].dropna()
        if len(closes) < 20:
            return None
        if float(closes.iloc[-1]) > float(closes.rolling(20).mean().iloc[-1]):
            return Signal(source_name=self.name or 'clean', action=Action.BUY,
                          probability=0.6, risk=0.4)
        return None
"""

CLEAN_PRECOMPUTE = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class CleanPrecompute(Strategy):
    EMA_PERIOD = 20

    def precompute(self, prices):
        ema = prices['close'].ewm(span=self.EMA_PERIOD, adjust=False).mean()
        return {'ema': ema.to_numpy()}

    def on_bar(self, prices, state, index):
        if index < self.EMA_PERIOD:
            return None
        close = float(prices['close'].iloc[index])
        ema = float(state['ema'][index])
        if close != close or ema != ema:
            return None
        if close > ema * 1.001:
            return Signal(source_name=self.name or 'cp', action=Action.BUY,
                          probability=0.7, risk=0.3)
        return None
"""

NAN_CRASHER = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class NanCrasher(Strategy):
    def on_prices(self, prices):
        if prices.empty:
            return None
        last = float(prices['close'].iloc[-1])
        if last != last:
            raise ValueError('NaN close — crash')
        return None
"""

LOOKAHEAD_LEAKER = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class Leaker(Strategy):
    def precompute(self, prices):
        return {'future': prices['close'].shift(-1).to_numpy()}

    def on_bar(self, prices, state, index):
        return None
"""

DENIED_IMPORTER = """
import socket
from ib_async import IB
from trader.trading.strategy import Strategy

class Phones_Home(Strategy):
    def on_prices(self, prices):
        return None
"""

TWO_CLASSES = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class ClassA(Strategy):
    def on_prices(self, prices):
        return None

class ClassB(Strategy):
    def on_prices(self, prices):
        return None
"""


def _write(directory: Path, filename: str, body: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / filename
    path.write_text(body)
    return path


def _record_pass(duckdb_path: str, module_file: Path, class_name: str) -> str:
    code_hash = compute_strategy_hash(str(module_file))
    assert code_hash
    GauntletStore(duckdb_path).record(GauntletRecord(
        strategy_name=class_name,
        module_path=str(module_file),
        class_name=class_name,
        code_hash=code_hash,
        verdict='PASS',
        checks={'s1_imports': {'status': 'pass'}},
    ))
    return code_hash


# ---------------------------------------------------------------------------
# GauntletStore CRUD + hash keying
# ---------------------------------------------------------------------------

class TestGauntletStore:
    def test_record_and_latest_for_hash(self, tmp_duckdb_path):
        store = GauntletStore(tmp_duckdb_path)
        checks = {'s1_imports': {'status': 'pass'}, 's3_battery': {'status': 'pass'}}
        run_id = store.record(GauntletRecord(
            strategy_name='s1', module_path='strategies/a.py', class_name='A',
            code_hash='aaa', verdict='PASS', checks=checks, notes='n',
        ))
        assert run_id >= 1
        rec = store.latest_for_hash('aaa')
        assert rec is not None
        assert rec.class_name == 'A'
        assert rec.verdict == 'PASS'
        assert rec.checks == checks
        assert rec.notes == 'n'
        assert store.latest_for_hash('bbb') is None

    def test_has_pass_keyed_by_exact_hash(self, tmp_duckdb_path):
        store = GauntletStore(tmp_duckdb_path)
        store.record(GauntletRecord(
            strategy_name='s', module_path='m.py', class_name='C',
            code_hash='hash_a', verdict='PASS',
        ))
        store.record(GauntletRecord(
            strategy_name='s', module_path='m.py', class_name='C',
            code_hash='hash_b', verdict='FAIL',
        ))
        assert store.has_pass('hash_a') is True
        assert store.has_pass('hash_b') is False
        assert store.has_pass('hash_never_seen') is False
        assert store.has_pass('') is False

    def test_latest_for_hash_returns_most_recent(self, tmp_duckdb_path):
        store = GauntletStore(tmp_duckdb_path)
        store.record(GauntletRecord(
            strategy_name='s', module_path='m.py', class_name='C',
            code_hash='h', verdict='FAIL',
        ))
        store.record(GauntletRecord(
            strategy_name='s', module_path='m.py', class_name='C',
            code_hash='h', verdict='PASS',
        ))
        assert store.latest_for_hash('h').verdict == 'PASS'
        assert store.has_pass('h') is True

    def test_empty_hash_refused(self, tmp_duckdb_path):
        store = GauntletStore(tmp_duckdb_path)
        with pytest.raises(ValueError, match='empty code_hash'):
            store.record(GauntletRecord(
                strategy_name='s', module_path='m.py', class_name='C',
                code_hash='', verdict='PASS',
            ))

    def test_bad_verdict_refused(self, tmp_duckdb_path):
        store = GauntletStore(tmp_duckdb_path)
        with pytest.raises(ValueError, match='verdict'):
            store.record(GauntletRecord(
                strategy_name='s', module_path='m.py', class_name='C',
                code_hash='h', verdict='MAYBE',
            ))

    def test_latest_pass_for_class(self, tmp_duckdb_path):
        store = GauntletStore(tmp_duckdb_path)
        store.record(GauntletRecord(
            strategy_name='s', module_path='m.py', class_name='C',
            code_hash='old_hash', verdict='PASS',
        ))
        store.record(GauntletRecord(
            strategy_name='s', module_path='m.py', class_name='C',
            code_hash='newer_hash', verdict='FAIL',
        ))
        latest = store.latest_pass_for_class('C')
        assert latest is not None and latest.code_hash == 'old_hash'
        assert store.latest_pass_for_class('Unknown') is None

    def test_has_pass_keyed_by_hash_and_class(self, tmp_duckdb_path):
        """FIX 2: a PASS for ClassA in a file does NOT authorize ClassB in
        the same file (same code_hash, different class)."""
        store = GauntletStore(tmp_duckdb_path)
        store.record(GauntletRecord(
            strategy_name='a', module_path='m.py', class_name='ClassA',
            code_hash='shared_hash', verdict='PASS',
        ))
        # class-agnostic lookup (legacy call) still sees the PASS
        assert store.has_pass('shared_hash') is True
        # class-scoped lookups: only ClassA is authorized
        assert store.has_pass('shared_hash', 'ClassA') is True
        assert store.has_pass('shared_hash', 'ClassB') is False
        assert store.latest_for_hash('shared_hash', 'ClassA').verdict == 'PASS'
        assert store.latest_for_hash('shared_hash', 'ClassB') is None


# ---------------------------------------------------------------------------
# S1 — import allowlist scan
# ---------------------------------------------------------------------------

class TestImportScan:
    def test_denied_imports_fail_with_line_numbers(self):
        result = _gauntlet_scan_imports(DENIED_IMPORTER)
        assert result['status'] == 'fail'
        denied = {d['module']: d['line'] for d in result['denied']}
        assert denied['socket'] == 2
        assert denied['ib_async'] == 3

    def test_trader_messaging_and_sdk_denied(self):
        src = (
            'from trader.messaging.clientserver import RPCClient\n'
            'import trader.sdk\n'
        )
        result = _gauntlet_scan_imports(src)
        assert result['status'] == 'fail'
        modules = [d['module'] for d in result['denied']]
        assert 'trader.messaging.clientserver' in modules
        assert 'trader.sdk' in modules

    def test_os_and_submodules_denied(self):
        result = _gauntlet_scan_imports('import os.path\nfrom os import environ\n')
        assert result['status'] == 'fail'
        assert {d['line'] for d in result['denied']} == {1, 2}

    def test_allowed_imports_pass_clean(self):
        src = (
            'import math\n'
            'import numpy as np\n'
            'import pandas as pd\n'
            'from trader.trading.strategy import Strategy, Signal\n'
            'from trader.objects import Action\n'
        )
        result = _gauntlet_scan_imports(src)
        assert result['status'] == 'pass'
        assert result['denied'] == []
        assert result['warnings'] == []

    def test_unknown_import_warns_but_passes(self):
        result = _gauntlet_scan_imports('import statistics\n')
        assert result['status'] == 'pass'
        assert [w['module'] for w in result['warnings']] == ['statistics']

    def test_syntax_error_fails(self):
        result = _gauntlet_scan_imports('def broken(:\n')
        assert result['status'] == 'fail'
        assert 'parse' in result['detail']

    # FIX 1: dynamic-import / interpreter-escape calls the ast.Import scan
    # misses. Each form must FAIL S1 with a line number.
    def test_dunder_import_call_fails(self):
        src = 'x = 1\n__import__("os").system("echo pwn")\n'
        result = _gauntlet_scan_imports(src)
        assert result['status'] == 'fail'
        lines = {d['line'] for d in result['denied']}
        assert 2 in lines
        assert any('__import__' in d['module'] for d in result['denied'])

    def test_importlib_import_module_call_fails(self):
        src = 'import importlib\n\nm = importlib.import_module("socket")\n'
        result = _gauntlet_scan_imports(src)
        assert result['status'] == 'fail'
        # both the `import importlib` node and the call are flagged
        assert any(d['module'] == 'import_module()' or
                   d['module'].endswith('import_module() dynamic import')
                   for d in result['denied'])
        assert any(d['line'] == 3 for d in result['denied'])

    def test_getattr_builtins_import_fails(self):
        src = "f = getattr(__builtins__, '__import__')\nf('os')\n"
        result = _gauntlet_scan_imports(src)
        assert result['status'] == 'fail'
        assert any('getattr' in d['module'] and d['line'] == 1
                   for d in result['denied'])

    def test_eval_exec_compile_fail(self):
        for line_src, needle in (
            ('eval("1+1")\n', 'eval'),
            ('exec("x=1")\n', 'exec'),
            ('compile("x", "<s>", "eval")\n', 'compile'),
        ):
            result = _gauntlet_scan_imports(line_src)
            assert result['status'] == 'fail', line_src
            assert any(needle in d['module'] and d['line'] == 1
                       for d in result['denied']), line_src

    def test_clean_strategy_no_false_positive_on_dynamic_scan(self):
        """A clean strategy that uses ordinary getattr on a non-builtin,
        non-dunder target still passes S1."""
        src = (
            'import pandas as pd\n'
            'from trader.trading.strategy import Strategy\n'
            'def f(obj):\n'
            '    return getattr(obj, "close", None)\n'
        )
        result = _gauntlet_scan_imports(src)
        assert result['status'] == 'pass'
        assert result['denied'] == []

    def test_advisory_note_present(self):
        result = _gauntlet_scan_imports('import math\n')
        assert 'advisory' in result
        assert 'not a security sandbox' in result['advisory']

    def test_dynamic_import_scans_pass_end_to_end_is_fail(self, tmp_path,
                                                          tmp_duckdb_path):
        """Regression for the actual bypass: a module whose only reach-out
        is `__import__(...)` must FAIL the gauntlet (previously scanned
        PASS, then S2/S3 executed it in-process)."""
        body = (
            'from trader.trading.strategy import Strategy\n'
            '__import__("os")\n'
            'class Sneaky(Strategy):\n'
            '    def on_prices(self, prices):\n'
            '        return None\n'
        )
        f = _write(tmp_path / 'strategies', 'sneaky.py', body)
        result = _gauntlet_run(f, 'Sneaky', tmp_duckdb_path)
        assert result['checks']['s1_imports']['status'] == 'fail'
        assert result['checks']['s2_lookahead']['status'] == 'not_evaluated'
        assert result['verdict'] == 'FAIL'


# ---------------------------------------------------------------------------
# End-to-end gauntlet runs
# ---------------------------------------------------------------------------

class TestGauntletRun:
    def test_clean_onprices_passes(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        result = _gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path)
        checks = result['checks']
        assert checks['s1_imports']['status'] == 'pass'
        # Vacuous lookahead check counts as pass-with-note, NOT fail.
        assert checks['s2_lookahead']['status'] == 'not_evaluable'
        assert 'on_prices-only' in checks['s2_lookahead']['note']
        assert checks['s3_battery']['status'] == 'pass'
        assert checks['s4_psr']['status'] == 'not_evaluated'
        assert result['verdict'] == 'PASS'
        assert result['code_hash'] == compute_strategy_hash(str(f))

    def test_clean_precompute_passes(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_pc.py', CLEAN_PRECOMPUTE)
        result = _gauntlet_run(f, 'CleanPrecompute', tmp_duckdb_path)
        assert result['checks']['s2_lookahead']['status'] == 'pass'
        assert result['checks']['s3_battery']['status'] == 'pass'
        assert result['verdict'] == 'PASS'

    def test_nan_crasher_fails_s3(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'crasher.py', NAN_CRASHER)
        result = _gauntlet_run(f, 'NanCrasher', tmp_duckdb_path)
        s3 = result['checks']['s3_battery']
        assert s3['status'] == 'fail'
        failed_frames = {n for n, r in s3['frames'].items() if r['status'] == 'fail'}
        assert 'nan_rows' in failed_frames
        assert any('ValueError' in r.get('detail', '')
                   for r in s3['frames'].values() if r['status'] == 'fail')
        assert result['verdict'] == 'FAIL'

    def test_lookahead_leaker_fails_s2(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'leaker.py', LOOKAHEAD_LEAKER)
        result = _gauntlet_run(f, 'Leaker', tmp_duckdb_path)
        assert result['checks']['s2_lookahead']['status'] == 'fail'
        assert 'Lookahead' in result['checks']['s2_lookahead']['detail']
        assert result['verdict'] == 'FAIL'

    def test_denied_import_skips_module_import(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'phones_home.py', DENIED_IMPORTER)
        result = _gauntlet_run(f, 'Phones_Home', tmp_duckdb_path)
        checks = result['checks']
        assert checks['s1_imports']['status'] == 'fail'
        # The module must not be imported when S1 fails.
        assert checks['s2_lookahead']['status'] == 'not_evaluated'
        assert checks['s3_battery']['status'] == 'not_evaluated'
        assert result['verdict'] == 'FAIL'

    def test_missing_class_fails(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        result = _gauntlet_run(f, 'NoSuchClass', tmp_duckdb_path)
        assert result['checks']['s3_battery']['status'] == 'fail'
        assert 'not found' in result['checks']['s3_battery']['detail']
        assert result['verdict'] == 'FAIL'

    def test_min_psr_absent_run_does_not_fail(self, tmp_path, tmp_duckdb_path):
        """--min-psr with no backtest for this hash: not-evaluated, never
        an auto-fail."""
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        result = _gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path, min_psr=0.9)
        assert result['checks']['s4_psr']['status'] == 'not_evaluated'
        assert result['verdict'] == 'PASS'

    def _seed_backtest(self, duckdb_path: str, code_hash: str, values) -> None:
        curve = [{'time': f'2024-01-{i % 28 + 1:02d}', 'value': v}
                 for i, v in enumerate(values)]
        BacktestStore(duckdb_path).add(BacktestRecord(
            strategy_path='strategies/clean_op.py', class_name='CleanOnPrices',
            conids=[1], universe='', start_date=dt.datetime(2024, 1, 1),
            end_date=dt.datetime(2024, 3, 1), bar_size='1 day',
            initial_capital=100000.0, fill_policy='next_open',
            slippage_bps=0.0, commission_per_share=0.0,
            code_hash=code_hash,
            equity_curve_json=json.dumps(curve),
        ))

    def test_min_psr_enforced_against_matching_hash(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        code_hash = compute_strategy_hash(str(f))
        # Strictly-losing equity curve → PSR near 0.
        self._seed_backtest(tmp_duckdb_path, code_hash,
                            [100.0 - 0.5 * i for i in range(60)])
        result = _gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path, min_psr=0.5)
        s4 = result['checks']['s4_psr']
        assert s4['status'] == 'fail'
        assert s4['psr'] < 0.5
        assert result['verdict'] == 'FAIL'

    def test_psr_record_only_without_min(self, tmp_path, tmp_duckdb_path):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        code_hash = compute_strategy_hash(str(f))
        self._seed_backtest(tmp_duckdb_path, code_hash,
                            [100.0 - 0.5 * i for i in range(60)])
        result = _gauntlet_run(f, 'CleanOnPrices', tmp_duckdb_path)
        s4 = result['checks']['s4_psr']
        assert s4['status'] == 'pass'
        assert 'psr' in s4
        assert result['verdict'] == 'PASS'


# ---------------------------------------------------------------------------
# CLI handler: records verdicts to the store
# ---------------------------------------------------------------------------

@pytest.fixture
def gauntlet_db(monkeypatch, tmp_duckdb_path):
    import trader.mmr_cli as cli
    monkeypatch.setattr(cli, '_gauntlet_db_path', lambda: tmp_duckdb_path)
    return tmp_duckdb_path


class TestGauntletHandler:
    def test_pass_recorded_and_unlocks_gate(self, tmp_path, gauntlet_db):
        f = _write(tmp_path / 'strategies', 'clean_op.py', CLEAN_ONPRICES)
        _handle_strategies_gauntlet(argparse.Namespace(
            module_path=str(f), class_name='CleanOnPrices', min_psr=None, name=None,
        ))
        code_hash = compute_strategy_hash(str(f))
        store = GauntletStore(gauntlet_db)
        assert store.has_pass(code_hash) is True
        assert _gauntlet_pass_refusal(f, 'CleanOnPrices', str(f)) is None

    def test_fail_recorded_does_not_unlock(self, tmp_path, gauntlet_db):
        f = _write(tmp_path / 'strategies', 'crasher.py', NAN_CRASHER)
        _handle_strategies_gauntlet(argparse.Namespace(
            module_path=str(f), class_name='NanCrasher', min_psr=None, name=None,
        ))
        code_hash = compute_strategy_hash(str(f))
        store = GauntletStore(gauntlet_db)
        assert store.has_pass(code_hash) is False
        assert store.latest_for_hash(code_hash).verdict == 'FAIL'
        refusal = _gauntlet_pass_refusal(f, 'NanCrasher', str(f))
        assert refusal is not None
        assert 'mmr strategies gauntlet' in refusal


# ---------------------------------------------------------------------------
# Deploy / enable enforcement
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_home(tmp_path, monkeypatch):
    """Redirect ``Path('~/...').expanduser()`` to tmp_path (same idiom as
    test_strategy_deploy) so nothing leaks into the real ~/.config/mmr."""
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    return tmp_path


def _deploy_ns(**kw) -> argparse.Namespace:
    defaults = dict(
        name='test_strat',
        conids=None,
        universe=None,
        bar_size='1 min',
        days=90,
        paper_only=False,
        auto_execute=False,
        module=None,
        class_name=None,
        params=None,
    )
    defaults.update(kw)
    return argparse.Namespace(**defaults)


def _read_deployed(home: Path) -> dict:
    p = home / '.config' / 'mmr' / 'strategy_runtime.yaml'
    if not p.exists():
        return {'strategies': []}
    return yaml.safe_load(p.read_text()) or {'strategies': []}


class TestDeployEnforcement:
    def test_deploy_refused_without_pass(self, tmp_home, gauntlet_db, tmp_path):
        # auto_execute strategies REQUIRE a PASS.
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _handle_strategy_deploy(_deploy_ns(
            name='clean_strat', module=str(f), class_name='CleanOnPrices',
            auto_execute=True))
        assert _read_deployed(tmp_home)['strategies'] == []

    def test_deploy_allowed_with_pass(self, tmp_home, gauntlet_db, tmp_path):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _record_pass(gauntlet_db, f, 'CleanOnPrices')
        _handle_strategy_deploy(_deploy_ns(
            name='clean_strat', module=str(f), class_name='CleanOnPrices',
            auto_execute=True))
        entries = [s for s in _read_deployed(tmp_home)['strategies']
                   if s['name'] == 'clean_strat']
        assert len(entries) == 1
        assert entries[0]['class_name'] == 'CleanOnPrices'
        assert entries[0]['auto_execute'] is True

    def test_deploy_refused_after_file_edit(self, tmp_home, gauntlet_db, tmp_path):
        """A PASS applies to the EXACT source hash — editing one byte
        invalidates it."""
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _record_pass(gauntlet_db, f, 'CleanOnPrices')
        f.write_text(CLEAN_ONPRICES + '\n# edited after gauntlet\n')
        _handle_strategy_deploy(_deploy_ns(
            name='edited_strat', module=str(f), class_name='CleanOnPrices',
            auto_execute=True))
        assert _read_deployed(tmp_home)['strategies'] == []

    def test_deploy_signal_only_advisory_without_pass(self, tmp_home,
                                                      gauntlet_db, tmp_path):
        """FIX 3: a signal-only (auto_execute:false) strategy cannot place
        orders, so a missing PASS is advisory — deploy still succeeds."""
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _handle_strategy_deploy(_deploy_ns(
            name='signal_strat', module=str(f), class_name='CleanOnPrices',
            auto_execute=False))
        entries = [s for s in _read_deployed(tmp_home)['strategies']
                   if s['name'] == 'signal_strat']
        assert len(entries) == 1
        assert 'auto_execute' not in entries[0]

    def test_deploy_sibling_class_not_authorized_by_pass(self, tmp_home,
                                                         gauntlet_db, tmp_path):
        """FIX 2: a PASS for ClassA does NOT let ClassB (same file) deploy
        as an auto_execute strategy."""
        f = _write(tmp_path / 'mods', 'two.py', TWO_CLASSES)
        _record_pass(gauntlet_db, f, 'ClassA')
        # ClassA deploys (authorized)
        _handle_strategy_deploy(_deploy_ns(
            name='a_strat', module=str(f), class_name='ClassA',
            auto_execute=True))
        # ClassB refused despite the shared hash
        _handle_strategy_deploy(_deploy_ns(
            name='b_strat', module=str(f), class_name='ClassB',
            auto_execute=True))
        names = {s['name'] for s in _read_deployed(tmp_home)['strategies']}
        assert 'a_strat' in names
        assert 'b_strat' not in names

    def test_deploy_refuses_nonexistent_module(self, tmp_home, gauntlet_db):
        _handle_strategy_deploy(_deploy_ns(
            name='ghost',
            module='strategies/definitely_not_a_real_module_xyz.py',
            class_name='Ghost'))
        assert _read_deployed(tmp_home)['strategies'] == []

    def test_deploy_refuses_missing_class(self, tmp_home, gauntlet_db, tmp_path):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        _record_pass(gauntlet_db, f, 'CleanOnPrices')
        _handle_strategy_deploy(_deploy_ns(
            name='wrong_class', module=str(f), class_name='NoSuchClass'))
        assert _read_deployed(tmp_home)['strategies'] == []

    def test_deploy_yaml_safe_load(self, tmp_home, gauntlet_db, tmp_path):
        """Python-object tags in the config must be refused, not executed."""
        cfg_dir = tmp_home / '.config' / 'mmr'
        cfg_dir.mkdir(parents=True)
        (cfg_dir / 'strategy_runtime.yaml').write_text(
            'strategies:\n'
            '  - name: evil\n'
            '    module: !!python/object/apply:os.system ["echo pwn"]\n'
        )
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        with pytest.raises(yaml.constructor.ConstructorError):
            _handle_strategy_deploy(_deploy_ns(
                name='clean_strat', module=str(f), class_name='CleanOnPrices'))


class TestEnableEnforcement:
    def _write_config(self, tmp_home: Path, name: str, module: str,
                      class_name: str, auto_execute: bool = True) -> None:
        cfg_dir = tmp_home / '.config' / 'mmr'
        cfg_dir.mkdir(parents=True, exist_ok=True)
        (cfg_dir / 'strategy_runtime.yaml').write_text(yaml.safe_dump({
            'strategies': [{
                'name': name, 'module': module, 'class_name': class_name,
                'bar_size': '1 min', 'conids': [1],
                'auto_execute': auto_execute,
            }],
        }))

    def test_enable_refused_without_pass(self, tmp_home, gauntlet_db, tmp_path):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        self._write_config(tmp_home, 'clean_strat', str(f), 'CleanOnPrices',
                           auto_execute=True)
        mmr = MagicMock()
        _handle_strategies(mmr, argparse.Namespace(
            strat_action='enable', name='clean_strat'))
        mmr.enable_strategy.assert_not_called()

    def test_enable_allowed_with_pass(self, tmp_home, gauntlet_db, tmp_path):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        self._write_config(tmp_home, 'clean_strat', str(f), 'CleanOnPrices',
                           auto_execute=True)
        _record_pass(gauntlet_db, f, 'CleanOnPrices')
        mmr = MagicMock()
        _handle_strategies(mmr, argparse.Namespace(
            strat_action='enable', name='clean_strat'))
        mmr.enable_strategy.assert_called_once_with('clean_strat')

    def test_enable_refused_after_file_edit(self, tmp_home, gauntlet_db, tmp_path):
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        self._write_config(tmp_home, 'clean_strat', str(f), 'CleanOnPrices',
                           auto_execute=True)
        _record_pass(gauntlet_db, f, 'CleanOnPrices')
        f.write_text(CLEAN_ONPRICES + '\n# edited\n')
        mmr = MagicMock()
        _handle_strategies(mmr, argparse.Namespace(
            strat_action='enable', name='clean_strat'))
        mmr.enable_strategy.assert_not_called()

    def test_enable_signal_only_allowed_without_pass(self, tmp_home,
                                                     gauntlet_db, tmp_path):
        """FIX 3: re-enabling a signal-only (auto_execute:false) strategy
        without a PASS succeeds — it cannot place any order, so the gate is
        advisory, not blocking."""
        f = _write(tmp_path / 'mods', 'clean_op.py', CLEAN_ONPRICES)
        self._write_config(tmp_home, 'signal_strat', str(f), 'CleanOnPrices',
                           auto_execute=False)
        assert _gauntlet_enable_refusal('signal_strat') is None
        mmr = MagicMock()
        _handle_strategies(mmr, argparse.Namespace(
            strat_action='enable', name='signal_strat'))
        mmr.enable_strategy.assert_called_once_with('signal_strat')

    def test_enable_refused_when_not_in_yaml(self, tmp_home, gauntlet_db):
        refusal = _gauntlet_enable_refusal('unknown_strat')
        assert refusal is not None
        assert 'refused' in refusal


# ---------------------------------------------------------------------------
# StrategyRuntime arm gate: warn-only default vs MMR_GAUNTLET_ENFORCE=1
# ---------------------------------------------------------------------------

def _make_runtime(tmp_path, strategies_dir, duckdb_path,
                  paper_trading=True) -> StrategyRuntime:
    """Minimally-wired StrategyRuntime, same idiom as
    test_strategy_runtime_reconcile."""
    rt = StrategyRuntime.__new__(StrategyRuntime)  # skip __init__
    rt.strategies_directory = str(strategies_dir)
    rt.strategy_config_file = str(tmp_path / 'strategy_runtime.yaml')
    rt.strategy_implementations = []
    rt.strategies = {}
    rt.streams = {}
    rt.storage = None  # type: ignore
    rt.universe_accessor = None  # type: ignore
    rt._config_mtime = 0.0
    rt.trader_client = None  # type: ignore
    rt.paper_trading = paper_trading
    rt.duckdb_path = duckdb_path
    return rt


_ARMED_STRATEGY_BODY = """
from trader.trading.strategy import Strategy, Signal
from trader.objects import Action

class V1(Strategy):
    def on_prices(self, prices):
        return None
"""


class TestLoadStrategyArmGate:
    def _load(self, rt, strategies_dir: Path, auto_execute=True):
        rt.load_strategy(
            name='armed', bar_size_str='1 min', conids=[1],
            universe=None, historical_days_prior=0,
            module=str(strategies_dir / 'armed.py'), class_name='V1',
            description='', auto_execute=auto_execute,
        )

    def test_warn_mode_arms_with_warning(self, tmp_path, tmp_duckdb_path,
                                         caplog, monkeypatch):
        monkeypatch.delenv('MMR_GAUNTLET_ENFORCE', raising=False)
        strategies = tmp_path / 'strategies'
        module_file = _write(strategies, 'armed.py', _ARMED_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, tmp_duckdb_path)

        with caplog.at_level(stdlib_logging.WARNING):
            self._load(rt, strategies)
        assert len(rt.strategy_implementations) == 1
        assert rt.strategy_implementations[0].ctx.auto_execute is True
        code_hash = compute_strategy_hash(str(module_file))
        assert 'gauntlet' in caplog.text
        assert f'current={code_hash}' in caplog.text

    def test_enforce_mode_loads_disarmed(self, tmp_path, tmp_duckdb_path,
                                         caplog, monkeypatch):
        monkeypatch.setenv('MMR_GAUNTLET_ENFORCE', '1')
        strategies = tmp_path / 'strategies'
        _write(strategies, 'armed.py', _ARMED_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, tmp_duckdb_path)

        with caplog.at_level(stdlib_logging.ERROR):
            self._load(rt, strategies)
        # Strategy still loads (so the executor can close attributed
        # positions) but auto_execute is stripped — it cannot open.
        assert len(rt.strategy_implementations) == 1
        assert rt.strategy_implementations[0].ctx.auto_execute is False
        assert 'refusing to arm' in caplog.text

    def test_enforce_mode_arms_with_pass(self, tmp_path, tmp_duckdb_path,
                                         monkeypatch):
        monkeypatch.setenv('MMR_GAUNTLET_ENFORCE', '1')
        strategies = tmp_path / 'strategies'
        module_file = _write(strategies, 'armed.py', _ARMED_STRATEGY_BODY)
        _record_pass(tmp_duckdb_path, module_file, 'V1')
        rt = _make_runtime(tmp_path, strategies, tmp_duckdb_path)

        self._load(rt, strategies)
        assert len(rt.strategy_implementations) == 1
        assert rt.strategy_implementations[0].ctx.auto_execute is True

    def test_enforce_stale_hash_disarms_with_hash_pair(self, tmp_path,
                                                       tmp_duckdb_path,
                                                       caplog, monkeypatch):
        """PASS exists for an OLD hash of the same class — the log names
        both hashes so the operator sees exactly what drifted."""
        monkeypatch.setenv('MMR_GAUNTLET_ENFORCE', '1')
        strategies = tmp_path / 'strategies'
        module_file = _write(strategies, 'armed.py', _ARMED_STRATEGY_BODY)
        old_hash = _record_pass(tmp_duckdb_path, module_file, 'V1')
        module_file.write_text(_ARMED_STRATEGY_BODY + '\n# drift\n')
        rt = _make_runtime(tmp_path, strategies, tmp_duckdb_path)

        with caplog.at_level(stdlib_logging.ERROR):
            self._load(rt, strategies)
        assert rt.strategy_implementations[0].ctx.auto_execute is False
        new_hash = compute_strategy_hash(str(module_file))
        assert f'current={new_hash}' in caplog.text
        assert f'last_pass={old_hash}' in caplog.text

    def test_gate_not_applied_without_auto_execute(self, tmp_path,
                                                   tmp_duckdb_path,
                                                   monkeypatch):
        monkeypatch.setenv('MMR_GAUNTLET_ENFORCE', '1')
        strategies = tmp_path / 'strategies'
        _write(strategies, 'armed.py', _ARMED_STRATEGY_BODY)
        rt = _make_runtime(tmp_path, strategies, tmp_duckdb_path)

        self._load(rt, strategies, auto_execute=False)
        assert len(rt.strategy_implementations) == 1
        assert rt.strategy_implementations[0].ctx.auto_execute is False


# ---------------------------------------------------------------------------
# Synthetic markets: conftest fixtures delegate 1:1
# ---------------------------------------------------------------------------

class TestSyntheticMarketsDelegation:
    def test_fixture_frames_identical(self, ohlcv_with_gaps, ohlcv_high_volatility,
                                      ohlcv_zero_volume, ohlcv_halted):
        import pandas as pd

        from trader.simulation import synthetic_markets
        pd.testing.assert_frame_equal(ohlcv_with_gaps, synthetic_markets.ohlcv_with_gaps())
        pd.testing.assert_frame_equal(
            ohlcv_high_volatility, synthetic_markets.ohlcv_high_volatility())
        pd.testing.assert_frame_equal(
            ohlcv_zero_volume, synthetic_markets.ohlcv_zero_volume())
        pd.testing.assert_frame_equal(ohlcv_halted, synthetic_markets.ohlcv_halted())

    def test_battery_shapes(self):
        from trader.simulation import synthetic_markets
        frames = synthetic_markets.battery()
        assert set(frames) == {
            'with_gaps', 'high_volatility', 'zero_volume', 'halted',
            'nan_rows', 'trending', 'choppy',
        }
        for name, df in frames.items():
            assert list(df.columns) == ['open', 'high', 'low', 'close', 'volume'], name
            assert df.index.name == 'date', name
            assert len(df) >= 30, name
        # nan_rows: the *trailing* bars are broken — the dangerous direction.
        assert frames['nan_rows']['close'].iloc[-5:].isna().all()
        assert frames['nan_rows']['close'].iloc[:-5].notna().all()

    def test_generators_deterministic(self):
        from trader.simulation import synthetic_markets
        import pandas as pd
        pd.testing.assert_frame_equal(
            synthetic_markets.ohlcv_trending(), synthetic_markets.ohlcv_trending())
        pd.testing.assert_frame_equal(
            synthetic_markets.ohlcv_choppy(), synthetic_markets.ohlcv_choppy())
