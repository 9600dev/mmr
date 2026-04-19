"""Tests for the nightly-sweep pipeline: schema round-trip, manifest
validation, freshness guard, and digest writing.

End-to-end sweep execution is exercised via a live CLI test elsewhere;
this file locks the pure-function pieces so regressions surface fast.
"""

import datetime as dt
import json
from pathlib import Path

import pytest

from trader.data.backtest_store import (
    BacktestRecord, BacktestStore, SweepRecord,
)
from trader.mmr_cli import (
    _sweep_manifest_validate, _write_sweep_digest,
)


# ---------------------------------------------------------------------------
# Storage — sweeps table round-trip
# ---------------------------------------------------------------------------

def _make_record(**kw) -> BacktestRecord:
    defaults = dict(
        strategy_path='strategies/rsi.py', class_name='RSIStrategy',
        conids=[265598], universe='',
        start_date=dt.datetime(2024, 1, 1), end_date=dt.datetime(2024, 12, 31),
        bar_size='1 day', initial_capital=100_000.0, fill_policy='next_open',
        slippage_bps=1.0, commission_per_share=0.005,
        params={}, code_hash='abc',
        total_trades=42, total_return=0.05, sharpe_ratio=1.2,
        max_drawdown=-0.08, win_rate=0.55, final_equity=105_000.0,
    )
    defaults.update(kw)
    return BacktestRecord(**defaults)


class TestSweepStorage:

    def test_create_sweep_assigns_id(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        sid = store.create_sweep(SweepRecord(
            name='smoke', manifest_yaml='sweeps: [{name: smoke}]',
            config_hash='abc', n_runs_planned=4, concurrency=2, note='test',
        ))
        assert sid == 1

    def test_get_sweep_round_trips_fields(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        sid = store.create_sweep(SweepRecord(
            name='my-sweep',
            manifest_yaml='sweeps:\n  - name: my-sweep\n',
            config_hash='deadbeef',
            n_runs_planned=10, concurrency=4, note='the note',
        ))
        got = store.get_sweep(sid)
        assert got.name == 'my-sweep'
        assert got.manifest_yaml.startswith('sweeps:')
        assert got.config_hash == 'deadbeef'
        assert got.n_runs_planned == 10
        assert got.concurrency == 4
        assert got.note == 'the note'
        assert got.status == 'running'  # default on create
        assert got.started_at is not None
        assert got.finished_at is None

    def test_finalize_sweep_transitions_status(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        sid = store.create_sweep(SweepRecord(
            name='x', manifest_yaml='', config_hash='',
            n_runs_planned=5, concurrency=1,
        ))
        store.finalize_sweep(
            sid, status='completed', n_runs_successful=4,
            n_runs_failed=1, digest_path='/tmp/digest.md',
        )
        got = store.get_sweep(sid)
        assert got.status == 'completed'
        assert got.n_runs_successful == 4
        assert got.n_runs_failed == 1
        assert got.digest_path == '/tmp/digest.md'
        assert got.finished_at is not None

    def test_backtest_row_carries_sweep_id(self, tmp_duckdb_path):
        """Runs stamped with sweep_id on insert survive the round-trip
        and can be filtered via ``list(sweep_id=...)``."""
        store = BacktestStore(tmp_duckdb_path)
        sid = store.create_sweep(SweepRecord(
            name='parent', manifest_yaml='', config_hash='',
            n_runs_planned=2, concurrency=1,
        ))
        rid_in_sweep = store.add(_make_record(sweep_id=sid))
        rid_other = store.add(_make_record())  # no sweep_id

        got = store.get(rid_in_sweep)
        assert got.sweep_id == sid
        got2 = store.get(rid_other)
        assert got2.sweep_id is None

        # Filter returns only the sweep's child.
        scoped = store.list(sweep_id=sid, limit=10)
        assert [r.id for r in scoped] == [rid_in_sweep]

    def test_list_sweeps_newest_first(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        import time
        for i in range(3):
            store.create_sweep(SweepRecord(
                name=f'sweep-{i}', manifest_yaml='', config_hash='',
                n_runs_planned=1, concurrency=1,
            ))
            time.sleep(0.005)
        sweeps = store.list_sweeps(limit=10)
        assert [s.name for s in sweeps] == ['sweep-2', 'sweep-1', 'sweep-0']

    def test_existing_db_migration_adds_sweep_id_column(self, tmp_duckdb_path):
        """Regression: a DB created under the pre-sweep schema (no
        sweep_id column) must still work after upgrade. Simulate by
        dropping the column, then reopening — the migration path in
        ``_ensure_table`` should re-add it."""
        store = BacktestStore(tmp_duckdb_path)
        # Add a row, then drop the column to simulate an old-schema DB.
        store.add(_make_record())

        def _drop(conn):
            conn.execute('ALTER TABLE backtest_runs DROP COLUMN sweep_id')
        store.db.execute_atomic(_drop)

        # Reopen — _ensure_table should ALTER the column back in.
        store2 = BacktestStore(tmp_duckdb_path)
        rec = store2.list(limit=1)[0]
        assert rec.sweep_id is None  # default from the migrated column


# ---------------------------------------------------------------------------
# Manifest validation
# ---------------------------------------------------------------------------

class TestManifestValidate:

    def test_accepts_minimal_valid_manifest(self):
        manifest = {
            'sweeps': [
                {
                    'name': 'smoke', 'strategy': '/abs/x.py', 'class': 'X',
                    'symbols': ['SPY'],
                }
            ]
        }
        cleaned = _sweep_manifest_validate(manifest)
        assert len(cleaned) == 1
        assert cleaned[0]['name'] == 'smoke'
        assert cleaned[0]['days'] == 365  # default
        assert cleaned[0]['bar_size'] == '1 min'  # default
        assert cleaned[0]['param_grid'] == {}  # default — means run-defaults

    def test_rejects_missing_sweeps_key(self):
        with pytest.raises(ValueError, match="top-level 'sweeps'"):
            _sweep_manifest_validate({'other': []})

    def test_rejects_empty_sweeps_list(self):
        with pytest.raises(ValueError, match='non-empty list'):
            _sweep_manifest_validate({'sweeps': []})

    def test_rejects_missing_required_fields(self):
        with pytest.raises(ValueError, match="missing required field 'name'"):
            _sweep_manifest_validate({
                'sweeps': [{'strategy': '/x.py', 'class': 'X', 'symbols': ['SPY']}]
            })
        with pytest.raises(ValueError, match="missing required field 'strategy'"):
            _sweep_manifest_validate({
                'sweeps': [{'name': 's', 'class': 'X', 'symbols': ['SPY']}]
            })
        with pytest.raises(ValueError, match="missing required field 'class'"):
            _sweep_manifest_validate({
                'sweeps': [{'name': 's', 'strategy': '/x.py', 'symbols': ['SPY']}]
            })

    def test_requires_exactly_one_symbol_source(self):
        """symbols / conids / universe are mutually exclusive — the
        validator should reject 0 or 2+."""
        with pytest.raises(ValueError, match='exactly one of'):
            _sweep_manifest_validate({
                'sweeps': [{'name': 's', 'strategy': '/x.py', 'class': 'X'}]
            })
        with pytest.raises(ValueError, match='exactly one of'):
            _sweep_manifest_validate({
                'sweeps': [{
                    'name': 's', 'strategy': '/x.py', 'class': 'X',
                    'symbols': ['SPY'], 'conids': [1, 2],
                }]
            })

    def test_rejects_non_list_param_grid_value(self):
        """Typo like ``EMA_PERIOD: 20`` (scalar) instead of ``[20]`` must
        fail loudly — a scalar would be silently ignored by the expander."""
        with pytest.raises(ValueError, match='non-empty list'):
            _sweep_manifest_validate({
                'sweeps': [{
                    'name': 's', 'strategy': '/x.py', 'class': 'X',
                    'symbols': ['SPY'],
                    'param_grid': {'EMA_PERIOD': 20},
                }]
            })

    def test_rejects_empty_param_grid_list(self):
        with pytest.raises(ValueError, match='non-empty list'):
            _sweep_manifest_validate({
                'sweeps': [{
                    'name': 's', 'strategy': '/x.py', 'class': 'X',
                    'symbols': ['SPY'],
                    'param_grid': {'EMA_PERIOD': []},
                }]
            })

    def test_accepts_conids_alternative(self):
        cleaned = _sweep_manifest_validate({
            'sweeps': [{
                'name': 's', 'strategy': '/x.py', 'class': 'X',
                'conids': [756733, 265598],
            }]
        })
        assert cleaned[0]['conids'] == [756733, 265598]
        assert cleaned[0]['symbols'] is None
        assert cleaned[0]['universe'] is None


# ---------------------------------------------------------------------------
# Digest writing
# ---------------------------------------------------------------------------

class TestDigestWriting:

    def test_writes_markdown_with_expected_sections(self, tmp_path, monkeypatch):
        """Digest markdown should have the header, strong-candidates
        table (if any qualify), all-runs table, and a reference to
        `backtests list --sweep`. Redirect to tmp_path."""
        monkeypatch.setenv('HOME', str(tmp_path))
        monkeypatch.setattr(Path, 'home', lambda: tmp_path)

        spec = {
            'name': 'unit_test_sweep', 'class': 'X', 'strategy': '/x.py',
            'bar_size': '1 min', 'days': 30,
            'param_grid': {'EMA_PERIOD': [10, 20]},
        }
        results = [
            {
                'status': 'ok',
                'job': {'symbol': 'SPY', 'params': {'EMA_PERIOD': 10}},
                'summary': {
                    'run_id': 100, 'total_return': 0.05,
                    'sharpe_ratio': 3.0, 'sortino_ratio': 3.5,
                    'profit_factor': 2.5, 'total_trades': 100,
                    'max_drawdown': -0.02,
                },
            },
            {
                'status': 'ok',
                'job': {'symbol': 'QQQ', 'params': {'EMA_PERIOD': 20}},
                'summary': {
                    'run_id': 101, 'total_return': 0.02,
                    'sharpe_ratio': 1.0, 'sortino_ratio': 1.2,
                    'profit_factor': 1.3, 'total_trades': 50,
                    'max_drawdown': -0.05,
                },
            },
        ]
        path_str = _write_sweep_digest(
            sweep_id=42, spec=spec, results=results,
            elapsed_s=180.0, status='completed',
        )
        assert path_str, 'digest path should be non-empty on success'
        text = Path(path_str).read_text()
        assert '# Sweep #42: unit_test_sweep' in text
        assert 'Status**: completed' in text
        assert '2/2 ok' in text
        assert 'backtests list --sweep 42' in text
        # Strong candidates table should include the SPY run (sharpe 3, pf 2.5).
        assert 'SPY' in text
        assert '"EMA_PERIOD"=10' in text  # json.dumps with separators=(',', '=')

    def test_handles_all_failures_gracefully(self, tmp_path, monkeypatch):
        """If no runs succeed, digest should still render with a
        Failures section and no leaderboard tables."""
        monkeypatch.setattr(Path, 'home', lambda: tmp_path)

        spec = {
            'name': 'all_failed', 'class': 'X', 'strategy': '/x.py',
            'bar_size': '1 min', 'days': 30, 'param_grid': {},
        }
        results = [
            {'status': 'error', 'job': {'symbol': 'SPY', 'params': {}},
             'error': 'boom'},
            {'status': 'timeout', 'job': {'symbol': 'QQQ', 'params': {}}},
        ]
        path_str = _write_sweep_digest(
            sweep_id=99, spec=spec, results=results,
            elapsed_s=1.0, status='failed',
        )
        assert path_str
        text = Path(path_str).read_text()
        assert 'Failures (2)' in text
        assert 'boom' in text
        assert 'timeout' in text

    def test_digest_write_failure_returns_empty_string(self, monkeypatch, tmp_path):
        """A write error (e.g. read-only dir) should not blow up a
        sweep — ``_write_sweep_digest`` swallows exceptions and returns
        an empty path so the caller can record 'no digest'."""
        # Point HOME at a path that can't be created.
        class _Exploding:
            def __truediv__(self, other):
                raise OSError('filesystem blew up')
            def mkdir(self, *a, **kw):
                raise OSError('nope')

        monkeypatch.setattr(Path, 'home', lambda: _Exploding())
        out = _write_sweep_digest(
            sweep_id=1, spec={'name': 'x', 'class': 'X', 'strategy': '/x',
                               'bar_size': '1 min', 'days': 30, 'param_grid': {}},
            results=[], elapsed_s=1.0, status='completed',
        )
        assert out == ''
