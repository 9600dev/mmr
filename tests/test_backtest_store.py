"""Tests for trader.data.backtest_store.BacktestStore."""

import datetime as dt
import pytest

from trader.data.backtest_store import BacktestRecord, BacktestStore, compute_strategy_hash


def _make_record(class_name='RSIStrategy', total_return=0.05, sharpe=1.2, note='') -> BacktestRecord:
    return BacktestRecord(
        strategy_path='strategies/rsi.py',
        class_name=class_name,
        conids=[265598],
        universe='',
        start_date=dt.datetime(2024, 1, 1),
        end_date=dt.datetime(2024, 12, 31),
        bar_size='1 day',
        initial_capital=100_000.0,
        fill_policy='next_open',
        slippage_bps=1.0,
        commission_per_share=0.005,
        params={'period': 14, 'oversold': 30},
        code_hash='abc123',
        total_trades=42,
        total_return=total_return,
        sharpe_ratio=sharpe,
        max_drawdown=-0.08,
        win_rate=0.55,
        final_equity=100_000 * (1 + total_return),
        note=note,
    )


class TestBacktestStore:
    def test_add_returns_id(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        rid = store.add(_make_record())
        assert rid == 1

    def test_add_auto_increments(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        r1 = store.add(_make_record())
        r2 = store.add(_make_record(class_name='Momentum'))
        r3 = store.add(_make_record(class_name='Ensemble'))
        assert [r1, r2, r3] == [1, 2, 3]

    def test_get_round_trip(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        rec = _make_record(total_return=0.1234, note='regression test')
        rid = store.add(rec)

        got = store.get(rid)
        assert got is not None
        assert got.id == rid
        assert got.class_name == 'RSIStrategy'
        assert got.conids == [265598]
        assert got.params == {'period': 14, 'oversold': 30}
        assert got.total_return == pytest.approx(0.1234)
        assert got.note == 'regression test'
        assert got.created_at is not None

    def test_get_nonexistent(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        assert store.get(999) is None

    def test_list_most_recent_first(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        import time
        store.add(_make_record(note='first'))
        time.sleep(0.005)
        store.add(_make_record(note='second'))

        records = store.list()
        assert len(records) == 2
        assert records[0].note == 'second'
        assert records[1].note == 'first'

    def test_list_filter_by_strategy_class(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        store.add(_make_record(class_name='RSIStrategy'))
        store.add(_make_record(class_name='Momentum'))
        store.add(_make_record(class_name='RSIStrategy'))

        rsi_runs = store.list(strategy_class='RSIStrategy')
        assert len(rsi_runs) == 2
        assert all(r.class_name == 'RSIStrategy' for r in rsi_runs)

        mom_runs = store.list(strategy_class='Momentum')
        assert len(mom_runs) == 1

    def test_list_sort_by_sharpe_descending(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        store.add(_make_record(class_name='A', sharpe=1.0))
        store.add(_make_record(class_name='B', sharpe=3.5))
        store.add(_make_record(class_name='C', sharpe=2.0))

        by_sharpe = store.list(sort_by='sharpe_ratio', descending=True)
        assert [r.class_name for r in by_sharpe] == ['B', 'C', 'A']

    def test_list_sort_ascending(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        store.add(_make_record(class_name='A', sharpe=1.0))
        store.add(_make_record(class_name='B', sharpe=3.5))
        store.add(_make_record(class_name='C', sharpe=2.0))

        worst_first = store.list(sort_by='sharpe_ratio', descending=False)
        assert [r.class_name for r in worst_first] == ['A', 'C', 'B']

    def test_list_sort_by_unknown_column_rejected(self, tmp_duckdb_path):
        """Whitelist prevents SQL injection via --sort-by."""
        store = BacktestStore(tmp_duckdb_path)
        store.add(_make_record())
        with pytest.raises(ValueError, match='cannot sort by'):
            store.list(sort_by='id; DROP TABLE backtest_runs')

    def test_list_limit(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        for i in range(5):
            store.add(_make_record(note=f'run-{i}'))
        assert len(store.list(limit=3)) == 3

    def test_delete(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        rid = store.add(_make_record())
        assert store.get(rid) is not None
        assert store.delete(rid) is True
        assert store.get(rid) is None

    def test_params_json_complex(self, tmp_duckdb_path):
        """Nested params survive round-trip."""
        store = BacktestStore(tmp_duckdb_path)
        rec = _make_record()
        rec.params = {
            'windows': [10, 20, 50],
            'thresholds': {'upper': 70.0, 'lower': 30.0},
            'use_volume': True,
        }
        rid = store.add(rec)
        got = store.get(rid)
        assert got.params == rec.params

    def test_trades_and_equity_curve_persisted_when_provided(self, tmp_duckdb_path):
        """Opt-in trade/equity detail survives storage."""
        store = BacktestStore(tmp_duckdb_path)
        rec = _make_record()
        rec.trades_json = '[{"ts": "2024-01-01", "action": "BUY"}]'
        rec.equity_curve_json = '[{"ts": "2024-01-01", "value": 100000.0}]'
        rid = store.add(rec)
        got = store.get(rid)
        assert '"BUY"' in got.trades_json
        assert '100000' in got.equity_curve_json

    def test_numpy_typed_trade_fields_serialize(self, tmp_duckdb_path):
        """Regression: ``--save-trades`` used to fail with "Object of type
        int64 is not JSON serializable" because trades carry numpy scalars
        from pandas groupby. The CLI now coerces to native Python types;
        this test verifies a round-trip with numpy inputs produces valid
        JSON that the store accepts without losing the values."""
        import json
        import numpy as np

        # Simulate what the CLI builds for --save-trades. If the coercion
        # (int()/float()) is missing, json.dumps will raise TypeError here.
        fake_trades = [
            {'ts': '2024-01-01', 'conid': np.int64(13824), 'price': np.float64(100.5)},
        ]
        coerced = [{
            'ts': t['ts'],
            'conid': int(t['conid']),
            'price': float(t['price']),
        } for t in fake_trades]
        trades_json = json.dumps(coerced)

        store = BacktestStore(tmp_duckdb_path)
        rec = _make_record()
        rec.trades_json = trades_json
        rid = store.add(rec)

        got = store.get(rid)
        parsed = json.loads(got.trades_json)
        assert parsed[0]['conid'] == 13824
        assert parsed[0]['price'] == 100.5
        assert isinstance(parsed[0]['conid'], int)  # native Python int, not np.int64


class TestSchemaMigration:
    """Regression: ``ALTER TABLE ADD COLUMN`` appends at the END of the
    stored table, while our CREATE_TABLE declares new metrics in the
    MIDDLE. If ``_rows_to_records`` reads positionally from ``SELECT *``,
    a migrated (old) database returns columns in a different order than
    a fresh database, and the reader misinterprets the row — live bug
    hit was ``TypeError: '>' not supported between datetime.datetime and
    float`` when ``profit_factor`` landed where ``created_at`` was.
    """

    def test_migrated_old_schema_reads_correctly(self, tmp_duckdb_path):
        """Simulate a database created BEFORE the extended-metrics release:
        fresh table with only the original columns, one existing row, then
        apply the migration via BacktestStore(...) and confirm that
        ``get()`` returns sensible values rather than the column-skew bug."""
        import duckdb as _duck
        conn = _duck.connect(tmp_duckdb_path)
        conn.execute("""
            CREATE TABLE backtest_runs (
                id INTEGER PRIMARY KEY,
                strategy_path VARCHAR NOT NULL,
                class_name VARCHAR NOT NULL,
                conids VARCHAR NOT NULL,
                universe VARCHAR DEFAULT '',
                start_date TIMESTAMP NOT NULL,
                end_date TIMESTAMP NOT NULL,
                bar_size VARCHAR NOT NULL,
                initial_capital DOUBLE NOT NULL,
                fill_policy VARCHAR DEFAULT 'next_open',
                slippage_bps DOUBLE DEFAULT 0.0,
                commission_per_share DOUBLE DEFAULT 0.0,
                params VARCHAR DEFAULT '{}',
                code_hash VARCHAR DEFAULT '',
                total_trades INTEGER DEFAULT 0,
                total_return DOUBLE DEFAULT 0.0,
                sharpe_ratio DOUBLE DEFAULT 0.0,
                max_drawdown DOUBLE DEFAULT 0.0,
                win_rate DOUBLE DEFAULT 0.0,
                final_equity DOUBLE DEFAULT 0.0,
                trades_json VARCHAR DEFAULT '',
                equity_curve_json VARCHAR DEFAULT '',
                created_at TIMESTAMP NOT NULL,
                note VARCHAR DEFAULT ''
            )
        """)
        # Seed the sequence past our hand-inserted id so follow-up INSERTs
        # (via store.add) don't collide on the primary key.
        conn.execute("CREATE SEQUENCE backtest_runs_id_seq START 100")
        conn.execute("""
            INSERT INTO backtest_runs VALUES (
                1, 'strategies/x.py', 'OldSchemaStrategy', '[4391]', '',
                TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-14 00:00:00',
                '1 min', 100000.0, 'next_open', 1.0, 0.005, '{}', 'abc',
                42, 0.03, 1.2, -0.08, 0.55, 103000.0,
                '', '', TIMESTAMP '2024-01-14 12:00:00', 'pre-migration'
            )
        """)
        conn.close()

        # Instantiate the store — this triggers the idempotent ALTER TABLE
        # migration that appends the 5 new metric columns at the END.
        store = BacktestStore(tmp_duckdb_path)

        # Reading the pre-existing row must still return the RIGHT values
        # in the RIGHT fields despite the stored column order now differing
        # from _CREATE_TABLE's declared order.
        got = store.get(1)
        assert got is not None
        assert got.class_name == 'OldSchemaStrategy'
        assert got.total_trades == 42
        assert got.total_return == pytest.approx(0.03)
        assert got.sharpe_ratio == pytest.approx(1.2)
        assert got.final_equity == pytest.approx(103000.0)
        # New columns exist with default 0.0 for the pre-migration row.
        assert got.sortino_ratio == 0.0
        assert got.calmar_ratio == 0.0
        assert got.profit_factor == 0.0
        # trades_json should still be the empty string the pre-migration
        # row had — NOT a timestamp, which is what used to happen.
        assert got.trades_json == ''
        assert got.note == 'pre-migration'

        # New writes into the migrated table should also round-trip cleanly.
        new_rid = store.add(_make_record(class_name='PostMigration'))
        fresh = store.get(new_rid)
        assert fresh.class_name == 'PostMigration'
        assert fresh.sortino_ratio == 0.0  # default; _make_record doesn't set it


class TestComputeStrategyHash:
    def test_returns_sha256_for_existing_file(self, tmp_path):
        p = tmp_path / 'strat.py'
        p.write_bytes(b'class Foo: pass\n')
        h = compute_strategy_hash(str(p))
        assert len(h) == 64  # sha256 hex digest length
        # Deterministic for the same content
        assert h == compute_strategy_hash(str(p))

    def test_different_content_different_hash(self, tmp_path):
        a = tmp_path / 'a.py'
        b = tmp_path / 'b.py'
        a.write_bytes(b'class A: pass\n')
        b.write_bytes(b'class B: pass\n')
        assert compute_strategy_hash(str(a)) != compute_strategy_hash(str(b))

    def test_missing_file_returns_empty(self):
        assert compute_strategy_hash('/tmp/definitely-does-not-exist-xyz.py') == ''


class TestArchive:
    """Soft-delete lifecycle — archive hides runs from the default list but
    keeps the data; unarchive restores; delete still permanently removes."""

    def test_new_runs_are_not_archived(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        rid = store.add(_make_record())
        got = store.get(rid)
        assert got.archived is False

    def test_archive_hides_from_default_list(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        keep_id = store.add(_make_record(note='keep'))
        hide_id = store.add(_make_record(note='hide'))

        affected = store.set_archived([hide_id], archived=True)
        assert affected == 1

        default_list = store.list()
        assert [r.id for r in default_list] == [keep_id]

    def test_include_archived_shows_all(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        keep_id = store.add(_make_record(note='keep'))
        hide_id = store.add(_make_record(note='hide'))
        store.set_archived([hide_id], archived=True)

        all_list = store.list(include_archived=True)
        assert sorted(r.id for r in all_list) == sorted([keep_id, hide_id])
        # The archived flag survives the round-trip.
        archived_flags = {r.id: r.archived for r in all_list}
        assert archived_flags == {keep_id: False, hide_id: True}

    def test_archived_only_excludes_active(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        active_id = store.add(_make_record(note='active'))
        hide_id = store.add(_make_record(note='hide'))
        store.set_archived([hide_id], archived=True)

        only = store.list(archived_only=True)
        assert [r.id for r in only] == [hide_id]

    def test_unarchive_restores_to_default_list(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        rid = store.add(_make_record(note='bring-back'))
        store.set_archived([rid], archived=True)
        assert store.list() == []

        restored = store.set_archived([rid], archived=False)
        assert restored == 1
        assert [r.id for r in store.list()] == [rid]

    def test_bulk_archive_returns_affected_count(self, tmp_duckdb_path):
        store = BacktestStore(tmp_duckdb_path)
        ids = [store.add(_make_record(note=f'run-{i}')) for i in range(5)]
        affected = store.set_archived(ids, archived=True)
        assert affected == 5
        assert store.list() == []

    def test_archive_missing_ids_returns_zero_not_raises(self, tmp_duckdb_path):
        """Passing ids that don't exist (or are already in target state)
        produces a 0-count report, not an exception — the CLI reports
        partial-success based on this."""
        store = BacktestStore(tmp_duckdb_path)
        rid = store.add(_make_record())
        # Already-active; archiving to active is a no-op
        affected = store.set_archived([rid], archived=False)
        assert affected == 0

        # Non-existent id
        affected = store.set_archived([99999], archived=True)
        assert affected == 0

    def test_empty_ids_list_is_noop(self, tmp_duckdb_path):
        """set_archived([]) must not run an invalid SQL statement — the
        CLI shouldn't be able to blow up the DB by passing nothing."""
        store = BacktestStore(tmp_duckdb_path)
        store.add(_make_record())
        assert store.set_archived([], archived=True) == 0
        # Active list unchanged
        assert len(store.list()) == 1

    def test_filter_by_strategy_respects_archive(self, tmp_duckdb_path):
        """strategy_class filter + archive filter combine via AND, not
        either-or — archived runs of the same class still hide."""
        store = BacktestStore(tmp_duckdb_path)
        a_active = store.add(_make_record(class_name='A', note='active'))
        a_hidden = store.add(_make_record(class_name='A', note='hidden'))
        store.add(_make_record(class_name='B'))
        store.set_archived([a_hidden], archived=True)

        default_a = store.list(strategy_class='A')
        assert [r.id for r in default_a] == [a_active]

        all_a = store.list(strategy_class='A', include_archived=True)
        assert sorted(r.id for r in all_a) == sorted([a_active, a_hidden])
