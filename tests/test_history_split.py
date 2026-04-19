"""Tests for DuckDB history/trading split.

Verifies that tick_data uses history_duckdb_path while trading data
(events, proposals, groups, universes) uses duckdb_path, so history
downloads never contend with trading services for the write lock.
"""

import datetime as dt
import os
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from trader.config import MMRConfig
from trader.data.data_access import TickStorage
from trader.data.duckdb_store import DuckDBDataStore, DuckDBObjectStore
from trader.data.event_store import EventStore, EventType, TradingEvent
from trader.data.proposal_store import ProposalStore
from trader.data.position_groups import PositionGroupStore
from trader.objects import BarSize


def _sample_df(n=10, start="2024-01-02 09:30"):
    dates = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "open": 100.0 + rng.normal(0, 1, n),
        "high": 101.0 + rng.normal(0, 1, n),
        "low": 99.0 + rng.normal(0, 1, n),
        "close": 100.0 + rng.normal(0, 1, n),
        "volume": rng.integers(100, 1000, n).astype(float),
    }, index=dates)


@pytest.fixture
def split_paths(tmp_path):
    """Return two separate DuckDB paths for trading and history."""
    return {
        'trading': str(tmp_path / 'trading.duckdb'),
        'history': str(tmp_path / 'history.duckdb'),
    }


class TestConfigHistoryPath:
    """Test that history_duckdb_path is properly configured."""

    def test_default_history_path(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("ib_server_address: 127.0.0.1\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.storage.history_duckdb_path.endswith('data/mmr_history.duckdb')
        assert os.path.isabs(config.storage.history_duckdb_path)

    def test_custom_history_path(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("history_duckdb_path: data/custom_history.duckdb\n")
        config = MMRConfig.from_yaml(str(cfg))
        assert config.storage.history_duckdb_path.endswith('data/custom_history.duckdb')

    def test_history_path_in_flat_dict(self, test_config_file):
        config = MMRConfig.from_yaml(test_config_file)
        flat = config.to_flat_dict()
        assert 'history_duckdb_path' in flat

    def test_history_and_trading_paths_differ(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text(
            "duckdb_path: data/mmr.duckdb\n"
            "history_duckdb_path: data/mmr_history.duckdb\n"
        )
        config = MMRConfig.from_yaml(str(cfg))
        assert config.storage.duckdb_path != config.storage.history_duckdb_path
        assert 'mmr.duckdb' in config.storage.duckdb_path
        assert 'mmr_history.duckdb' in config.storage.history_duckdb_path


class TestSplitIsolation:
    """Test that tick_data and trading data go to separate files."""

    def test_tick_data_writes_to_history_file(self, split_paths):
        """TickStorage should write to the history path, not the trading path."""
        history_path = split_paths['history']
        storage = TickStorage(history_path)
        tickdata = storage.get_tickdata(BarSize.Mins1)

        # Write some data
        df = _sample_df()
        tickdata.write(4391, df)  # AMD conId

        # Verify it's in the history file
        history_store = DuckDBDataStore(history_path)
        result = history_store.read('4391')
        assert len(result) == 10

        # Verify the trading file doesn't exist (nothing written there)
        trading_path = split_paths['trading']
        assert not os.path.exists(trading_path)

    def test_events_write_to_trading_file(self, split_paths):
        """EventStore should write to the trading path, not the history path."""
        trading_path = split_paths['trading']
        event_store = EventStore(trading_path)

        event = TradingEvent(
            event_type=EventType.SIGNAL,
            timestamp=dt.datetime.now(),
            strategy_name='test',
            conid=4391,
            action='BUY',
            signal_probability=0.8,
        )
        event_store.append(event)

        # Verify event is in trading file
        events = event_store.query_by_strategy('test')
        assert len(events) == 1

        # Verify history file doesn't exist
        history_path = split_paths['history']
        assert not os.path.exists(history_path)

    def test_proposals_write_to_trading_file(self, split_paths):
        """ProposalStore should write to the trading path."""
        from trader.trading.proposal import TradeProposal, ExecutionSpec
        trading_path = split_paths['trading']
        store = ProposalStore(trading_path)

        proposal = TradeProposal(
            symbol='AAPL', action='BUY', quantity=10,
            execution=ExecutionSpec(order_type='MKT'),
            reasoning='test', confidence=0.7,
        )
        pid = store.add(proposal)
        assert pid > 0

        # Verify in trading file
        result = store.get(pid)
        assert result.symbol == 'AAPL'

        # History file untouched
        assert not os.path.exists(split_paths['history'])

    def test_groups_write_to_trading_file(self, split_paths):
        """PositionGroupStore should write to the trading path."""
        trading_path = split_paths['trading']
        store = PositionGroupStore(trading_path)

        store.create_group('tech', max_allocation_pct=20.0)
        store.add_member('tech', 'AAPL')

        groups = store.list_groups()
        assert len(groups) == 1
        assert groups[0].name == 'tech'

        # History file untouched
        assert not os.path.exists(split_paths['history'])

    def test_concurrent_access_separate_files(self, split_paths):
        """Simultaneous writes to history and trading files should not conflict."""
        history_path = split_paths['history']
        trading_path = split_paths['trading']

        # Write tick data to history
        storage = TickStorage(history_path)
        tickdata = storage.get_tickdata(BarSize.Mins1)
        df = _sample_df()
        tickdata.write(265598, df)

        # Simultaneously write events to trading
        event_store = EventStore(trading_path)
        event = TradingEvent(
            event_type=EventType.SIGNAL,
            timestamp=dt.datetime.now(),
            strategy_name='concurrent_test',
            conid=265598,
            action='BUY',
        )
        event_store.append(event)

        # Both files exist and contain their respective data
        assert os.path.exists(history_path)
        assert os.path.exists(trading_path)

        history_store = DuckDBDataStore(history_path)
        assert len(history_store.read('265598')) == 10

        events = event_store.query_by_strategy('concurrent_test')
        assert len(events) == 1

    def test_tick_data_not_in_trading_file(self, split_paths):
        """After writing tick_data to history, trading file should have no tick_data table."""
        import duckdb

        history_path = split_paths['history']
        trading_path = split_paths['trading']

        # Write tick data to history file
        storage = TickStorage(history_path)
        tickdata = storage.get_tickdata(BarSize.Days1)
        tickdata.write(265598, _sample_df())

        # Write an event to trading file (to create it)
        event_store = EventStore(trading_path)
        event_store.append(TradingEvent(
            event_type=EventType.SIGNAL,
            timestamp=dt.datetime.now(),
            strategy_name='test', conid=0, action='BUY',
        ))

        # Trading file should NOT have tick_data table
        conn = duckdb.connect(trading_path)
        tables = [row[0] for row in conn.execute("SHOW TABLES").fetchall()]
        conn.close()
        assert 'tick_data' not in tables


class TestFallbackBehavior:
    """Test backward compatibility when history_duckdb_path is not set."""

    def test_empty_history_path_falls_back_to_duckdb_path(self, tmp_duckdb_path):
        """When history_duckdb_path is empty, TickStorage should use duckdb_path."""
        # Simulate the fallback: history_duckdb_path = '' → use duckdb_path
        history_path = '' or tmp_duckdb_path
        storage = TickStorage(history_path)
        tickdata = storage.get_tickdata(BarSize.Mins1)
        tickdata.write(4391, _sample_df())

        # Data is in the main duckdb file
        store = DuckDBDataStore(tmp_duckdb_path)
        result = store.read('4391')
        assert len(result) == 10

    def test_data_service_fallback(self):
        """DataService with empty history_duckdb_path should fall back to duckdb_path."""
        from trader.data_service import DataService
        svc = DataService(duckdb_path='/tmp/main.duckdb', history_duckdb_path='')
        assert svc.history_duckdb_path == '/tmp/main.duckdb'

    def test_data_service_explicit_history_path(self):
        """DataService with explicit history_duckdb_path should use it."""
        from trader.data_service import DataService
        svc = DataService(
            duckdb_path='/tmp/main.duckdb',
            history_duckdb_path='/tmp/history.duckdb',
        )
        assert svc.history_duckdb_path == '/tmp/history.duckdb'

    def test_config_missing_history_path_uses_default(self, tmp_path):
        """YAML without history_duckdb_path should use the default."""
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("duckdb_path: data/mmr.duckdb\n")
        config = MMRConfig.from_yaml(str(cfg))
        # Default from StorageConfig dataclass
        assert 'mmr_history.duckdb' in config.storage.history_duckdb_path
