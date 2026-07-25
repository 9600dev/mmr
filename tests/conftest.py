import datetime as dt
import logging
import os
import sys
import tempfile
from uuid import uuid4

# Keep test logging OUT of the live operational log directory. That directory is
# the container bind-mount source, so a host pytest run used to write its output
# next to real service logs — and because these tests deliberately assert on
# strings like 'STALE exit claim' and 'placement refused', grepping the
# operational logs surfaced test noise that looked exactly like trading events
# (AUDIT_ROADMAP G7). MUST be set before any `trader` import: MMR_LOG_DIR is read
# at logging_helper import time. setdefault so an explicit override still wins.
os.environ.setdefault(
    'MMR_LOG_DIR', os.path.join(tempfile.gettempdir(), 'mmr-pytest-logs'))

import numpy as np
import pandas as pd
import pytest

# Ensure project root is on sys.path so 'trader' is importable
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from trader.data.duckdb_store import DuckDBConnection, DuckDBDataStore, DuckDBObjectStore
from trader.data.data_access import TickStorage
from trader.data.event_store import EventStore
from trader.data.proposal_store import ProposalStore
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState


# ---------------------------------------------------------------------------
# Autouse: clear DuckDBConnection singleton cache between tests
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _clear_duckdb_instances():
    yield
    DuckDBConnection._instances.clear()


# ---------------------------------------------------------------------------
# DuckDB temp path
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_duckdb_path(tmp_path):
    return str(tmp_path / f"test_{uuid4().hex[:8]}.duckdb")


# ---------------------------------------------------------------------------
# Config file fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def test_config_file(tmp_path, tmp_duckdb_path):
    config_content = f"""\
root_directory: {tmp_path}
config_file: {tmp_path / 'trader.yaml'}
logfile: {tmp_path / 'trader.log'}
duckdb_path: {tmp_duckdb_path}
universe_library: Universes
ib_server_address: 127.0.0.1
ib_paper_account: TESTPAPER
ib_live_account: TESTLIVE
ib_paper_port: 7497
ib_live_port: 7496
trading_runtime_ib_client_id: 5
strategy_runtime_ib_client_id: 7
zmq_rpc_server_address: tcp://127.0.0.1
zmq_rpc_server_port: 42001
zmq_pubsub_server_address: tcp://127.0.0.1
zmq_pubsub_server_port: 42002
zmq_strategy_rpc_server_address: tcp://127.0.0.1
zmq_strategy_rpc_server_port: 42005
zmq_messagebus_server_address: tcp://127.0.0.1
zmq_messagebus_server_port: 42006
strategies_directory: strategies
strategy_config_file: config_defaults/strategy_runtime.yaml
trading_mode: paper
"""
    config_path = tmp_path / "trader.yaml"
    config_path.write_text(config_content)
    return str(config_path)


# ---------------------------------------------------------------------------
# EventStore fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def event_store(tmp_duckdb_path):
    return EventStore(tmp_duckdb_path)


# ---------------------------------------------------------------------------
# ProposalStore fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def proposal_store(tmp_duckdb_path):
    return ProposalStore(tmp_duckdb_path)


# ---------------------------------------------------------------------------
# RiskGate fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def risk_gate(event_store):
    return RiskGate(limits=RiskLimits(), event_store=event_store)


# ---------------------------------------------------------------------------
# Sample OHLCV DataFrame (100-bar uptrend)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_ohlcv():
    n = 100
    base = 90.0
    rng = np.random.default_rng(42)
    close = base + np.cumsum(rng.normal(0.1, 0.5, n))
    dates = pd.date_range("2024-01-02 09:30", periods=n, freq="1min", tz="UTC")
    df = pd.DataFrame({
        "open": close - rng.uniform(0, 0.5, n),
        "high": close + rng.uniform(0, 1.0, n),
        "low": close - rng.uniform(0, 1.0, n),
        "close": close,
        "volume": rng.integers(100, 10000, n).astype(float),
    }, index=dates)
    df.index.name = "date"
    return df


# ---------------------------------------------------------------------------
# Edge-case OHLCV fixtures — for testing indicator robustness / backtesting
# against the kinds of real-world artifacts that sample_ohlcv doesn't expose.
# The generators live in trader.simulation.synthetic_markets (same seeds and
# shapes — identical frames) so non-test code, notably the strategy gauntlet,
# can run the same battery.
# ---------------------------------------------------------------------------

from trader.simulation import synthetic_markets


@pytest.fixture
def ohlcv_with_gaps():
    """OHLCV with NaN rows (simulates market halts / future-date API padding)."""
    return synthetic_markets.ohlcv_with_gaps()


@pytest.fixture
def ohlcv_high_volatility():
    """High-ATR synthetic OHLCV — tests position sizing / volatility adjustment."""
    return synthetic_markets.ohlcv_high_volatility()


@pytest.fixture
def ohlcv_zero_volume():
    """OHLCV with zero-volume bars — simulates pre/post-market or illiquid
    periods that must not divide-by-zero or get flagged as opportunities."""
    return synthetic_markets.ohlcv_zero_volume()


@pytest.fixture
def ohlcv_halted():
    """ATR-zero scenario: price stuck at a halt price for many bars."""
    return synthetic_markets.ohlcv_halted()


# ---------------------------------------------------------------------------
# Concrete Strategy for testing
# ---------------------------------------------------------------------------

class BuyAbove100Strategy(Strategy):
    """Returns BUY when close > 100, SELL when close < 95, else None."""

    def on_prices(self, prices: pd.DataFrame):
        if prices.empty:
            return None
        last_close = float(prices["close"].iloc[-1])
        if last_close > 100:
            return Signal(
                source_name=self.name or "test",
                action=Action.BUY,
                probability=0.8,
                risk=0.2,
                conid=self.conids[0] if self.conids else 0,
            )
        elif last_close < 95:
            return Signal(
                source_name=self.name or "test",
                action=Action.SELL,
                probability=0.7,
                risk=0.3,
                conid=self.conids[0] if self.conids else 0,
            )
        return None


# ---------------------------------------------------------------------------
# StrategyContext factory fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def make_strategy_context(tmp_duckdb_path):
    def _factory(name="test_strategy", conids=None, bar_size=BarSize.Mins1):
        storage = TickStorage(duckdb_path=tmp_duckdb_path)
        universe_accessor = UniverseAccessor.__new__(UniverseAccessor)
        universe_accessor.duckdb_path = tmp_duckdb_path
        universe_accessor.universe_library = "Universes"
        return StrategyContext(
            name=name,
            bar_size=bar_size,
            conids=conids or [4391],
            universe=None,
            historical_days_prior=5,
            paper_only=False,
            storage=storage,
            universe_accessor=universe_accessor,
            logger=logging.getLogger("test"),
        )
    return _factory


# ---------------------------------------------------------------------------
# Installed strategy fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def installed_strategy(make_strategy_context):
    strategy = BuyAbove100Strategy()
    ctx = make_strategy_context()
    strategy.install(ctx)
    return strategy
