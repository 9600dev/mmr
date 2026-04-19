from dataclasses import dataclass, field, fields
from typing import Any, Dict, Optional

import os
import yaml

# Standard IB ports by mode
# TWS / local IB Gateway
IB_LIVE_PORT = 7496
IB_PAPER_PORT = 7497
# Docker sidecar (ib-gateway container internal ports)
IB_GATEWAY_LIVE_PORT = 4003
IB_GATEWAY_PAPER_PORT = 4004


@dataclass
class IBConfig:
    server_address: str = '127.0.0.1'
    server_port: int = 7496
    account: str = ''
    paper_account: str = ''
    live_account: str = ''
    paper_port: int = IB_PAPER_PORT
    live_port: int = IB_LIVE_PORT
    trading_runtime_client_id: int = 5
    strategy_runtime_client_id: int = 7


@dataclass
class StorageConfig:
    duckdb_path: str = '~/.local/share/mmr/data/mmr.duckdb'
    history_duckdb_path: str = '~/.local/share/mmr/data/mmr_history.duckdb'
    universe_library: str = 'Universes'


@dataclass
class ZMQConfig:
    rpc_server_address: str = 'tcp://127.0.0.1'
    rpc_server_port: int = 42001
    pubsub_server_address: str = 'tcp://127.0.0.1'
    pubsub_server_port: int = 42002
    strategy_rpc_server_address: str = 'tcp://127.0.0.1'
    strategy_rpc_server_port: int = 42005
    messagebus_server_address: str = 'tcp://127.0.0.1'
    messagebus_server_port: int = 42006


@dataclass
class PycronConfig:
    config_file: str = '~/.config/mmr/pycron.yaml'
    server_address: str = '127.0.0.1'
    server_port: int = 8081


@dataclass
class MassiveConfig:
    api_key: str = ''
    feed: str = 'stocks'
    delayed: bool = False


@dataclass
class TwelveDataConfig:
    api_key: str = ''


@dataclass
class StrategyRuntimeConfig:
    strategies_directory: str = 'strategies'
    config_file: str = '~/.config/mmr/strategy_runtime.yaml'


@dataclass
class MMRConfig:
    ib: IBConfig = field(default_factory=IBConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    zmq: ZMQConfig = field(default_factory=ZMQConfig)
    pycron: PycronConfig = field(default_factory=PycronConfig)
    strategy: StrategyRuntimeConfig = field(default_factory=StrategyRuntimeConfig)
    massive: MassiveConfig = field(default_factory=MassiveConfig)
    twelvedata: TwelveDataConfig = field(default_factory=TwelveDataConfig)
    root_directory: str = '.'
    config_file: str = '~/.config/mmr/trader.yaml'
    logfile: str = '~/.local/share/mmr/logs/trader.log'
    trading_mode: str = 'live'
    paper_trading: bool = False
    simulation: bool = False

    # Mapping from flat YAML keys to nested config fields
    _FLAT_KEY_MAP: Dict[str, tuple] = field(default=None, init=False, repr=False)

    def __post_init__(self):
        self._FLAT_KEY_MAP = {
            # IB
            'ib_server_address': ('ib', 'server_address'),
            'ib_server_port': ('ib', 'server_port'),
            'ib_account': ('ib', 'account'),
            'ib_paper_account': ('ib', 'paper_account'),
            'ib_live_account': ('ib', 'live_account'),
            'ib_paper_port': ('ib', 'paper_port'),
            'ib_live_port': ('ib', 'live_port'),
            'trading_runtime_ib_client_id': ('ib', 'trading_runtime_client_id'),
            'strategy_runtime_ib_client_id': ('ib', 'strategy_runtime_client_id'),
            # Storage
            'duckdb_path': ('storage', 'duckdb_path'),
            'history_duckdb_path': ('storage', 'history_duckdb_path'),
            'universe_library': ('storage', 'universe_library'),
            # ZMQ
            'zmq_rpc_server_address': ('zmq', 'rpc_server_address'),
            'zmq_rpc_server_port': ('zmq', 'rpc_server_port'),
            'zmq_pubsub_server_address': ('zmq', 'pubsub_server_address'),
            'zmq_pubsub_server_port': ('zmq', 'pubsub_server_port'),
            'zmq_strategy_rpc_server_address': ('zmq', 'strategy_rpc_server_address'),
            'zmq_strategy_rpc_server_port': ('zmq', 'strategy_rpc_server_port'),
            'zmq_messagebus_server_address': ('zmq', 'messagebus_server_address'),
            'zmq_messagebus_server_port': ('zmq', 'messagebus_server_port'),
            # Pycron
            'pycron_config_file': ('pycron', 'config_file'),
            'pycron_server_address': ('pycron', 'server_address'),
            'pycron_server_port': ('pycron', 'server_port'),
            # Strategy
            'strategies_directory': ('strategy', 'strategies_directory'),
            'strategy_config_file': ('strategy', 'config_file'),
            # Massive
            'massive_api_key': ('massive', 'api_key'),
            'massive_feed': ('massive', 'feed'),
            'massive_delayed': ('massive', 'delayed'),
            # TwelveData
            'twelvedata_api_key': ('twelvedata', 'api_key'),
            # Top-level
            'root_directory': ('root_directory',),
            'config_file': ('config_file',),
            'logfile': ('logfile',),
            'trading_mode': ('trading_mode',),
        }

    @staticmethod
    def from_yaml(path: str) -> 'MMRConfig':
        with open(path, 'r') as f:
            raw: Dict[str, Any] = yaml.load(f, Loader=yaml.FullLoader) or {}

        config = MMRConfig()
        config.config_file = path

        for flat_key, nested_path in config._FLAT_KEY_MAP.items():
            # Check env var override first (uppercase)
            env_val = os.getenv(flat_key.upper())
            yaml_val = raw.get(flat_key)

            value = env_val if env_val is not None else yaml_val
            if value is None:
                continue

            if len(nested_path) == 1:
                # Top-level field
                attr = nested_path[0]
                current_val = getattr(config, attr)
                setattr(config, attr, type(current_val)(value) if current_val is not None else value)
            elif len(nested_path) == 2:
                # Nested field
                section = getattr(config, nested_path[0])
                attr = nested_path[1]
                current_val = getattr(section, attr)
                if isinstance(current_val, int):
                    setattr(section, attr, int(value))
                elif isinstance(current_val, bool):
                    setattr(section, attr, bool(value))
                else:
                    setattr(section, attr, str(value))

        # Fall back to native MASSIVE_API_KEY env var if not set via config
        if not config.massive.api_key:
            env_key = os.getenv('MASSIVE_API_KEY', '')
            if env_key:
                config.massive.api_key = env_key

        # Fall back to native TWELVEDATA_API_KEY env var if not set via config
        if not config.twelvedata.api_key:
            env_key = os.getenv('TWELVEDATA_API_KEY', '')
            if env_key:
                config.twelvedata.api_key = env_key

        # Derive paper_trading flag from trading_mode
        config.paper_trading = config.trading_mode == 'paper'

        # Auto-resolve active account from trading_mode unless IB_ACCOUNT env var is set.
        if not os.getenv('IB_ACCOUNT'):
            if config.trading_mode == 'paper':
                config.ib.account = config.ib.paper_account
            else:
                config.ib.account = config.ib.live_account

        # Auto-resolve active port from trading_mode unless IB_SERVER_PORT env var is set.
        if not os.getenv('IB_SERVER_PORT'):
            if config.trading_mode == 'paper':
                config.ib.server_port = config.ib.paper_port
            else:
                config.ib.server_port = config.ib.live_port

        # Resolve duckdb paths: expand ~ first, then resolve relative paths against project root
        from trader.container import mmr_root
        for attr in ('duckdb_path', 'history_duckdb_path'):
            val = getattr(config.storage, attr)
            if val:
                val = os.path.expanduser(val)
                if not os.path.isabs(val):
                    val = str(mmr_root() / val)
                setattr(config.storage, attr, val)

        return config

    def to_flat_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for flat_key, nested_path in self._FLAT_KEY_MAP.items():
            if len(nested_path) == 1:
                result[flat_key] = getattr(self, nested_path[0])
            elif len(nested_path) == 2:
                section = getattr(self, nested_path[0])
                result[flat_key] = getattr(section, nested_path[1])
        return result
