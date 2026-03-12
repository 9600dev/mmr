from pathlib import Path
from trader.config import MMRConfig
from typing import ClassVar, Dict, Optional, Type

import inspect
import os
import shutil
import yaml


MMR_CONFIG_DIR = Path('~/.config/mmr').expanduser()


def mmr_root() -> Path:
    """Find the MMR project root directory."""
    # Check env var first
    if os.getenv('MMR_ROOT'):
        return Path(os.getenv('MMR_ROOT'))
    # Walk up from this file to find the project root (contains configs/)
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / 'configs' / 'trader.yaml').exists():
            return current
        current = current.parent
    # Fallback to CWD
    return Path.cwd()


def _bundled_configs_dir() -> Path:
    """Return the path to the bundled configs/ directory in the project."""
    return mmr_root() / 'configs'


def ensure_config_dir() -> Path:
    """Ensure ~/.config/mmr exists, copying bundled configs if needed.

    Returns the path to ~/.config/mmr.
    """
    MMR_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    bundled = _bundled_configs_dir()
    if bundled.exists():
        for src in bundled.glob('*.yaml'):
            dest = MMR_CONFIG_DIR / src.name
            if not dest.exists():
                shutil.copy2(src, dest)
    return MMR_CONFIG_DIR


def default_config_path() -> str:
    """Return the default config file path (~/.config/mmr/trader.yaml)."""
    config_dir = ensure_config_dir()
    return str(config_dir / 'trader.yaml')


class Container():
    _instance: ClassVar[Optional['Container']] = None

    def __init__(self, config_file: str = ''):
        if not config_file:
            config_file = default_config_path()

        if os.getenv('TRADER_CONFIG'):
            self.config_file = str(os.getenv('TRADER_CONFIG'))
        else:
            self.config_file = config_file

        if not os.path.exists(self.config_file):
            raise ValueError('configuration_file is not found {} or TRADER_CONFIG set incorrectly'.format(self.config_file))

        with open(self.config_file, 'r') as conf_file:
            self.configuration: Dict = yaml.load(conf_file, Loader=yaml.FullLoader)
            self.type_instance_cache: Dict[Type, object] = {}

        # Resolve relative paths against the project root so CLI works from any cwd
        root = mmr_root()
        for key in ('duckdb_path', 'logfile', 'root_directory'):
            val = self.configuration.get(key)
            if val and isinstance(val, str) and not os.path.isabs(val) and not val.startswith('~'):
                self.configuration[key] = str(root / val)

        self.mmr_config: MMRConfig = MMRConfig.from_yaml(self.config_file)

        # Sync auto-resolved values back to the raw config dict so that
        # Container.resolve() picks them up (it reads from self.configuration).
        self.configuration['ib_account'] = self.mmr_config.ib.account
        self.configuration['ib_server_port'] = self.mmr_config.ib.server_port
        self.configuration['paper_trading'] = self.mmr_config.paper_trading
        self.configuration['trading_mode'] = self.mmr_config.trading_mode
        self.configuration['massive_api_key'] = self.mmr_config.massive.api_key
        self.configuration['massive_feed'] = self.mmr_config.massive.feed
        self.configuration['massive_delayed'] = self.mmr_config.massive.delayed

    @classmethod
    def create(cls, config_file: str = '') -> 'Container':
        cls._instance = cls(config_file)
        return cls._instance

    @classmethod
    def instance(cls) -> 'Container':
        if cls._instance is None:
            # Auto-create with default config for backward compatibility
            cls._instance = cls()
        return cls._instance

    def resolve(self, t: Type, **extra_args):
        args = {}
        for param in inspect.signature(t.__init__).parameters.values():
            if param.name == 'self':
                continue
            if extra_args and param.name in extra_args.keys():
                args[param.name] = extra_args[param.name]
            elif os.getenv(param.name.upper()) is not None:
                args[param.name] = os.getenv(param.name.upper())
            elif param.name in self.configuration and self.configuration[param.name] is not None:
                args[param.name] = self.configuration[param.name]
        # Expand ~ in string values that look like paths
        for key, value in args.items():
            if isinstance(value, str) and value.startswith('~'):
                args[key] = os.path.expanduser(value)
        return t(**args)

    def config(self) -> Dict:
        return self.configuration

    def typed_config(self) -> MMRConfig:
        return self.mmr_config

    def resolve_cache(self, t: Type, **extra_args):
        if t in self.type_instance_cache:
            return self.type_instance_cache[t]
        else:
            self.type_instance_cache[t] = self.resolve(t, **extra_args)
            return self.type_instance_cache[t]
