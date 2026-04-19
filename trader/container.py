from pathlib import Path
from trader.config import MMRConfig
from typing import ClassVar, Dict, Optional, Set, Type

import inspect
import os
import shutil
import threading
import yaml


class ContainerResolutionError(ValueError):
    """Raised when Container.resolve cannot satisfy a constructor parameter."""


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
    _instance_lock: ClassVar[threading.Lock] = threading.Lock()

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
            # safe_load refuses Python-object tags — guards against YAML-based
            # arbitrary-code-execution in trader.yaml.
            self.configuration: Dict = yaml.safe_load(conf_file) or {}
            self.type_instance_cache: Dict[Type, object] = {}

        self._resolve_lock = threading.Lock()
        # Tracks types currently being resolved (for circular-dep detection).
        self._resolving: Set[Type] = set()

        # Resolve relative paths against the project root so CLI works from any cwd
        root = mmr_root()
        for key in ('duckdb_path', 'history_duckdb_path', 'logfile', 'root_directory'):
            val = self.configuration.get(key)
            if val and isinstance(val, str):
                val = os.path.expanduser(val)
                if not os.path.isabs(val):
                    val = str(root / val)
                self.configuration[key] = val

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
        self.configuration['twelvedata_api_key'] = self.mmr_config.twelvedata.api_key

    @classmethod
    def create(cls, config_file: str = '') -> 'Container':
        with cls._instance_lock:
            cls._instance = cls(config_file)
            return cls._instance

    @classmethod
    def instance(cls) -> 'Container':
        with cls._instance_lock:
            if cls._instance is None:
                # Auto-create with default config for backward compatibility
                cls._instance = cls()
            return cls._instance

    def _coerce_env_value(self, raw: str, param: inspect.Parameter):
        """Best-effort coerce an env-var string into the annotated type.

        Unannotated or ``str`` params pass through unchanged. For ``int``,
        ``float``, and ``bool`` we convert; a malformed value raises
        ContainerResolutionError with context on which parameter and what
        value failed, instead of the cryptic TypeError you'd otherwise get
        when the constructor runs.
        """
        ann = param.annotation
        if ann is inspect.Parameter.empty or ann is str:
            return raw
        try:
            if ann is int:
                return int(raw)
            if ann is float:
                return float(raw)
            if ann is bool:
                return raw.lower() in ('1', 'true', 'yes', 'on')
        except (TypeError, ValueError) as ex:
            raise ContainerResolutionError(
                f'env var {param.name.upper()}={raw!r} cannot be coerced to {ann.__name__}: {ex}'
            ) from ex
        return raw

    def resolve(self, t: Type, **extra_args):
        """Build an instance of ``t`` by resolving constructor parameters from
        extra_args, env vars, and the configuration dict — in that order."""
        with self._resolve_lock:
            if t in self._resolving:
                chain = ' → '.join(x.__name__ for x in self._resolving) + f' → {t.__name__}'
                raise ContainerResolutionError(f'circular dependency while resolving {chain}')
            self._resolving.add(t)

        try:
            args: Dict[str, object] = {}
            missing_required: list = []
            for param in inspect.signature(t.__init__).parameters.values():
                if param.name == 'self':
                    continue
                # Skip *args and **kwargs — they're never "required" in the
                # sense that the caller has to supply them, even though
                # their ``default`` is Parameter.empty.
                if param.kind in (
                    inspect.Parameter.VAR_POSITIONAL,
                    inspect.Parameter.VAR_KEYWORD,
                ):
                    continue
                if extra_args and param.name in extra_args.keys():
                    args[param.name] = extra_args[param.name]
                elif os.getenv(param.name.upper()) is not None:
                    args[param.name] = self._coerce_env_value(os.getenv(param.name.upper()), param)
                elif param.name in self.configuration and self.configuration[param.name] is not None:
                    args[param.name] = self.configuration[param.name]
                elif param.default is inspect.Parameter.empty:
                    # Required param with no source — capture it so we can
                    # report all at once instead of erroring on the first.
                    missing_required.append(param.name)

            if missing_required:
                raise ContainerResolutionError(
                    f'cannot resolve {t.__name__}: missing required constructor parameter(s) '
                    f'{missing_required!r}; provide them via extra_args, env vars '
                    f'(UPPERCASED), or the configuration file.'
                )

            # Expand ~ in string values that look like paths
            for key, value in args.items():
                if isinstance(value, str) and value.startswith('~'):
                    args[key] = os.path.expanduser(value)
            return t(**args)
        finally:
            with self._resolve_lock:
                self._resolving.discard(t)

    def config(self) -> Dict:
        return self.configuration

    def typed_config(self) -> MMRConfig:
        return self.mmr_config

    def resolve_cache(self, t: Type, **extra_args):
        with self._resolve_lock:
            if t in self.type_instance_cache:
                return self.type_instance_cache[t]
        instance = self.resolve(t, **extra_args)
        with self._resolve_lock:
            # Another thread may have won the race; prefer whichever landed first.
            self.type_instance_cache.setdefault(t, instance)
            return self.type_instance_cache[t]
