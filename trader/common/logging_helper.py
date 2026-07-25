from enum import IntEnum
from functools import wraps
from logging import Logger
from rich.logging import RichHandler
from rich.traceback import install
from types import FrameType
from typing import cast, Dict, List

import datetime as dt
import inspect
import logging
import logging.config
import os
import warnings
import yaml


# Session timestamp generated once per process at import time.
_session_timestamp = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


# The operational log directory: bind-mounted into the container, tailed by the
# monitors, and the single place `mmr verify` / triage looks.
_DEFAULT_LOG_DIR = os.path.expanduser('~/.local/share/mmr/logs')

# Overridable so a NON-operational process can be kept out of it. The host and
# the container share this directory (it is the bind-mount source), so a host
# pytest run wrote its output alongside live-service logs — hundreds of files,
# and test fixtures that deliberately contain strings like 'STALE exit claim'
# and 'placement refused'. Grepping the operational logs then turned up test
# noise indistinguishable at a glance from real trading events (AUDIT_ROADMAP
# G7). tests/conftest.py sets this to a temp dir; the container never sets it.
MMR_LOG_DIR = os.path.abspath(
    os.path.expanduser(os.environ.get('MMR_LOG_DIR') or _DEFAULT_LOG_DIR))


def _redirect_if_overridden(filename: str) -> str:
    """Map a configured log path under the DEFAULT dir into MMR_LOG_DIR.

    The handler filenames come from logging.yaml (e.g.
    ``~/.local/share/mmr/logs/debug.log``), not from MMR_LOG_DIR — so pointing
    the constant elsewhere would redirect nothing without this rewrite.
    """
    if MMR_LOG_DIR == _DEFAULT_LOG_DIR:
        return filename
    if filename == _DEFAULT_LOG_DIR or filename.startswith(_DEFAULT_LOG_DIR + os.sep):
        return os.path.join(MMR_LOG_DIR, os.path.relpath(filename, _DEFAULT_LOG_DIR))
    return filename

# Logs that live in MMR_LOG_DIR but must NOT be session-stamped. debug.log is
# the single complete cross-session triage file (see docs/MONITORING.md) — a
# per-process timestamp would fragment it into thousands of partial files,
# which is exactly what it exists to avoid.
_UNSTAMPED_LOGS = frozenset({'debug.log'})


def _stamp_log_filenames(config: dict) -> dict:
    """Expand ~ in log paths, ensure directory exists, and inject session timestamp."""
    for handler_config in config.get('handlers', {}).values():
        filename = handler_config.get('filename', '')
        if not filename:
            continue
        # Expand ~ to home directory, then honour an MMR_LOG_DIR override so a
        # non-operational process (pytest) cannot write into the live log dir.
        filename = _redirect_if_overridden(os.path.expanduser(filename))
        # Add session timestamp to log files in the mmr log directory
        if (filename.endswith('.log')
                and MMR_LOG_DIR in filename
                and os.path.basename(filename) not in _UNSTAMPED_LOGS):
            base = filename[:-4]
            filename = f'{base}_{_session_timestamp}.log'
        handler_config['filename'] = filename
    # Ensure log directory exists
    os.makedirs(MMR_LOG_DIR, exist_ok=True)
    return config


def _drop_unwritable_handlers(config: dict) -> List[str]:
    """Drop file handlers whose target can't be opened, return their names.

    dictConfig raises on the FIRST unopenable handler, and the caller's only
    recourse is basicConfig() — so one unwritable path used to silently take
    down the entire logging config (console, service logs, errors.log) with
    it. That is exactly what happened when the services ran as root and left
    a root-owned debug log that `mmr` CLI runs (uid trader) could not open.
    Degrade per-handler instead: the rest of the config still applies.
    """
    dropped: List[str] = []
    for name, handler_config in list(config.get('handlers', {}).items()):
        filename = handler_config.get('filename', '')
        if not filename:
            continue
        try:
            parent = os.path.dirname(filename) or '.'
            os.makedirs(parent, exist_ok=True)
            if os.path.exists(filename):
                # Authoritative check — this is the access the handler takes.
                with open(filename, 'a', encoding='utf8'):
                    pass
            elif not os.access(parent, os.W_OK | os.X_OK):
                raise PermissionError(13, 'Permission denied', parent)
            # Deliberately does NOT create a missing file: the handler owns
            # that decision (and may be configured with delay: true).
        except OSError as e:
            dropped.append(name)
            del config['handlers'][name]
            print(f'logging: handler {name!r} disabled — cannot write {filename}: {e}')

    if dropped:
        # Purge the dropped names from every handler list, or dictConfig
        # fails again on the dangling reference.
        sections = [config.get('root', {})] + list(config.get('loggers', {}).values())
        for section in sections:
            handlers = section.get('handlers')
            if handlers:
                section['handlers'] = [h for h in handlers if h not in dropped]
    return dropped


class LogLevels(IntEnum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    NOTSET = 0


global_loggers: Dict[str, Logger] = {}


def setup_logging(default_path='',
                  module_name='root',
                  default_level=logging.DEBUG,
                  env_key='LOG_CFG',
                  suppress_external_info=False) -> Logger:
    global global_loggers

    if not default_path:
        from trader.container import ensure_config_dir
        default_path = str(ensure_config_dir() / 'logging.yaml')

    warnings.filterwarnings(
        'ignore',
        message='The zone attribute is specific to pytz\'s interface; please migrate to a new time zone provider. For more details on how to do so, see https://pytz-deprecation-shim.readthedocs.io/en/latest/migration.html'  # noqa: E501
    )

    # rich tracebacks
    # install(show_locals=False)

    # ipython repl has a nasty habit of being polluted with debug crap from parso
    logging.getLogger('parso.python.diff').setLevel(logging.WARNING)
    logging.getLogger('parso').setLevel(logging.WARNING)
    logging.getLogger('parso.cache.pickle').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)

    if module_name in global_loggers:
        return global_loggers[module_name]

    warnings.simplefilter(action='ignore', category=FutureWarning)
    if suppress_external_info:
        suppress_external()
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            try:
                config = yaml.safe_load(f.read())
                _stamp_log_filenames(config)
                _drop_unwritable_handlers(config)
                logging.config.dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        print('Failed to load configuration file. Using default configs')

    global_loggers[module_name] = logging.getLogger(module_name)
    return global_loggers[module_name]

def set_log_level(module_name: str, level: LogLevels):
    global global_loggers
    if module_name in global_loggers:
        global_loggers[module_name].setLevel(level)

def set_external_log_level(level: LogLevels):
    logging.getLogger('ib_async.wrapper').setLevel(level)
    logging.getLogger('ib_async.client').setLevel(level)
    logging.getLogger('ib_async.ib').setLevel(level)

def set_all_log_level(level: LogLevels):
    set_external_log_level(level)
    logging.getLogger().setLevel(level)

def suppress_external():
    set_external_log_level(LogLevels.ERROR)

def suppress_all():
    suppress_external()
    logging.getLogger().setLevel(logging.ERROR)

def verbose():
    set_all_log_level(LogLevels.DEBUG)

def get_callstack(frames: int = 0) -> List[str]:
    def walk_stack(frame: FrameType, counter: int = 1) -> List[str]:
        mod = inspect.getmodule(frame)
        m = mod.__name__ if mod else ''
        if frames > 0 and counter == frames:
            return [str(m + '.' + frame.f_code.co_name)]

        if frame.f_back:
            return [str(m + '.' + frame.f_code.co_name)] + walk_stack(frame.f_back, counter + 1)
        else:
            return [m + '.' + str(frame.f_code.co_name)]

    current_frame = inspect.currentframe()
    if current_frame and current_frame.f_back:
        return walk_stack(cast(FrameType, current_frame.f_back))
    return []


def log_callstack_debug(frames: int = 0, module_filter: str = ''):
    callstack = get_callstack(frames)
    if callstack:
        if module_filter:
            callstack = [a for a in callstack if module_filter in a]
        result = ' <- '.join(callstack)
        logging.debug(result)

def log_method(func):
    global logging

    @wraps(func)
    def wrapper(*args, **kwargs):
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str = ", ".join(map("{0[0]} = {0[1]!r}".format, [item for item in func_args.items() if item[0] != 'self']))
        logging.debug(f"{func.__module__}.{func.__qualname__}({func_args_str})")
        return func(*args, **kwargs)

    return wrapper
