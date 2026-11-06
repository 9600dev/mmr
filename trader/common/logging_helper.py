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
import re
import sys
import time
import warnings
import yaml


# Session timestamp generated once per process at import time.
_session_timestamp = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Below DEBUG: for per-event chatter (every portfolio mark, ...) that would
# otherwise dominate the service logs at their default DEBUG level. Not
# emitted unless a logger is explicitly set to TRACE (``level: TRACE`` in
# logging.yaml — the name is registered here, before dictConfig runs).
TRACE = 5
logging.addLevelName(TRACE, 'TRACE')


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


def _dedupe_propagated_handlers(config: dict) -> Dict[str, List[str]]:
    """Remove handlers a logger would ALSO reach through propagation.

    A handler attached both to a propagating logger and to an ancestor (root,
    usually) receives every record twice — Python calls the logger's handlers,
    then each ancestor's, without checking for repeats. logging.yaml shipped
    exactly that for ``error_file_handler`` (on every service logger AND on
    root), so every ERROR landed in ``errors_<ts>.log`` twice; on 2026-11-05
    that made each IB error look like it was being logged repeatedly. Doing
    this in code (not only in the template) fixes existing
    ``~/.config/mmr/logging.yaml`` copies too. Semantics are otherwise
    unchanged: the record still reaches the handler, once, via the ancestor.

    Returns {logger_name: [removed handler names]} for the loggers changed.
    """
    loggers = config.get('loggers') or {}
    root_handlers = list((config.get('root') or {}).get('handlers') or [])
    original = {name: list((lc or {}).get('handlers') or []) for name, lc in loggers.items()}
    removed: Dict[str, List[str]] = {}
    for name, logger_config in loggers.items():
        if not logger_config or not logger_config.get('propagate', True):
            continue
        own = original[name]
        if not own:
            continue
        inherited: set = set()
        parts = name.split('.')
        reaches_root = True
        for i in range(len(parts) - 1, 0, -1):
            ancestor = '.'.join(parts[:i])
            if ancestor not in loggers:
                continue
            inherited.update(original[ancestor])
            if not (loggers[ancestor] or {}).get('propagate', True):
                reaches_root = False
                break
        if reaches_root:
            inherited.update(root_handlers)
        dupes = [h for h in own if h in inherited]
        if dupes:
            logger_config['handlers'] = [h for h in own if h not in inherited]
            removed[name] = dupes
    return removed


# Service-file handlers are routed by LOGGER name, but several loggers are
# shared between processes (``ib_history`` is used by data_service AND by
# strategy_service's IBHistoryWorker). Without a guard, the strategy_service
# process created its own ``data_service_<its timestamp>.log`` — a file named
# for a service that never wrote it, one of the reasons a single start left
# several overlapping file sets behind. Each of these handlers now refuses
# records only in a process POSITIVELY identified as a DIFFERENT service; a
# process whose role is unknown (CLI, pytest, ad-hoc scripts) keeps the old
# behaviour, so a service launched an unexpected way never loses its own log.
_SERVICE_HANDLERS = {
    'trader_service_handler': 'trader_service',
    'strategy_service_handler': 'strategy_service',
    'data_service_handler': 'data_service',
}


def _process_role() -> str:
    """'trader_service' / 'strategy_service' / 'data_service', or '' if this
    process is not one of the services (``python -m trader.<role>`` or
    ``python trader/<role>.py``)."""
    main = sys.modules.get('__main__')
    spec = getattr(main, '__spec__', None)
    candidates = []
    if spec is not None and getattr(spec, 'name', None):
        candidates.append(spec.name.rsplit('.', 1)[-1])
    if sys.argv and sys.argv[0]:
        candidates.append(os.path.splitext(os.path.basename(sys.argv[0]))[0])
    for candidate in candidates:
        if candidate in _SERVICE_HANDLERS.values():
            return candidate
    return ''


class ServiceRoleFilter(logging.Filter):
    """Drop records in a process identified as a different service."""

    def __init__(self, role: str = '', process_role: str | None = None):
        super().__init__()
        self.role = role
        self.process_role = _process_role() if process_role is None else process_role

    def filter(self, record: logging.LogRecord) -> bool:
        return not self.process_role or self.process_role == self.role


def _attach_service_role_filters(config: dict, process_role: str | None = None) -> None:
    handlers = config.get('handlers') or {}
    filters = config.setdefault('filters', {})
    for handler_name, role in _SERVICE_HANDLERS.items():
        handler_config = handlers.get(handler_name)
        if handler_config is None:
            continue
        filter_name = f'_mmr_service_role_{role}'
        spec = {'()': ServiceRoleFilter, 'role': role}
        if process_role is not None:
            spec['process_role'] = process_role
        filters[filter_name] = spec
        attached = list(handler_config.get('filters') or [])
        if filter_name not in attached:
            attached.append(filter_name)
        handler_config['filters'] = attached


# ---------------------------------------------------------------------------
# Log retention
# ---------------------------------------------------------------------------

LOG_RETENTION_ENV = 'MMR_LOG_RETENTION_DAYS'
DEFAULT_LOG_RETENTION_DAYS = 14

# A session-stamped log or one of its rotations:
#   trader_service_2026-11-05_13-11-32.log, errors_2026-07-27_21-15-54.log.3
_STAMPED_LOG = re.compile(r'^(?P<base>.+)_(?P<stamp>\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.log(?:\.\d+)?$')


def log_retention_days() -> int:
    """``MMR_LOG_RETENTION_DAYS`` (default 14). 0 disables pruning.

    Raises:
        ValueError: the value is not a non-negative integer.
    """
    raw = os.environ.get(LOG_RETENTION_ENV, '').strip()
    if not raw:
        return DEFAULT_LOG_RETENTION_DAYS
    try:
        days = int(raw)
    except ValueError:
        raise ValueError(f'{LOG_RETENTION_ENV}={raw!r} is not an integer number of days') from None
    if days < 0:
        raise ValueError(f'{LOG_RETENTION_ENV}={raw!r} must be >= 0 (0 disables pruning)')
    return days


def _open_log_paths() -> set:
    """Absolute paths of every file a handler in THIS process holds/targets."""
    paths = set()
    loggers = [logging.getLogger()] + [
        lg for lg in logging.Logger.manager.loggerDict.values() if isinstance(lg, logging.Logger)]
    for lg in loggers:
        for handler in lg.handlers:
            base = getattr(handler, 'baseFilename', None)
            if base:
                paths.add(os.path.abspath(base))
    return paths


def prune_old_logs(log_dir: str | None = None,
                   max_age_days: int | None = None,
                   now: float | None = None,
                   logger: logging.Logger | None = None) -> List[str]:
    """Delete session-stamped log files whose whole session went quiet > N days ago.

    Every process stamps its own file set (``<name>_<YYYY-MM-DD_HH-MM-SS>.log``
    plus rotations ``.log.N``) and nothing ever removed them: 4,038 files /
    1.6GB by 2026-11-05. Retention is decided per SESSION (all files sharing a
    stamp), by the newest mtime in that group — so a long-running service that
    is still writing its pulse keeps its rarely-written ``errors_<ts>.log``
    (deleting a file another live process holds open would silently send its
    future lines to an unlinked inode). Never touched: this process's own
    session, any file a handler here has open, and unstamped files
    (``debug.log``, ``pycron_cron.log``, ... rotate in place under their own
    bounds).

    Args:
        log_dir: directory to prune (default ``MMR_LOG_DIR``).
        max_age_days: retention in days (default ``log_retention_days()``);
            0 disables pruning.
        now: epoch seconds to measure age from (tests).
        logger: where to report (default this module's logger).

    Returns:
        The deleted paths. A file that cannot be deleted is reported at WARNING
        and skipped — log housekeeping must not take a service down.
    """
    log = logger or logging.getLogger(__name__)
    directory = os.path.abspath(log_dir or MMR_LOG_DIR)
    days = log_retention_days() if max_age_days is None else max_age_days
    if days < 0:
        raise ValueError(f'max_age_days must be >= 0, got {days!r}')
    if days == 0 or not os.path.isdir(directory):
        return []
    cutoff = (time.time() if now is None else now) - days * 86400.0
    protected = _open_log_paths()

    sessions: Dict[str, List[tuple]] = {}
    with os.scandir(directory) as entries:
        for entry in entries:
            match = _STAMPED_LOG.match(entry.name)
            if not match or not entry.is_file(follow_symlinks=False):
                continue
            try:
                mtime = entry.stat(follow_symlinks=False).st_mtime
            except FileNotFoundError:
                continue
            sessions.setdefault(match.group('stamp'), []).append((entry.path, mtime))

    deleted: List[str] = []
    failed = 0
    freed = 0
    for stamp, files in sessions.items():
        if stamp == _session_timestamp:
            continue
        if max(mtime for _, mtime in files) >= cutoff:
            continue
        if any(os.path.abspath(path) in protected for path, _ in files):
            continue
        for path, _ in files:
            try:
                size = os.path.getsize(path)
                os.remove(path)
            except FileNotFoundError:
                continue
            except OSError as e:
                failed += 1
                log.warning('log retention: could not delete %s: %s', path, e)
                continue
            deleted.append(path)
            freed += size
    if deleted or failed:
        log.info('log retention: deleted %d session log file(s) (%.1f MB) idle > %d days in %s%s',
                 len(deleted), freed / 1e6, days, directory,
                 f'; {failed} could not be deleted' if failed else '')
    return deleted


class LogLevels(IntEnum):
    CRITICAL = 50
    FATAL = CRITICAL
    ERROR = 40
    WARNING = 30
    WARN = WARNING
    INFO = 20
    DEBUG = 10
    TRACE = 5
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
                _dedupe_propagated_handlers(config)
                _attach_service_role_filters(config)
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
