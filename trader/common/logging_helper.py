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


MMR_LOG_DIR = os.path.expanduser('~/.local/share/mmr/logs')


def _stamp_log_filenames(config: dict) -> dict:
    """Expand ~ in log paths, ensure directory exists, and inject session timestamp."""
    for handler_config in config.get('handlers', {}).values():
        filename = handler_config.get('filename', '')
        if not filename:
            continue
        # Expand ~ to home directory
        filename = os.path.expanduser(filename)
        # Add session timestamp to log files in the mmr log directory
        if filename.endswith('.log') and MMR_LOG_DIR in filename:
            base = filename[:-4]
            filename = f'{base}_{_session_timestamp}.log'
        handler_config['filename'] = filename
    # Ensure log directory exists
    os.makedirs(MMR_LOG_DIR, exist_ok=True)
    return config


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
