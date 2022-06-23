import os
import yaml
import logging.config
import logging
import coloredlogs
import warnings
import inspect
from rich.logging import RichHandler
from logging import Logger
from typing import Dict, cast, List
from types import FrameType

global_loggers: Dict[str, Logger] = {}

def setup_logging(default_path='/home/trader/mmr/configs/logging.yaml',
                  module_name='root',
                  default_level=logging.DEBUG,
                  env_key='LOG_CFG',
                  suppress_external_info=False):
    global global_loggers

    # ipython repl has a nasty habit of being polluted with debug crap from parso
    logging.getLogger('parso.python.diff').setLevel(logging.WARNING)
    logging.getLogger('parso').setLevel(logging.WARNING)
    logging.getLogger('parso.cache.pickle').setLevel(logging.WARNING)

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
                logging.config.dictConfig(config)
            except Exception as e:
                print(e)
                print('Error in Logging Configuration. Using default configs')
                logging.basicConfig(level=default_level)
                coloredlogs.install(level=default_level)
    else:
        logging.basicConfig(level=default_level)
        coloredlogs.install(level=default_level)
        print('Failed to load configuration file. Using default configs')

    global_loggers[module_name] = logging.getLogger(module_name)
    return global_loggers[module_name]

def set_external_log_level(level):
    logging.getLogger('ib_insync.wrapper').setLevel(level)
    logging.getLogger('ib_insync.client').setLevel(level)
    logging.getLogger('ib_insync.ib').setLevel(level)
    logging.getLogger('arctic.tickstore.tickstore').setLevel(level)
    logging.getLogger('arctic.arctic').setLevel(level)
    logging.getLogger('arctic.store.version_store').setLevel(level)

def set_all_log_level(level):
    set_external_log_level(level)
    logging.getLogger().setLevel(level)

def suppress_external():
    set_external_log_level(logging.ERROR)

def suppress_all():
    suppress_external()
    logging.getLogger().setLevel(logging.ERROR)

def verbose():
    set_all_log_level(logging.DEBUG)

def get_callstack(frames: int = 0):
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

def log_callstack_debug(frames: int = 0, module_filter: str = ''):
    callstack = get_callstack(frames)
    if callstack:
        if module_filter:
            callstack = [a for a in callstack if module_filter in a]
        result = ' <- '.join(callstack)
        logging.debug(result)
