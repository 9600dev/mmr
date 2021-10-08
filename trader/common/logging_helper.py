import os
import yaml
import logging.config
import logging
import coloredlogs
import warnings
from rich.logging import RichHandler
from logging import Logger
from typing import Dict

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
