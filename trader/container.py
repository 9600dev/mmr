from trader.common.singleton import Singleton
from typing import Dict, Type

import inspect
import os
import yaml


class Container(metaclass=Singleton):
    def __init__(self, config_file: str = '/home/trader/mmr/configs/trader.yaml'):
        if config_file == '/home/trader/mmr/configs/trader.yaml' and os.getenv('TRADER_CONFIG'):
            self.config_file = str(os.getenv('TRADER_CONFIG'))  # type: ignore
        else:
            self.config_file = config_file

        if not os.path.exists(self.config_file):  # type: ignore
            raise ValueError('configuration_file is not found {} or TRADER_CONFIG set incorrectly'.format(config_file))

        with open(self.config_file, 'r') as conf_file:
            self.configuration: Dict = yaml.load(conf_file, Loader=yaml.FullLoader)
            self.type_instance_cache: Dict[Type, object] = {}

    def resolve(self, t: Type, **extra_args):
        args = {}
        for param in inspect.signature(t.__init__).parameters.values():
            if param == 'self':
                continue
            if extra_args and param.name in extra_args.keys():
                args[param.name] = extra_args[param.name]
            elif os.getenv(param.name.upper()):
                args[param.name] = os.getenv(param.name.upper())
            elif param.name in self.configuration and self.configuration[param.name]:
                args[param.name] = self.configuration[param.name]
        return t(**args)

    def config(self) -> Dict:
        return self.configuration

    def resolve_cache(self, t: Type, **extra_args):
        if t in self.type_instance_cache:
            return self.type_instance_cache[t]
        else:
            self.type_instance_cache[t] = self.resolve(t, extra_args=extra_args)
            return self.type_instance_cache[t]
