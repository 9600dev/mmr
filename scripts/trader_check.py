import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import pandas as pd
import datetime as dt
import click
import random
import os
import asyncio
import time

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union
from ib_insync import Stock, IB, Contract, Forex, BarData, Future, contract

from trader.listeners.ibrx import IBRx
from trader.container import Container
from trader.data.data_access import TickData
from redis import Redis
from rq import Queue

from trader.common.logging_helper import setup_logging, suppress_all, verbose
logging = setup_logging(module_name='trader_check')


def test_platform(ib_server_address: str,
                  ib_server_port: int,
                  arctic_server_address: str,
                  arctic_library: str,
                  redis_server_address: str,
                  redis_server_port: int) -> bool:
    succeeded = True
    try:
        ib_client = IB()
        ib_client.connect(ib_server_address, ib_server_port, clientId=101)
        ibrx = IBRx(ib_client)
        result = ibrx.get_conid_sync(['AMD'])
        if not result:
            raise Exception('cannot get AMD Contract details')
        ib_client.disconnect()
    except Exception as ex:
        logging.error('interactive brokers connection could not be made: {}'.format(ex))
        succeeded = False
    try:
        data = TickData(arctic_server_address, arctic_library)
    except Exception as ex:
        logging.error('arctic database broken: {}'.format(ex))
        succeeded = False
    try:
        redis_conn = Redis(host=redis_server_address, port=redis_server_port)
        queue = Queue(connection=redis_conn)
        len(queue.jobs)
    except Exception as ex:
        logging.error('redis/rq broken: {}'.format(ex))
        succeeded = False
    logging.info('check complete')
    return succeeded


def test_platform_config(config_file) -> bool:
    container = Container(config_file)
    ib_server_address = container.config()['ib_server_address']
    ib_server_port = container.config()['ib_server_port']
    arctic_server_address = container.config()['arctic_server_address']
    arctic_library = container.config()['arctic_library']
    redis_server_address = container.config()['redis_server_address']
    redis_server_port = container.config()['redis_server_port']

    result = test_platform(ib_server_address, ib_server_port, arctic_server_address,
                           arctic_library, redis_server_address, redis_server_port)
    return result


async def test_platform_config_async(config_file) -> bool:
    return test_platform_config(config_file)


def health_check(config_file) -> bool:
    suppress_all()
    result = test_platform_config(config_file)
    if not result:
        verbose()
        test_platform_config(config_file)
        return False
    else:
        return True


@click.command()
@click.option('--config', required=False, default='/home/trader/mmr/configs/trader.yaml')
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='tws trader api address')
@click.option('--ib_server_port', required=False, default=7496, help='port for tws server api')
@click.option('--arctic_server_address', required=False, default='127.0.0.1', help='arctic server ip address: 127.0.0.1')
@click.option('--arctic_library', required=False, default='Historical', help='tick store library name: Historical')
@click.option('--redis_server_address', required=False, default='127.0.0.1', help='redis server ip address: 127.0.0.1')
@click.option('--redis_server_port', required=False, default=6379, help='redis server port: 6379')
def main(config: str,
         ib_server_address: str,
         ib_server_port: int,
         arctic_server_address: str,
         arctic_library: str,
         redis_server_address: str,
         redis_server_port: int):

    result = False
    if config:
        result = test_platform_config(config)
    else:
        result = test_platform(ib_server_address, ib_server_port, arctic_server_address,
                               arctic_library, redis_server_address, redis_server_port)
    print(result)


if __name__ == '__main__':
    main()
