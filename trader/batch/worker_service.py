import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import datetime as dt
import ib_insync as ibapi
import click
import pandas as pd

from redis import Redis
from rq import Queue
from rq.job import Job
from rq import Connection, Worker
from dateutil.tz import tzlocal
from typing import Tuple, List, Optional, Dict

from trader.listeners.ib_history_worker import IBHistoryWorker
from trader.listeners.ibaiorx import IBAIORx
from trader.common.command_line import common_options, default_config, cli_norepl
from trader.data.data_access import TickData
from trader.common.logging_helper import setup_logging
from trader.common.helpers import date_range, dateify, symbol_to_contract, rich_dict, rich_list
from trader.common.listener_helpers import Helpers
from trader.data.universe import UniverseAccessor, Universe
from trader.batch.non_fork_worker import NonForkWorker
import nest_asyncio

nest_asyncio.apply()
logging = setup_logging(module_name='worker_service')

class WorkerService():
    def __init__(
        self,
        redis_server_address: str,
        redis_server_port: int,
        work_queue: str
    ):
        self.redis_server_address = redis_server_address
        self.redis_server_port = redis_server_port
        self.work_queue = work_queue
        self.redis_conn: Optional[Redis] = None

    def _connect(self):
        self.redis_conn = Redis(host=self.redis_server_address, port=self.redis_server_port)

    def start(self):
        if not self.redis_conn: self._connect()
        queue = Queue(connection=self.redis_conn, name=self.work_queue)

        with Connection():
            w = NonForkWorker([queue], connection=self.redis_conn)
            w.work()

    def list_queues(self) -> Dict[str, int]:
        if not self.redis_conn: self._connect()
        return {q.name: q.count for q in Queue.all(connection=self.redis_conn)}


@cli_norepl.command()
@click.option('--queue_name', required=True, help='name of RQ job queue to use, eg: ib_history_queue')
@common_options()
@default_config()
def start(
    redis_server_address: str,
    redis_server_port: int,
    queue_name: str,
    **args
):
    service = WorkerService(redis_server_address, redis_server_port, queue_name)
    service.start()

@cli_norepl.command()
@common_options()
@default_config()
def list(
    redis_server_address: str,
    redis_server_port: int,
    **args,
):
    service = WorkerService(redis_server_address, redis_server_port, '')
    rich_dict(service.list_queues())


if __name__ == '__main__':
    cli_norepl(obj={})
