import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from redis import Redis
from rq import Connection, Queue
from trader.batch.non_fork_worker import NonForkWorker
from trader.cli.command_line import cli_norepl, common_options, default_config
from trader.common.helpers import rich_dict
from trader.common.logging_helper import setup_logging
from typing import Dict, Optional

import click
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
        logging.debug('starting worker_service')
        if not self.redis_conn: self._connect()
        queue = Queue(connection=self.redis_conn, name=self.work_queue)

        with Connection():
            w = NonForkWorker([queue], connection=self.redis_conn)
            w.work()

    def list_queues(self) -> Dict[str, int]:
        logging.debug('list_queues')
        if not self.redis_conn: self._connect()
        return {q.name: q.count for q in Queue.all(connection=self.redis_conn)}


@cli_norepl.command()
@click.option('--queue_name', required=True, help='name of RQ job queue to use, eg: history')
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
