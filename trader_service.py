from asyncio import AbstractEventLoop
from trader.common.helpers import get_network_ip
from trader.common.logging_helper import set_all_log_level, setup_logging
from trader.container import Container
from trader.trading.trading_runtime import Trader

import asyncio
import click
import logging as log
import os
import signal


logging = setup_logging(module_name='trading_runtime')


@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--config', required=False, default='/home/trader/mmr/configs/trader.yaml',
              help='trader.yaml config file location')
def main(simulation: bool,
         config: str):

    is_stopping = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    container = Container(config)
    trader = container.resolve(Trader, simulation=simulation)

    def stop_loop(loop: AbstractEventLoop):
        nonlocal is_stopping
        if loop.is_running() and not is_stopping:
            is_stopping = True
            loop.run_until_complete(trader.shutdown())
            pending_tasks = [
                task for task in asyncio.all_tasks() if not task.done()
            ]

            if len(pending_tasks) > 0:
                for task in pending_tasks:
                    logging.debug(task.get_stack())

                logging.debug('waiting five seconds for {} pending tasks'.format(len(pending_tasks)))
                loop.run_until_complete(asyncio.wait(pending_tasks, timeout=5))
            loop.stop()

    if simulation:
        # ib_client = HistoricalIB(logger=logging)
        raise ValueError('simulation not implemented yet')

    try:
        if os.environ.get('TRADER_NODEBUG'):
            set_all_log_level(log.CRITICAL)
        else:
            loop.set_debug(enabled=True)

        loop.add_signal_handler(signal.SIGINT, stop_loop, loop)

        trader.connect()

        ip_address = get_network_ip()
        logging.debug('starting trading_runtime at network address: {}'.format(ip_address))

        # start the trader
        logging.debug('starting trader run() loop')
        trader.run()

    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt')
        stop_loop(loop)
        exit()


if __name__ == '__main__':
    main()
