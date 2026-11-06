from asyncio import AbstractEventLoop
from trader.common.helpers import get_network_ip
from trader.common.logging_helper import LogLevels, prune_old_logs, set_all_log_level, setup_logging
from trader.common.loop_health import configure_event_loop
from trader.container import Container, default_config_path
from trader.trading.trading_runtime import Trader

import asyncio
import click
import logging as log
import os
import signal


logging = setup_logging(module_name='trader_service')


@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--debug', is_flag=True, default=False,
              help='enable verbose ib_async debug logging AND asyncio debug mode '
                   '(asyncio debug alone: MMR_ASYNCIO_DEBUG=1)')
@click.option('--config', required=False, default='',
              help='trader.yaml config file location')
def main(simulation: bool,
         debug: bool,
         config: str):

    if not config:
        config = default_config_path()

    # Session log retention (MMR_LOG_RETENTION_DAYS, default 14; 0 = off).
    prune_old_logs(logger=logging)

    is_stopping = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    container = Container.create(config)
    trader = container.resolve(Trader, simulation=simulation)

    async def graceful_shutdown():
        nonlocal is_stopping
        if is_stopping:
            return
        is_stopping = True
        logging.info('shutting down...')
        try:
            await trader.shutdown()
        except Exception as ex:
            logging.error('error during shutdown: {}'.format(ex))

        pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        if pending:
            logging.debug('waiting five seconds for {} pending tasks'.format(len(pending)))
            await asyncio.wait(pending, timeout=5)
            # Cancel anything still running
            still_pending = [t for t in pending if not t.done()]
            for t in still_pending:
                t.cancel()
            if still_pending:
                # Give cancelled tasks a moment to handle CancelledError
                await asyncio.wait(still_pending, timeout=2)
        loop.stop()

    def handle_sigint():
        asyncio.ensure_future(graceful_shutdown())

    if simulation:
        logging.info('simulation mode: use the backtest CLI command instead')

    try:
        # TRADER_NODEBUG is a LOG-LEVEL switch only (historical name). asyncio
        # debug mode is opt-in via --debug / MMR_ASYNCIO_DEBUG and is no
        # longer implied by its absence — see trader/common/loop_health.py.
        if os.environ.get('TRADER_NODEBUG'):
            set_all_log_level(LogLevels.CRITICAL)
        configure_event_loop(loop, logging, cli_debug=debug)

        if debug:
            from trader.common.logging_helper import set_external_log_level
            set_external_log_level(LogLevels.DEBUG)
            logging.info('verbose ib_async debug logging enabled')

        loop.add_signal_handler(signal.SIGINT, handle_sigint)
        loop.add_signal_handler(signal.SIGTERM, handle_sigint)

        trader.connect()

        ip_address = get_network_ip()
        logging.debug('starting trading_runtime at network address: {}'.format(ip_address))

        logging.debug('starting trader run() loop')
        trader.run()

    except KeyboardInterrupt:
        pass
    except SystemExit:
        pass


if __name__ == '__main__':
    main()
