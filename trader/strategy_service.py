from asyncio import AbstractEventLoop
from trader.common.logging_helper import LogLevels, set_all_log_level, setup_logging
from trader.container import Container, default_config_path
from trader.strategy.strategy_runtime import StrategyRuntime

import asyncio
import click
import os
import signal


logging = setup_logging(module_name='strategy_service')

MAX_RUNTIME_RESTARTS = 5
RESTART_DELAY_BASE = 5


@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--config', required=False, default='',
              help='trader.yaml config file location')
def main(simulation: bool,
         config: str):

    if not config:
        config = default_config_path()

    is_stopping = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def stop_loop(loop: AbstractEventLoop):
        nonlocal is_stopping
        if loop.is_running() and not is_stopping:
            is_stopping = True
            pending_tasks = [
                task for task in asyncio.all_tasks() if not task.done()
            ]

            if len(pending_tasks) > 0:
                for task in pending_tasks:
                    logging.debug(task.get_stack())

                logging.debug('waiting five seconds for {} pending tasks'.format(len(pending_tasks)))
                loop.run_until_complete(asyncio.wait(pending_tasks, timeout=5))
            loop.stop()
            exit(0)

    if simulation:
        raise ValueError('simulation not implemented yet')

    try:
        if os.environ.get('TRADER_NODEBUG'):
            set_all_log_level(LogLevels.CRITICAL)
        else:
            loop.set_debug(enabled=True)

        loop.add_signal_handler(signal.SIGINT, stop_loop, loop)

        container = Container.create(config)
        strategy_runtime = container.resolve(StrategyRuntime)

        strategy_runtime.connect()

        restart_count = 0

        def on_runtime_done(task: asyncio.Task):
            nonlocal restart_count, is_stopping
            if is_stopping:
                return
            ex = task.exception() if not task.cancelled() else None
            if ex is None:
                logging.info('strategy_runtime task completed normally')
                return

            restart_count += 1
            logging.error('strategy_runtime crashed (attempt {}/{}): {}'.format(
                restart_count, MAX_RUNTIME_RESTARTS, ex))

            if restart_count > MAX_RUNTIME_RESTARTS:
                logging.error('strategy_runtime exceeded max restarts ({}), stopping service'.format(
                    MAX_RUNTIME_RESTARTS))
                loop.call_soon(stop_loop, loop)
                return

            delay = min(RESTART_DELAY_BASE * restart_count, 60)
            logging.info('restarting strategy_runtime in {}s'.format(delay))
            loop.call_later(delay, _start_runtime)

        def _start_runtime():
            task = loop.create_task(strategy_runtime.run())
            task.add_done_callback(on_runtime_done)

        _start_runtime()
        loop.run_forever()
    except KeyboardInterrupt:
        logging.debug('KeyboardInterrupt')
        stop_loop(loop)
        exit()
    except SystemExit:
        logging.debug('SystemExit')
        os._exit(0)


if __name__ == '__main__':
    main()
