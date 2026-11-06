from asyncio import AbstractEventLoop
from trader.common.logging_helper import LogLevels, prune_old_logs, set_all_log_level, setup_logging
from trader.common.loop_health import configure_event_loop
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
@click.option('--debug', is_flag=True, default=False,
              help='enable asyncio debug mode (same as MMR_ASYNCIO_DEBUG=1)')
def main(simulation: bool,
         config: str,
         debug: bool = False):

    if not config:
        config = default_config_path()

    # Session log retention (MMR_LOG_RETENTION_DAYS, default 14; 0 = off).
    prune_old_logs(logger=logging)

    is_stopping = False
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def stop_loop(loop: AbstractEventLoop):
        nonlocal is_stopping
        if loop.is_running() and not is_stopping:
            is_stopping = True
            pending_tasks = [
                task for task in asyncio.all_tasks(loop) if not task.done()
            ]
            for task in pending_tasks:
                task.cancel()
            async def finish_shutdown():
                # Keep the loop alive to run runtime.finally, reap strategy
                # children, and release sockets before the process exits.
                if pending_tasks:
                    await asyncio.wait(pending_tasks, timeout=30)
                loop.stop()
            loop.create_task(finish_shutdown())

    if simulation:
        raise ValueError('simulation not implemented yet')

    try:
        # TRADER_NODEBUG is a LOG-LEVEL switch only (historical name). asyncio
        # debug mode is opt-in via --debug / MMR_ASYNCIO_DEBUG and is no
        # longer implied by its absence — see trader/common/loop_health.py.
        if os.environ.get('TRADER_NODEBUG'):
            set_all_log_level(LogLevels.CRITICAL)
        configure_event_loop(loop, logging, cli_debug=debug)

        loop.add_signal_handler(signal.SIGINT, stop_loop, loop)
        loop.add_signal_handler(signal.SIGTERM, stop_loop, loop)

        container = Container.create(config)
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
            async def run_generation():
                # A stopped executor/thread pool/socket set is not reusable.
                # Each supervised restart owns a fresh runtime generation.
                strategy_runtime = container.resolve(StrategyRuntime)
                await asyncio.to_thread(strategy_runtime.connect)
                await strategy_runtime.run()
            task = loop.create_task(run_generation())
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
