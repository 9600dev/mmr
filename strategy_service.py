from asyncio import AbstractEventLoop
from trader.common.logging_helper import LogLevels, set_all_log_level, setup_logging
from trader.container import Container
from trader.strategy.strategy_runtime import StrategyRuntime

import asyncio
import click
import os
import signal


logging = setup_logging(module_name='strategy_service')


@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--config', required=False, default='/home/trader/mmr/configs/trader.yaml',
              help='trader.yaml config file location')
def main(simulation: bool,
         config: str):

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
            # SystemExit's out of the current asyncio loop
            exit(0)

    if simulation:
        raise ValueError('simulation not implemented yet')

    try:
        if os.environ.get('TRADER_NODEBUG'):
            set_all_log_level(LogLevels.CRITICAL)
        else:
            loop.set_debug(enabled=True)

        loop.add_signal_handler(signal.SIGINT, stop_loop, loop)

        # run stuff here.
        container = Container(config)
        strategy_runtime = container.resolve(StrategyRuntime)

        # connect the runtime to all the services
        strategy_runtime.connect()

        # enter run loop
        asyncio.get_event_loop().create_task(strategy_runtime.run())
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        logging.debug('KeyboardInterrupt')
        stop_loop(loop)
        exit()
    except SystemExit:
        logging.debug('SystemExit')
        os._exit(0)


if __name__ == '__main__':
    main()
