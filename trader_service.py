import click
import nest_asyncio
import signal
nest_asyncio.apply()
import asyncio
import aioreactive as rx

from trader.common.logging_helper import setup_logging, log_callstack_debug
logging = setup_logging(module_name='trading_runtime')

from asyncio import iscoroutinefunction
from trader.container import Container
from trader.trading.trading_runtime import Trader
from trader.common.helpers import get_network_ip
from typing import TypeVar, Callable, Awaitable, Optional, Any

_TSource = TypeVar('_TSource')

async def anoop(value: Optional[Any] = None):  # type: ignore
    pass


def monkeypatch_asyncanonymousobserver(
        self,
        asend: Callable[[_TSource], Awaitable[None]] = anoop,
        athrow: Callable[[Exception], Awaitable[None]] = anoop,
        aclose: Callable[[], Awaitable[None]] = anoop
    ) -> None:
    log_callstack_debug(frames=0, module_filter='')
    assert iscoroutinefunction(asend)
    self._asend = asend

    assert iscoroutinefunction(athrow)
    self._athrow = athrow

    assert iscoroutinefunction(aclose)
    self._aclose = aclose


@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--config', required=False, default='/home/trader/mmr/configs/trader.yaml',
              help='trader.yaml config file location')
def main(simulation: bool,
         config: str):

    # useful to find out where rogue subscriptions that aren't disposed of are
    # rx.observers.AsyncAnonymousObserver.__init__ = monkeypatch_asyncanonymousobserver  # type: ignore

    container = Container(config)
    trader = container.resolve(Trader, simulation=simulation)

    def stop_loop(loop):
        if loop.is_running():
            loop.run_until_complete(trader.shutdown())
            pending_tasks = [
                task for task in asyncio.all_tasks() if not task.done()
            ]

            for task in pending_tasks:
                logging.debug(task.print_stack())

            logging.debug('waiting five seconds for pending tasks')
            asyncio.run(asyncio.wait(pending_tasks, timeout=5))
            loop.stop()

    # required for nested asyncio calls and avoids RuntimeError: This event loop is already running
    loop = asyncio.get_event_loop()

    if simulation:
        # ib_client = HistoricalIB(logger=logging)
        raise ValueError('simulation not implemented yet')

    try:
        loop.set_debug(enabled=True)
        # loop.add_signal_handler(signal.SIGINT, stop_loop, loop)

        trader.connect()

        ip_address = get_network_ip()
        logging.debug('starting trading_runtime at network address: {}'.format(ip_address))

        # start the trader
        logging.debug('starting trader run() loop')
        trader.run()

        asyncio.get_event_loop().run_forever()

    except KeyboardInterrupt:
        logging.info('KeyboardInterrupt')
        stop_loop(loop)
        exit()


if __name__ == '__main__':
    main()
