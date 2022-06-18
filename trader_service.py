import click
import nest_asyncio
nest_asyncio.apply()
import asyncio

from trader.common.logging_helper import setup_logging
logging = setup_logging(module_name='trading_runtime')

from trader.container import Container
from trader.trading.trading_runtime import Trader
from trader.common.helpers import get_network_ip

@click.command()
@click.option('--simulation', required=False, default=False, help='load with historical data')
@click.option('--config', required=False, default='/home/trader/mmr/configs/trader.yaml',
              help='trader.yaml config file location')
def main(simulation: bool,
         config: str):
    # required for nested asyncio calls and avoids RuntimeError: This event loop is already running
    loop = asyncio.get_event_loop()

    if simulation:
        raise ValueError('not implemented yet')
        # ib_client = HistoricalIB(logger=logging)

    container = Container(config)
    trader = container.resolve(Trader, simulation=simulation)
    trader.connect()

    ip_address = get_network_ip()
    logging.debug('starting trading_runtime at network address: {}'.format(ip_address))

    # start the trader
    logging.debug('starting trader run() loop')
    trader.run()

    asyncio.get_event_loop().run_forever()


if __name__ == '__main__':
    main()
