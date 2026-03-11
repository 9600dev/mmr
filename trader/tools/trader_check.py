import warnings


from trader.common.logging_helper import setup_logging, suppress_all, verbose
from trader.container import Container
from trader.data.data_access import TickStorage
from trader.listeners.ibreactive import IBAIORx

import asyncio
import click


warnings.simplefilter(action='ignore', category=FutureWarning)

logging = setup_logging(module_name='trader_check')


def test_platform(ib_server_address: str,
                  ib_server_port: int,
                  ib_client_id: int,
                  duckdb_path: str) -> bool:
    succeeded = True
    try:
        with (IBAIORx(ib_server_address, ib_server_port, ib_client_id)) as ibrx:
            result = asyncio.run(ibrx.get_conid(['AMD']))
            if not result:
                raise Exception('cannot get AMD Contract details')
    except Exception as ex:
        logging.error('interactive brokers connection could not be made: {}'.format(ex))
        succeeded = False
    try:
        storage = TickStorage(duckdb_path)
    except Exception as ex:
        logging.error('data storage broken: {}'.format(ex))
        succeeded = False
    logging.info('check complete')
    return succeeded


def test_platform_config(config_file) -> bool:
    container = Container(config_file)
    ib_server_address = container.config()['ib_server_address']
    ib_server_port = container.config()['ib_server_port']
    ib_client_id = 100
    duckdb_path = container.config()['duckdb_path']

    result = test_platform(
        ib_server_address,
        ib_server_port,
        ib_client_id,
        duckdb_path,
    )
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
@click.option('--config', required=False, default='')
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='IB Gateway address')
@click.option('--ib_server_port', required=False, default=7496, help='IB Gateway API port')
@click.option('--ib_client_id', required=False, default=100, help='IB client id [default: 100]')
@click.option('--duckdb_path', required=False, default='data/mmr.duckdb', help='duckdb database path')
def main(config: str,
         ib_server_address: str,
         ib_server_port: int,
         ib_client_id: int,
         duckdb_path: str):

    result = False
    if config:
        result = test_platform_config(config)
    else:
        result = test_platform(
            ib_server_address,
            ib_server_port,
            ib_client_id,
            duckdb_path,
        )
    print(result)


if __name__ == '__main__':
    main()
