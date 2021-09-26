import sys
import os

# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import logging
import coloredlogs
from trader.common.logging_helper import setup_logging, suppress_external

setup_logging()
suppress_external()

import pandas as pd
import datetime as dt
import click
import random
import os

from typing import List, Dict, Tuple, Callable, Optional, Set, Generic, TypeVar, cast, Union, Iterator
from ib_insync import Stock, IB, Contract, Forex, BarData, Future, ContractDetails
from trader.listeners.ibrx import IBRx

def contract_dict(contract: Contract):
    return {
        'symbol': contract.symbol,
        'conId': int(contract.conId),
        'secType': contract.secType,
        'exchange': contract.exchange,
        'primaryExchange': contract.primaryExchange,
        'currency': contract.currency,
    }

def contractdetails_dict(details: ContractDetails):
    return {
        'symbol': details.contract.symbol,
        'conId': int(details.contract.conId),
        'secType': details.contract.secType,
        'exchange': details.contract.exchange,
        'primaryExchange': details.contract.primaryExchange,
        'currency': details.contract.currency,
        'marketName': details.marketName,
        'minTick': details.minTick,
        'orderTypes': details.orderTypes,
        'validExchanges': details.validExchanges,
        'priceMagnifier': details.priceMagnifier,
        'longName': details.longName,
        'industry': details.industry,
        'category': details.category,
        'subcategory': details.subcategory,
        'tradingHours': details.tradingHours,
        'timeZoneId': details.timeZoneId,
        'liquidHours': details.liquidHours,
        'stockType': details.stockType,
        'bondType': details.bondType,
        'couponType': details.couponType,
        'callable': details.callable,
        'putable': details.putable,
        'coupon': details.coupon,
        'convertable': details.convertible,
        'maturity': details.maturity,
        'issueDate': details.issueDate,
        'nextOptionDate': details.nextOptionDate,
        'nextOptionPartial': details.nextOptionPartial,
        'nextOptionType': details.nextOptionType,
        'marketRuleIds': details.marketRuleIds
    }



def resolve_symbol(ib: IBRx,
                   symbol: str,
                   sec_type: str = 'STK',
                   exchange: str = 'SMART',
                   primary_exchange: str = '',
                   currency: str = 'USD') -> Optional[ContractDetails]:
    contract = Contract(symbol=symbol,
                        secType=sec_type,
                        exchange=exchange,
                        primaryExchange=primary_exchange,
                        currency=currency)
    result = ib.get_contract_details(contract)
    if len(result) > 1:
        logging.info('found multiple results for {}:'.format(symbol))
        for d in result:
            logging.info('  {}'.format(d))
        return result[0]
    elif len(result) == 0:
        logging.info('no matching results found for {}'.format(symbol))
        return None
    else:
        c = result[0]
        logging.info('{} {} {}'.format(c.contract.conId, c.contract.symbol, c.longName))
        return result[0]


def resolve_symbols(ib: IBRx,
                    symbols: List[str],
                    sec_type: str = 'STK',
                    exchange: str = 'SMART',
                    primary_exchange: str = '',
                    currency: str = 'USD') -> Iterator[ContractDetails]:
    for symbol in symbols:
        result = resolve_symbol(ib, symbol, sec_type, exchange, primary_exchange, currency)
        if result:
            yield result


def resolve_contracts(ib: IBRx,
                      contracts: List[Contract]) -> Iterator[ContractDetails]:
    for contract in contracts:
        result = resolve_symbol(ib,
                                contract.symbol,
                                contract.secType,
                                contract.exchange,
                                contract.currency)
        if result:
            yield result


def fill_csv(ib: IBRx,
             full: bool,
             csv_file: str,
             csv_output_file: str,
             sec_type: str = 'STK',
             exchange: str = 'SMART',
             primary_exchange: str = 'NASDAQ',
             currency: str = 'USD'):
    def resolve(symbol: str) -> ContractDetails:
        contract_detail = resolve_symbol(ib, symbol, sec_type, exchange, primary_exchange, currency)
        if contract_detail:
            return contract_detail
        else:
            return ContractDetails()

    df = pd.read_csv(csv_file)
    if not csv_output_file:
        csv_output_file = csv_file

    if not full:
        df['conId'] = df['symbol'].apply(lambda x: resolve(x).contract.conId)
    else:
        list_contracts = []
        for index, row in df.iterrows():
            result = resolve(row.symbol)
            if result and result.contract:
                # d = contract_dict(result)
                d = contractdetails_dict(result)
                list_contracts.append(d)
        df = pd.merge(df, pd.DataFrame.from_dict(list_contracts), how='left', on='symbol')

    df['conId'] = df['conId'].fillna(-1)  # type: ignore
    df['conId'] = df['conId'].astype(int)  # type: ignore
    if csv_output_file:
        df.to_csv(csv_output_file, header=True, index=False)
    else:
        df.to_csv(csv_file, header=True, index=False)


@click.command()
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='127.0.0.1 ib server address')
@click.option('--ib_server_port', required=False, default=7496, help='port for tws server api: 7496')
@click.option('--symbol', required=False, help='symbol to resolve to conid')
@click.option('--full', required=False, is_flag=True, help='symbol to resolve to conid')
@click.option('--sectype', required=False, default='STK', help='security type')
@click.option('--exchange', required=False, default='SMART', help='exchange: SMART')
@click.option('--primary_exchange', required=False, default='NASDAQ', help='primary exchange: NASDAQ')
@click.option('--currency', required=False, default='USD', help='currency: USD')
@click.option('--csv_file', required=False, help='csv file with symbol as column header')
@click.option('--csv_output_file', required=False, help='output file for csv')
def main(ib_server_address: str,
         ib_server_port: int,
         symbol: str,
         full: bool,
         sectype: str,
         exchange: str,
         primary_exchange: str,
         currency: str,
         csv_file: str,
         csv_output_file: str):

    logging.getLogger('ib_insync.wrapper').setLevel(logging.CRITICAL)
    logging.getLogger('ibrx').setLevel(logging.ERROR)

    random_client_id = random.randint(10, 100)
    ib = IBRx()
    ib.connect(ib_server_address=ib_server_address, ib_server_port=ib_server_port, client_id=random_client_id)

    if symbol:
        result = resolve_symbol(ib, symbol, sectype, exchange, primary_exchange, currency)
        if result and result.contract and not full:
            print(result.contract.conId)
        elif result and full:
            print(result)
        else:
            print(-1)
    elif csv_file:
        fill_csv(ib, full, csv_file, csv_output_file, sectype, exchange, primary_exchange, currency)


if __name__ == '__main__':
    coloredlogs.install(level='INFO')
    main()
