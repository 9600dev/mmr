import os
import sys


# in order to get __main__ to work, we follow: https://stackoverflow.com/questions/16981921/relative-imports-in-python-3
PACKAGE_PARENT = '../'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from ib_insync.contract import Contract, ContractDetails
from trader.common.helpers import rich_dict
from trader.common.logging_helper import setup_logging, suppress_external
from trader.listeners.ibreactive import IBAIORx
from typing import AsyncIterator, List, Optional

import asyncio
import click
import coloredlogs
import logging
import os
import pandas as pd


setup_logging()
suppress_external()


class IBResolver():
    def __init__(
        self,
        client: IBAIORx
    ):
        self.client = client

    @staticmethod
    def contract_dict(contract: Contract):
        return {
            'symbol': contract.symbol,
            'conId': int(contract.conId),
            'secType': contract.secType,
            'exchange': contract.exchange,
            'primaryExchange': contract.primaryExchange,
            'currency': contract.currency,
        }

    @staticmethod
    def contractdetails_dict(details: ContractDetails):
        return {
            'symbol': details.contract.symbol if details.contract else '',
            'conId': int(details.contract.conId) if details.contract else -1,
            'secType': details.contract.secType if details.contract else '',
            'exchange': details.contract.exchange if details.contract else '',
            'primaryExchange': details.contract.primaryExchange if details.contract else '',
            'currency': details.contract.currency if details.contract else '',
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

    async def resolve_symbol(
        self,
        symbol: str,
        sec_type: str = 'STK',
        exchange: str = 'SMART',
        primary_exchange: str = '',
        currency: str = 'USD'
    ) -> Optional[ContractDetails]:
        contract = Contract(symbol=symbol,
                            secType=sec_type,
                            exchange=exchange,
                            primaryExchange=primary_exchange,
                            currency=currency)
        result = await self.client.get_contract_details(contract)
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

    async def resolve_symbols(
        self,
        symbols: List[str],
        sec_type: str = 'STK',
        exchange: str = 'SMART',
        primary_exchange: str = '',
        currency: str = 'USD'
    ) -> AsyncIterator[ContractDetails]:
        for symbol in symbols:
            result = await self.resolve_symbol(symbol, sec_type, exchange, primary_exchange, currency)
            if result:
                yield result

    async def resolve_contract(self, contract: Contract) -> Optional[ContractDetails]:
        result = await self.client.get_contract_details(contract)
        if len(result) > 1:
            logging.info('found multiple results for {}:'.format(contract))
            for d in result:
                logging.info('  {}'.format(d))
            return result[0]
        elif len(result) == 0:
            logging.info('no matching results found for {}'.format(contract))
            return None
        else:
            c = result[0]
            logging.info('{} {} {}'.format(c.contract.conId, c.contract.symbol, c.longName))
            return result[0]

        return result

    async def resolve_contracts(self, contracts: List[Contract]) -> AsyncIterator[ContractDetails]:
        for contract in contracts:
            result = await self.resolve_contract(contract)
            if result:
                yield result

    async def resolve_contracts_df(self, contracts_df: pd.DataFrame) -> pd.DataFrame:
        contract_details_list = []
        for row in contracts_df.itertuples():
            symbol = row['symbol']
            sec_type = row['secType'] if 'secType' in row else ''
            exchange = row['exchange'] if 'exchange' in row else ''
            primary_exchange = row['primaryExchange'] if 'primaryExchange' in row else ''
            currency = row['currency'] if 'currency' in row else ''
            contract_details = await self.resolve_symbol(symbol, sec_type, exchange, primary_exchange, currency)
            if contract_details:
                contract_details_list.append(self.contractdetails_dict(contract_details))
        return pd.DataFrame.from_records(contract_details_list)

    async def fill_csv(
        self,
        csv_file: str,
        csv_output_file: str,
        sec_type_default: str = 'STK',
        exchange_default: str = 'SMART',
        primary_exchange_default: str = 'NASDAQ',
        currency_default: str = 'USD'
    ) -> None:
        # overwrite current csv if no output is specified
        df = pd.read_csv(csv_file)

        list_contracts = []
        for index, row in df.iterrows():
            symbol = str(row['symbol'])
            sec_type = str(row['secType']) if 'secType' in row else sec_type_default
            exchange = str(row['exchange']) if 'exchange' in row else exchange_default
            primary_exchange = str(row['primaryExchange']) if 'primaryExchange' in row else primary_exchange_default
            currency = str(row['currency']) if 'currency' in row else currency_default

            result = await self.resolve_symbol(symbol, sec_type, exchange, primary_exchange, currency)
            if result and result.contract:
                # d = contract_dict(result)
                d = self.contractdetails_dict(result)
                list_contracts.append(d)

        # drop the columns we're looking for if they already exist, as we'll replace them
        df = df.drop(['conId', 'secType', 'exchange', 'primaryExchange', 'currency'], axis=1)
        # merge the contractdetails with the existing csv data
        df = pd.merge(df, pd.DataFrame.from_dict(list_contracts), how='left', on='symbol', suffixes=('', '_drop'))

        #Drop the duplicate columns
        df.drop([col for col in df.columns if 'drop' in col], axis=1, inplace=True)

        df['conId'] = df['conId'].fillna(-1)  # type: ignore
        df['conId'] = df['conId'].astype(int)  # type: ignore
        df.to_csv(csv_output_file, header=True, index=False)


@click.command()
@click.option('--ib_server_address', required=False, default='127.0.0.1', help='127.0.0.1 ib server address')
@click.option('--ib_server_port', required=False, default=7496, help='port for tws server api: 7496')
@click.option('--symbol', required=False, help='symbol to resolve to conid')
@click.option('--sectype', required=False, default='STK', help='security type')
@click.option('--exchange', required=False, default='SMART', help='exchange: SMART')
@click.option('--primary_exchange', required=False, default='NASDAQ', help='primary exchange: NASDAQ')
@click.option('--currency', required=False, default='USD', help='currency: USD')
@click.option('--csv_file', required=False, help='csv file with symbol as column header')
@click.option('--csv_output_file', required=False, help='output file for csv')
@click.option('--just_conid', required=False, is_flag=True, help='symbol to resolve to conid')
def main(ib_server_address: str,
         ib_server_port: int,
         symbol: str,
         sectype: str,
         exchange: str,
         primary_exchange: str,
         currency: str,
         csv_file: str,
         csv_output_file: str,
         just_conid: bool):

    logging.getLogger('ib_insync.wrapper').setLevel(logging.CRITICAL)
    logging.getLogger('ibrx').setLevel(logging.ERROR)

    client = IBAIORx(ib_server_address, ib_server_port)
    client.connect()
    resolver = IBResolver(client)

    if symbol:
        result = asyncio.run(resolver.resolve_symbol(symbol, sectype, exchange, primary_exchange, currency))
        if result and result.contract and just_conid:
            print(result.contract.conId)
        elif result and not just_conid:
            rich_dict(IBResolver.contractdetails_dict(result))
        else:
            print(-1)
    elif csv_file:
        if not csv_output_file:
            csv_output_file = csv_file

        asyncio.run(resolver.fill_csv(csv_file, csv_output_file, sectype, exchange, primary_exchange, currency))
        print('done')

if __name__ == '__main__':
    coloredlogs.install(level='INFO')
    main()
