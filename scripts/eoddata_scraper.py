import requests
import string
import pandas as pd
import logging
import coloredlogs
import click
from bs4 import BeautifulSoup
from typing import List, Dict

class EodDataScraper():
    def scrape_page(self, exchange: str, url: str):
        def strip(s: str):
            return s.replace(',', '').strip()
        symbols = []
        logging.info('getting {}'.format(url))
        content = requests.get(url).content
        soup = BeautifulSoup(content, features='lxml')
        quotes = soup.find('table', {'class': 'quotes'})
        rows_ro = quotes.find_all('tr', {'class': 'ro'})
        rows_re = quotes.find_all('tr', {'class': 're'})
        list = rows_ro + rows_re
        for row in list:
            columns = row.find_all('td')
            symbol = {
                'symbol': columns[0].a.text,
                'name': columns[1].text,
                'exchange': exchange,
                'high': float(strip(columns[2].text)),
                'low': float(strip(columns[3].text)),
                'close': float(strip(columns[4].text)),
                'volume': float(strip(columns[5].text)),
                'change': float(strip(columns[6].text))
            }
            symbols.append(symbol)
        return symbols

    def get_symbol_page_list(self, soup: BeautifulSoup) -> List[str]:
        table = soup.find('table', {'class': 'lett'})
        chars: List[str] = ['A']
        for td in table.find_all('td', {'class': 'ld'}):
            chars.append(td.a.text)
        return chars

    def scrape(self, exchange: str):
        symbols: List[Dict] = []
        soup = BeautifulSoup(requests.get('http://eoddata.com/stocklist/{}/A.htm'.format(exchange)).content,
                             features='lxml')
        letters = self.get_symbol_page_list(soup)
        for char in letters:
            page = self.scrape_page(exchange, 'http://eoddata.com/stocklist/{}/{}.htm'.format(str(exchange), str(char)))
            symbols = symbols + page
        table = pd.DataFrame.from_dict(symbols)
        # add total volume
        table['total_traded'] = table.close * table.volume
        table = table.rename(columns={'name': 'company_name'})
        return table


@click.command()
@click.option('--exchange', required=True, default='NASDAQ', help='NASDAQ')
@click.option('--csv_output_file', required=True, help='csv output file')
def main(exchange: str, csv_output_file: str):
    coloredlogs.install(level='INFO')
    pd.set_option('display.float_format', str)
    eod = EodDataScraper()
    result = eod.scrape(exchange)

    if csv_output_file:
        result.to_csv(csv_output_file, header=True, index=False)


if __name__ == '__main__':
    main()
