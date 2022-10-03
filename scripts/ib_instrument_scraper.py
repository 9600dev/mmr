from bs4 import BeautifulSoup
from ib_insync import Contract
from typing import Callable, List, Tuple

import requests


class IBInstrument():
    def __init__(
        self,
        conid: int,
        ib_symbol: str,
        symbol: str,
        name: str,
        currency: str,
        exchange: str,
        exchange_full_name: str,
        secType: str,
        instrument_type: str,
    ):
        self.conId = conid
        self.symbol = symbol
        self.ib_symbol = ib_symbol
        self.name = name
        self.currency = currency
        self.exchange = exchange
        self.exchange_full_name = exchange_full_name
        self.secType = secType
        self.instrument_type = instrument_type

    def __str__(self):
        return '{} {} {} {} {}: {}'.format(
            self.conId,
            self.ib_symbol,
            self.currency,
            self.exchange,
            self.instrument_type,
            self.name
        )

    def __repr__(self):
        return self.__str__()

    def to_contract(self) -> Contract:
        if self.conId > 0:
            return Contract(
                secType=self.secType,
                conId=self.conId,
            )
        else:
            return Contract(
                symbol=self.ib_symbol,
                secType=self.secType,
                primaryExchange=self.exchange,
                currency=self.currency,
                exchange=self.exchange,
            )


def __readtoken(s: str, character_check: Callable[[str], bool], token: str = '') -> str:
    if token not in s:
        return ''

    right = s[s.index(token) + len(token):]
    acc = ''
    for c in right:
        if character_check(c):
            acc += c
        else:
            break
    return acc


def __readint(s: str, token: str = ''):
    result = __readtoken(s, str.isnumeric, token)
    if result:
        return int(result)
    else:
        return -1


def __readstr(s: str, token: str = ''):
    return __readtoken(s, str.isalpha, token)


# https://www.interactivebrokers.com/en/index.php?f=1563&p=stk


def scrape_product_page(exchange: str, url: str) -> Tuple[List[IBInstrument], str]:
    content = requests.get(url).content
    soup = BeautifulSoup(content, features='lxml')
    trs = soup.find_all(name='tr')
    instruments: List[IBInstrument] = []

    exchange_full_name = soup.h2.text.strip()
    instrument_type = ''
    instrument_type_node = soup.find('div', {'class': 'btn-selectors'})
    if instrument_type_node:
        instrument_type = instrument_type_node.find('a', {'class': 'active'}).text.strip()

    sec_type = __readstr(url, 'showcategories=')

    if 'ETF' in sec_type:
        sec_type = 'STK'

    if 'OPTGRP' in sec_type:
        sec_type = 'OPT'

    if 'FUTGRP' in sec_type:
        sec_type = 'FUT'

    # we seem to get pacing violations pretty easily for warrants -- potentially
    # ignore them?

    for tr in trs:
        ahref = tr.find('a', {'class': 'linkexternal'})
        if hasattr(ahref, 'href') and 'conid' in ahref['href']:
            ib_symbol = tr.td.text.strip()
            conid = __readint(ahref['href'], 'conid=')
            name = ahref.text.strip()
            currency = tr.find_all('td')[-1].text.strip()
            symbol = tr.find_all('td')[-2].text.strip()
            instruments.append(IBInstrument(
                conid=conid,
                ib_symbol=ib_symbol,
                symbol=symbol,
                name=name,
                currency=currency,
                exchange=exchange,
                exchange_full_name=exchange_full_name,
                secType=sec_type,
                instrument_type=instrument_type,
            ))

    # check to see if there is a next page, otherwise, return
    next_page = soup.find('ul', {'class': 'pagination'})
    if next_page:
        next_page = next_page.find('li', {'class': 'active'}).next_sibling.next_sibling
        if next_page.has_attr('class') and len(next_page['class']) >= 1 and next_page['class'][0] == 'disabled':
            # now we have to see if there are extra classes of instrument to search before returning empty string
            instrument_class_node = soup.find('div', {'class': 'btn-selectors'})

            if instrument_class_node:
                instrument_class = instrument_class_node.find('a', {'class': 'active'})
            else:
                return instruments, ''

            if instrument_class.parent.next_sibling.next_sibling:
                return instruments, instrument_class.parent.next_sibling.next_sibling.a['href']
            else:
                return instruments, ''
        else:
            return instruments, next_page.a['href']
    else:
        instrument_class = soup.find('div', {'class': 'btn-selectors'})
        if instrument_class:
            instrument_class = instrument_class.find('a', {'class': 'active'})
            if instrument_class.parent.next_sibling.next_sibling:
                return instruments, instrument_class.parent.next_sibling.next_sibling.a['href']
            else:
                return instruments, ''

def scrape_products(exchange: str, exchange_url: str) -> List[IBInstrument]:
    instruments: List[IBInstrument] = []

    instruments, next_page = scrape_product_page(exchange, exchange_url)

    while next_page:
        url = 'https://www.interactivebrokers.com' + next_page
        print('{} total: {}, scraping: {}'.format(exchange, len(instruments), url))
        temp_instruments, next_page = scrape_product_page(exchange, url)
        instruments = instruments + (temp_instruments)

    return instruments


