import expression
import pandas as pd
from expression import pipe
from expression.collections import seq, Seq
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from ib_insync.order import Trade
from trader.common.logging_helper import setup_logging
from trader.common.helpers import ListHelper

logging = setup_logging(module_name='book')

from typing import List, Dict, Tuple

class Book():
    def __init__(self):
        self.trades: Dict[Contract, Trade] = {}

    def add_update_trade(self, trade: Trade):
        logging.debug('updating book with {}'.format(trade))
        self.trades[trade.contract] = trade

    def get_book(self) -> List[Trade]:
        return list(self.trades.values())
