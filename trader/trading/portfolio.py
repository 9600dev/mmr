import expression
import itertools
from expression import pipe
from expression.collections import seq, Seq
from ib_insync.objects import Position, PortfolioItem
from ib_insync.contract import Contract
from trader.common.logging_helper import setup_logging
from trader.common.helpers import ListHelper

logging = setup_logging(module_name='portfolio')

from typing import List, Dict, Tuple

class Portfolio():
    def __init__(self):
        self.positions: Dict[Tuple[str, Contract], Position] = {}
        self.portfolio_items: Dict[Tuple[str, Contract], PortfolioItem] = {}

    def add_position(self, position: Position) -> None:
        key = (position.account, position.contract)
        if key in self.positions:
            logging.debug('updating position {}'.format(position))

        self.positions[key] = position

    def get_positions(self) -> List[Position]:
        return list(self.positions.values())

    def add_portfolio_item(self, portfolio_item: PortfolioItem):
        key = (portfolio_item.account, portfolio_item.contract)
        if key in self.portfolio_items:
            logging.debug('updating portfolio item {}'.format(portfolio_item))

        self.portfolio_items[key] = portfolio_item

    def get_portfolio_items(self) -> List[PortfolioItem]:
        return list(self.portfolio_items.values())

