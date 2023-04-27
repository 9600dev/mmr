from ib_insync.contract import Contract
from ib_insync.objects import PortfolioItem, Position
from trader.common.logging_helper import log_method, setup_logging


logging = setup_logging(module_name='portfolio')

from typing import Dict, List, Optional, Tuple


class Portfolio():
    def __init__(self, ib_account: Optional[str] = None):
        self.positions: Dict[Tuple[str, Contract], Position] = {}
        self.portfolio_items: Dict[Tuple[str, Contract], PortfolioItem] = {}
        self.ib_account = ib_account

    def add_position(self, position: Position) -> None:
        if self.ib_account and position.account != self.ib_account:
            return

        key = (position.account, position.contract)
        if key in self.positions:
            logging.debug('updating position {}'.format(position))

        self.positions[key] = position

    def get_positions(self) -> List[Position]:
        return list(self.positions.values())

    def add_portfolio_item(self, portfolio_item: PortfolioItem):
        if self.ib_account and portfolio_item.account != self.ib_account:
            return

        key = (portfolio_item.account, portfolio_item.contract)
        if key in self.portfolio_items:
            logging.debug('updating portfolio item {}'.format(portfolio_item))

        self.portfolio_items[key] = portfolio_item

    def get_portfolio_items(self) -> List[PortfolioItem]:
        return list(self.portfolio_items.values())
