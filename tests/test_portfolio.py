import pytest
from ib_async.contract import Contract
from ib_async.objects import PortfolioItem, Position

from trader.trading.portfolio import Portfolio


def _make_contract(conid=4391, symbol="AMD"):
    c = Contract()
    c.conId = conid
    c.symbol = symbol
    return c


def _make_position(account="TEST123", conid=4391, symbol="AMD", position=100.0, avg_cost=150.0):
    c = _make_contract(conid, symbol)
    return Position(account=account, contract=c, position=position, avgCost=avg_cost)


def _make_portfolio_item(account="TEST123", conid=4391, symbol="AMD"):
    c = _make_contract(conid, symbol)
    return PortfolioItem(
        account=account,
        contract=c,
        position=100.0,
        marketPrice=155.0,
        marketValue=15500.0,
        averageCost=150.0,
        unrealizedPNL=500.0,
        realizedPNL=0.0,
    )


class TestPortfolio:
    def test_add_retrieve_position(self):
        portfolio = Portfolio()
        pos = _make_position()
        portfolio.add_position(pos)
        positions = portfolio.get_positions()
        assert len(positions) == 1
        assert positions[0].position == 100.0

    def test_update_overwrites_position(self):
        portfolio = Portfolio()
        portfolio.add_position(_make_position(position=100.0))
        portfolio.add_position(_make_position(position=200.0))
        positions = portfolio.get_positions()
        assert len(positions) == 1
        assert positions[0].position == 200.0

    def test_account_filter_ignores_wrong_account(self):
        portfolio = Portfolio(ib_account="MINE")
        portfolio.add_position(_make_position(account="OTHER"))
        assert len(portfolio.get_positions()) == 0
        portfolio.add_position(_make_position(account="MINE"))
        assert len(portfolio.get_positions()) == 1

    def test_add_retrieve_portfolio_item(self):
        portfolio = Portfolio()
        item = _make_portfolio_item()
        portfolio.add_portfolio_item(item)
        items = portfolio.get_portfolio_items()
        assert len(items) == 1
        assert items[0].marketValue == 15500.0
