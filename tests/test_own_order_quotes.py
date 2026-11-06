"""IB quotes never present our own resting limit order as the market's bid/ask."""
import math
from types import SimpleNamespace
from unittest.mock import MagicMock

from trader.sdk import MMR


def _trade(sym, action, lmt, status='Submitted', oid=1):
    return [SimpleNamespace(contract=SimpleNamespace(symbol=sym, conId=9599491),
                            order=SimpleNamespace(action=action, lmtPrice=lmt, auxPrice=0.0, orderId=oid,
                                                  orderType='LMT', totalQuantity=1.0, tif='DAY', parentId=0),
                            orderStatus=SimpleNamespace(status=status, filled=0.0, remaining=1.0, avgFillPrice=0.0))]


def _mmr(trades, batch=None):
    m = object.__new__(MMR)
    rpc = MagicMock()
    rpc.get_trades.return_value = trades
    rpc.get_snapshots_batch.return_value = batch or []
    rpc.get_account_values.return_value = {}
    client = MagicMock()
    client.is_setup = True
    client.rpc.return_value = rpc
    m._client = client
    m._to_contract = lambda c: c
    return m, rpc


def test_own_bid_nulled_with_note():
    m, _ = _mmr({1: _trade('F', 'BUY', 1.0)})
    q = m._strip_own_quotes([{'symbol': 'F', 'bid': 1.0, 'bidSize': 100.0, 'ask': 12.57, 'last': 12.565}])[0]
    assert math.isnan(q['bid']) and q['ask'] == 12.57 and q['quote_notes']


def test_untouched_without_own_orders_or_on_rpc_failure():
    m, _ = _mmr({})
    q = m._strip_own_quotes([{'symbol': 'F', 'bid': 10.0, 'ask': 12.4, 'last': 12.39}])[0]
    assert q['bid'] == 10.0 and 'quote_notes' not in q
    m2, rpc2 = _mmr({})
    rpc2.get_trades.side_effect = RuntimeError('down')
    q2 = m2._strip_own_quotes([{'symbol': 'F', 'bid': 10.0, 'ask': 12.4, 'last': 12.39}])[0]
    assert q2['bid'] == 10.0


def test_filled_or_cancelled_orders_ignored():
    m, _ = _mmr({1: _trade('F', 'BUY', 1.0, status='Cancelled')})
    q = m._strip_own_quotes([{'symbol': 'F', 'bid': 1.0, 'ask': 12.57, 'last': 12.565}])[0]
    assert q['bid'] == 1.0


def test_orders_view_uses_live_quotes_and_hides_own_bid():
    m, rpc = _mmr({1: _trade('F', 'BUY', 1.0)},
                  batch=[{'symbol': 'F', 'bid': 1.0, 'ask': 12.57, 'last': 12.565}])
    df = m.orders()
    row = df.iloc[0]
    assert row['bid'] is None or (isinstance(row['bid'], float) and math.isnan(row['bid']))
    assert row['ask'] == 12.57
    assert rpc.get_snapshots_batch.call_args[0][1] is False      # live, not delayed
