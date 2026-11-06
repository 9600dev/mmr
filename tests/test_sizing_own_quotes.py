"""Our own resting limit order must not masquerade as the market's best bid/ask."""
from trader.trading.position_sizing import LiquidityInfo, discount_own_quotes


def test_own_buy_limit_as_bid_is_dropped():
    # Observed: resting BUY LMT 10 on F, NBBO ~12.39 -> snapshot bid=10.0
    bid, ask, notes = discount_own_quotes(10.0, 12.40, 12.39, [('BUY', 10.0)])
    assert bid == 0.0 and ask == 12.40 and notes
    assert LiquidityInfo(bid=bid, ask=ask, last=12.39).spread == 0.0   # no phantom spread penalty


def test_own_sell_limit_as_ask_is_dropped():
    bid, ask, notes = discount_own_quotes(12.38, 15.0, 12.39, [('SELL', 15.0)])
    assert ask == 0.0 and bid == 12.38 and notes


def test_genuine_wide_market_untouched():
    bid, ask, notes = discount_own_quotes(10.0, 12.40, 12.39, [('BUY', 9.5)])
    assert (bid, ask, notes) == (10.0, 12.40, [])


def test_own_order_at_touch_near_last_kept():
    # Our BUY joined at the real NBBO bid — it IS the market; keep it.
    bid, ask, notes = discount_own_quotes(12.38, 12.40, 12.39, [('BUY', 12.38)])
    assert (bid, ask, notes) == (12.38, 12.40, [])


def test_side_mismatch_and_no_orders():
    assert discount_own_quotes(10.0, 12.4, 12.39, [('SELL', 10.0)])[:2] == (10.0, 12.4)
    assert discount_own_quotes(10.0, 12.4, 12.39, [])[:2] == (10.0, 12.4)
    assert discount_own_quotes(10.0, 12.4, 0.0, [('BUY', 10.0)])[:2] == (10.0, 12.4)  # no last: can't judge
