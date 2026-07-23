import datetime as dt
import pytest
import pandas as pd
import numpy as np

from trader.messaging.clientserver import pack, unpack


class TestSerialization:
    def test_primitives(self):
        for val in [42, 3.14, "hello", True, None, [1, 2, 3], {"a": 1}]:
            assert unpack(pack(val)) == val

    def test_datetime(self):
        now = dt.datetime.now(tz=dt.timezone.utc)
        result = unpack(pack(now))
        assert isinstance(result, dt.datetime)
        assert abs((result - now).total_seconds()) < 0.01

    def test_date(self):
        d = dt.date(2024, 6, 15)
        result = unpack(pack(d))
        assert result == d

    def test_time(self):
        t = dt.time(14, 30, 45, 123456)
        result = unpack(pack(t))
        assert result == t

    def test_timedelta(self):
        td = dt.timedelta(days=3, seconds=3600)
        result = unpack(pack(td))
        assert abs(result.total_seconds() - td.total_seconds()) < 0.01

    def test_dataframe(self):
        df = pd.DataFrame({
            "a": [1, 2, 3],
            "b": [4.0, 5.0, 6.0],
            "c": ["x", "y", "z"],
        })
        result = unpack(pack(df))
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["a", "b", "c"]
        assert len(result) == 3
        assert result["a"].tolist() == [1, 2, 3]

    def test_nested_structures(self):
        data = {
            "date": dt.date(2024, 1, 1),
            "values": [1, 2, 3],
            "nested": {"key": "val"},
        }
        result = unpack(pack(data))
        assert result["date"] == dt.date(2024, 1, 1)

    def test_arbitrary_local_class_now_refused(self):
        # A locally-defined class pickles BY VALUE (dill embeds the class
        # definition via code-construction globals). That path is exactly the
        # code-execution surface the deny-by-default boundary closes, so it is
        # now refused rather than reconstructed. Real wire types are all
        # module-level in allowlisted packages and round-trip fine (see
        # TestDomainTypeRoundTrip).
        from trader.messaging.clientserver import DillDeserializationError

        class Foo:
            def __init__(self, x):
                self.x = x
        with pytest.raises(DillDeserializationError):
            unpack(pack(Foo(42)))


class TestDomainTypeRoundTrip:
    """Every object the live system actually ships over ZMQ must survive the
    pack → wire → restricted-unpickler → unpack round trip with semantics
    intact. These are the payloads the deny-by-default boundary must NOT
    reject; the allowlist prefixes are derived from exactly these types.

    Ticker ticks travel on PubSub, SuccessFail[Trade]/Contract/Order on RPC,
    Signal/StrategyConfig on the MessageBus.
    """

    def _make_contract(self):
        from ib_async import Stock
        c = Stock('AAPL', 'SMART', 'USD')
        c.conId = 265598
        return c

    def _make_trade(self, now):
        from ib_async import (CommissionReport, Execution, Fill, LimitOrder,
                              OrderStatus, Trade, TradeLogEntry)
        contract = self._make_contract()
        order = LimitOrder('BUY', 100, 100.0)
        order.orderRef = 'orb_strategy'
        status = OrderStatus(orderId=1, status='Filled', filled=100,
                             remaining=0, avgFillPrice=100.0)
        fill = Fill(
            contract=contract,
            execution=Execution(execId='e1', time=now, side='BOT',
                                shares=100, price=100.0),
            commissionReport=CommissionReport(commission=1.0, currency='USD'),
            time=now,
        )
        return Trade(contract=contract, order=order, orderStatus=status,
                     fills=[fill],
                     log=[TradeLogEntry(time=now, status='Filled', message='ok')])

    def test_ib_contract(self):
        c = self._make_contract()
        out = unpack(pack(c))
        assert out.conId == 265598
        assert out.symbol == 'AAPL'
        assert out.exchange == 'SMART'

    def test_ib_forex_contract(self):
        from ib_async import Forex
        fx = Forex('EURUSD')
        out = unpack(pack(fx))
        assert out.symbol == 'EUR'
        assert out.currency == 'USD'

    def test_ib_order_nested(self):
        from ib_async import LimitOrder
        order = LimitOrder('BUY', 250, 42.5)
        order.orderRef = 'strat_x'
        out = unpack(pack(order))
        assert out.action == 'BUY'
        assert out.totalQuantity == 250
        assert out.lmtPrice == 42.5
        assert out.orderRef == 'strat_x'

    def _make_ticker(self, now):
        from ib_async import Ticker
        from ib_async.ticker import TickData
        contract = self._make_contract()
        # ib_async sets quote fields post-construction (from tick events), not
        # via __init__ kwargs — mirror that so the fields actually populate.
        ticker = Ticker(contract=contract, time=now)
        ticker.bid, ticker.ask, ticker.last = 100.0, 100.5, 100.2
        ticker.bidSize, ticker.askSize, ticker.lastSize = 10, 12, 5
        ticker.volume = 1_000_000
        ticker.ticks = [TickData(time=now, tickType=4, price=100.2, size=5)]
        return ticker

    def test_ib_ticker_with_ticks(self):
        from ib_async import Ticker
        now = dt.datetime.now(dt.timezone.utc)
        out = unpack(pack(self._make_ticker(now)))
        assert isinstance(out, Ticker)
        assert out.bid == 100.0
        assert out.ask == 100.5
        assert out.contract.conId == 265598
        assert out.ticks[0].price == 100.2

    def test_ib_trade_nested(self):
        from ib_async import Trade
        now = dt.datetime.now(dt.timezone.utc)
        trade = self._make_trade(now)
        out = unpack(pack(trade))
        assert isinstance(out, Trade)
        assert out.order.action == 'BUY'
        assert out.orderStatus.status == 'Filled'
        assert out.fills[0].execution.shares == 100
        assert out.fills[0].commissionReport.commission == 1.0

    def test_successfail_plain(self):
        from trader.common.reactivex import SuccessFail
        sf = SuccessFail.success(obj={'ok': 1})
        out = unpack(pack(sf))
        assert out.is_success()
        assert out.obj == {'ok': 1}

    def test_successfail_of_trade(self):
        from trader.common.reactivex import SuccessFail
        now = dt.datetime.now(dt.timezone.utc)
        sf = SuccessFail.success(obj=self._make_trade(now))
        out = unpack(pack(sf))
        assert out.is_success()
        assert out.obj.order.totalQuantity == 100
        assert out.obj.fills[0].execution.price == 100.0

    def test_successfail_fail_with_exception(self):
        from trader.common.reactivex import SuccessFail, SuccessFailEnum
        sf = SuccessFail.fail(error='rejected', exception=ValueError('bad qty'))
        out = unpack(pack(sf))
        assert out.success_fail == SuccessFailEnum.FAIL
        assert out.error == 'rejected'
        assert isinstance(out.exception, ValueError)
        assert 'bad qty' in str(out.exception)

    def test_trader_signal(self):
        from zoneinfo import ZoneInfo
        from trader.trading.strategy import Signal
        from trader.objects import Action
        now = dt.datetime.now(dt.timezone.utc)
        sig = Signal(source_name='orb', action=Action.BUY, probability=0.8,
                     risk=0.1, conid=265598, quantity=10.0,
                     date_time=dt.datetime.now(ZoneInfo('America/New_York')),
                     metadata={'bar_time': now, 'note': 'breakout'},
                     max_hold_bars=20, close_by_time=dt.time(15, 45))
        out = unpack(pack(sig))
        assert out.source_name == 'orb'
        assert out.action == Action.BUY
        assert out.conid == 265598
        assert out.max_hold_bars == 20
        assert out.close_by_time == dt.time(15, 45)
        assert out.metadata['note'] == 'breakout'

    def test_strategy_config(self):
        from trader.trading.strategy import StrategyConfig, StrategyState
        from trader.objects import BarSize
        cfg = StrategyConfig(name='orb', state=StrategyState.RUNNING,
                             bar_size=BarSize.Mins1, conids=[265598],
                             module='strategies.orb', class_name='ORB',
                             historical_days_prior=5, paper_only=True,
                             auto_execute=True, params={'RANGE_MINUTES': 30})
        out = unpack(pack(cfg))
        assert out.name == 'orb'
        assert out.state == StrategyState.RUNNING
        assert out.conids == [265598]
        assert out.auto_execute is True
        assert out.params == {'RANGE_MINUTES': 30}

    def test_named_tuple_over_rpc_flattens_then_reconstructs(self):
        # NamedTuples (PortfolioItem, Position, ...) are tuple subclasses, so
        # msgpack flattens them to plain arrays — they never hit dill.
        import typing

        class _Row(typing.NamedTuple):
            conid: int
            price: float

        wire = unpack(pack(_Row(101, 1.5)))
        assert wire == [101, 1.5]  # arrives as a plain list, reconstructed by return_type

    def test_action_enum(self):
        from trader.objects import Action
        assert unpack(pack(Action.SELL)) == Action.SELL

    def test_barsize_enum(self):
        from trader.objects import BarSize
        # BarSize is an IntEnum; msgpack packs it as a plain int (never dill),
        # but the value must round-trip equal.
        assert unpack(pack(BarSize.Mins1)) == BarSize.Mins1

    def test_datetime_pytz(self):
        import pytz
        d = dt.datetime.now(pytz.timezone('Australia/Sydney'))
        out = unpack(pack(d))
        assert abs((out - d).total_seconds()) < 0.01

    def test_datetime_dateutil(self):
        from dateutil.tz import gettz
        d = dt.datetime.now(gettz('America/New_York'))
        out = unpack(pack(d))
        assert abs((out - d).total_seconds()) < 0.01

    def test_datetime_zoneinfo(self):
        from zoneinfo import ZoneInfo
        d = dt.datetime.now(ZoneInfo('America/New_York'))
        out = unpack(pack(d))
        assert abs((out - d).total_seconds()) < 0.01

    def test_numpy_scalar(self):
        out = unpack(pack(np.float64(1.5)))
        assert float(out) == 1.5

    def test_numpy_array(self):
        out = unpack(pack(np.array([1.0, 2.0, 3.0])))
        assert list(out) == [1.0, 2.0, 3.0]

    def test_pandas_timestamp(self):
        now = dt.datetime.now(dt.timezone.utc)
        out = unpack(pack(pd.Timestamp(now)))
        assert abs((out - now).total_seconds()) < 0.01

    def test_reconstructed_exception(self):
        out = unpack(pack(TimeoutError('slow')))
        assert isinstance(out, TimeoutError)
        assert 'slow' in str(out)

    def test_nested_container_of_domain_types(self):
        from trader.objects import Action
        now = dt.datetime.now(dt.timezone.utc)
        payload = {'action': Action.BUY, 'ticker': self._make_ticker(now),
                   'stamps': [dt.date(2026, 1, 1), None]}
        out = unpack(pack(payload))
        assert out['action'] == Action.BUY
        assert out['ticker'].bid == 100.0
        assert out['stamps'][0] == dt.date(2026, 1, 1)
