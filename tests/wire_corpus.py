"""Representative instances of every type that crosses RPC, PubSub or the
MessageBus in production — the corpus the deserialization boundary's
allowlist (``_DILL_GLOBALS`` in ``trader/messaging/clientserver.py``) was
harvested from and is tested against.

``build_corpus()`` returns ``[(label, factory)]``. A factory builds a fresh
instance per call, so no test can see an object another test touched.
``same(a, b)`` is structural deep equality that understands numpy / pandas /
NaN / dataclasses / ``__slots__`` (``==`` is wrong or ambiguous for most of
those); it returns ``None`` on equality, else a description of the first
difference.

Adding a wire type: add one line to ``_DILL_GLOBALS`` and one instance here.
"""
import collections
import dataclasses
import datetime as dt
import decimal
import enum
import fractions
import importlib
import inspect
import math
import uuid
import zoneinfo

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# structural equality
# ---------------------------------------------------------------------------

_ATOMS = (str, bytes, bytearray, int, bool, complex, type(None), dt.date, dt.time, dt.timedelta,
          dt.tzinfo, decimal.Decimal, fractions.Fraction, uuid.UUID, enum.Enum, range, slice, type)


def _equal_or_both_nan(a, b) -> bool:
    try:
        if a != a and b != b:      # NaT, Decimal('NaN')
            return True
    except Exception:
        pass
    return bool(a == b)


def same(a, b, path='obj'):
    if type(a) is not type(b):
        return (f'{path}: type {type(a).__module__}.{type(a).__qualname__} != '
                f'{type(b).__module__}.{type(b).__qualname__}')
    if a is b:
        return None
    if isinstance(a, (pd.DataFrame, pd.Series, pd.Index)):
        check = (pd.testing.assert_frame_equal if isinstance(a, pd.DataFrame)
                 else pd.testing.assert_series_equal if isinstance(a, pd.Series)
                 else pd.testing.assert_index_equal)
        try:
            check(a, b)
        except AssertionError as ex:
            return f'{path}: {ex}'
        return None
    if isinstance(a, np.ndarray):
        # Byte order is compared by VALUE: pickling normalises a big-endian
        # array to native order (plain pickle does the same).
        if (a.dtype.kind, a.dtype.itemsize, a.dtype.names) != (b.dtype.kind, b.dtype.itemsize, b.dtype.names) \
                or a.shape != b.shape:
            return f'{path}: ndarray {a.dtype}{a.shape} != {b.dtype}{b.shape}'
        return same(a.tolist(), b.tolist(), path)
    if isinstance(a, np.generic):
        if a.dtype != b.dtype:
            return f'{path}: numpy scalar dtype {a.dtype} != {b.dtype}'
        return None if a.tobytes() == b.tobytes() else f'{path}: numpy scalar {a!r} != {b!r}'
    if isinstance(a, float):
        return None if (a == b or (math.isnan(a) and math.isnan(b))) else f'{path}: {a!r} != {b!r}'
    if isinstance(a, dict):
        if list(map(repr, a)) != list(map(repr, b)):
            return f'{path}: dict keys {list(a)!r} != {list(b)!r}'
        if isinstance(a, collections.defaultdict) and a.default_factory is not b.default_factory:
            return f'{path}: default_factory differs'
        for (ka, va), (_, vb) in zip(a.items(), b.items()):
            diff = same(va, vb, f'{path}[{ka!r}]')
            if diff:
                return diff
        return None
    if isinstance(a, (list, tuple, collections.deque)):
        if len(a) != len(b):
            return f'{path}: len {len(a)} != {len(b)}'
        for i, (x, y) in enumerate(zip(a, b)):
            diff = same(x, y, f'{path}[{i}]')
            if diff:
                return diff
        if isinstance(a, collections.deque) and a.maxlen != b.maxlen:
            return f'{path}: deque maxlen differs'
        # list subclasses that also carry attributes (ib_async BarDataList)
        return _same_attrs(a, b, path) if hasattr(a, '__dict__') else None
    if isinstance(a, (set, frozenset)):
        return None if a == b else f'{path}: {a!r} != {b!r}'
    if isinstance(a, BaseException):
        return same(a.args, b.args, f'{path}.args') or _same_attrs(a, b, path)
    if isinstance(a, dt.datetime):
        if a != b or a.utcoffset() != b.utcoffset() or str(a.tzinfo) != str(b.tzinfo):
            return f'{path}: {a!r} != {b!r}'
        return None
    if isinstance(a, _ATOMS):
        return None if _equal_or_both_nan(a, b) else f'{path}: {a!r} != {b!r}'
    return _same_attrs(a, b, path)


def _slot_names(obj):
    names = []
    for klass in type(obj).__mro__:
        slots = vars(klass).get('__slots__', ())
        names.extend(s for s in ((slots,) if isinstance(slots, str) else slots)
                     if s not in ('__dict__', '__weakref__'))
    return names


def _same_attrs(a, b, path):
    compared = False
    if hasattr(a, '__dict__'):
        compared = True
        if set(vars(a)) != set(vars(b)):
            return f'{path}: attribute names {sorted(vars(a))} != {sorted(vars(b))}'
        for name, value in vars(a).items():
            diff = same(value, vars(b)[name], f'{path}.{name}')
            if diff:
                return diff
    for name in _slot_names(a):
        compared = True
        if hasattr(a, name) != hasattr(b, name):
            return f'{path}.{name}: set on one side only'
        if hasattr(a, name):
            diff = same(getattr(a, name), getattr(b, name), f'{path}.{name}')
            if diff:
                return diff
    if not compared:
        return None if _equal_or_both_nan(a, b) else f'{path}: {a!r} != {b!r}'
    return None


# ---------------------------------------------------------------------------
# ib_async
# ---------------------------------------------------------------------------

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 13, 14, 30, 5, 123456, tzinfo=UTC)

# The IB data model. Every class these modules define is wire-eligible and
# must be on the allowlist (test_every_ib_async_data_class_is_allowlisted).
IB_DATA_MODULES = ('ib_async.contract', 'ib_async.order', 'ib_async.objects')


def ib_data_classes():
    out = []
    for modname in IB_DATA_MODULES:
        module = importlib.import_module(modname)
        out.extend(obj for _, obj in sorted(vars(module).items())
                   if isinstance(obj, type) and obj.__module__ == modname)
    return out


def _default_instance(cls):
    """An instance with every required constructor argument filled in."""
    if issubclass(cls, tuple):
        return cls(*[cls._field_defaults.get(name, 0) for name in cls._fields])
    required = [p for p in inspect.signature(cls).parameters.values()
                if p.default is p.empty and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return cls(*[0 for _ in required])


def stock():
    from ib_async import Stock
    contract = Stock('AAPL', 'SMART', 'USD')
    contract.conId = 265598
    contract.primaryExchange = 'NASDAQ'
    return contract


def trade():
    from ib_async import (CommissionReport, Execution, Fill, LimitOrder, OrderStatus,
                          PriceCondition, TagValue, Trade, TradeLogEntry)
    order = LimitOrder('BUY', 100, 100.0)
    order.orderRef = 'orb_googl'
    order.orderId = 7
    order.algoStrategy = 'Adaptive'
    order.algoParams = [TagValue('adaptivePriority', 'Normal')]
    order.conditions = [PriceCondition(conId=265598, exch='SMART', price=99.0)]
    status = OrderStatus(orderId=7, status='Filled', filled=100, remaining=0, avgFillPrice=100.0)
    fill = Fill(
        contract=stock(),
        execution=Execution(execId='e1', time=NOW, side='BOT', shares=100, price=100.0, orderRef='orb_googl'),
        commissionReport=CommissionReport(execId='e1', commission=1.0, currency='USD'),
        time=NOW,
    )
    return Trade(contract=stock(), order=order, orderStatus=status, fills=[fill],
                 log=[TradeLogEntry(time=NOW, status='Filled', message='ok')])


def ticker():
    from ib_async import Ticker
    from ib_async.objects import DOMLevel, FundamentalRatios, OptionComputation, TickData
    t = Ticker(contract=stock(), time=NOW)
    t.bid, t.ask, t.last, t.close = 100.0, 100.5, 100.2, 99.0
    t.bidSize, t.askSize, t.lastSize, t.volume = 10.0, 12.0, 5.0, 1_000_000.0
    t.ticks = [TickData(time=NOW, tickType=4, price=100.2, size=5)]
    t.domBids = [DOMLevel(price=100.0, size=10, marketMaker='NSDQ')]
    t.domAsks = [DOMLevel(price=100.5, size=12, marketMaker='ARCA')]
    t.modelGreeks = OptionComputation(tickAttrib=0, impliedVol=0.3, delta=0.5, optPrice=1.0,
                                      pvDividend=0.0, gamma=0.1, vega=0.2, theta=-0.1, undPrice=100.0)
    t.fundamentalRatios = FundamentalRatios(PEEXCLXOR=28.5, MKTCAP=3.4e6)
    return t


def _bar_list(cls, *rows):
    out = cls()
    out.extend(rows)
    out.reqId = 11
    out.contract = stock()
    return out


# ---------------------------------------------------------------------------
# trader
# ---------------------------------------------------------------------------

def security_definition():
    from trader.data.data_access import SecurityDefinition
    kwargs = {}
    for field in dataclasses.fields(SecurityDefinition):
        kind = field.type if isinstance(field.type, str) else getattr(field.type, '__name__', str(field.type))
        kwargs[field.name] = (False if 'bool' in kind else 265598 if 'int' in kind
                              else 0.01 if 'float' in kind else field.name[:4] if 'str' in kind else None)
    return SecurityDefinition(**kwargs)


def signal():
    from trader.objects import Action
    from trader.trading.strategy import Signal
    return Signal(source_name='orb', action=Action.BUY, probability=0.8, risk=0.1, conid=265598, quantity=10.0,
                  date_time=dt.datetime(2026, 11, 13, 9, 45, tzinfo=zoneinfo.ZoneInfo('America/New_York')),
                  metadata={'bar_time': NOW, 'note': 'breakout', 'atr': np.float64(1.25)},
                  max_hold_bars=20, close_by_time=dt.time(15, 45))


def strategy_config():
    from trader.objects import BarSize
    from trader.trading.strategy import StrategyConfig, StrategyState
    return StrategyConfig(name='orb', state=StrategyState.RUNNING, bar_size=BarSize.Mins1, conids=[265598],
                          module='strategies.orb', class_name='ORB', historical_days_prior=5, paper_only=True,
                          auto_execute=True, params={'RANGE_MINUTES': 30, 'SESSION_TZ': 'America/New_York'},
                          deployment_generation='g1', effective_config_hash='abc', source_hash='def',
                          requested_params={'RANGE_MINUTES': '30'})


def proposal():
    from trader.trading.proposal import ExecutionSpec, TradeProposal
    return TradeProposal(symbol='AMD', action='BUY', quantity=100.0,
                         execution=ExecutionSpec(order_type='LIMIT', limit_price=165.0, exit_type='BRACKET',
                                                 take_profit_price=180.0, stop_loss_price=150.0, tif='GTC'),
                         reasoning='breakout', confidence=0.7, metadata={'strategy': 'orb', 'sizing': {'atr': 2.5}},
                         id=3, status='PENDING', created_at=NOW, updated_at=NOW, order_ids=[1, 2])


def _success_with_warnings():
    from trader.common.reactivex import SuccessFail
    result = SuccessFail.success(obj=[trade()])
    result.warnings = ['cancelled working exit 12 (STP SELL 10 @ 90.0)']
    return result


# ---------------------------------------------------------------------------
# pandas / numpy / stdlib values
# ---------------------------------------------------------------------------

def _frames():
    naive = pd.date_range('2026-11-02 09:30', periods=4, freq='1min', name='date')
    aware = naive.tz_localize('America/New_York')
    cols = {'open': [1.0, 2.0, 3.0, 4.0], 'high': [1.5, 2.5, 3.5, 4.5], 'low': [0.5, 1.5, 2.5, 3.5],
            'close': [1.2, 2.2, np.nan, 4.2], 'volume': [10, 20, 30, 40], 'bar_count': [1, 2, 3, 4]}
    return {
        'DataFrame naive DatetimeIndex': pd.DataFrame(cols, index=naive),
        'DataFrame tz-aware DatetimeIndex': pd.DataFrame(cols, index=aware),
        'DataFrame UTC DatetimeIndex': pd.DataFrame(cols, index=naive.tz_localize('UTC')),
        'DataFrame RangeIndex mixed dtypes': pd.DataFrame({'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0],
                                                            'c': ['x', 'y', 'z'], 'd': [True, False, True]}),
        'DataFrame empty': pd.DataFrame({'a': pd.Series([], dtype='float64')}),
        'DataFrame datetime column': pd.DataFrame({'t': aware, 'v': [1.0, 2.0, 3.0, 4.0]}),
        'Series float naive DatetimeIndex': pd.Series([1.0, 2.0, 3.0, 4.0], index=naive, name='close'),
        'Series float tz-aware DatetimeIndex': pd.Series([1.0, 2.0, 3.0, 4.0], index=aware, name='close'),
        'Series int RangeIndex': pd.Series([1, 2, 3], name='n'),
        'Series str': pd.Series(['a', 'b'], name='s'),
        'DatetimeIndex naive': naive,
        'DatetimeIndex tz-aware': aware,
        'Index int': pd.Index([1, 2, 3], name='conid'),
        'MultiIndex': pd.MultiIndex.from_tuples([('a', 1), ('b', 2)], names=['x', 'y']),
    }


def _values():
    import dateutil.tz
    import pytz
    ny_zoneinfo = zoneinfo.ZoneInfo('America/New_York')
    ny_dateutil = dateutil.tz.gettz('America/New_York')
    ny_pytz = pytz.timezone('America/New_York')
    naive = dt.datetime(2026, 11, 13, 9, 30, 15, 250000)
    return {
        'pd.Timestamp naive': pd.Timestamp('2026-11-13 09:30:15.123456'),
        'pd.Timestamp UTC': pd.Timestamp('2026-11-13 09:30:15', tz='UTC'),
        'pd.Timestamp tz America/New_York': pd.Timestamp('2026-11-13 09:30:15', tz='America/New_York'),
        'pd.Timedelta': pd.Timedelta(days=1, seconds=5, microseconds=7),
        'pd.NaT': pd.NaT,
        'np.float64': np.float64(1.5), 'np.float32': np.float32(1.5), 'np.float64 nan': np.float64('nan'),
        'np.int64': np.int64(-3), 'np.int32': np.int32(3), 'np.int8': np.int8(3), 'np.uint64': np.uint64(2 ** 63),
        'np.bool_': np.bool_(True), 'np.str_': np.str_('abc'), 'np.bytes_': np.bytes_(b'abc'),
        'np.datetime64[ns]': np.datetime64('2026-11-13T09:30:15.123456789'),
        'np.datetime64[D]': np.datetime64('2026-11-13'),
        'np.timedelta64': np.timedelta64(5, 's'),
        'np.complex128': np.complex128(1 + 2j),
        'ndarray float64': np.array([1.0, 2.0, np.nan]), 'ndarray float32': np.array([1.0, 2.0], dtype='float32'),
        'ndarray int64': np.arange(5), 'ndarray int32 2d': np.arange(6, dtype='int32').reshape(2, 3),
        'ndarray uint8': np.frombuffer(b'abc', dtype=np.uint8), 'ndarray bool': np.array([True, False]),
        'ndarray datetime64[ns]': np.array(['2026-11-13T09:30', '2026-11-14'], dtype='datetime64[ns]'),
        'ndarray timedelta64': np.array([1, 2], dtype='timedelta64[s]'),
        'ndarray str': np.array(['a', 'bc']), 'ndarray bytes': np.array([b'a', b'bc']),
        'ndarray object': np.array([1, 'a', None], dtype=object),
        'ndarray structured': np.array([(1, 2.0)], dtype=[('a', '<i4'), ('b', '<f8')]),
        'ndarray big-endian': np.array([1.0, 2.0], dtype='>f8'),
        'ndarray empty': np.array([], dtype='float64'), 'ndarray 0-d': np.array(3.5),
        'ndarray fortran order': np.asfortranarray(np.arange(6.0).reshape(2, 3)),
        'np.dtype': np.dtype('float64'),
        'datetime naive': naive,
        'datetime UTC': naive.replace(tzinfo=UTC),
        'datetime fixed offset': naive.replace(tzinfo=dt.timezone(dt.timedelta(hours=10), 'AEST')),
        'datetime zoneinfo': naive.replace(tzinfo=ny_zoneinfo),
        'datetime zoneinfo UTC': naive.replace(tzinfo=zoneinfo.ZoneInfo('UTC')),
        'datetime pytz localized': ny_pytz.localize(naive),
        'datetime pytz utc': naive.replace(tzinfo=pytz.utc),
        'datetime pytz fixed offset': naive.replace(tzinfo=pytz.FixedOffset(600)),
        'datetime dateutil gettz': naive.replace(tzinfo=ny_dateutil),
        'datetime dateutil tzutc': naive.replace(tzinfo=dateutil.tz.tzutc()),
        'datetime dateutil tzoffset': naive.replace(tzinfo=dateutil.tz.tzoffset('X', 3600)),
        'datetime dateutil tzlocal': naive.replace(tzinfo=dateutil.tz.tzlocal()),
        'date': dt.date(2026, 11, 13), 'time': dt.time(15, 45, 1, 5),
        'time tz-aware': dt.time(15, 45, tzinfo=UTC),
        'timedelta': dt.timedelta(days=3, seconds=3600, microseconds=5),
        'timezone.utc': UTC, 'zoneinfo.ZoneInfo': ny_zoneinfo, 'pytz timezone': ny_pytz, 'pytz.utc': pytz.utc,
        'dateutil gettz tz': ny_dateutil,
        'Decimal': decimal.Decimal('1.10'), 'Decimal NaN': decimal.Decimal('NaN'),
        'Fraction': fractions.Fraction(1, 3),
        'UUID': uuid.UUID('12345678-1234-5678-1234-567812345678'),
        'int big': 2 ** 80, 'int negative big': -2 ** 70, 'float inf': float('inf'), 'float nan': float('nan'),
        'complex': 1 + 2j, 'bytes': b'abc', 'bytearray': bytearray(b'abc'), 'str': 'héllo', 'None': None,
        'bool': True, 'range': range(1, 10, 2), 'slice': slice(1, 5, 2),
        'set': {1, 2, 3}, 'frozenset': frozenset({'a', 'b'}), 'tuple': (1, 'a', 2.5),
        'nested containers': {'a': [1, (2, 3), {4, 5}, frozenset({6})], 'b': {'c': (None, True)},
                              ('tuple', 'key'): 1, 7: 'int key'},
        'OrderedDict': collections.OrderedDict(a=1, b=2), 'defaultdict(list)': collections.defaultdict(list, a=[1]),
        'deque': collections.deque([1, 2], maxlen=5), 'deque unbounded': collections.deque([1, 2]),
        'Counter': collections.Counter('aab'),
    }


def _exceptions():
    import asyncio
    from ib_async.wrapper import RequestError
    from trader.common.exceptions import TraderConnectionException, TraderException
    return {
        'ValueError': ValueError('bad qty'), 'TimeoutError': TimeoutError('slow'),
        'KeyError': KeyError('k'), 'ConnectionError': ConnectionError('gone'),
        'ConnectionRefusedError': ConnectionRefusedError(61, 'refused'),
        'OSError': OSError(2, 'No such file'), 'RuntimeError': RuntimeError('x'),
        'ZeroDivisionError': ZeroDivisionError('division by zero'), 'TypeError': TypeError('t'),
        'AttributeError': AttributeError('a'), 'NotImplementedError': NotImplementedError('n'),
        'AssertionError': AssertionError('a'), 'IndexError': IndexError('i'),
        'PermissionError': PermissionError(13, 'denied'), 'FileNotFoundError': FileNotFoundError(2, 'nf'),
        'NameError': NameError('n'), 'UnicodeDecodeError': UnicodeDecodeError('utf-8', b'x', 0, 1, 'bad'),
        'Exception': Exception('generic'), 'asyncio.TimeoutError': asyncio.TimeoutError('x'),
        'asyncio.CancelledError': asyncio.CancelledError('x'),
        'asyncio.InvalidStateError': asyncio.InvalidStateError('x'),
        'ib_async RequestError': RequestError(1, 200, 'no sec def'),
        'TraderException': TraderException('boom', True, False, NOW, NOW, ValueError('inner'), ['frame 1']),
        'TraderConnectionException': TraderConnectionException('down', True, False, NOW, NOW),
    }


# Corpus entries that do NOT survive a trip through the unpickler, and why.
# Measured with this same corpus on the boundary as it was before 2026-11-14:
# none of them round-tripped then either, with ONE exception — the structured
# numpy array, refused on purpose since. They are pinned so that admitting
# one is a deliberate edit here rather than a side effect.
NOT_WIRE_SAFE = {
    'value ndarray structured': 'structured / sub-array dtypes: numpy applies their pickled field layout '
                                'unchecked (an object field over raw bytes segfaults). Worked before 2026-11-14',
    **{f'pandas {label}': 'pandas objects cross as Arrow IPC (top-level DataFrame) or not at all'
       for label in _frames()},
    'value pd.Timestamp naive': 'pandas scalar inside a pickled object (top level it is a datetime)',
    'value pd.Timestamp UTC': 'pandas scalar inside a pickled object',
    'value pd.Timestamp tz America/New_York': 'pandas scalar inside a pickled object',
    'value pd.Timedelta': 'pandas scalar inside a pickled object (top level it is a timedelta)',
    'value pd.NaT': 'pandas scalar inside a pickled object',
    'value datetime pytz localized': 'pytz timezones rebuild through pytz._p',
    'value datetime pytz utc': 'pytz timezones rebuild through pytz._UTC',
    'value datetime pytz fixed offset': 'pytz timezones rebuild through pytz.FixedOffset',
    'value pytz timezone': 'pytz timezones rebuild through pytz._p',
    'value pytz.utc': 'pytz timezones rebuild through pytz._UTC',
    'value datetime dateutil gettz': 'dateutil file timezones rebuild through dateutil.tz._tzfile._unpickle',
    'value dateutil gettz tz': 'dateutil file timezones rebuild through dateutil.tz._tzfile._unpickle',
    'value range': 'dill encodes range as dill._dill._eval_repr, i.e. eval',
    'exception asyncio.CancelledError': 'not a builtin exception; SuccessFail ships non-builtins as text',
    'exception asyncio.InvalidStateError': 'not a builtin exception; SuccessFail ships non-builtins as text',
    'exception ib_async RequestError': 'cannot be unpickled at all (its __init__ needs three arguments)',
}


def build_corpus():
    from ib_async import (Bag, BracketOrder, ComboLeg, Contract, ContractDetails, Forex, Future, LimitOrder,
                          MarketOrder, Option, Order, StopLimitOrder, StopOrder)
    from ib_async.contract import ScanData
    from ib_async.objects import (AccountValue, BarData, BarDataList, PnL, PnLSingle, PortfolioItem, Position,
                                  RealTimeBar, RealTimeBarList, ScanDataList)
    from ib_async.ticker import TickerUpdateEvent
    from trader.common.dataclass_cache import RemoveEvent, UpdateEvent
    from trader.common.reactivex import SuccessFail
    from trader.data.data_access import PortfolioSummary
    from trader.data.universe import Universe
    from trader.objects import (Action, BarSize, BasketCondition, ExecutorCondition, ReportType, TickList,
                                TradeLogSimple, WhatToShow)
    from trader.trading.strategy import StrategyState

    items = []

    def add(label, factory):
        items.append((label, factory))

    # ib_async: every data class with default field values, then realistic ones
    for cls in ib_data_classes():
        add(f'ib default {cls.__module__}.{cls.__qualname__}', lambda cls=cls: _default_instance(cls))
    add('ib Stock', stock)
    add('ib Forex', lambda: Forex('EURUSD'))
    add('ib Option', lambda: Option('AAPL', '20260320', 250.0, 'C', 'SMART', currency='USD'))
    add('ib Future', lambda: Future('ES', '202612', 'CME'))
    add('ib Contract(conId)', lambda: Contract(conId=265598, exchange='SMART'))
    add('ib Bag with ComboLeg', lambda: Bag(symbol='AAPL', exchange='SMART', currency='USD',
                                            comboLegs=[ComboLeg(conId=1, ratio=1, action='BUY', exchange='SMART')]))
    add('ib ContractDetails', lambda: ContractDetails(contract=stock(), marketName='NMS', minTick=0.01,
                                                      longName='APPLE INC', timeZoneId='US/Eastern',
                                                      tradingHours='20261113:0400-20261113:2000', stockType='COMMON'))
    add('ib Order (TRAIL)', lambda: Order(action='SELL', totalQuantity=5, orderType='TRAIL', trailingPercent=2.0, tif='GTC'))
    add('ib LimitOrder', lambda: LimitOrder('BUY', 250, 42.5, orderRef='strat_x'))
    add('ib MarketOrder', lambda: MarketOrder('SELL', 10))
    add('ib StopOrder', lambda: StopOrder('SELL', 10, 90.0, tif='GTC', orderRef='orb_googl'))
    add('ib StopLimitOrder', lambda: StopLimitOrder('SELL', 10, 90.0, 89.5))
    add('ib BracketOrder', lambda: BracketOrder(LimitOrder('BUY', 1, 10.0), LimitOrder('SELL', 1, 12.0),
                                                StopOrder('SELL', 1, 9.0)))
    add('ib Trade (order, status, fill, log, events)', trade)
    add('ib Ticker (ticks, dom, greeks, ratios, event)', ticker)
    add('ib TickerUpdateEvent', lambda: TickerUpdateEvent('updateEvent'))
    add('ib Position', lambda: Position('DU1', stock(), 100.0, 99.5))
    add('ib PortfolioItem', lambda: PortfolioItem(stock(), 100.0, 101.0, 10100.0, 99.5, 150.0, 0.0, 'DU1'))
    add('ib PnL', lambda: PnL(account='DU1', dailyPnL=1.0, unrealizedPnL=2.0, realizedPnL=3.0))
    add('ib PnLSingle', lambda: PnLSingle(account='DU1', conId=265598, dailyPnL=1.0, position=100))
    add('ib AccountValue', lambda: AccountValue('DU1', 'NetLiquidation', '17000.5', 'USD', ''))
    add('ib BarDataList', lambda: _bar_list(BarDataList, BarData(date=NOW, open=1.0, high=2.0, low=0.5, close=1.5, volume=10)))
    add('ib RealTimeBarList', lambda: _bar_list(RealTimeBarList, RealTimeBar(time=NOW, open_=1.0, high=2.0, low=0.5, close=1.5)))
    add('ib ScanDataList', lambda: _bar_list(ScanDataList, ScanData(
        rank=0, contractDetails=ContractDetails(contract=stock()), distance='', benchmark='', projection='', legsStr='')))
    add('rpc list[Trade]', lambda: [trade(), trade()])
    add('rpc dict[int, list[Trade]]', lambda: {7: [trade()]})
    add("pubsub ('ticker', Ticker)", lambda: ('ticker', ticker()))
    add("pubsub ('dataclass', UpdateEvent(Trade))", lambda: ('dataclass', UpdateEvent(trade())))
    add("pubsub ('dataclass', UpdateEvent(Order))", lambda: ('dataclass', UpdateEvent(LimitOrder('BUY', 1, 10.0))))
    add("pubsub ('dataclass', UpdateEvent(PnLSingle))", lambda: ('dataclass', UpdateEvent(PnLSingle(account='DU1', conId=1))))
    add("pubsub ('dataclass', RemoveEvent)", lambda: ('dataclass', RemoveEvent('DU1:265598')))
    add("messagebus ('signal', Signal)", lambda: ('signal', signal()))

    # trader
    add('trader SecurityDefinition', security_definition)
    add('trader list[SecurityDefinition]', lambda: [security_definition(), security_definition()])
    add('trader PortfolioSummary', lambda: PortfolioSummary(stock(), 100.0, 101.0, 10100.0, 99.5, 150.0, 0.0, 'DU1', 12.5))
    add('trader Universe', lambda: Universe('portfolio', [security_definition()]))
    add('trader TradeLogSimple', lambda: TradeLogSimple(time=NOW, conId=265598, symbol='AAPL', status='Filled'))
    add('trader Signal', signal)
    add('trader StrategyConfig', strategy_config)
    add('trader list[StrategyConfig]', lambda: [strategy_config()])
    add('trader TradeProposal', proposal)
    add('trader ExecutionSpec', lambda: proposal().execution)
    for member in (Action.BUY, Action.SELL, BarSize.Mins1, BarSize.Days1, WhatToShow.TRADES,
                   ReportType.ReportSnapshot, list(TickList)[0], list(BasketCondition)[0],
                   list(ExecutorCondition)[0], StrategyState.RUNNING, StrategyState.DISABLED):
        add(f'trader enum {type(member).__name__}.{member.name}', lambda member=member: member)
    add('SuccessFail.success(dict)', lambda: SuccessFail.success(obj={'ok': 1}))
    add('SuccessFail.success(Trade)', lambda: SuccessFail.success(obj=trade()))
    add('SuccessFail.success(list[Trade])', lambda: SuccessFail.success(obj=[trade()]))
    add('SuccessFail.success(StrategyState)', lambda: SuccessFail.success(obj=StrategyState.RUNNING))
    add('SuccessFail.success(list[StrategyConfig])', lambda: SuccessFail.success(obj=[strategy_config()]))
    add('SuccessFail.fail(error, ValueError)', lambda: SuccessFail.fail(error='rejected', exception=ValueError('bad qty')))
    add('SuccessFail.never_sent', lambda: SuccessFail.never_sent(error='refused before placement'))
    add('SuccessFail with warnings', _success_with_warnings)

    for label in _exceptions():
        add(f'exception {label}', lambda label=label: _exceptions()[label])
    for label in _frames():
        add(f'pandas {label}', lambda label=label: _frames()[label])
    for label in _values():
        add(f'value {label}', lambda label=label: _values()[label])
    return items
