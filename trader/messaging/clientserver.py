from contextlib import contextmanager
from datetime import date, time, timedelta, tzinfo
from functools import partial
from reactivex.disposable import Disposable
from reactivex.observable import Observable
from reactivex.observer import Observer
from reactivex.subject import Subject
from trader.common.logging_helper import setup_logging
from trader.common.reactivex import ObservableIterHelper
from typing import Any, Callable, cast, Coroutine, Dict, Generic, Optional, Tuple, Type, TypeVar

import asyncio
import builtins
import collections
import datetime as dt
import dill
import functools
import importlib
import inspect
import io
import msgpack
import math
import numpy as np
import pickle
import queue
import re
import pandas as pd
import pyarrow as pa
import reactivex as rx
import reactivex.abc as abc
import struct
import threading
import time as time_
import types
import typing
import uuid
import zmq
import zmq.asyncio


logging = setup_logging(module_name='trader.messaging.clientserver')


T = TypeVar('T')
TSub = TypeVar('TSub')


# ---------------------------------------------------------------------------
# Dill deserialization policy
# ---------------------------------------------------------------------------

# dill.loads() can execute arbitrary Python on untrusted input, and a payload
# is decoded BEFORE the RPC method check, so any peer that can reach a ZMQ
# port hands this module a pickle. A poisoned peer must never get code
# execution, a file write, or a state change inside the trading process.
# Three layers, innermost first:
#
#   1. _RestrictedUnpickler — ALWAYS ON, the actual boundary. Every dill
#      payload (EXT_OBJECT and the raw-dill legacy fallbacks) is loaded by it.
#      See "The boundary" below.
#
#   2. _dill_type_whitelist — optional registry callers can populate to
#      additionally restrict the TOP-LEVEL type of the loaded object (by
#      fully-qualified module:qualname).
#
#   3. MMR_DILL_STRICT=1 in the environment disables the dill fallback
#      entirely — any EXT_OBJECT payload raises instead of being loaded.
import os as _os  # noqa: E402

DILL_STRICT_MODE = _os.environ.get('MMR_DILL_STRICT', '').lower() in ('1', 'true', 'yes')
_dill_type_whitelist: Optional[set] = None


def set_dill_whitelist(types):
    """Restrict dill deserialization to the supplied classes.

    Pass ``None`` to clear and allow all types (default). Each entry may be a
    class or a ``module.qualname`` string.
    """
    global _dill_type_whitelist
    if types is None:
        _dill_type_whitelist = None
    else:
        names = set()
        for t in types:
            if isinstance(t, str):
                names.add(t)
            else:
                names.add(f'{t.__module__}.{t.__qualname__}')
        _dill_type_whitelist = names


class DillDeserializationError(RuntimeError):
    """Raised when a wire payload is rejected by the deserialization policy
    or cannot be decoded at all."""


# The boundary (rewritten 2026-11-14, docs/AUDIT_2026-11-14.md B2.1)
# ------------------------------------------------------------------
# A pickle is a program for a small stack machine. Four of its instructions
# reach outside the byte stream:
#
#   GLOBAL / STACK_GLOBAL   import a module and fetch a name
#   REDUCE                  call an object on the stack with arguments
#   NEWOBJ / NEWOBJ_EX      cls.__new__(cls, *args)
#   BUILD                   obj.__setstate__(state), or write obj.__dict__ /
#                           setattr(obj, ...) directly
#
# The previous boundary policed only the first — WHICH NAMES may be fetched,
# by module prefix — and trusted the other three with whatever reached the
# stack. Each of these returned normally from unpack():
#
#   numpy.memmap(path, 'uint8', 'w+', 0, (1,))        truncated a file: "any
#       class under an allowed prefix" admits constructors with side effects
#   getattr(numpy.ndarray, 'tofile')(arr, path)        wrote chosen bytes to a
#       file: the guarded getattr returned ANY non-dunder attribute
#   getattr(pandas.DataFrame, 'eval')(df, '...')       ran an expression
#   RiskLimits + BUILD                                 setattr on the live CLASS
#       (max_position_size_pct = 1e18 for every later RiskLimits())
#   dill._dill._create_namedtuple('Popen', (), 'subprocess')(argv)
#       ran a command: that "pure-data" helper imports the module it is told
#       to and returns ANY class in it (found while fixing the four above)
#
# What is enforced now, at the instruction that does the work:
#
#   names   EXACT (module, qualname) pairs in _DILL_GLOBALS. No prefixes: a
#           name that is not listed is never imported. An attribute lookup is
#           an EXACT (owner, attribute) pair in _DILL_ATTRIBUTES.
#   calls   REDUCE may call only an object this module itself resolved from
#           those tables (checked by identity, so a callable produced some
#           other way is refused whatever its name), only if the entry grants
#           _CALL, and only with arguments its check accepts. A class that is
#           built by allocation (every dataclass) is NOT callable: its
#           __init__ / __post_init__ never run on wire data.
#   helpers that call or import on the pickle's behalf (_create_namedtuple,
#           _create_array, _load_type, getattr, numpy.dtype) are replaced by
#           stand-ins that apply the same tables to their arguments.
#   NEWOBJ  only for a class whose entry grants _NEW.
#   BUILD   only on an object THIS load allocated (NEWOBJ, or a REDUCE whose
#           entry is _FRESH). That is what stops state being written onto a
#           class, a function, a module — or onto a shared instance such as an
#           enum member or a cached timezone, which the old gate also allowed.
#   numpy   state is checked before numpy applies it (numpy does not check it
#           itself; see _checked_dtype_state / _checked_array_state).
#   APPEND(S) / SETITEM(S) / ADDITEMS only on a real list / dict / set / deque.
#   INST, OBJ, EXT*, PERSID, BINPERSID are refused: pickle protocols 2+ never
#           emit them for these types.
#
# REDUCE, NEWOBJ and BUILD can only be intercepted in the pure-Python
# unpickler (the C one exposes find_class and nothing else), so that is the
# base class. Measured on the hot payloads (2026-11-14, M-series laptop): a
# Ticker decodes in 0.14 ms instead of 0.02 ms, a Trade in 0.23 ms instead of
# 0.04 ms, a 20-trade reply in 3.4 ms instead of 0.6 ms. dill.dumps of the
# same objects costs 0.28 / 0.43 / 5.1 ms, so decoding is still the cheaper
# half of a round trip.
#
# WHY EXACT LISTS AND NOT A NARROW PREFIX. A prefix is a promise about code
# nobody here has read; every hole above was an unread member of an allowed
# package. The price is brittleness, and it is paid in the right place:
# tests/test_serialization_security.py round-trips a representative instance
# of every production wire type (tests/wire_corpus.py) and asserts that every
# class ib_async's data modules define is listed, so a library upgrade that
# adds or moves a class fails the suite instead of dropping live messages. A
# refused name is reported with its exact (module, name), so the fix is one
# reviewed line here.
#
# Deliberately NOT listed. Refused by the previous gate too: pandas objects —
# a DataFrame crosses as Arrow IPC (EXT_DATAFRAME), never as a pickle, and
# rebuilding one from a pickle means driving pandas._libs block / index
# internals with attacker-chosen state; pd.Timestamp / Timedelta / NaT inside
# a pickled object (top level they are datetimes); pytz and dateutil-file
# timezones (their rebuilders are functions over shared caches — zoneinfo,
# datetime.timezone and dateutil tzutc / tzoffset / tzlocal ARE listed);
# `range` (dill encodes it as dill._dill._eval_repr, i.e. eval). Accepted
# before and refused now: numpy structured / sub-array dtypes (see above),
# and anything else that happened to live under an old prefix. Dropped as
# unused: copyreg._reconstructor / __newobj__ and _codecs.encode (protocol
# < 2 artefacts, never emitted at dill's protocol 4), numpy.core.* (numpy 1
# spelling; both ends run the same lock).

_CALL = 1     # REDUCE may call it
_NEW = 2      # NEWOBJ / NEWOBJ_EX may allocate an instance (cls.__new__ only)
_FRESH = 4    # what a REDUCE call returns is newly allocated: BUILD may fill it


class _Global(typing.NamedTuple):
    caps: int
    # Arguments a REDUCE may pass; None accepts any (pure value constructors).
    check: Optional[Callable[[tuple], bool]] = None
    # Guarded replacement handed to the pickle instead of the real object.
    stand_in: Optional[Callable] = None


def _args(*kinds) -> Callable[[tuple], bool]:
    """Check: exactly these positional argument types, in this order."""
    def check(args: tuple) -> bool:
        return len(args) == len(kinds) and all(isinstance(a, k) for a, k in zip(args, kinds))
    return check


def _one_arg(args: tuple) -> bool:
    # Enum lookup by value. A second argument selects Enum's functional API,
    # which creates a new class.
    return len(args) == 1


def _defaultdict_args(args: tuple) -> bool:
    # The factory is called later, by whoever reads a missing key.
    return len(args) == 1 and (args[0] is None or any(
        args[0] is kind for kind in (list, dict, set, int, float, str, tuple, bool)))


def _slice_args(args: tuple) -> bool:
    return len(args) == 3 and all(a is None or type(a) is int for a in args)


def _packed_time_args(args: tuple) -> bool:
    # datetime / time pickle as (packed_bytes,) or (packed_bytes, tzinfo).
    return (len(args) in (1, 2) and type(args[0]) is bytes
            and (len(args) == 1 or isinstance(args[1], dt.tzinfo)))


def _timezone_args(args: tuple) -> bool:
    return (len(args) in (1, 2) and type(args[0]) is dt.timedelta
            and (len(args) == 1 or type(args[1]) is str))


def _deque_args(args: tuple) -> bool:
    # deque(items, maxlen), or deque() followed by APPENDS when unbounded.
    return not args or (len(args) == 2 and type(args[0]) in (tuple, list)
                        and (args[1] is None or type(args[1]) is int))


def _tzoffset_args(args: tuple) -> bool:
    return (len(args) == 2 and (args[0] is None or isinstance(args[0], str))
            and isinstance(args[1], (dt.timedelta, int, float)))


def _reconstruct_args(args: tuple) -> bool:
    # numpy's own shape: _reconstruct(ndarray, (0,), b'b'), state follows.
    # EXACTLY ndarray — a subclass here is how memmap / recarray get built.
    return (len(args) == 3 and args[0] is np.ndarray
            and type(args[1]) is tuple and args[1] == (0,)
            and type(args[2]) is bytes and args[2] == b'b')


# numpy applies pickled state WITHOUT validating it, and its own docs call
# unpickling unsafe. Reproduced on numpy 2.5 while writing this boundary:
# ndarray.__setstate__ with an object dtype and a list shorter than the shape,
# and a structured dtype with an object field laid over raw bytes, both
# SEGFAULT the process the first time the array is printed. So numpy never
# sees state from the wire that has not been checked here first:
#   * only PLAIN dtypes exist on this path — no fields, no sub-arrays, no
#     StringDType. (Structured arrays are therefore refused; nothing in MMR
#     ships one.)
#   * a dtype's state must be EXACTLY what numpy itself emits for that plain
#     dtype, and numpy is handed its own tuple, not the pickle's.
#   * an array's state must agree with itself: shape x itemsize == len(bytes),
#     or for object arrays len(list) == number of elements.
_PLAIN_DTYPE_CODE = re.compile(r'[A-Za-z][0-9]*\Z')
_PLAIN_DTYPE_KINDS = 'biufcmMOSUV'


def _plain_dtype(obj) -> bool:
    return (isinstance(obj, np.dtype) and obj.kind in _PLAIN_DTYPE_KINDS
            and obj.names is None and obj.subdtype is None)


def _checked_dtype_state(target, state) -> tuple:
    # (version, byteorder, subarray, names, fields, itemsize, alignment, flags
    #  [, (None, (unit, n, 1, 1))])  — the last only for datetime64/timedelta64.
    bad = DillDeserializationError(
        'refusing numpy dtype state in dill payload: not the state of a plain dtype')
    if not _plain_dtype(target) or type(state) is not tuple or len(state) not in (8, 9) \
            or type(state[1]) is not str or state[1] not in ('<', '>', '|'):
        raise bad
    unit = ''
    if len(state) == 9:
        meta = state[8]
        if not (type(meta) is tuple and len(meta) == 2 and type(meta[1]) is tuple and len(meta[1]) == 4
                and type(meta[1][0]) is bytes and type(meta[1][1]) is int):
            raise bad
        if meta[1][0] != b'generic':
            unit = f'[{meta[1][1]}{meta[1][0].decode("ascii", "replace")}]'
    # Rebuild the dtype the state CLAIMS to describe through numpy's public,
    # validating constructor, and compare what numpy would have pickled.
    candidate = np.dtype(state[1] + target.str[1:].split('[')[0] + unit)
    emitted: Any = candidate.__reduce__()[2]
    if not _plain_dtype(candidate) or (candidate.kind, candidate.itemsize) != (target.kind, target.itemsize) \
            or type(emitted) is not tuple or emitted != state:
        raise bad
    return emitted


def _checked_array_state(state) -> tuple:
    # (version, shape, dtype, is_fortran, data)
    if type(state) is tuple and len(state) == 5 and type(state[0]) is int and state[0] == 1 \
            and type(state[1]) is tuple and all(type(n) is int and n >= 0 for n in state[1]) \
            and _plain_dtype(state[2]) and type(state[3]) is bool:
        count, dtype, data = math.prod(state[1]), state[2], state[4]
        if dtype.hasobject:
            if type(data) is list and len(data) == count:
                return state
        elif type(data) is bytes and len(data) == count * dtype.itemsize and (dtype.itemsize or not count):
            return state
    raise DillDeserializationError(
        'refusing numpy array state in dill payload: shape, dtype and data do not agree')


def _scalar_args(args: tuple) -> bool:
    return (len(args) == 2 and _plain_dtype(args[0]) and not args[0].hasobject
            and type(args[1]) is bytes and len(args[1]) == args[0].itemsize)


def _clip(text, limit: int = 120) -> str:
    # Names in a refusal come from the payload; keep them printable and short.
    text = repr(text)
    return text if len(text) <= limit else text[:limit] + '...'


def _describe(obj) -> str:
    if isinstance(obj, type):
        return f'class {obj.__module__}.{obj.__qualname__}'
    if isinstance(obj, (types.FunctionType, types.BuiltinFunctionType, types.MethodType)):
        return f'function {getattr(obj, "__module__", "?")}.{getattr(obj, "__qualname__", "?")}'
    if isinstance(obj, types.ModuleType):
        return f'module {obj.__name__}'
    return f'a {type(obj).__module__}.{type(obj).__qualname__} instance'


def _guarded_getattr(*args):
    # Legit pickles use getattr for exactly one thing here: ZoneInfo pickles
    # as getattr(ZoneInfo, '_unpickle')(key, from_cache). "Any non-dunder
    # attribute" also handed out ndarray.tofile and DataFrame.eval.
    if len(args) != 2 or type(args[1]) is not str:
        raise DillDeserializationError('refusing getattr in dill payload: expected (owner, name)')
    owner, name = args
    entry = _policy_by_id.get(id(owner))
    key = entry[2] if entry is not None and entry[0] is owner else None
    spec = _DILL_ATTRIBUTES.get((key, name)) if key is not None else None
    if spec is None:
        raise DillDeserializationError(
            f'refusing getattr({_describe(owner)}, {_clip(name)}) in dill payload: '
            'not on the attribute allowlist')
    target = _resolved_attributes.get((key, name))
    if target is None:
        with _resolve_lock:
            target = _resolved_attributes.get((key, name))
            if target is None:
                target = getattr(owner, name)
                _policy_by_id[id(target)] = (target, spec, (key, name))
                _resolved_attributes[(key, name)] = target
    return target


# Types dill._dill._load_type may hand back that are only ever REFERENCED.
# Everything else it resolves must be a listed builtin (a value type or an
# exception) — never CodeType / FunctionType / MethodType / CellType /
# ModuleType, which let a pickle assemble and call arbitrary code.
_DILL_INERT_LOADED_TYPES = (type(None), type(NotImplemented), type(Ellipsis))


def _guarded_load_type(*args):
    if len(args) != 1 or type(args[0]) is not str:
        raise DillDeserializationError('refusing dill._dill._load_type in dill payload: expected (name,)')
    try:
        loaded = dill._dill._load_type(args[0])
    except KeyError:
        loaded = None
    if any(loaded is kind for kind in _DILL_INERT_LOADED_TYPES):
        return loaded
    if isinstance(loaded, type) and loaded.__module__ == 'builtins' \
            and ('builtins', loaded.__name__) in _DILL_GLOBALS \
            and _resolve_global('builtins', loaded.__name__) is loaded:
        return loaded
    raise DillDeserializationError(
        f'refusing dill._dill._load_type({_clip(args[0])}) in dill payload')


def _guarded_create_namedtuple(*args):
    # dill pickles a namedtuple CLASS as _create_namedtuple(name, fields,
    # module[, defaults]). The real helper imports `module` and returns any
    # class it finds there (subprocess.Popen included), or else manufactures a
    # new class. Here it is a lookup in the same table as every other name.
    if len(args) not in (3, 4) or type(args[0]) is not str or type(args[2]) is not str:
        raise DillDeserializationError(
            'refusing dill._dill._create_namedtuple in dill payload: expected (name, fields, module)')
    cls = _resolve_global(args[2], args[0])
    if not (isinstance(cls, type) and issubclass(cls, tuple) and hasattr(cls, '_fields')):
        raise DillDeserializationError(
            f'refusing dill._dill._create_namedtuple({_clip(args[0])}, ..., {_clip(args[2])}): '
            'not a namedtuple class')
    return cls


def _guarded_create_array(*args):
    # dill pickles an ndarray as _create_array(f, args, state, npdict), and
    # the real helper CALLS f. Only numpy's own rebuilder, only its own
    # argument shape, and no instance __dict__ (plain ndarrays have none).
    if len(args) not in (3, 4) or (len(args) == 4 and args[3] is not None) \
            or args[0] is not _resolve_global('numpy._core.multiarray', '_reconstruct') \
            or type(args[1]) is not tuple or not _reconstruct_args(args[1]):
        raise DillDeserializationError(
            'refusing dill._dill._create_array in dill payload: not a plain numpy.ndarray rebuild')
    state = _checked_array_state(args[2])
    array = args[0](*args[1])
    array.__setstate__(state)
    return array


def _guarded_numpy_dtype(*args):
    # numpy pickles a dtype as dtype(<code>, False, True) followed by BUILD,
    # which rewrites the byte order IN PLACE. copy=True is what makes the
    # target a private object; dtype('f8') alone is the process-wide float64
    # singleton, and NEWOBJ would return that singleton too (so no _NEW).
    # The code is one type letter and a size: 'i4,O' or '(2,3)f8' would build
    # a structured / sub-array dtype straight from the string.
    if len(args) != 3 or type(args[0]) is not str or args[1] is not False or args[2] is not True \
            or not _PLAIN_DTYPE_CODE.match(args[0]):
        raise DillDeserializationError(
            'refusing numpy.dtype in dill payload: expected (code, False, True)')
    made = np.dtype(args[0], False, True)
    if made is np.dtype(args[0]) or not _plain_dtype(made):
        raise DillDeserializationError('refusing numpy.dtype in dill payload: not a private plain dtype')
    return made


# ---- The allowlist. Harvested by instrumenting find_class / REDUCE / NEWOBJ /
# BUILD / getattr over the full test suite plus a representative instance of
# every type that crosses RPC, PubSub and the MessageBus (tests/wire_corpus.py),
# then read entry by entry. To admit a new wire type add ONE line here and one
# instance to that corpus. Before granting _CALL, read the constructor: it
# runs on attacker-chosen arguments. Before granting _NEW, check the class has
# no __del__, no __setstate__ with side effects and no property setters: BUILD
# writes its attributes (TestAllowlistShape asserts the mechanical parts).
_DILL_GLOBALS: Dict[Tuple[str, str], _Global] = {}


def _allow(module: str, names: str, caps: int,
           check: Optional[Callable[[tuple], bool]] = None) -> None:
    spec = _Global(caps, check)
    for name in names.split():
        _DILL_GLOBALS[(module, name)] = spec


# builtins: value types. A protocol-4 pickle builds ints, strings, bytes,
# lists, tuples, dicts and sets with its own opcodes, so these are only ever
# REFERENCED (defaultdict(list), a type stored as a value) — they are not
# callable from the wire. That matters: list(range(10**9)) is two allowed
# names and eight gigabytes, and str(x) / dict(x) run x's own methods.
# Absent on purpose: eval, exec, compile, __import__, open, setattr, delattr,
# vars, globals, type, and the real getattr (see _guarded_getattr).
_allow('builtins', 'bool bytes dict float frozenset int list object range set str tuple', 0)
# The three builtins pickle does rebuild by calling them, in the shape it uses.
_allow('builtins', 'bytearray', _CALL, _args(bytes))
_allow('builtins', 'complex', _CALL, _args(float, float))
_allow('builtins', 'slice', _CALL, _slice_args)
# builtins: every exception class EXCEPT the three that end a process when
# raised. They cross inside SuccessFail.fail and RPC error args; constructing
# one stores its arguments and nothing else. dill names them through
# _load_type, which resolves against these same entries.
_allow('builtins', ' '.join(sorted(
    name for name, obj in vars(builtins).items()
    if isinstance(obj, type) and issubclass(obj, BaseException) and obj.__name__ == name
    and not issubclass(obj, (SystemExit, KeyboardInterrupt, GeneratorExit)))), _CALL | _FRESH)

# Standard-library values, each in exactly the argument shape its own
# __reduce__ produces — these constructors accept far more than that.
_allow('datetime', 'date', _CALL, _args(bytes))
_allow('datetime', 'datetime time', _CALL, _packed_time_args)
_allow('datetime', 'timedelta', _CALL, _args(int, int, int))
_allow('datetime', 'timezone', _CALL, _timezone_args)
_allow('decimal', 'Decimal', _CALL, _args(str))
_allow('fractions', 'Fraction', _CALL, _args(int, int))
_allow('uuid', 'UUID', _NEW)
_allow('collections', 'Counter', _CALL, _args(dict))
_allow('collections', 'OrderedDict', _CALL, _args())
_allow('collections', 'deque', _CALL, _deque_args)
_allow('collections', 'defaultdict', _CALL, _defaultdict_args)
# Timezones. ZoneInfo is listed only as the owner of the _unpickle lookup in
# _DILL_ATTRIBUTES (the key is validated by zoneinfo against path traversal).
# tzutc / tzoffset return cached shared instances, so they are not _FRESH.
_allow('zoneinfo', 'ZoneInfo', 0)
_allow('dateutil.tz.tz', 'tzutc', _CALL, _args())
_allow('dateutil.tz.tz', 'tzoffset', _CALL, _tzoffset_args)
_allow('dateutil.tz.tz', 'tzlocal', _NEW)

# numpy: plain arrays and scalars. `ndarray` is listed only so it can be
# NAMED as the first argument of _reconstruct; it grants nothing itself.
# Absent on purpose: memmap, recarray, matrix, ma.*, lib.npyio, fromfile,
# load, save, and every other numpy name.
_allow('numpy', 'ndarray', 0)
_allow('numpy._core.multiarray', '_reconstruct', _CALL | _FRESH, _reconstruct_args)
_allow('numpy._core.multiarray', 'scalar', _CALL, _scalar_args)
_DILL_GLOBALS[('numpy', 'dtype')] = _Global(_CALL | _FRESH, stand_in=_guarded_numpy_dtype)

# Helpers that call, import or look things up on the pickle's behalf: the
# real ones are never handed out.
_DILL_GLOBALS[('builtins', 'getattr')] = _Global(_CALL, stand_in=_guarded_getattr)
_DILL_GLOBALS[('dill._dill', '_load_type')] = _Global(_CALL, stand_in=_guarded_load_type)
_DILL_GLOBALS[('dill._dill', '_create_namedtuple')] = _Global(_CALL, stand_in=_guarded_create_namedtuple)
_DILL_GLOBALS[('dill._dill', '_create_array')] = _Global(_CALL, stand_in=_guarded_create_array)

# ib_async: the IB data model (contract.py, order.py, objects.py) plus the
# Ticker. All plain dataclasses / NamedTuples / list subclasses, allocated and
# then filled — never constructed. Events are the exception: Ticker and Trade
# carry eventkit Events, which pickle as Event(name, flag) with no listeners.
# Absent on purpose: IB, Client, Wrapper and the tick-pipeline operators
# (Tickfilter, TimeBars, ...), which are machinery, not data.
_allow('ib_async.contract',
       'Bag Bond CFD ComboLeg Commodity ContFuture Contract ContractDescription '
       'ContractDetails Crypto DeltaNeutralContract Forex Future FuturesOption Index '
       'MutualFund Option ScanData Stock Warrant TagValue TradingSession', _NEW)
_allow('ib_async.order',
       'BracketOrder ExecutionCondition LimitOrder MarginCondition MarketOrder Order '
       'OrderComboLeg OrderCondition OrderState OrderStateNumeric OrderStatus '
       'PercentChangeCondition PriceCondition StopLimitOrder StopOrder TimeCondition '
       'Trade VolumeCondition', _NEW)
_allow('ib_async.objects',
       'AccountValue BarData BarDataList CommissionReport ConnectionStats DOMLevel '
       'DepthMktDataDescription Dividends DynamicObject EfpData Execution ExecutionFilter '
       'FamilyCode Fill FundamentalRatios HistogramData HistoricalNews HistoricalSchedule '
       'HistoricalSession HistoricalTick HistoricalTickBidAsk HistoricalTickLast IBDefaults '
       'MktDepthData NewsArticle NewsBulletin NewsProvider NewsTick OptionChain '
       'OptionComputation PnL PnLSingle PortfolioItem Position PriceIncrement RealTimeBar '
       'RealTimeBarList ScanDataList ScannerSubscription SmartComponent SoftDollarTier '
       'TickAttrib TickAttribBidAsk TickAttribLast TickByTickAllLast TickByTickBidAsk '
       'TickByTickMidPoint TickData TradeLogEntry WshEventData', _NEW)
_allow('ib_async.ticker', 'Ticker', _NEW)
_allow('ib_async.ticker', 'TickerUpdateEvent', _CALL, _args(str, bool))
_allow('eventkit.event', 'Event', _CALL, _args(str, bool))

# trader: the wire types, by name. RPC arguments and results
# (trader_service_api / strategy_service_api), PubSub ('ticker', Ticker) and
# ('dataclass', UpdateEvent | RemoveEvent), MessageBus ('signal', Signal).
# Nothing else in the package is reachable — RiskLimits, RPCClient, the
# stores and the runtimes included.
_allow('trader.common.reactivex', 'SuccessFail', _NEW)
_allow('trader.common.reactivex', 'SuccessFailEnum', _CALL, _one_arg)
_allow('trader.common.dataclass_cache', 'UpdateEvent RemoveEvent', _NEW)
_allow('trader.common.exceptions', 'TraderException TraderConnectionException', _CALL | _FRESH)
_allow('trader.data.data_access', 'PortfolioSummary SecurityDefinition', _NEW)
_allow('trader.data.universe', 'Universe', _NEW)
_allow('trader.objects', 'TradeLogSimple', _NEW)
_allow('trader.objects',
       'Action BarSize BasketCondition ExecutorCondition ReportType TickList WhatToShow',
       _CALL, _one_arg)
_allow('trader.trading.proposal', 'ExecutionSpec TradeProposal', _NEW)
_allow('trader.trading.strategy', 'Signal StrategyConfig', _NEW)
_allow('trader.trading.strategy', 'StrategyState', _CALL, _one_arg)

# (owner, attribute) pairs the guarded getattr may return. The owner is named
# by its _DILL_GLOBALS key and matched by identity.
_DILL_ATTRIBUTES: Dict[Tuple[Tuple[str, str], str], _Global] = {
    (('zoneinfo', 'ZoneInfo'), '_unpickle'): _Global(_CALL, _args(str, int)),
}

# Everything the tables have resolved so far. A REDUCE / NEWOBJ target is
# looked up HERE, by identity — never by its name or by where it sits on the
# stack — so only an object this module fetched through the tables can be
# called. Entries are (object, spec, key); holding the object keeps its id
# from being reused.
_policy_by_id: Dict[int, Tuple[Any, _Global, Any]] = {}
_resolved_globals: Dict[Tuple[str, str], Any] = {}
_resolved_attributes: Dict[Tuple[Any, str], Any] = {}
_resolve_lock = threading.Lock()


def _policy_of(obj) -> Optional[_Global]:
    entry = _policy_by_id.get(id(obj))
    return entry[1] if entry is not None and entry[0] is obj else None


def _resolve_global(module, name):
    key = (module, name)
    target = _resolved_globals.get(key)
    if target is not None:
        return target
    # The table is consulted BEFORE any import, so a module an attacker names
    # is never loaded.
    spec = _DILL_GLOBALS.get(key) if type(module) is str and type(name) is str else None
    if spec is None:
        raise DillDeserializationError(
            f'refusing to deserialize global {_clip(f"{module}.{name}")}: not on the allowlist '
            '(_DILL_GLOBALS in trader/messaging/clientserver.py)')
    with _resolve_lock:
        target = _resolved_globals.get(key)
        if target is None:
            if spec.stand_in is not None:
                target = spec.stand_in
            else:
                try:
                    target = importlib.import_module(module)
                    for part in name.split('.'):
                        target = getattr(target, part)
                except (ImportError, AttributeError) as ex:
                    raise DillDeserializationError(
                        f'cannot resolve allowlisted global {module}.{name}: {ex}') from ex
            if spec.caps & _NEW and not isinstance(target, type):
                raise DillDeserializationError(
                    f'allowlisted global {module}.{name} is not a class; refusing to allocate it')
            _policy_by_id[id(target)] = (target, spec, key)
            _resolved_globals[key] = target
    return target


def _refused_opcode(opcode_name: str):
    def refuse(self):
        raise DillDeserializationError(
            f'refusing pickle opcode {opcode_name} in dill payload')
    return refuse


_NOT_FRESH = object()

# The pure-Python unpickler: the only one whose per-opcode handlers
# (load_reduce, load_build, ...) and stack can be overridden. Typed Any because
# typeshed describes just its public surface.
_PyUnpickler: Any = pickle._Unpickler


class _RestrictedUnpickler(_PyUnpickler):
    """The deserialization boundary. See "The boundary" above: names are an
    exact allowlist, and REDUCE / NEWOBJ / BUILD are each checked at the
    instruction that performs them rather than trusted once a name resolved."""

    def __init__(self, file, only: Optional[typing.FrozenSet[Tuple[str, str]]] = None,
                 described_as: str = ''):
        super().__init__(file)
        # Objects this load allocated: the only legal BUILD targets.
        self._fresh: Dict[int, Any] = {}
        # A caller's narrower list of names (see restricted_loads). None on
        # the wire, where _DILL_GLOBALS alone decides.
        self._only = only
        self._only_described_as = described_as

    def find_class(self, module, name):
        # Every GLOBAL / STACK_GLOBAL lands here, and nothing else in a
        # narrowed load can resolve a name: the stand-ins that look names up
        # themselves (_load_type, _create_namedtuple, getattr) are names too,
        # and have to pass this line first.
        if self._only is not None and (module, name) not in self._only:
            raise DillDeserializationError(
                f'refusing to deserialize global {_clip((module, name))}: '
                f'not among {self._only_described_as}')
        return _resolve_global(module, name)

    def persistent_load(self, pid):
        raise DillDeserializationError('refusing persistent id in dill payload')

    def load_reduce(self):
        stack = self.stack
        args = stack.pop()
        func = stack[-1]
        spec = _policy_of(func)
        if spec is None or not spec.caps & _CALL:
            raise DillDeserializationError(
                f'refusing to call {_describe(func)} from a dill payload: not a permitted callable')
        if type(args) is not tuple or (spec.check is not None and not spec.check(args)):
            raise DillDeserializationError(
                f'refusing to call {_describe(func)} from a dill payload: unexpected arguments')
        result = func(*args)
        if spec.caps & _FRESH:
            self._fresh[id(result)] = result
        stack[-1] = result

    def _allocate(self, cls, args, kwargs):
        spec = _policy_of(cls)
        if spec is None or not spec.caps & _NEW or not isinstance(cls, type) \
                or type(args) is not tuple or type(kwargs) is not dict:
            raise DillDeserializationError(
                f'refusing to allocate {_describe(cls)} from a dill payload: not a permitted class')
        obj = cls.__new__(cls, *args, **kwargs)
        if type(obj) is not cls:
            raise DillDeserializationError(
                f'refusing dill payload: allocating {_describe(cls)} returned another type')
        self._fresh[id(obj)] = obj
        return obj

    def load_newobj(self):
        args = self.stack.pop()
        cls = self.stack.pop()
        self.append(self._allocate(cls, args, {}))

    def load_newobj_ex(self):
        kwargs = self.stack.pop()
        args = self.stack.pop()
        cls = self.stack.pop()
        self.append(self._allocate(cls, args, kwargs))

    def load_build(self):
        # BUILD sets state on whatever is under it — an instance, but equally
        # a class, a function, or a shared singleton an earlier call returned.
        target = self.stack[-2]
        if self._fresh.get(id(target), _NOT_FRESH) is not target:
            raise DillDeserializationError(
                f'refusing BUILD on {_describe(target)} in dill payload: state may only be '
                'set on an object this payload allocated')
        state = self.stack[-1]
        if isinstance(target, np.dtype):
            self.stack[-1] = _checked_dtype_state(target, state)
        elif isinstance(target, np.ndarray):
            self.stack[-1] = _checked_array_state(state)
        elif type(state) is tuple and len(state) == 2 and not hasattr(target, '__setstate__'):
            # (dict, slots) state is applied with setattr, which reaches
            # __class__ and property setters. No listed class pickles this way.
            raise DillDeserializationError(
                f'refusing BUILD with slot state on {_describe(target)} in dill payload')
        super().load_build()

    def _require(self, target, kinds, opcode_name):
        # These opcodes call .append / .extend / .add / __setitem__ on whatever
        # is on the stack; a class is not an instance of any of these.
        if not isinstance(target, kinds):
            raise DillDeserializationError(
                f'refusing {opcode_name} on {_describe(target)} in dill payload')

    def load_append(self):
        self._require(self.stack[-2], (list, collections.deque), 'APPEND')
        super().load_append()

    def load_appends(self):
        self._require(self.metastack[-1][-1], (list, collections.deque), 'APPENDS')
        super().load_appends()

    def load_setitem(self):
        self._require(self.stack[-3], dict, 'SETITEM')
        super().load_setitem()

    def load_setitems(self):
        self._require(self.metastack[-1][-1], dict, 'SETITEMS')
        super().load_setitems()

    def load_additems(self):
        self._require(self.metastack[-1][-1], set, 'ADDITEMS')
        super().load_additems()

    dispatch = {
        **_PyUnpickler.dispatch,
        pickle.REDUCE[0]: load_reduce,
        pickle.NEWOBJ[0]: load_newobj,
        pickle.NEWOBJ_EX[0]: load_newobj_ex,
        pickle.BUILD[0]: load_build,
        pickle.APPEND[0]: load_append,
        pickle.APPENDS[0]: load_appends,
        pickle.SETITEM[0]: load_setitem,
        pickle.SETITEMS[0]: load_setitems,
        pickle.ADDITEMS[0]: load_additems,
        # Old-style instantiation, the copyreg extension registry and
        # persistent ids: never emitted for these types at protocol >= 2.
        **{getattr(pickle, opcode_name)[0]: _refused_opcode(opcode_name)
           for opcode_name in ('INST', 'OBJ', 'EXT1', 'EXT2', 'EXT4', 'PERSID', 'BINPERSID')},
    }


def _restricted_dill_loads(data: bytes, only: Optional[typing.FrozenSet[Tuple[str, str]]] = None,
                           described_as: str = ''):
    try:
        return _RestrictedUnpickler(io.BytesIO(data), only, described_as).load()
    except DillDeserializationError:
        raise
    except Exception as ex:
        # A malformed stream, or a permitted constructor rejecting its
        # arguments: one structured error type for "this payload did not
        # decode", whatever the unpickler happened to raise. The text can
        # quote the payload, so it is clipped.
        try:
            detail = str(ex)
        except Exception:
            detail = ''
        raise DillDeserializationError(
            f'dill payload could not be decoded: {type(ex).__name__}: {_clip(detail, 300)}') from ex


def restricted_loads(data: bytes, only: typing.Collection[Tuple[str, str]], described_as: str):
    """The same boundary for a pickle that did not arrive on the wire: a blob
    read back from a store (trader/data/duckdb_store.py is the caller).

    `only` is the exact set of (module, name) pairs that store writes, and
    this load resolves no other name. It can only NARROW: a listed name is
    still resolved through _DILL_GLOBALS, which goes on deciding what may be
    done with it (allocate, call, with which arguments), so a pair missing
    from that table is refused however a caller lists it. `described_as`
    finishes the sentence "refusing to deserialize global (m, n): not among
    ..." so the refusal says which list to read.

    The wire-only switches do not apply here: MMR_DILL_STRICT turns dill off
    for messages, and set_dill_whitelist names the top-level types of
    messages. A stored universe is neither.
    """
    return _restricted_dill_loads(data, frozenset(only), described_as)


def _safe_dill_loads(data: bytes):
    if DILL_STRICT_MODE:
        raise DillDeserializationError(
            'dill deserialization is disabled (MMR_DILL_STRICT=1)'
        )
    obj = _restricted_dill_loads(data)
    if _dill_type_whitelist is not None:
        name = f'{type(obj).__module__}.{type(obj).__qualname__}'
        if name not in _dill_type_whitelist:
            raise DillDeserializationError(
                f'dill type {name!r} is not in the whitelist'
            )
    return obj


# ---------------------------------------------------------------------------
# Serialization: msgpack + pyarrow IPC for DataFrames
# ---------------------------------------------------------------------------

def df_dumps(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df)
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def df_loads(df_bytes: bytes) -> pd.DataFrame:
    reader = pa.ipc.open_stream(df_bytes)
    return reader.read_pandas()


# Custom msgpack ExtType codes
EXT_DATETIME = 1
EXT_DATE = 2
EXT_TIME = 3
EXT_TIMEDELTA = 4
EXT_DATAFRAME = 5
EXT_OBJECT = 6  # fallback using dill


def ext_pack(obj):
    """Custom packer for msgpack."""
    if isinstance(obj, dt.datetime):
        data = struct.pack('!d', obj.timestamp())
        tz = str(obj.tzinfo) if obj.tzinfo else ''
        data += tz.encode()
        return msgpack.ExtType(EXT_DATETIME, data)
    elif isinstance(obj, dt.date):
        return msgpack.ExtType(EXT_DATE, struct.pack('!HBB', obj.year, obj.month, obj.day))
    elif isinstance(obj, dt.time):
        return msgpack.ExtType(EXT_TIME, struct.pack('!BBBL', obj.hour, obj.minute, obj.second, obj.microsecond))
    elif isinstance(obj, dt.timedelta):
        return msgpack.ExtType(EXT_TIMEDELTA, struct.pack('!id', obj.days, obj.seconds + obj.microseconds / 1e6))
    elif isinstance(obj, pd.DataFrame):
        return msgpack.ExtType(EXT_DATAFRAME, df_dumps(obj))
    else:
        return msgpack.ExtType(EXT_OBJECT, dill.dumps(obj))


def ext_unpack(code, data):
    """Custom unpacker for msgpack."""
    if code == EXT_DATETIME:
        ts = struct.unpack('!d', data[:8])[0]
        tz_str = data[8:].decode()
        result = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        if tz_str:
            from dateutil.tz import gettz
            result = result.astimezone(gettz(tz_str))
        return result
    elif code == EXT_DATE:
        y, m, d = struct.unpack('!HBB', data)
        return dt.date(y, m, d)
    elif code == EXT_TIME:
        h, m, s, us = struct.unpack('!BBBL', data)
        return dt.time(h, m, s, us)
    elif code == EXT_TIMEDELTA:
        days, secs = struct.unpack('!id', data)
        return dt.timedelta(days=days, seconds=secs)
    elif code == EXT_DATAFRAME:
        return df_loads(data)
    elif code == EXT_OBJECT:
        return _safe_dill_loads(data)
    return msgpack.ExtType(code, data)


def pack(obj) -> bytes:
    return msgpack.packb(obj, default=ext_pack, use_bin_type=True)


def unpack(data: bytes):
    return msgpack.unpackb(data, ext_hook=ext_unpack, raw=False, strict_map_key=False)


def _decode_payload(data: bytes):
    """Decode an inbound stream payload: msgpack-framed first, raw dill as the
    legacy fallback for in-flight messages from older peers. Both routes land
    in the restricted unpickler. Failures raise DillDeserializationError so
    stream read loops can drop the one message loudly instead of tearing down
    the whole subscription."""
    try:
        return unpack(data)
    except DillDeserializationError:
        raise
    except Exception as msgpack_err:
        try:
            return _safe_dill_loads(data)
        except DillDeserializationError:
            raise
        except Exception:
            raise DillDeserializationError(
                f'payload is neither valid msgpack nor an accepted dill blob: {msgpack_err}'
            ) from msgpack_err


# ---------------------------------------------------------------------------
# RPC decorator and handler base class
# ---------------------------------------------------------------------------

def rpcmethod(func):
    """Decorator marking a method as an RPC endpoint."""
    func._is_rpc_method = True
    return func


class RPCHandler:
    """Base class for RPC service handlers.

    Provides backward-compatible ``rpcmethod`` class-method decorator so that
    existing ``@RPCHandler.rpcmethod`` usage keeps working.
    """

    @classmethod
    def rpcmethod(cls, func):
        return rpcmethod(func)


# ---------------------------------------------------------------------------
# Helper: return-type conversion.
#
# NamedTuples (PortfolioItem, PortfolioSummary, Position, ...) are tuple
# subclasses, so msgpack packs them as plain arrays — the dill ExtType
# fallback never sees them — and unpackb(use_list=True) hands the client
# back plain lists. Dataclasses and other objects go through dill and
# arrive intact (the else branch). Reconstruct NamedTuples positionally.
# ---------------------------------------------------------------------------

def _convert_return_type(obj, return_type):
    if isinstance(obj, (list, tuple)) and hasattr(return_type, '__origin__') and return_type.__origin__ == list:
        inner = typing.get_args(return_type)
        if inner:
            return [_convert_return_type(o, inner[0]) for o in obj]
        return list(obj)
    elif return_type is list:
        # A bare (unparameterized) list wants the payload as-is. The previous
        # guard compared against type(list) — i.e. `type` — so this branch never
        # matched and a bare-list caller fell through to return_type(*obj),
        # which raises for any non-empty payload while an EMPTY result
        # coincidentally works: the failure reads as "nothing resolved".
        return list(obj) if isinstance(obj, (list, tuple)) else obj
    elif isinstance(obj, (list, tuple)) and hasattr(return_type, '__origin__') and return_type.__origin__ is tuple:
        return return_type(obj)
    elif isinstance(obj, (list, tuple)):
        return return_type(*obj)
    else:
        return obj


# ---------------------------------------------------------------------------
# consume() helper
# ---------------------------------------------------------------------------

C = TypeVar('C')


def consume(coro: Coroutine[Any, Any, C]) -> C:
    """Helper to consume an RPC coroutine that is actually just a remote call.

    In theory *coro* should never actually be a coroutine when called via the
    synchronous ``RPCClient.rpc()`` path, but this is useful for the type-checker.
    """
    if inspect.iscoroutine(coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)
    else:
        return cast(C, coro)


# ---------------------------------------------------------------------------
# PubSubSubject / _Handler (kept as-is for TopicPubSub)
# ---------------------------------------------------------------------------

class PubSubSubject(Subject[T]):
    def on_error(self, error: Exception) -> None:
        self.check_disposed()

        if self.is_stopped:
            return

        for obv in list(self.observers):
            obv.on_error(error)


class _Handler(Generic[T]):
    def __init__(self):
        self.subject = PubSubSubject[T]()

    def on_message(self, obj):
        self.subject.on_next(obj)

    def on_throw(self, ex):
        self.subject.on_error(ex)

    def on_close(self):
        self.subject.on_completed()

    def get_subject(self):
        return self.subject


# ---------------------------------------------------------------------------
# _SyncMethodCall  (replaces _AwaitedMethodCall)
# ---------------------------------------------------------------------------

# Stdlib exception types we preserve across the RPC boundary. Custom exception
# types can be registered per-RPCClient via ``error_table``.
_STDLIB_EXCEPTIONS: Dict[str, type] = {
    cls.__name__: cls
    for cls in (
        ValueError, TypeError, KeyError, IndexError, AttributeError,
        RuntimeError, ConnectionError, TimeoutError, FileNotFoundError,
        PermissionError, NotImplementedError, ArithmeticError,
        ZeroDivisionError, OverflowError, LookupError, OSError,
        AssertionError, StopIteration,
    )
}

# The decode boundary's own error must survive the RPC wire so a client sees
# WHY its request was refused instead of a generic RPCError.
_STDLIB_EXCEPTIONS[DillDeserializationError.__name__] = DillDeserializationError


class RPCError(RuntimeError):
    """Wrapper raised when an RPC server exception cannot be reconstructed
    locally (type unknown to client). ``exc_type`` preserves the server-side
    class name; ``exc_args`` preserves the args tuple for callers that want to
    reason about the original failure."""

    def __init__(self, message: str, exc_type: str = 'Exception', exc_args: Tuple = ()):
        super().__init__(message)
        self.exc_type = exc_type
        self.exc_args = exc_args


def _reconstruct_rpc_exception(
    exc_type: str,
    exc_args,
    error_table: Optional[Dict[str, type]] = None,
) -> Exception:
    """Recreate an exception raised by the remote RPC handler.

    Priority: caller-supplied error_table (both by short name and fully-
    qualified name), then stdlib exceptions, then a generic RPCError fallback
    that still carries the original type + args for inspection.
    """
    args_tuple = tuple(exc_args) if isinstance(exc_args, (list, tuple)) else (exc_args,)

    if error_table:
        cls = error_table.get(exc_type)
        if cls is None:
            short = exc_type.rsplit('.', 1)[-1]
            for key, candidate in error_table.items():
                if key.rsplit('.', 1)[-1] == short:
                    cls = candidate
                    break
        if cls is not None:
            try:
                return cls(*args_tuple)
            except Exception:
                # Constructor signature mismatch — fall through to stdlib/RPCError
                pass

    cls = _STDLIB_EXCEPTIONS.get(exc_type.rsplit('.', 1)[-1])
    if cls is not None:
        try:
            return cls(*args_tuple)
        except Exception:
            pass

    message = str(args_tuple[0]) if args_tuple else ''
    return RPCError(
        f'RPC error ({exc_type}): {message}' if message else f'RPC error ({exc_type})',
        exc_type=exc_type,
        exc_args=args_tuple,
    )


class _SyncMethodCall:
    __slots__ = ('_client', '_timeout', '_names', '_return_type', '_error_table')

    def __init__(self, client, timeout=None, names=(),
                 return_type: Optional[Type] = None,
                 error_table: Optional[Dict[str, type]] = None):
        self._client = client
        self._timeout = timeout
        self._names = names
        self._return_type = return_type
        self._error_table = error_table

    def __getattr__(self, name):
        return _SyncMethodCall(self._client, self._timeout,
                               self._names + (name,), self._return_type,
                               self._error_table)

    def __call__(self, *args, **kwargs):
        if not self._names:
            raise ValueError('RPC method name is empty')

        req_id = str(uuid.uuid4())
        request = {
            'method': '.'.join(self._names),
            'args': args,
            'kwargs': kwargs,
            'req_id': req_id,
        }
        method_str = '.'.join(self._names)

        # One monotonic budget includes waiting for this client's turn,
        # connection readiness, sending, and receiving. A disconnected DEALER
        # with IMMEDIATE=1 must never wait indefinitely inside send().
        timeout = self._timeout if self._timeout is not None else 10.0
        deadline = time_.monotonic() + timeout
        if not self._client._lock.acquire(timeout=max(0.0, deadline - time_.monotonic())):
            raise ConnectionError(f'RPC {method_str}: deadline expired before sending (client busy)')
        response = None
        sent = False
        try:
            socket = self._client._require_socket()
            payload = pack(request)
            remaining = deadline - time_.monotonic()
            if remaining <= 0 or not socket.poll(max(1, math.ceil(remaining * 1000)), zmq.POLLOUT):
                self._client._reset_socket()
                raise ConnectionError(f'RPC {method_str}: no route before send deadline')
            try:
                socket.send(payload, flags=zmq.NOBLOCK)
                sent = True
            except zmq.Again:
                self._client._reset_socket()
                raise ConnectionError(f'RPC {method_str}: request could not be sent')

            while True:
                remaining = deadline - time_.monotonic()
                if remaining <= 0 or not socket.poll(max(1, math.ceil(remaining * 1000)), zmq.POLLIN):
                    break
                frames = socket.recv_multipart(zmq.NOBLOCK)
                candidate = unpack(frames[-1])
                if not isinstance(candidate, dict) or candidate.get('req_id') != req_id:
                    logging.warning('RPC %s: discarding reply for another request', method_str)
                    continue
                # A reply for THIS request is in hand: accept it even if the
                # clock crossed the deadline while it was being read. It used
                # to be discarded here, turning a KNOWN placement outcome into
                # "timed out; outcome UNKNOWN" and forcing a reconcile for an
                # answer the server had already delivered. The deadline bounds
                # how long we WAIT, not whether we believe what arrived.
                response = candidate
                break
            if response is None:
                # This discards queued messages and late replies; it CANNOT
                # cancel a handler already executing on the server. The caller
                # must reconcile an ambiguous mutation rather than retry it.
                self._client._reset_socket()
                raise TimeoutError(f'RPC {method_str} timed out after {timeout}s; outcome UNKNOWN')
        except Exception as ex:
            if sent and not isinstance(ex, TimeoutError):
                self._client._reset_socket()
                raise TimeoutError(f'RPC {method_str}: reply unavailable after submission; outcome UNKNOWN') from ex
            raise
        finally:
            self._client._lock.release()

        if response.get('error'):
            exc_type = response.get('exc_type', 'Exception')
            exc_args = response.get('exc_args', ())
            raise _reconstruct_rpc_exception(exc_type, exc_args, self._error_table)

        result = response.get('result')

        # Handle return type conversion (msgpack converts lists to tuples etc.)
        if self._return_type and result is not None:
            result = _convert_return_type(result, self._return_type)

        return result


# ---------------------------------------------------------------------------
# RPCServer
# ---------------------------------------------------------------------------

class RPCServer(Generic[T]):
    def __init__(
        self,
        instance: T,
        zmq_rpc_server_address: str = 'tcp://127.0.0.1',
        zmq_rpc_server_port: int = 42000,
        **kwargs,
    ):
        self.instance = instance
        self.address = f"{zmq_rpc_server_address}:{zmq_rpc_server_port}"
        self.ctx = zmq.asyncio.Context()
        self.socket: Optional[zmq.asyncio.Socket] = None
        self._serve_task: Optional[asyncio.Task] = None
        self._requests: set[asyncio.Task] = set()
        self._request_slots = asyncio.Semaphore(64)

    async def serve(self):
        if self.socket is not None:
            raise RuntimeError('RPC server is already serving')
        # A supervised service may restart this same object after close().
        # Finish cancellation before replacing its request-slot semaphore.
        old_tasks = [task for task in (self._serve_task, *self._requests)
                     if task is not None]
        if old_tasks:
            await asyncio.gather(*old_tasks, return_exceptions=True)
        self._requests.clear()
        self._request_slots = asyncio.Semaphore(64)
        if self.ctx.closed:
            self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.setsockopt(zmq.LINGER, 0)
        # Retry the bind with backoff. After a crash-restart the previous
        # process's socket can briefly linger, and a one-shot bind would raise
        # EADDRINUSE and kill the service permanently (crash-loop) instead of
        # recovering once the old bind is released.
        last_err: Optional[Exception] = None
        for attempt in range(10):
            try:
                self.socket.bind(self.address)
                break
            except zmq.ZMQError as ex:
                last_err = ex
                logging.warning('RPC bind %s in use (attempt %d), retrying: %s',
                                self.address, attempt + 1, ex)
                await asyncio.sleep(min(2 ** attempt, 8) * 0.25)  # ~0.25s → 2s
        else:
            self.socket.close(linger=0)
            self.socket = None
            self.ctx.term()
            raise last_err  # type: ignore[misc]
        self._serve_task = asyncio.create_task(self._serve_loop())

    async def _serve_loop(self):
        while True:
            acquired = False
            try:
                await self._request_slots.acquire()
                acquired = True
                frames = await self.socket.recv_multipart()
                task = asyncio.create_task(self._dispatch(frames))
                self._requests.add(task)
                task.add_done_callback(self._requests.discard)
                acquired = False  # _dispatch now owns/relinquishes the slot
            except (zmq.ZMQError, asyncio.CancelledError):
                break
            except Exception:
                logging.exception('RPCServer receive error')
            finally:
                if acquired:
                    self._request_slots.release()

    async def _dispatch(self, frames):
        try:
            client_id = frames[0]
            try:
                request = await asyncio.to_thread(unpack, frames[-1])
            except Exception as ex:
                await self._reject_request(client_id, frames[-1], ex)
                return
            await self._handle_request(client_id, request)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.exception('RPCServer dispatch failed')
        finally:
            self._request_slots.release()

    async def _reject_request(self, client_id: bytes, raw: bytes, exc: Exception):
        # The payload didn't deserialize, so the req_id has to be recovered
        # from the msgpack framing alone — ext types (where the poison lives)
        # are replaced with placeholders; plain fields like req_id survive.
        req_id = ''
        try:
            headers = msgpack.unpackb(
                raw, ext_hook=lambda code, data: None, raw=False, strict_map_key=False)
            if isinstance(headers, dict):
                req_id = headers.get('req_id', '') or ''
        except Exception:
            # Framing itself is broken: there is no req_id to route the reply
            # to — the client will discard it as stale and time out, but the
            # refusal is at least logged loudly by the caller.
            pass
        response = {
            'req_id': req_id,
            'error': True,
            'exc_type': type(exc).__name__,
            'exc_args': (str(exc),),
        }
        try:
            await self.socket.send_multipart([client_id, b'', pack(response)])
        except Exception as ex:
            logging.exception(f"RPCServer failed to send rejection response: {ex}")

    async def _handle_request(self, client_id: bytes, request: dict):
        request = request if isinstance(request, dict) else {}
        method_name = request.get('method', '')
        args = request.get('args', ())
        kwargs = request.get('kwargs', {})
        req_id = request.get('req_id', '')

        # Convert args from list to tuple if needed (msgpack gives us lists)
        if isinstance(args, list):
            args = tuple(args)

        try:
            # Resolve ONLY a single-segment method registered with @rpcmethod on
            # the API instance. Dotted traversal (e.g. trader.place_order_simple,
            # trader.client.ib.reqGlobalCancel) is refused — otherwise any local
            # client could walk the whole object graph and place live orders,
            # bypassing the proposal-approval gate that lives on the API wrapper.
            if not isinstance(method_name, str) or not method_name or '.' in method_name or method_name.startswith('_'):
                raise AttributeError(f'RPC method {method_name!r} is not permitted')
            method = getattr(self.instance, method_name, None)
            if method is None or not getattr(method, '_is_rpc_method', False):
                raise AttributeError(
                    f'RPC method {method_name!r} is not a registered @rpcmethod endpoint')

            result = method(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result

            response = {'req_id': req_id, 'result': result, 'error': False}
        except Exception as exc:
            logging.debug(f"RPCServer error handling {method_name}: {exc}")
            response = {
                'req_id': req_id,
                'error': True,
                'exc_type': type(exc).__name__,
                'exc_args': exc.args,
            }

        try:
            payload = pack(response)
        except Exception as exc:
            # A result that won't serialize must still produce an error reply,
            # not a silent client-side timeout.
            logging.exception(f"RPCServer failed to pack response for {method_name}: {exc}")
            payload = pack({
                'req_id': req_id,
                'error': True,
                'exc_type': type(exc).__name__,
                'exc_args': (str(exc),),
            })
        try:
            await self.socket.send_multipart([client_id, b'', payload])
        except Exception as exc:
            logging.exception(f"RPCServer failed to send response: {exc}")

    def close(self):
        if self._serve_task:
            self._serve_task.cancel()
        for task in tuple(self._requests):
            task.cancel()
        if self.socket is not None:
            self.socket.close(linger=0)
            self.socket = None
        self.ctx.term()

    async def aclose(self):
        """Close and drain canceled handlers before the owning loop stops."""
        tasks = [task for task in (self._serve_task, *self._requests)
                 if task is not None]
        self.close()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._serve_task = None
        self._requests.clear()


# ---------------------------------------------------------------------------
# RPCClient
# ---------------------------------------------------------------------------


# The well-known live service ports (trader/data/strategy RPC). A test that
# dials one of these with the stack up lands on the REAL services — observed
# 2026-07-16 (AUDIT_ROADMAP G7): a full pytest run produced a burst of IB
# error-200 contract lookups for the fixture conId 12345 in the LIVE
# trader_service log. Harmless that day (read-only resolves, fail-loudly), but
# a test that PLACED anything would go through the same door.
_LIVE_RPC_PORTS = frozenset({42001, 42003, 42005})
# The live MessageBus port carries STRATEGY SIGNALS — a test publishing to it
# could hand the live auto-executor a signal, which is worse than a stray
# resolve. Guarded in MessageBusClient.connect, which is what dials it.
_LIVE_BUS_PORTS = frozenset({42006})


def _refuse_live_ports_under_pytest(address: str, port: int) -> None:
    """Refuse RPC connections to the live service ports from inside pytest.

    Armed by ``MMR_PYTEST=1`` (set in tests/conftest.py before any trader
    import), so production behaviour is byte-identical — the check reads one
    env var and returns. In-process test servers are unaffected: they bind
    ephemeral ports from ``_free_port()``, never the live 42001/3/5.

    ``MMR_ALLOW_LIVE_PORTS=1`` is the deliberate escape hatch for an
    integration run that KNOWS it wants the live stack.
    """
    if _os.environ.get('MMR_PYTEST') != '1':
        return
    if _os.environ.get('MMR_ALLOW_LIVE_PORTS') == '1':
        return
    if (port in _LIVE_RPC_PORTS or port in _LIVE_BUS_PORTS) \
            and ('127.0.0.1' in address or 'localhost' in address):
        raise ConnectionError(
            f'refusing to connect to live service port {port} from inside pytest '
            f'(AUDIT_ROADMAP G7): with the stack up this request would land on the '
            f'REAL trader/strategy/data service. Point the test at an in-process '
            f'stub or an ephemeral port; set MMR_ALLOW_LIVE_PORTS=1 only for a '
            f'deliberate integration run.')

class RPCClient(Generic[T]):
    def __init__(
        self,
        zmq_server_address: str = 'tcp://127.0.0.1',
        zmq_server_port: int = 42000,
        timeout: Optional[int] = None,
        error_table: Optional[Dict[str, Exception]] = None,
        **kwargs,
    ):
        self.zmq_server_address = zmq_server_address
        self.zmq_server_port = zmq_server_port
        self.address = f"{zmq_server_address}:{zmq_server_port}"
        self.ctx = zmq.Context()  # sync context for sync calls
        self.socket: Optional[zmq.Socket] = None
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError('RPC timeout must be finite and positive')
        self.timeout = timeout if timeout is not None else 10.0
        self.is_setup: bool = False
        self._lock = threading.Lock()
        self.error_table = error_table

    def _configure_socket(self, socket):
        # LINGER=0: closing the socket immediately discards any queued request
        #   instead of leaking it for later redelivery (see _SyncMethodCall).
        # IMMEDIATE=1: never queue a send to a peer that has no live connection —
        #   an order to a down trader_service fails loudly rather than firing later.
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.IMMEDIATE, 1)
        socket.setsockopt(zmq.RCVTIMEO, math.ceil(self.timeout * 1000))
        socket.setsockopt(zmq.SNDTIMEO, math.ceil(self.timeout * 1000))

    async def connect(self, loop=None):
        logging.debug('trying RPCClient.connect()')
        _refuse_live_ports_under_pytest(self.zmq_server_address, self.zmq_server_port)
        if not self._lock.acquire(timeout=self.timeout):
            raise TimeoutError('RPC connection is busy')
        try:
            if self.socket is not None:
                self.socket.close(linger=0)
            if self.ctx.closed:
                self.ctx = zmq.Context()
            self.socket = self.ctx.socket(zmq.DEALER)
            self._configure_socket(self.socket)
            self.socket.connect(self.address)
            self.is_setup = True
        finally:
            self._lock.release()

    def wait_ready(self, timeout: float = 2.0) -> bool:
        """True once a server peer is connected (the socket is writable).

        With IMMEDIATE=1 the DEALER only reports POLLOUT when a live peer
        exists, so this is an honest liveness probe that sends nothing.
        Lets callers fail in ~2s with a clear message instead of every RPC
        burning its full timeout against a service that isn't running.
        """
        if not (self.socket and self.is_setup):
            return False
        try:
            return bool(self.socket.poll(max(1, int(timeout * 1000)), zmq.POLLOUT))
        except zmq.ZMQError:
            return False

    def _require_socket(self):
        if not (self.socket and self.is_setup):
            raise ConnectionError('not connected')
        return self.socket

    def _reset_socket(self):
        """Drop and recreate the DEALER socket with a fresh ZMQ identity.

        Called after a timeout/send failure so a stale in-flight request cannot
        be redelivered on reconnect and buffered mismatched replies are cleared.
        Must be called while holding self._lock.
        """
        old = self.socket
        self.socket = None
        if old is not None:
            try:
                old.close(linger=0)
            except Exception:
                pass
        try:
            self.socket = self.ctx.socket(zmq.DEALER)
            self._configure_socket(self.socket)
            self.socket.connect(self.address)
        except Exception:
            self.is_setup = False
            raise

    def rpc(self, return_type: Optional[Type] = None, *, timeout: Optional[float] = None) -> T:
        if timeout is not None and (not math.isfinite(timeout) or timeout <= 0):
            raise ValueError('RPC timeout must be finite and positive')
        if self.socket and self.is_setup:
            return _SyncMethodCall(
                self, self.timeout if timeout is None else timeout,
                return_type=return_type,
                error_table=self.error_table,
            )  # type: ignore
        else:
            raise ConnectionError('not connected')

    def arpc(self, return_type: Optional[Type] = None) -> T:
        if self.socket and self.is_setup:
            return _SyncMethodCall(
                self, self.timeout,
                return_type=return_type,
                error_table=self.error_table,
            )  # type: ignore
        else:
            raise ConnectionError('not connected')

    def consume(self, future):
        if future is asyncio.Future:
            return future.result()
        else:
            return future

    def close(self):
        with self._lock:
            if self.socket is not None:
                self.socket.close(linger=0)
                self.socket = None
            self.is_setup = False
            self.ctx.term()


# ---------------------------------------------------------------------------
# MessageBusServer  (raw zmq ROUTER, replacing aiozmq.create_zmq_stream)
# ---------------------------------------------------------------------------

class MessageBusServer:
    """Best-effort topic router with bounded work and explicit worker lifetime."""

    def __init__(self, zmq_address: str, zmq_port: int, queue_size: int = 1024, **kwargs):
        if queue_size <= 0:
            raise ValueError('message bus queue_size must be positive')
        self.zmq_address, self.zmq_port = zmq_address, zmq_port
        self.clients = {}
        self.server = None
        self.ctx = None
        self.read_thread = None
        self.sentinel_flag = True
        self.wait_handle = threading.Event()
        self._stop_event = threading.Event()
        self._queue = queue.Queue(maxsize=queue_size)
        self._startup_error = None

    @staticmethod
    def message_subscribe(topic):
        return [topic.encode() if isinstance(topic, str) else topic, b'subscribe']

    @staticmethod
    def message_disconnect():
        return [b'disconnect', b'disconnect']

    @staticmethod
    def message(topic, val: Any):
        return [topic.encode() if isinstance(topic, str) else topic, pack(val)]

    def put(self, topic_item):
        if self.read_thread is None or not self.read_thread.is_alive() or self._stop_event.is_set():
            raise RuntimeError('message bus is not running')
        self._queue.put_nowait(topic_item)

    def write(self, topic: str, val: Any):
        topic_bytes, payload = self.message(topic, val)
        self.put((b'', topic_bytes, payload))

    def _route(self, client_id, topic, val):
        if val == b'subscribe':
            self.clients[(client_id, topic)] = True
            return
        if val == b'disconnect':
            for key in list(self.clients):
                if key[0] == client_id:
                    self.clients.pop(key)
            return
        for cid, subscribed in list(self.clients):
            if cid != client_id and subscribed == topic:
                try:
                    self.server.send_multipart([cid, topic, val], flags=zmq.NOBLOCK)
                except zmq.Again:
                    logging.error('message bus subscriber queue full; message rejected')

    def _run(self):
        try:
            self.ctx = zmq.Context()
            self.server = self.ctx.socket(zmq.ROUTER)
            self.server.setsockopt(zmq.LINGER, 0)
            self.server.bind(f'{self.zmq_address}:{self.zmq_port}')
            self.wait_handle.set()
            while not self._stop_event.is_set():
                if self.server.poll(50, zmq.POLLIN):
                    frames = self.server.recv_multipart()
                    if len(frames) >= 3:
                        self._route(frames[0], frames[1], frames[2])
                for _ in range(128):
                    try:
                        item = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    try:
                        self._route(*item)
                    finally:
                        self._queue.task_done()
        except BaseException as ex:
            self._startup_error = ex
            logging.exception('message bus worker failed')
        finally:
            if self.server is not None:
                self.server.close(linger=0)
                self.server = None
            if self.ctx is not None:
                self.ctx.term()
                self.ctx = None
            self.sentinel_flag = True
            self.wait_handle.set()

    def _start(self, timeout):
        if self.read_thread is not None and self.read_thread.is_alive():
            raise RuntimeError('message bus already started')
        self.wait_handle.clear()
        self._stop_event.clear()
        self.clients.clear()
        self._startup_error = None
        self.sentinel_flag = False
        self.read_thread = threading.Thread(target=self._run, daemon=True, name='mmr-messagebus')
        self.read_thread.start()
        if not self.wait_handle.wait(timeout):
            self._stop_event.set()
            raise TimeoutError('message bus startup timed out')
        if self._startup_error is not None:
            self.read_thread.join(timeout)
            raise RuntimeError('message bus could not start') from self._startup_error

    async def start(self, timeout: float = 5.0):
        await asyncio.to_thread(self._start, timeout)

    def stop(self, timeout: float = 5.0):
        self._stop_event.set()
        if self.read_thread is not None and self.read_thread is not threading.current_thread():
            self.read_thread.join(timeout)
            if self.read_thread.is_alive():
                raise TimeoutError('message bus shutdown timed out')
        while True:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                break

    async def wait(self):
        while not self.sentinel_flag:
            await asyncio.sleep(.1)


# ---------------------------------------------------------------------------
# MessageBusClient  (raw zmq DEALER, replacing aiozmq.create_zmq_stream)
# ---------------------------------------------------------------------------

class MessageBusClient(Generic[T]):
    def __init__(
        self,
        zmq_address: str,
        zmq_port: int,
    ):
        self.zmq_address = zmq_address
        self.zmq_port = zmq_port
        self.client: Optional[zmq.asyncio.Socket] = None
        self.ctx = zmq.asyncio.Context()

    async def connect(self) -> None:
        _refuse_live_ports_under_pytest(self.zmq_address, self.zmq_port)
        if self.client is not None:
            self.client.close(linger=0)
        if self.ctx.closed:
            self.ctx = zmq.asyncio.Context()
        self.client = self.ctx.socket(zmq.DEALER)
        self.client.setsockopt(zmq.LINGER, 0)
        self.client.connect(f'{self.zmq_address}:{self.zmq_port}')

    async def __iterable_read(self):
        while True:
            try:
                yield await self.read()
            except zmq.ZMQError:
                return
            except DillDeserializationError as ex:
                # One poisoned message must not tear down the whole signal
                # subscription — log loudly and keep reading.
                logging.error(f'MessageBusClient: dropping rejected message: {ex}')
                continue
            except Exception as ex:
                raise ex

    def subscribe(
        self,
        topic: str,
        observer: abc.ObserverBase[T],
    ) -> abc.DisposableBase:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message_subscribe(topic))
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        return ObservableIterHelper(loop).from_aiter(self.__iterable_read()).subscribe(observer)

    def subscribe_topic(self, topic: str) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message_subscribe(topic))

    def write(self, topic: str, val: T) -> None:
        if not self.client:
            raise ValueError('client is not initialized')

        self.client.send_multipart(MessageBusServer.message(topic, val))

    async def disconnect(self) -> None:
        client, self.client = self.client, None
        try:
            if client is not None:
                # Best-effort subscription cleanup, with a bounded flush. The
                # local socket/context must close even if its peer has gone.
                await asyncio.wait_for(
                    client.send_multipart(MessageBusServer.message_disconnect()), .25)
                await asyncio.sleep(.05)
        except (TimeoutError, zmq.ZMQError):
            logging.warning('message bus disconnect notification could not be sent')
        finally:
            if client is not None:
                client.close(linger=0)
            self.ctx.term()

    async def read(self) -> T:
        if not self.client:
            raise ValueError('client is not initialized')

        frames = await self.client.recv_multipart()
        # frames: [topic, val]
        return _decode_payload(frames[-1])


# ---------------------------------------------------------------------------
# TopicPubSub  (raw zmq PUB/SUB replacing aiozmq.rpc pubsub)
# ---------------------------------------------------------------------------

class TopicPubSub(Generic[T]):
    def __init__(
        self,
        zmq_pubsub_server_address: str,
        zmq_pubsub_server_port: int,
        **kwargs,
    ):
        self.zmq_server_address = zmq_pubsub_server_address
        self.zmq_server_port = zmq_pubsub_server_port
        self.handler: Optional[_Handler[T]] = None
        self.zmq_subscriber: Optional[zmq.asyncio.Socket] = None
        self.zmq_publisher: Optional[zmq.Socket] = None
        self.lock = threading.Lock()
        self._sub_task: Optional[asyncio.Task] = None
        self._sub_ctx: Optional[zmq.asyncio.Context] = None
        self._pub_ctx: Optional[zmq.Context] = None

    @contextmanager
    def aquire_timeout(self, lock: threading.Lock, timeout):
        result = lock.acquire(timeout=timeout)
        try:
            yield result
        finally:
            if result:
                lock.release()

    async def subscriber(
        self,
        topic: str = 'default',
    ) -> rx.Observable[T]:
        if not self.handler:
            self.handler = _Handler[T]()
            self._sub_ctx = zmq.asyncio.Context()
            self.zmq_subscriber = self._sub_ctx.socket(zmq.SUB)
            self.zmq_subscriber.connect(f'{self.zmq_server_address}:{self.zmq_server_port}')
            self.zmq_subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)

            self._sub_task = asyncio.create_task(self._subscriber_loop())

        return self.handler.get_subject()

    async def _subscriber_loop(self):
        while True:
            try:
                frames = await self.zmq_subscriber.recv_multipart()
                # frames: [topic, payload]
                if len(frames) >= 2:
                    payload = frames[1]
                    try:
                        obj = _decode_payload(payload)
                    except DillDeserializationError as ex:
                        # A poisoned message must not on_error the subject —
                        # that would terminate every downstream ticker
                        # subscription. Drop it loudly and keep reading.
                        logging.error(f'TopicPubSub: dropping rejected message: {ex}')
                        continue
                    self.handler.on_message(obj)
            except zmq.ZMQError:
                break
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logging.exception(f"TopicPubSub subscriber error: {ex}")
                self.handler.on_throw(ex)

    def subscriber_close(self):
        if self._sub_task:
            self._sub_task.cancel()
            self._sub_task = None
        if self.zmq_subscriber:
            logging.debug('subscriber_close()')
            self.zmq_subscriber.close(linger=0)
            self.zmq_subscriber = None
        if self._sub_ctx is not None:
            self._sub_ctx.term()
            self._sub_ctx = None
        # Closing this transport has historically been cancellation, not a
        # stream-completion event. Keep that contract while allowing a fresh
        # subject/socket on an explicit subsequent subscription.
        self.handler = None

    async def publisher(
        self,
        obj: T,
        topic: str = 'default',
    ):
        try:
            if not self.zmq_publisher:
                self._pub_ctx = zmq.Context()
                self.zmq_publisher = self._pub_ctx.socket(zmq.PUB)
                self.zmq_publisher.bind(f'{self.zmq_server_address}:{self.zmq_server_port}')
                # Give subscribers time to connect
                await asyncio.sleep(0.1)

            topic_bytes = topic.encode() if isinstance(topic, str) else topic
            payload = pack(obj)
            self.zmq_publisher.send_multipart([topic_bytes, payload])
        except Exception as e:
            logging.exception(e)
            logging.debug(
                f'self.zmq_server_address: {self.zmq_server_address}, '
                f'self.zmq_server_port: {self.zmq_server_port}'
            )
            raise e

    async def publisher_close(self):
        if self.zmq_publisher:
            logging.debug('publisher_close()')
            self.zmq_publisher.close(linger=0)
            self.zmq_publisher = None
        if self._pub_ctx is not None:
            self._pub_ctx.term()
            self._pub_ctx = None


# ---------------------------------------------------------------------------
# MultithreadedTopicPubSub
# ---------------------------------------------------------------------------

class MultithreadedTopicPubSub(Generic[T], TopicPubSub[T]):
    """A bounded publisher queue whose socket is owned by its worker thread."""

    def __init__(self, zmq_pubsub_server_address: str,
                 zmq_pubsub_server_port: int, queue_size: int = 1024, **kwargs):
        super().__init__(zmq_pubsub_server_address, zmq_pubsub_server_port)
        if queue_size <= 0:
            raise ValueError('publisher queue_size must be positive')
        self.wait_handle = threading.Event()
        self._queue = queue.Queue(maxsize=queue_size)
        self._stop_event = threading.Event()
        self._thread = None
        self._startup_error = None
        self.rejected_messages = 0

    def put(self, topic_item: Tuple[str, T]):
        if self._thread is None or not self._thread.is_alive() or self._stop_event.is_set():
            raise RuntimeError('publisher is not running')
        try:
            self._queue.put_nowait(topic_item)
        except queue.Full:
            self.rejected_messages += 1
            logging.error('publisher queue full: rejecting message (%s rejected)', self.rejected_messages)
            raise

    async def publisher_close(self):
        # The inherited API must also respect the worker's socket ownership.
        await asyncio.to_thread(self.stop)

    def _publisher_loop(self):
        try:
            self._pub_ctx = zmq.Context()
            self.zmq_publisher = self._pub_ctx.socket(zmq.PUB)
            self.zmq_publisher.setsockopt(zmq.LINGER, 0)
            self.zmq_publisher.bind(f'{self.zmq_server_address}:{self.zmq_server_port}')
            self.wait_handle.set()
            while not self._stop_event.is_set():
                try:
                    topic, value = self._queue.get(timeout=.1)
                except queue.Empty:
                    continue
                try:
                    topic_bytes = topic.encode() if isinstance(topic, str) else topic
                    self.zmq_publisher.send_multipart([topic_bytes, pack(value)])
                except Exception:
                    logging.exception('publisher could not deliver message')
                finally:
                    self._queue.task_done()
        except BaseException as ex:
            self._startup_error = ex
            logging.exception('publisher worker failed')
        finally:
            if self.zmq_publisher is not None:
                self.zmq_publisher.close(linger=0)
                self.zmq_publisher = None
            if self._pub_ctx is not None:
                self._pub_ctx.term()
                self._pub_ctx = None
            self.wait_handle.set()

    def start(self, timeout: float = 5.0):
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError('publisher already started')
        self.wait_handle.clear()
        self._stop_event.clear()
        self._startup_error = None
        self._thread = threading.Thread(target=self._publisher_loop, daemon=True,
                                        name='mmr-publisher')
        self._thread.start()
        if not self.wait_handle.wait(timeout):
            self._stop_event.set()
            raise TimeoutError('publisher startup timed out')
        if self._startup_error is not None:
            self._thread.join(timeout)
            raise RuntimeError('publisher could not start') from self._startup_error

    def stop(self, timeout: float = 5.0):
        self._stop_event.set()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(timeout)
            if self._thread.is_alive():
                raise TimeoutError('publisher shutdown timed out')
        # Never replay stale ticks when the same publisher is restarted.
        while True:
            try:
                self._queue.get_nowait()
                self._queue.task_done()
            except queue.Empty:
                break
