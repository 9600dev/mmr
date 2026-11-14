"""Adversarial tests for the dill deserialization boundary.

The ZMQ message layer accepts dill blobs (EXT_OBJECT, plus a raw-dill legacy
fallback) because the live hot path — Ticker ticks, SuccessFail[Trade] RPC
results, Contract args, MessageBus Signals — travels that way. dill.loads is
arbitrary code execution on untrusted input, so a restricted unpickler is the
ACTUAL boundary: it refuses hostile globals in find_class, which runs BEFORE
any REDUCE opcode can call them. These tests craft malicious __reduce__
payloads and assert the refusal happens before the embedded gadget executes
(a sentinel file that the gadget would create must be absent).
"""

import os
import pickle
import tempfile
import time

import dill
import msgpack
import numpy as np
import pandas as pd
import pytest

import wire_corpus
from trader.messaging import clientserver as cs
from trader.messaging.clientserver import (
    DillDeserializationError,
    EXT_OBJECT,
    _decode_payload,
    _safe_dill_loads,
    pack,
    set_dill_whitelist,
    unpack,
)


@pytest.fixture
def sentinel(tmp_path):
    """A path the malicious gadgets would create if their code ran. The test
    asserts it stays absent — proof refusal happened before execution."""
    p = tmp_path / 'pwned.txt'
    if p.exists():
        p.unlink()
    yield p
    if p.exists():
        p.unlink()


# ---------------------------------------------------------------------------
# Malicious payload factories. Each __reduce__ returns a callable + args that,
# if unpickling executed them, would create the sentinel file (or worse).
# ---------------------------------------------------------------------------

def _os_system_pickle(sentinel):
    class _Evil:
        def __reduce__(self):
            return (os.system, (f'touch {sentinel}',))
    return dill.dumps(_Evil())


def _eval_pickle(sentinel):
    class _Evil:
        def __reduce__(self):
            return (eval, (f"__import__('os').system('touch {sentinel}')",))
    return dill.dumps(_Evil())


def _subprocess_pickle(sentinel):
    import subprocess

    class _Evil:
        def __reduce__(self):
            return (subprocess.Popen, (['touch', str(sentinel)],))
    return dill.dumps(_Evil())


def _rmtree_pickle(target):
    import shutil

    class _Evil:
        def __reduce__(self):
            return (shutil.rmtree, (str(target),))
    return dill.dumps(_Evil())


def _as_ext_object(blob: bytes) -> bytes:
    """Wrap a raw dill blob as the EXT_OBJECT payload unpack() sees on the wire."""
    ext = msgpack.ExtType(EXT_OBJECT, blob)
    return msgpack.packb(ext, use_bin_type=True)


ALL_GADGETS = ('os_system', 'eval', 'subprocess', 'rmtree')


def _make_gadget(name, sentinel):
    if name == 'os_system':
        return _os_system_pickle(sentinel)
    if name == 'eval':
        return _eval_pickle(sentinel)
    if name == 'subprocess':
        return _subprocess_pickle(sentinel)
    if name == 'rmtree':
        # rmtree a decoy dir so "execution" would be observable; the sentinel
        # is a marker file inside it.
        decoy = sentinel.parent / 'decoy'
        decoy.mkdir(exist_ok=True)
        (decoy / 'marker').write_text('x')
        return _rmtree_pickle(decoy)
    raise ValueError(name)


class TestMaliciousPayloadsRefused:
    def setup_method(self):
        set_dill_whitelist(None)

    def teardown_method(self):
        set_dill_whitelist(None)

    @pytest.mark.parametrize('gadget', ALL_GADGETS)
    def test_ext_object_path_refuses_before_execution(self, gadget, sentinel):
        """The primary EXT_OBJECT route (every RPC/PubSub payload) refuses the
        gadget, and refusal is BEFORE execution — sentinel absent."""
        blob = _make_gadget(gadget, sentinel)
        with pytest.raises(DillDeserializationError):
            unpack(_as_ext_object(blob))
        assert not sentinel.exists(), f'{gadget} executed via EXT_OBJECT path'

    @pytest.mark.parametrize('gadget', ALL_GADGETS)
    def test_raw_dill_fallback_path_refuses_before_execution(self, gadget, sentinel):
        """The raw-dill legacy fallback (_decode_payload on a non-msgpack blob)
        must route through the same boundary — it does not bypass policy."""
        blob = _make_gadget(gadget, sentinel)
        with pytest.raises(DillDeserializationError):
            _decode_payload(blob)
        assert not sentinel.exists(), f'{gadget} executed via raw-dill fallback'

    @pytest.mark.parametrize('gadget', ALL_GADGETS)
    def test_safe_dill_loads_refuses_before_execution(self, gadget, sentinel):
        blob = _make_gadget(gadget, sentinel)
        with pytest.raises(DillDeserializationError):
            _safe_dill_loads(blob)
        assert not sentinel.exists(), f'{gadget} executed via _safe_dill_loads'

    def test_error_names_the_offending_global(self, sentinel):
        blob = _os_system_pickle(sentinel)
        with pytest.raises(DillDeserializationError) as exc:
            unpack(_as_ext_object(blob))
        # posix.system on this platform, os.system elsewhere — either way the
        # offending module.name is named so an operator can see what was tried.
        assert 'system' in str(exc.value)


class TestCallableGadgetsRefused:
    """The allowlist gate must be TYPE-first: prefix-matching is correct for the
    CLASS of a reconstructed instance, but a plain FUNCTION under an allowed
    prefix (pandas.read_pickle, pandas.read_parquet, numpy.load) is a
    code-execution / side-effect gadget the moment REDUCE calls it. The pre-fix
    prefix allowlist admitted every callable of an allowed package — these
    tests would PASS (gadget executes) under that logic and must FAIL-to-refuse
    now. RPCClient is a `trader`-prefix class whose construction opens a live
    ZMQ socket and is never wire content — it is denied explicitly."""

    def setup_method(self):
        set_dill_whitelist(None)

    def teardown_method(self):
        set_dill_whitelist(None)

    def _reduce_pickle(self, target, args):
        class _Evil:
            def __reduce__(self):
                return (target, args)
        return dill.dumps(_Evil())

    def test_pandas_read_pickle_refused_before_reentrant_escape(self, sentinel, tmp_path):
        # pandas.read_pickle re-enters UNRESTRICTED stdlib pickle.load, so a
        # crafted inner pickle would run arbitrary code — the full sandbox
        # escape. The inner blob, if loaded, creates the sentinel; the gate must
        # refuse read_pickle BEFORE it is ever called.
        import pickle
        import os as _os
        inner = tmp_path / 'inner.pkl'

        class _InnerEvil:
            def __reduce__(self):
                return (_os.system, (f'touch {sentinel}',))
        inner.write_bytes(pickle.dumps(_InnerEvil()))

        blob = self._reduce_pickle(pd.read_pickle, (str(inner),))
        with pytest.raises(DillDeserializationError):
            unpack(_as_ext_object(blob))
        assert not sentinel.exists(), 'pandas.read_pickle re-entrant escape executed'

    def test_numpy_load_refused(self, sentinel, tmp_path):
        # numpy.load(allow_pickle=...) is likewise a file-reading, pickle-
        # re-entering gadget under the numpy prefix — refuse the callable.
        inner = tmp_path / 'inner.npy'
        blob = self._reduce_pickle(np.load, (str(inner),))
        with pytest.raises(DillDeserializationError):
            unpack(_as_ext_object(blob))
        assert not sentinel.exists(), 'numpy.load executed'

    def test_pandas_read_parquet_refused(self, sentinel, tmp_path):
        inner = tmp_path / 'inner.parquet'
        blob = self._reduce_pickle(pd.read_parquet, (str(inner),))
        with pytest.raises(DillDeserializationError):
            unpack(_as_ext_object(blob))
        assert not sentinel.exists(), 'pandas.read_parquet executed'

    def test_rpcclient_construction_refused(self):
        # RPCClient is a class under the `trader` prefix; the type-first rule
        # would otherwise admit it, but a crafted (RPCClient, args) REDUCE would
        # run __init__ and stand up a live ZMQ socket. It is denied explicitly.
        from trader.messaging.clientserver import RPCClient
        blob = self._reduce_pickle(RPCClient, ('tcp://127.0.0.1', 42099))
        with pytest.raises(DillDeserializationError):
            unpack(_as_ext_object(blob))


class TestEscapeHatchesClosed:
    """The old whitelist checked the top-level type AFTER dill.loads ran, so
    embedded gadgets already executed. These target the classic pickle escapes
    that a naive allowlist misses."""

    def setup_method(self):
        set_dill_whitelist(None)

    def teardown_method(self):
        set_dill_whitelist(None)

    def test_dunder_getattr_gadget_refused(self, sentinel):
        # getattr is allowed for legit non-dunder classmethod lookups, but
        # dunder traversal (obj.__class__.__bases__, cls.__globals__, ...) is
        # the escape route — it must be refused.
        class _Evil:
            def __reduce__(self):
                return (getattr, (dict, '__class__'))
        with pytest.raises(DillDeserializationError):
            _decode_payload(dill.dumps(_Evil()))

    def test_code_object_construction_refused(self):
        import types

        def _f():
            return None

        class _Evil:
            def __reduce__(self):
                return (types.FunctionType, (_f.__code__, {}))
        with pytest.raises(DillDeserializationError):
            _decode_payload(dill.dumps(_Evil()))

    def test_import_module_gadget_refused(self):
        # dill._dill._import_module can pull in any module — only the pure-data
        # helpers (_create_namedtuple, _create_array) are allowed from _dill.
        class _Evil:
            def __reduce__(self):
                import dill._dill as _d
                return (_d._import_module, ('os',))
        with pytest.raises(DillDeserializationError):
            _decode_payload(dill.dumps(_Evil()))

    def test_legit_non_dunder_getattr_still_works(self):
        # ZoneInfo pickles through getattr(cls, '_unpickle') — a NON-dunder
        # classmethod lookup. That legit use must survive.
        import datetime as dt
        from zoneinfo import ZoneInfo
        d = dt.datetime.now(ZoneInfo('America/New_York'))
        out = _decode_payload(dill.dumps(d))
        assert out.tzinfo is not None


class TestPolicyLayersStack:
    """The restricted unpickler is the always-on inner boundary; the optional
    whitelist and MMR_DILL_STRICT remain as additional outer layers with the
    same semantics as before."""

    def setup_method(self):
        set_dill_whitelist(None)

    def teardown_method(self):
        set_dill_whitelist(None)

    def test_whitelist_still_rejects_allowed_prefix_but_unregistered_type(self):
        # A dict passes the restricted unpickler (no hostile globals), but an
        # explicit whitelist of [list] must still reject it at the outer layer.
        set_dill_whitelist([list])
        blob = dill.dumps({'x': 1})
        with pytest.raises(DillDeserializationError):
            unpack(_as_ext_object(blob))

    def test_strict_mode_disables_dill_entirely(self, monkeypatch):
        import trader.messaging.clientserver as cs
        monkeypatch.setattr(cs, 'DILL_STRICT_MODE', True)
        blob = dill.dumps({'x': 1})
        with pytest.raises(DillDeserializationError):
            cs.unpack(_as_ext_object(blob))

    def test_undecodable_payload_raises_dill_error(self):
        # Neither valid msgpack nor an accepted dill blob → loud refusal, not a
        # silent None, so read loops can drop-and-continue.
        with pytest.raises(DillDeserializationError):
            _decode_payload(b'\xff\xff not msgpack not dill \x00\x01')


class TestGadgetsDoNotExecuteLiveProbe:
    """Live adversarial probe, 2026-07-27, run against the running container
    and over the live RPC socket. Each gadget's payload, IF EXECUTED, writes a
    marker file; the probe asserted on the marker's ABSENCE rather than on the
    exception, because "it raised" and "it raised before running the payload"
    are different claims and only the second one is security.

    All four refused pre-execution, the msgpack ExtType wire path refused, the
    live trader_service returned a structured DillDeserializationError instead
    of hanging or dying, and it was still answering RPC afterwards.
    """

    def _detonate(self, tmp_path, gadget_factory):
        import dill
        from trader.messaging.clientserver import _safe_dill_loads
        marker = tmp_path / 'DETONATED'
        payload = dill.dumps(gadget_factory(str(marker)))
        with pytest.raises(Exception):
            _safe_dill_loads(payload)
        return marker.exists()

    def test_os_system_gadget_does_not_run(self, tmp_path):
        import os

        class G:
            def __init__(self, path): self.path = path
            def __reduce__(self): return (os.system, (f'touch {self.path}',))

        assert self._detonate(tmp_path, G) is False

    def test_subprocess_gadget_does_not_run(self, tmp_path):
        import subprocess

        class G:
            def __init__(self, path): self.path = path
            def __reduce__(self): return (subprocess.call, (['touch', self.path],))

        assert self._detonate(tmp_path, G) is False

    def test_eval_gadget_does_not_run(self, tmp_path):
        class G:
            def __init__(self, path): self.path = path
            def __reduce__(self):
                return (eval, (f"__import__('os').system('touch {self.path}')",))

        assert self._detonate(tmp_path, G) is False

    def test_legitimate_payloads_still_load(self):
        """The other half: a boundary that refuses everything is not a
        boundary, it is an outage."""
        import datetime as dt
        import dill
        from trader.messaging.clientserver import _safe_dill_loads
        for obj in ({'a': 1}, [1, 2, 3], dt.datetime(2026, 1, 1), 'x', 42):
            assert _safe_dill_loads(dill.dumps(obj)) == obj


# ---------------------------------------------------------------------------
# 2026-11-14 — docs/AUDIT_2026-11-14.md B2.1.
#
# The gate above policed which NAMES a pickle may fetch (by module prefix) and
# trusted everything a fetched object was then used for. A reviewer drove the
# real unpack() to a file write, a file truncation, an arbitrary call and a
# setattr on a live class; a fifth payload found while fixing those ran a
# command. The boundary now checks each instruction that does work (REDUCE,
# NEWOBJ, BUILD, getattr) against exact allowlists. Every test below asserts
# on the SIDE EFFECT as well as the exception: "it raised" and "it raised
# before doing anything" are different claims.
# ---------------------------------------------------------------------------

class _Call:
    """Pickles as REDUCE(f, args). ``f`` may itself be a _Call, so calls chain
    exactly as a hand-written pickle would."""

    def __init__(self, f, *args):
        self.f, self.args = f, args

    def __call__(self, *args, **kwargs):
        raise RuntimeError('only ever called by an unpickler')

    def __reduce__(self):
        return (self.f, self.args)


def _getattr(obj, name):
    return _Call(getattr, obj, name)


def _reduce(obj) -> bytes:
    return pickle.dumps(obj, protocol=4)


def _global(module: str, name: str) -> bytes:
    """STACK_GLOBAL module.name"""
    return (b'\x8c' + bytes([len(module)]) + module.encode()
            + b'\x8c' + bytes([len(name)]) + name.encode() + b'\x93')


def _push(obj) -> bytes:
    """The opcodes that push a plain value, without PROTO / FRAME / STOP."""
    body = pickle.dumps(obj, protocol=4)[2:-1]
    return body[9:] if body.startswith(b'\x95') else body


def _program(*parts: bytes) -> bytes:
    return b'\x80\x04' + b''.join(parts) + b'.'


_REDUCE, _BUILD, _NEWOBJ, _SETITEM, _APPEND = b'R', b'b', b'\x81', b's', b'a'


def _refused(blob: bytes) -> str:
    """The payload must be refused on every route into the unpickler; returns
    the refusal text."""
    with pytest.raises(DillDeserializationError):
        _decode_payload(blob)
    with pytest.raises(DillDeserializationError):
        _safe_dill_loads(blob)
    with pytest.raises(DillDeserializationError) as refusal:
        unpack(msgpack.packb({'method': 'ping', 'args': [msgpack.ExtType(EXT_OBJECT, blob)],
                              'kwargs': {}, 'req_id': 'x'}, use_bin_type=True))
    return str(refusal.value)


class TestReviewerPayloads:
    """The payloads from the 2026-11-14 review, verbatim in substance. Before
    the fix each of these returned normally from unpack()."""

    def setup_method(self):
        set_dill_whitelist(None)

    def test_unbound_method_through_getattr_cannot_write_a_file(self, tmp_path):
        # getattr(numpy.ndarray, 'tofile')(arr, path): attacker-chosen bytes
        # at an attacker-chosen path.
        target = tmp_path / 'written_by_tofile.py'
        arr = np.frombuffer(b"print('attacker controlled bytes')\n", dtype=np.uint8)
        text = _refused(_reduce(_Call(_getattr(np.ndarray, 'tofile'), arr, str(target))))
        assert 'tofile' in text
        assert not target.exists()

    def test_side_effecting_constructor_cannot_truncate_a_file(self, tmp_path):
        # numpy.memmap(path, 'uint8', 'w+', 0, (1,)): no getattr at all — a
        # class under an allowed prefix whose constructor opens a file 'w+'.
        target = tmp_path / 'pretend_mmr.duckdb'
        target.write_bytes(b'D' * 4096)
        text = _refused(_reduce(_Call(np.memmap, str(target), 'uint8', 'w+', 0, (1,))))
        assert 'memmap' in text
        assert target.read_bytes() == b'D' * 4096

    def test_dataframe_eval_cannot_run_an_expression(self, tmp_path):
        target = tmp_path / 'created_by_eval'
        expr = f'a.__class__.__init__.__globals__.get("sys").modules.get("os").mkdir("{target}")'
        frame = _Call(pd.DataFrame, {'a': [1]})
        _refused(_reduce(_Call(_getattr(pd.DataFrame, 'eval'), frame, expr)))
        assert not target.exists()

    def test_build_cannot_setattr_on_the_risk_limits_class(self):
        # GLOBAL RiskLimits; (None, {field: 1e18}); BUILD — setattr on the live
        # CLASS, so every later RiskLimits() carried the attacker's default.
        from trader.trading.risk_gate import RiskLimits
        before = RiskLimits.max_position_size_pct
        _refused(_program(_global('trader.trading.risk_gate', 'RiskLimits'),
                          _push((None, {'max_position_size_pct': 1e18})), _BUILD))
        assert RiskLimits.max_position_size_pct == before
        assert RiskLimits().max_position_size_pct == before

    @pytest.mark.parametrize('module, name, attribute', [
        ('trader.common.reactivex', 'SuccessFail', 'is_success'),
        ('ib_async.contract', 'Contract', 'conId'),
        ('trader.trading.strategy', 'Signal', 'quantity'),
        ('builtins', 'ValueError', 'pwned'),
    ])
    def test_build_cannot_setattr_on_an_allowlisted_class_either(self, module, name, attribute):
        # RiskLimits is refused by name now. The property that matters is the
        # one that holds for classes a pickle IS allowed to name.
        import importlib
        cls = getattr(importlib.import_module(module), name)
        missing = object()
        before = cls.__dict__.get(attribute, missing)
        for state in ((None, {attribute: 1e18}), {attribute: 1e18}):
            text = _refused(_program(_global(module, name), _push(state), _BUILD))
            assert 'BUILD' in text
            assert cls.__dict__.get(attribute, missing) is before

    def test_create_namedtuple_cannot_fetch_an_arbitrary_class(self, tmp_path):
        # Found while fixing the four above. dill._dill._create_namedtuple(
        # name, fields, module) imports `module` and returns ANY class in it;
        # the old gate listed it as a pure-data helper. With subprocess.Popen
        # that was command execution.
        import dill._dill as dill_internals
        marker = tmp_path / 'ran'
        popen = _Call(dill_internals._create_namedtuple, 'Popen', (), 'subprocess')
        text = _refused(_reduce(_Call(popen, ['touch', str(marker)])))
        assert 'subprocess.Popen' in text
        time.sleep(0.2)     # a spawned `touch` would need a moment
        assert not marker.exists()

    def test_create_namedtuple_cannot_fetch_a_file_opening_class(self, tmp_path):
        # Same hole, synchronous side effect: logging.FileHandler(path)
        # creates the file in its constructor.
        import dill._dill as dill_internals
        marker = tmp_path / 'handler.log'
        handler = _Call(dill_internals._create_namedtuple, 'FileHandler', (), 'logging')
        _refused(_reduce(_Call(handler, str(marker))))
        assert not marker.exists()

    def test_create_array_cannot_call_its_first_argument(self, tmp_path):
        # dill._dill._create_array(f, args, state) calls f(*args).
        import dill._dill as dill_internals
        target = tmp_path / 'pretend.duckdb'
        target.write_bytes(b'D' * 4096)
        _refused(_reduce(_Call(dill_internals._create_array, np.memmap,
                               (str(target), 'uint8', 'w+', 0, (1,)), None)))
        assert target.read_bytes() == b'D' * 4096


class TestSharedObjectsCannotBeMutated:
    """BUILD writes state onto whatever is under it on the stack. A class was
    the reported case; a shared INSTANCE that an allowed call returns is the
    same hole one step along — and it persists for the life of the process."""

    def setup_method(self):
        set_dill_whitelist(None)

    def test_enum_member_keeps_its_value(self):
        # Action(1) is a lookup: it returns THE member. BUILD on it rewrote
        # Action.BUY's name and value for the whole process.
        from trader.objects import Action
        before = (Action.BUY.name, Action.BUY.value, dict(vars(Action.BUY)))
        text = _refused(_program(_global('trader.objects', 'Action'), _push((Action.BUY.value,)), _REDUCE,
                                 _push({'_value_': Action.SELL.value, '_name_': 'SELL'}), _BUILD))
        assert 'BUILD' in text
        assert (Action.BUY.name, Action.BUY.value, dict(vars(Action.BUY))) == before

    def test_numpy_dtype_singleton_is_never_a_build_target(self):
        # numpy pickles dtype(code, False, True) — a private copy — then BUILD
        # rewrites it in place. dtype(code) alone is the process-wide singleton.
        for args in (('f8',), ('f8', False, False), ('f8', False)):
            _refused(_program(_global('numpy', 'dtype'), _push(args), _REDUCE,
                              _push((3, '>', None, None, None, -1, -1, 0)), _BUILD))
        assert np.dtype('f8').byteorder == '='
        assert np.array([1.5]).tobytes() == np.array([1.5], dtype='<f8').tobytes()

    def test_cached_timezone_is_never_a_build_target(self):
        import dateutil.tz
        _refused(_program(_global('dateutil.tz.tz', 'tzutc'), _push(()), _REDUCE,
                          _push({'pwned': 1}), _BUILD))
        assert not hasattr(dateutil.tz.tzutc(), 'pwned')

    def test_stand_in_functions_are_not_build_targets(self):
        _refused(_program(_global('builtins', 'getattr'), _push({'pwned': 1}), _BUILD))
        assert not hasattr(cs._guarded_getattr, 'pwned')

    def test_slot_state_is_refused(self):
        # (dict, slots) state is applied with setattr, which reaches __class__.
        _refused(_program(_global('ib_async.objects', 'DynamicObject'), _push(()), _NEWOBJ,
                          _push((None, {'__class__': 1})), _BUILD))

    def test_container_opcodes_need_a_container(self):
        text = _refused(_program(_global('builtins', 'dict'), _push('k'), _push('v'), _SETITEM))
        assert 'SETITEM' in text
        text = _refused(_program(_global('collections', 'deque'), _push(1), _APPEND))
        assert 'APPEND' in text


class TestCallsAreCheckedWhereTheyHappen:
    """Admitting a name is not permission to call it. REDUCE / NEWOBJ look the
    target up by identity in what the allowlist resolved."""

    def setup_method(self):
        set_dill_whitelist(None)

    def test_getattr_returns_only_the_listed_pair(self):
        from zoneinfo import ZoneInfo
        assert list(cs._DILL_ATTRIBUTES) == [(('zoneinfo', 'ZoneInfo'), '_unpickle')]
        for name in ('no_cache', 'clear_cache', 'from_file', '__subclasses__', '__class__'):
            text = _refused(_reduce(_Call(_getattr(ZoneInfo, name))))
            assert name in text
        # getattr with a default, or on an object the allowlist never resolved
        _refused(_reduce(_Call(getattr, ZoneInfo, '_unpickle', None)))
        _refused(_reduce(_getattr({'a': 1}, 'get')))

    def test_a_dataclass_is_allocated_never_constructed(self):
        # __init__ / __post_init__ must not run on wire data.
        from ib_async import Stock, Trade
        from trader.common.reactivex import SuccessFail
        for cls in (Trade, Stock, SuccessFail):
            text = _refused(_reduce(_Call(cls)))
            assert cls.__name__ in text

    def test_builtin_value_types_are_references_not_callables(self):
        import collections
        # list(range(10**9)): two builtin names and eight gigabytes. A
        # protocol-4 pickle never CALLS these — it has opcodes for them.
        for payload in (_Call(list, _Call(range, 10 ** 9)), _Call(range, 10 ** 9), _Call(bytes, 10 ** 10),
                        _Call(str, b'x', 'idna'), _Call(dict, [('a', 1)]), _Call(int, '1'), _Call(object),
                        _Call(tuple, [1]), _Call(set, [1]), _Call(float, '1')):
            assert 'not a permitted callable' in _refused(_reduce(payload))
        # They are still fine as VALUES.
        got = _safe_dill_loads(dill.dumps({'kind': int, 'by_key': collections.defaultdict(list, a=[1])}))
        assert got['kind'] is int and got['by_key'].default_factory is list

    def test_value_constructors_take_only_the_shape_pickle_emits(self):
        import collections
        import datetime as dt
        import decimal
        import fractions
        for payload in (
            _Call(bytearray, 10 ** 11),                         # allocate 100 GB
            _Call(complex, '1+2j'),
            _Call(dt.datetime, 2026, 1, 1),
            _Call(dt.datetime, b'\x07\xea\x0b\r\t\x1e\x0f\x03\xd0\x90', 'not a tzinfo'),
            _Call(dt.timedelta, 1.5),
            _Call(dt.timezone, dt.timedelta(0), 5),
            _Call(decimal.Decimal, (0, (1,), 2)),
            _Call(fractions.Fraction, '1/3'),
            _Call(collections.Counter, [1, 2]),
            _Call(collections.OrderedDict, [('a', 1)]),
            _Call(collections.deque, 'abc'),
            _Call(collections.defaultdict, dt.datetime),
            _Call(collections.defaultdict, list, {'a': 1}),
        ):
            assert 'unexpected arguments' in _refused(_reduce(payload))

    def test_lookup_types_cannot_be_allocated(self):
        # NEWOBJ is cls.__new__(cls, *args). For a dtype or an Enum that is a
        # LOOKUP returning a shared object, which BUILD could then rewrite.
        _refused(_program(_global('numpy', 'dtype'), _push(('f8',)), _NEWOBJ))
        _refused(_program(_global('trader.objects', 'Action'), _push((1,)), _NEWOBJ))
        _refused(_program(_global('numpy', 'ndarray'), _push(((2,),)), _NEWOBJ))

    def test_enum_functional_api_is_refused(self):
        # Action('X', 'A B') manufactures a new Enum class.
        _refused(_program(_global('trader.objects', 'Action'), _push(('X', 'A B')), _REDUCE))

    def test_numpy_only_rebuilds_plain_arrays(self):
        _refused(_program(_global('numpy', 'ndarray'), _push(((2,),)), _REDUCE))
        _refused(_reduce(_Call(np._core.multiarray._reconstruct, np.memmap, (0,), b'b')))
        _refused(_reduce(_Call(np._core.multiarray._reconstruct, np.recarray, (0,), b'b')))
        for name in ('memmap', 'fromfile', 'load', 'save', 'savetxt', 'loadtxt', 'recarray', 'matrix'):
            _refused(_program(_global('numpy', name)))
        _refused(_program(_global('numpy.lib.npyio', 'NpzFile')))

    def test_pandas_is_not_reachable_at_all(self):
        for module, name in (('pandas', 'DataFrame'), ('pandas', 'eval'), ('pandas', 'read_pickle'),
                             ('pandas', 'read_csv'), ('pandas.io.pickle', 'read_pickle'),
                             ('pandas.core.frame', 'DataFrame'), ('pandas._libs.internals', '_unpickle_block'),
                             ('pandas.core.computation.eval', 'eval'), ('pandas', 'HDFStore')):
            _refused(_program(_global(module, name)))

    def test_a_callable_made_by_an_allowed_call_is_not_callable(self):
        # eventkit Events are callable (Event.__call__ is emit). The one on the
        # stack came from an allowed REDUCE, not from the allowlist.
        from eventkit import Event
        _refused(_program(_global('eventkit.event', 'Event'), _push(('x', False)), _REDUCE,
                          _push(()), _REDUCE))
        assert callable(Event('x', False))

    @pytest.mark.parametrize('name, body', [
        ('OBJ', b'(' + _global('ib_async.contract', 'Stock') + b'o'),
        ('INST', b'(iib_async.contract\nStock\n'),
        ('EXT1', b'\x82\x01'),
        ('EXT2', b'\x83\x01\x00'),
        ('EXT4', b'\x84\x01\x00\x00\x00'),
        ('PERSID', b'P1\n'),
        ('BINPERSID', _push('1') + b'Q'),
    ], ids=lambda value: value if isinstance(value, str) else '')
    def test_legacy_instantiation_opcodes_are_refused(self, name, body):
        # Old-style instantiation, the copyreg extension registry, persistent
        # ids: other ways to make an object that no protocol >= 2 pickle of
        # these types uses.
        assert name in _refused(_program(body))

    def test_a_module_is_never_imported_for_a_refused_name(self, tmp_path, monkeypatch):
        # Importing runs module-level code. The allowlist is consulted first.
        import sys
        marker = tmp_path / 'imported'
        (tmp_path / 'mmr_unlisted_module.py').write_text(
            f'open({str(marker)!r}, "w").close()\nclass Thing:\n    pass\n')
        monkeypatch.syspath_prepend(str(tmp_path))
        _refused(_program(_global('mmr_unlisted_module', 'Thing')))
        import dill._dill as dill_internals
        _refused(_reduce(_Call(dill_internals._create_namedtuple, 'Thing', (), 'mmr_unlisted_module')))
        assert not marker.exists()
        assert 'mmr_unlisted_module' not in sys.modules


class _Build(_Call):
    """Pickles as REDUCE(f, args) followed by BUILD(state)."""

    def __init__(self, f, args, state):
        super().__init__(f, *args)
        self.state = state

    def __reduce__(self):
        return (self.f, self.args, self.state)


class TestNumpyStateIsCheckedBeforeNumpySeesIt:
    """numpy applies pickled state without validating it. Two of these states
    SEGFAULT numpy 2.5 the first time the resulting array is printed (run in
    a child process while writing the boundary): an object array whose list
    is shorter than its shape, and a structured dtype with an object field
    laid over raw bytes. Neither reaches numpy now."""

    _RECONSTRUCT = (np.ndarray, (0,), b'b')

    def setup_method(self):
        set_dill_whitelist(None)

    def _array(self, state):
        """Both routes an array state can take: dill's _create_array, and
        plain pickle's _reconstruct + BUILD."""
        import dill._dill as dill_internals
        rebuild = np._core.multiarray._reconstruct
        return (_reduce(_Call(dill_internals._create_array, rebuild, self._RECONSTRUCT, state)),
                _reduce(_Build(rebuild, self._RECONSTRUCT, state)))

    @pytest.mark.parametrize('state', [
        (1, (5,), np.dtype('O'), False, [1]),                    # the segfault
        (1, (1,), np.dtype('O'), False, [1, 2, 3]),
        (1, (1,), np.dtype('O'), False, b'AAAAAAAA'),
        (1, (1000000,), np.dtype('f8'), False, b'12345678'),
        (1, (1,), np.dtype('f8'), False, b'123456789'),
        (1, (2 ** 40,), np.dtype('V0'), False, b''),             # 2**40 zero-size items
        (1, (2 ** 62, 2 ** 62), np.dtype('V0'), False, b''),     # element count overflows int64
        (1, (-1,), np.dtype('f8'), False, b''),
        (1, [1], np.dtype('f8'), False, b'12345678'),
        (1, (1,), 'f8', False, b'12345678'),
        (2, (1,), np.dtype('f8'), False, b'12345678'),
        (1, (1,), np.dtype('f8'), False),
    ], ids=lambda state: f'{state[1]}-{getattr(state[2], "str", state[2])}-{type(state[-1]).__name__}')
    def test_inconsistent_array_state_is_refused(self, state):
        for blob in self._array(state):
            _refused(blob)

    @pytest.mark.parametrize('code, state', [
        # an object field over 8 raw bytes: attacker-chosen pointers (the other segfault)
        ('V8', (3, '|', None, ('a',), {'a': (np.dtype('O'), 0)}, 8, 8, 0)),
        # a field 100000 bytes past a 4-byte item: out-of-bounds read
        ('V4', (3, '|', None, ('a',), {'a': (np.dtype('f8'), 100000)}, 4, 1, 16)),
        ('V8', (3, '|', (np.dtype('f8'), (100000,)), None, None, 8, 8, 0)),      # sub-array
        ('f8', (3, '<', None, None, None, -1, -1, 1)),                             # "holds objects" flag
        ('f8', (3, '<', None, None, None, 1 << 20, -1, 0)),                        # itemsize
        ('V4', (3, '|', None, None, None, 1 << 20, 1, 0)),
        ('i1', (3, '<', None, None, None, -1, -1, 0)),                             # not what numpy emits
        ('f8', (3, '=', None, None, None, -1, -1, 0)),
        ('M8', (4, '<', None, None, None, -1, -1, 0, ({'x': 1}, (b'ns', 1, 1, 1)))),
        ('M8', (4, '<', None, None, None, -1, -1, 0, (None, (b'ns]', 1, 1, 1)))),
        ('f8', [3, '<', None, None, None, -1, -1, 0]),
    ], ids=lambda value: value if isinstance(value, str) else '')
    def test_dtype_state_must_be_what_numpy_emits_for_a_plain_dtype(self, code, state):
        _refused(_reduce(_Build(np.dtype, (code, False, True), state)))

    @pytest.mark.parametrize('code', ['i4,O', '(2,3)f8', 'T', 'O,O', 'f8 ', '', '2f8', 'f8,'])
    def test_dtype_code_is_one_plain_type(self, code):
        # numpy builds structured and sub-array dtypes straight from a string.
        _refused(_reduce(_Call(np.dtype, code, False, True)))

    def test_scalar_needs_a_plain_dtype_and_exactly_its_bytes(self):
        scalar = np._core.multiarray.scalar
        for dtype, data in ((np.dtype('O'), b'AAAAAAAA'), (np.dtype('f8'), b'1234'),
                            (np.dtype('f8'), b'123456789'), (np.dtype('f8'), [1.0]), ('f8', b'12345678')):
            _refused(_reduce(_Call(scalar, dtype, data)))

    @pytest.mark.parametrize('array', [
        np.array([1.0, 2.0], dtype='>f8'), np.array([1, 2], dtype='<i2'), np.array(['a', 'bc']),
        np.array([b'a', b'bc']), np.array(['2026-11-13T09:30', 'NaT'], dtype='datetime64[ns]'),
        np.array([1, 2], dtype='timedelta64[15m]'), np.array([1, 'a', None], dtype=object),
        np.array([[1, 'a'], [None, 2.5]], dtype=object), np.array(3.5), np.array([], dtype='f4'),
        np.zeros((2, 0, 3)), np.asfortranarray(np.arange(6.0).reshape(2, 3)), np.array([True, False]),
        np.array([1 + 2j]), np.array([1.5], dtype='f2'),
    ], ids=lambda array: f'{array.dtype.str}{array.shape}')
    def test_real_arrays_pass_the_checks_on_both_routes(self, array):
        # dill's route, and a peer that pickled with the standard library.
        for blob in (dill.dumps(array), pickle.dumps(array, protocol=4)):
            diff = wire_corpus.same(array, _safe_dill_loads(blob))
            assert diff is None, diff


class TestAllowlistShape:
    """The audit assumptions the allowlist rests on, as assertions — so an
    entry added later (or a library upgrade) that breaks one fails here."""

    def test_every_entry_resolves(self):
        # A typo, or a class a library upgrade renamed, is found by the suite
        # rather than by a dropped message.
        for module, name in cs._DILL_GLOBALS:
            assert cs._resolve_global(module, name) is not None, (module, name)

    def test_allocatable_classes_are_plain_data(self):
        import datetime as dt
        import enum
        for (module, name), spec in cs._DILL_GLOBALS.items():
            if not spec.caps & cs._NEW:
                continue
            cls = cs._resolve_global(module, name)
            where = f'{module}.{name}'
            assert isinstance(cls, type), where
            # BUILD fills these with wire data; a finaliser or a setter would
            # then run on it.
            assert not hasattr(cls, '__del__'), where
            assert not issubclass(cls, enum.Enum), where       # NEWOBJ would return a shared member
            assert not issubclass(cls, BaseException), where
            # Allocation must be allocation: object / tuple / list / tzinfo.
            assert (cls.__new__ is object.__new__ or cls.__new__ is dt.tzinfo.__new__
                    or issubclass(cls, (tuple, list))), where
            setters = [attr for attr, value in vars(cls).items()
                       if isinstance(value, property) and value.fset is not None]
            assert not setters, (where, setters)

    def test_only_inert_or_guarded_things_are_callable(self):
        # Every REDUCE-callable entry is a builtin value type, an exception, an
        # Enum, one of the listed pure constructors, or a guarded stand-in.
        import builtins
        import enum
        allowed_functions = {('numpy._core.multiarray', '_reconstruct'), ('numpy._core.multiarray', 'scalar')}
        for (module, name), spec in cs._DILL_GLOBALS.items():
            if not spec.caps & cs._CALL:
                continue
            target = cs._resolve_global(module, name)
            if spec.stand_in is not None:
                assert target is spec.stand_in and target.__module__ == cs.__name__, (module, name)
            elif not isinstance(target, type):
                assert (module, name) in allowed_functions, (module, name)
                assert spec.check is not None, (module, name)
            elif issubclass(target, enum.Enum):
                assert spec.check is cs._one_arg, (module, name)
        for name in ('eval', 'exec', 'compile', 'open', '__import__', 'setattr', 'delattr', 'vars',
                     'globals', 'locals', 'type', 'input', 'breakpoint', 'memoryview', 'super',
                     'SystemExit', 'KeyboardInterrupt', 'GeneratorExit'):
            assert hasattr(builtins, name) and ('builtins', name) not in cs._DILL_GLOBALS, name
        assert cs._DILL_GLOBALS[('builtins', 'getattr')].stand_in is cs._guarded_getattr

    def test_no_package_is_admitted_by_prefix(self):
        modules = {module for module, _ in cs._DILL_GLOBALS}
        assert not any(module.split('.')[0] == 'pandas' for module in modules)
        assert {name for module, name in cs._DILL_GLOBALS if module.split('.')[0] == 'numpy'} == {
            'ndarray', 'dtype', '_reconstruct', 'scalar'}
        assert {name for module, name in cs._DILL_GLOBALS if module.startswith('dill')} == {
            '_load_type', '_create_namedtuple', '_create_array'}
        assert not hasattr(cs, '_DILL_ALLOWED_MODULE_PREFIXES')

    def test_every_ib_async_data_class_is_allowlisted(self):
        # The IB data model nests freely (a Ticker holds a Contract, ticks, DOM
        # levels, greeks...). An ib_async upgrade that adds a class must fail
        # HERE, not as dropped ticks in production.
        missing = [f'{cls.__module__}.{cls.__qualname__}' for cls in wire_corpus.ib_data_classes()
                   if (cls.__module__, cls.__qualname__) not in cs._DILL_GLOBALS]
        assert not missing, f'add to _DILL_GLOBALS in trader/messaging/clientserver.py: {missing}'

    def test_load_type_resolves_only_listed_builtins(self):
        assert cs._guarded_load_type('ValueError') is ValueError
        assert cs._guarded_load_type('NoneType') is type(None)
        for name in ('CodeType', 'FunctionType', 'MethodType', 'CellType', 'ModuleType', 'TypeType',
                     'BuiltinMethodType', 'PartialType', 'no_such_type'):
            with pytest.raises(DillDeserializationError):
                cs._guarded_load_type(name)


_CORPUS = dict(wire_corpus.build_corpus())
_WIRE_SAFE = [label for label in _CORPUS if label not in wire_corpus.NOT_WIRE_SAFE]


class TestProductionTypesStillRoundTrip:
    """The other half of the boundary: every type that crosses RPC, PubSub or
    the MessageBus must come back intact. The allowlist was harvested from
    exactly these instances (plus the whole test suite), so tightening it
    later cannot silently break one of them."""

    def setup_method(self):
        set_dill_whitelist(None)

    def test_corpus_covers_the_wire_types(self):
        assert len(_CORPUS) == len(wire_corpus.build_corpus()), 'duplicate corpus label'
        assert len(_WIRE_SAFE) > 200
        assert set(wire_corpus.NOT_WIRE_SAFE) <= set(_CORPUS)

    @pytest.mark.parametrize('label', _WIRE_SAFE)
    def test_survives_the_unpickler(self, label):
        # The whole object as ONE pickle: every nested value crosses the
        # boundary, nothing is peeled off by msgpack first.
        sent = _CORPUS[label]()
        diff = wire_corpus.same(sent, _safe_dill_loads(dill.dumps(sent)))
        assert diff is None, diff

    @pytest.mark.parametrize('label', _WIRE_SAFE)
    def test_survives_inside_an_rpc_reply(self, label):
        # As production ships it: nested in a SuccessFail, through pack/unpack.
        from trader.common.reactivex import SuccessFail
        sent = SuccessFail.success(obj={'value': _CORPUS[label](), 'in_list': [_CORPUS[label]()]})
        got = unpack(pack(sent))
        assert got.is_success()
        diff = wire_corpus.same(sent.obj, got.obj)
        assert diff is None, diff

    @pytest.mark.parametrize('label', [label for label in _WIRE_SAFE if label != 'value nested containers'])
    def test_survives_as_an_rpc_argument(self, label):
        # Top level, msgpack decides what is native (tuples and NamedTuples
        # flatten to lists, IntEnums to ints) — so only "it decodes" is
        # asserted here. ('nested containers' has a tuple dict key, which
        # msgpack itself cannot decode; nothing to do with the unpickler.)
        unpack(pack({'method': 'm', 'args': [_CORPUS[label]()], 'kwargs': {}, 'req_id': 'r'}))
        _decode_payload(pack(('topic', _CORPUS[label]())))

    @pytest.mark.parametrize('label', sorted(wire_corpus.NOT_WIRE_SAFE))
    def test_known_non_wire_types_are_refused_cleanly(self, label):
        # These did not round-trip before the allowlist either. Pinned so that
        # admitting one is a deliberate change, and so the refusal stays a
        # structured error rather than a TypeError from some constructor.
        with pytest.raises(DillDeserializationError):
            _safe_dill_loads(dill.dumps(_CORPUS[label]()))

    def test_dataframes_cross_as_arrow_not_as_pickles(self):
        for label, frame in wire_corpus._frames().items():
            if not isinstance(frame, pd.DataFrame):
                continue
            got = unpack(pack({'frame': frame}))['frame']
            pd.testing.assert_frame_equal(got, frame, check_freq=False)

    def test_timezone_aware_datetimes_cross_at_top_level_whatever_the_tz_library(self):
        # A datetime is msgpack-native here (EXT_DATETIME), so pytz and
        # dateutil zones work at top level even though their tzinfo objects
        # are not on the pickle allowlist.
        for label in ('datetime pytz localized', 'datetime dateutil gettz', 'datetime zoneinfo',
                      'pd.Timestamp tz America/New_York'):
            sent = wire_corpus._values()[label]
            got = unpack(pack([sent]))[0]
            assert abs((got - sent).total_seconds()) < 1e-3, label


class _Echo(cs.RPCHandler):
    @cs.rpcmethod
    def add(self, a: int, b: int) -> int:
        return a + b


def test_rpc_server_refuses_the_payloads_before_dispatch_and_keeps_serving(tmp_path):
    """Decoding happens before the method check, in the server process. The
    reviewer's proof used an UNREGISTERED method name: the file was written
    anyway. The server must answer with a structured error (not a timeout),
    leave nothing behind, and still serve the next request."""
    import asyncio
    import socket
    import threading

    import zmq

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    ready = threading.Event()
    holder = {}

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        server = cs.RPCServer[_Echo](instance=_Echo(), zmq_rpc_server_address='tcp://127.0.0.1',
                                     zmq_rpc_server_port=port)
        holder['server'], holder['loop'] = server, loop
        loop.run_until_complete(server.serve())
        ready.set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    assert ready.wait(timeout=5.0), 'server thread did not start'

    written = tmp_path / 'rpc_side_effect.txt'
    truncated = tmp_path / 'pretend_mmr.duckdb'
    truncated.write_bytes(b'D' * 4096)
    made_dir = tmp_path / 'created_by_eval'
    arr = np.frombuffer(b'written inside the RPC server before any method lookup\n', dtype=np.uint8)
    from trader.common.reactivex import SuccessFail
    payloads = {
        'tofile': _reduce(_Call(_getattr(np.ndarray, 'tofile'), arr, str(written))),
        'memmap': _reduce(_Call(np.memmap, str(truncated), 'uint8', 'w+', 0, (1,))),
        'eval': _reduce(_Call(_getattr(pd.DataFrame, 'eval'), _Call(pd.DataFrame, {'a': [1]}),
                              f'a.__class__.__init__.__globals__.get("sys").modules.get("os").mkdir("{made_dir}")')),
        'class-build': _program(_global('trader.common.reactivex', 'SuccessFail'),
                                _push((None, {'is_success': 1})), _BUILD),
    }
    is_success = SuccessFail.__dict__['is_success']

    context = zmq.Context()
    sock = context.socket(zmq.DEALER)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(f'tcp://127.0.0.1:{port}')
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    def call(request):
        sock.send_multipart([b'', msgpack.packb(request, use_bin_type=True)])
        assert poller.poll(5000), 'no reply: the server dropped the request or died'
        return unpack(sock.recv_multipart()[-1])

    try:
        for name, blob in payloads.items():
            reply = call({'method': 'no_such_method', 'args': [msgpack.ExtType(EXT_OBJECT, blob)],
                          'kwargs': {}, 'req_id': f'poison-{name}'})
            assert reply['req_id'] == f'poison-{name}'
            assert reply['error'] is True, name
            assert reply['exc_type'] == 'DillDeserializationError', (name, reply)
        assert not written.exists()
        assert truncated.read_bytes() == b'D' * 4096
        assert not made_dir.exists()
        assert SuccessFail.__dict__['is_success'] is is_success
        # ... and the same server still answers a real call.
        reply = call({'method': 'add', 'args': [2, 3], 'kwargs': {}, 'req_id': 'after'})
        assert reply.get('result') == 5 and not reply.get('error'), reply
    finally:
        sock.close()
        context.term()
        loop = holder.get('loop')
        if loop is not None:
            asyncio.run_coroutine_threadsafe(holder['server'].aclose(), loop).result(timeout=5)
            loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5.0)
