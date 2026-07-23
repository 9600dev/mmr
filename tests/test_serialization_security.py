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
import tempfile

import dill
import msgpack
import numpy as np
import pandas as pd
import pytest

from trader.messaging.clientserver import (
    DillDeserializationError,
    EXT_OBJECT,
    _decode_payload,
    _safe_dill_loads,
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
