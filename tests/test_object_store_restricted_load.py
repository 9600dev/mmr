"""`DuckDBObjectStore` reads its blobs through the restricted unpickler.

docs/AUDIT_2026-11-14.md B5.8: `read` was a bare `dill.loads` — the one place
left where a pickle was executed with no policy at all. Whoever can write a
row into `object_store` (a restored backup, a copied database, another process
with the file open) got code execution in every process that lists a universe:
the CLI, trader_service, strategy_service.

What the store actually holds is small: universes (`Universe` holding
`SecurityDefinition`s) and one dict of plain values (`_portfolio_snapshot`).
So the read admits exactly those two classes — narrower than the wire
allowlist — and refuses anything else, naming the offending (module, name).
"""

import csv
import dataclasses
import decimal
import io
import math
import os
import pickle

import dill
import numpy as np
import pytest

from ib_async.contract import Contract, ContractDetails
from trader.data import duckdb_store
from trader.data.data_access import DictData, SecurityDefinition
from trader.data.duckdb_store import DuckDBConnection, DuckDBObjectStore
from trader.data.universe import Universe, UniverseAccessor
from trader.messaging import clientserver as cs
from trader.messaging.clientserver import DillDeserializationError


def _put_raw(db_path: str, key: str, blob: bytes) -> None:
    """Place bytes in the table the way anything but `write()` would: a row."""
    DuckDBObjectStore(db_path)   # creates the table
    DuckDBConnection.get_instance(db_path).execute(
        'INSERT OR REPLACE INTO object_store (key, data) VALUES (?, ?)', [key, blob], fetch='none')


def _definition(**overrides) -> SecurityDefinition:
    """One value of each type `SecurityDefinition` declares, in every field."""
    by_type = {'str': 'x', 'int': 265598, 'float': 0.01, 'bool': True}
    values = {}
    for field in dataclasses.fields(SecurityDefinition):
        name = field.type if isinstance(field.type, str) else field.type.__name__
        values[field.name] = by_type[name]
    values.update(overrides)
    return SecurityDefinition(**values)


def _ib_definition() -> SecurityDefinition:
    """A definition built the way trader_service builds one, from the value
    types ib_async's decoder leaves on a ContractDetails (`Decoder.parse`
    coerces by the type of each field's DEFAULT: `priceMagnifier` and `coupon`
    are ints there although `SecurityDefinition` annotates them float)."""
    details = ContractDetails(
        contract=Contract(symbol='BRK B', secType='STK', conId=72063691, exchange='SMART',
                          primaryExchange='NYSE', currency='USD', tradingClass='BRK B',
                          multiplier=''),
        minTick=0.01, orderTypes='ACTIVETIM,AD,ADJUST', validExchanges='SMART,NYSE,ARCA',
        priceMagnifier=1, longName='BERKSHIRE HATHAWAY INC-CL B', industry='Financial',
        category='Insurance', subcategory='Property/Casualty Ins', timeZoneId='US/Eastern',
        tradingHours='20261116:0400-20261116:2000;20261117:0400-20261117:2000',
        liquidHours='20261116:0930-20261116:1600', stockType='COMMON', minSize=0.0001,
        sizeIncrement=0.0001, suggestedSizeIncrement=100.0, marketRuleIds='26,26', coupon=0,
    )
    return SecurityDefinition.from_contract_details(details)


def _csv_definition() -> SecurityDefinition:
    """`universe import` builds definitions from CSV text: EVERY field is a str."""
    names = [field.name for field in dataclasses.fields(SecurityDefinition)]
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=names)
    writer.writeheader()
    writer.writerow({name: '1' for name in names})
    row = next(csv.DictReader(out.getvalue().splitlines()))
    return SecurityDefinition(**row)


def _stored_universes():
    odd = _definition(
        symbol='0700', longName='Tencent 騰訊控股 — "H" \'shares\'\n', conId=2 ** 40,
        minTick=float('nan'), priceMagnifier=float('inf'), coupon=-0.0, minSize=1.7976931348623157e308,
        includeExpired=False, multiplier='', description='',
    )
    shared = _definition(symbol='SHARED')
    return {
        'every field type': Universe('u', [_definition()]),
        'from IB contract details': Universe('portfolio', [_ib_definition()]),
        'from a CSV import': Universe('imported', [_csv_definition()]),
        'edge values': Universe('edge', [odd]),
        'empty': Universe('empty', []),
        'same definition twice': Universe('memo', [shared, shared]),
        '600 definitions': Universe('big', [_definition(conId=n, symbol=f'S{n}') for n in range(600)]),
    }


def _same(a, b) -> bool:
    return a == b or (isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b))


def _assert_same_universe(got, want) -> None:
    assert type(got) is Universe
    assert got.name == want.name
    assert len(got.security_definitions) == len(want.security_definitions)
    for g, w in zip(got.security_definitions, want.security_definitions):
        assert type(g) is SecurityDefinition
        for field in dataclasses.fields(SecurityDefinition):
            gv, wv = getattr(g, field.name), getattr(w, field.name)
            assert type(gv) is type(wv) and _same(gv, wv), (field.name, gv, wv)


class TestHostileBlobsAreRefused:
    def test_gadget_in_a_stored_blob_does_not_execute(self, tmp_duckdb_path, tmp_path):
        made = tmp_path / 'made-by-the-blob'

        class Gadget:
            def __reduce__(self):
                return (os.mkdir, (str(made),))

        _put_raw(tmp_duckdb_path, 'portfolio', pickle.dumps(Gadget()))
        with pytest.raises(DillDeserializationError) as refused:
            DuckDBObjectStore(tmp_duckdb_path).read('portfolio')
        assert not made.exists(), 'the stored blob ran os.mkdir while being read'
        # posix.mkdir on this platform, nt.mkdir on Windows: named exactly.
        assert repr((os.mkdir.__module__, 'mkdir')) in str(refused.value)

    def test_gadget_does_not_execute_through_the_universe_accessor(self, tmp_duckdb_path, tmp_path):
        made = tmp_path / 'made-by-the-blob'

        class Gadget:
            def __reduce__(self):
                return (os.mkdir, (str(made),))

        hostile = Universe('portfolio', [])
        setattr(hostile, 'security_definitions', [_ib_definition(), Gadget()])
        _put_raw(tmp_duckdb_path, 'portfolio', dill.dumps(hostile))
        accessor = UniverseAccessor(tmp_duckdb_path, 'Universes')
        with pytest.raises(DillDeserializationError):
            accessor.get('portfolio')
        with pytest.raises(DillDeserializationError):
            accessor.resolve_symbol('AAPL')
        assert not made.exists()

    @pytest.mark.parametrize('label, value, offending', [
        # Each of these IS on the wire allowlist. The store does not write
        # them, so the store does not read them.
        ('an ib_async Contract', Contract(conId=265598), ('ib_async.contract', 'Contract')),
        ('a Decimal', decimal.Decimal('1.5'), ('decimal', 'Decimal')),
        ('a numpy scalar', np.int64(265598), ('numpy._core.multiarray', 'scalar')),
        ('a type held as a value', {'kind': type(None)}, ('dill._dill', '_load_type')),
        ('an exception', ValueError('x'), ('dill._dill', '_load_type')),
    ])
    def test_wire_types_the_store_never_writes_are_refused_by_name(
            self, tmp_duckdb_path, label, value, offending):
        _put_raw(tmp_duckdb_path, 'k', dill.dumps(value))
        with pytest.raises(DillDeserializationError) as refused:
            DuckDBObjectStore(tmp_duckdb_path).read('k')
        assert repr(offending) in str(refused.value), label

    def test_a_definition_field_holding_another_type_is_refused(self, tmp_duckdb_path):
        poisoned = Universe('u', [_definition(conId=np.int64(265598))])
        _put_raw(tmp_duckdb_path, 'u', dill.dumps(poisoned))
        with pytest.raises(DillDeserializationError) as refused:
            UniverseAccessor(tmp_duckdb_path, 'Universes').get('u')
        assert repr(('numpy._core.multiarray', 'scalar')) in str(refused.value)

    def test_garbage_bytes_are_a_structured_refusal(self, tmp_duckdb_path):
        _put_raw(tmp_duckdb_path, 'k', b'\x00not a pickle')
        with pytest.raises(DillDeserializationError):
            DuckDBObjectStore(tmp_duckdb_path).read('k')


class TestWhatTheStoreWritesStillLoads:
    @pytest.mark.parametrize('label', sorted(_stored_universes()))
    @pytest.mark.parametrize('protocol', [2, 3, 4, 5])
    def test_blob_written_by_the_previous_code_loads(self, tmp_duckdb_path, label, protocol):
        """The previous `write` stored exactly `dill.dumps(data)` (protocol 4
        today; 2, 3 and 5 are what other interpreters would have emitted).
        Those bytes are placed directly, not through the new `write`."""
        want = _stored_universes()[label]
        _put_raw(tmp_duckdb_path, want.name, dill.dumps(want, protocol=protocol))
        got = UniverseAccessor(tmp_duckdb_path, 'Universes').get(want.name)
        _assert_same_universe(got, want)

    @pytest.mark.parametrize('label', sorted(_stored_universes()))
    def test_universe_round_trips_through_the_accessor(self, tmp_duckdb_path, label):
        want = _stored_universes()[label]
        accessor = UniverseAccessor(tmp_duckdb_path, 'Universes')
        accessor.update(want)
        _assert_same_universe(UniverseAccessor(tmp_duckdb_path, 'Universes').get(want.name), want)

    def test_repeated_definition_stays_one_object(self, tmp_duckdb_path):
        accessor = UniverseAccessor(tmp_duckdb_path, 'Universes')
        accessor.update(_stored_universes()['same definition twice'])
        first, second = accessor.get('memo').security_definitions
        assert first is second

    def test_loaded_definition_is_usable(self, tmp_duckdb_path):
        """Allocated-and-filled, never constructed — it must still behave as
        the frozen, hashable dataclass the resolver relies on."""
        accessor = UniverseAccessor(tmp_duckdb_path, 'Universes')
        accessor.update(Universe('portfolio', [_ib_definition()]))
        definition, = accessor.resolve_symbol(72063691)
        assert definition == _ib_definition() and hash(definition) == hash(_ib_definition())
        assert Universe.to_contract(definition).symbol == 'BRK B'
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(definition, 'symbol', 'other')

    def test_portfolio_snapshot_shape_round_trips(self, tmp_duckdb_path):
        snapshot = {
            'positions': {'AAPL': {'value': 1234.56, 'daily_pnl': -7.0, 'position': 10.0},
                          'BRK B': {'value': 0.0, 'daily_pnl': 0.0, 'position': -3.0}},
            'timestamp': '2026-11-14 16:00:00.123456',
        }
        store = DuckDBObjectStore(tmp_duckdb_path)
        store.write('_portfolio_snapshot', snapshot)
        assert store.read('_portfolio_snapshot') == snapshot

    @pytest.mark.parametrize('value', [
        None, True, 7, 2 ** 80, 1.5, 'text', b'bytes', [1, [2, 3]], (1, 'a'), {'a': {'b': [1.0]}},
        {1, 2}, frozenset({'a'}),
    ])
    def test_plain_values_round_trip(self, tmp_duckdb_path, value):
        dd = DictData(tmp_duckdb_path, 'lib')
        dd.write(1, value)
        got = dd.read(1)
        assert got == value and type(got) is type(value)

    def test_missing_key_is_none(self, tmp_duckdb_path):
        assert DuckDBObjectStore(tmp_duckdb_path).read('absent') is None


class TestWriteRefusesWhatReadWouldRefuse:
    """A blob that can be stored but never read back bricks its key: every
    later `get` raises. Refuse at the writer, where the traceback names it."""

    def test_unlisted_type_is_refused_at_write_and_nothing_is_stored(self, tmp_duckdb_path):
        store = DuckDBObjectStore(tmp_duckdb_path)
        with pytest.raises(DillDeserializationError) as refused:
            store.write('k', {'conId': np.int64(265598)})
        assert repr(('numpy._core.multiarray', 'scalar')) in str(refused.value)
        assert store.read('k') is None and store.list_symbols() == []

    def test_refused_update_keeps_the_stored_universe(self, tmp_duckdb_path):
        accessor = UniverseAccessor(tmp_duckdb_path, 'Universes')
        good = Universe('portfolio', [_ib_definition()])
        accessor.update(good)
        bad = Universe('portfolio', [_definition(conId=np.int64(1))])
        with pytest.raises(DillDeserializationError):
            accessor.update(bad)
        _assert_same_universe(UniverseAccessor(tmp_duckdb_path, 'Universes').get('portfolio'), good)


class TestTheStoreListIsPartOfTheBoundary:
    def test_the_store_admits_exactly_the_two_classes_it_writes(self):
        assert duckdb_store.OBJECT_STORE_GLOBALS == frozenset({
            ('trader.data.universe', 'Universe'),
            ('trader.data.data_access', 'SecurityDefinition'),
        })

    def test_every_store_entry_is_on_the_wire_allowlist_as_allocate_only(self):
        """The store list only NARROWS: what may be done with a name is still
        decided by `_DILL_GLOBALS`. Allocate-only means `__init__` never runs
        on stored bytes and nothing on the list can be called."""
        for key in duckdb_store.OBJECT_STORE_GLOBALS:
            assert cs._DILL_GLOBALS[key].caps == cs._NEW, key

    def test_a_name_missing_from_the_wire_allowlist_cannot_be_admitted_by_a_caller(self):
        blob = pickle.dumps(os.getcwd)
        key = (os.getcwd.__module__, 'getcwd')
        assert key not in cs._DILL_GLOBALS
        with pytest.raises(DillDeserializationError):
            cs.restricted_loads(blob, only=frozenset({key}), described_as='a test list')

    def test_the_wire_boundary_is_unchanged(self):
        """Narrowing is per call. The wire still admits its own allowlist."""
        assert cs._safe_dill_loads(dill.dumps(decimal.Decimal('1.5'))) == decimal.Decimal('1.5')
        contract = cs._safe_dill_loads(dill.dumps(Contract(conId=265598)))
        assert type(contract) is Contract and contract.conId == 265598

    def test_strict_wire_mode_does_not_make_universes_unreadable(self, tmp_duckdb_path, monkeypatch):
        """MMR_DILL_STRICT refuses dill on the WIRE. Universes are not wire
        payloads; strict mode must not take contract resolution down."""
        accessor = UniverseAccessor(tmp_duckdb_path, 'Universes')
        accessor.update(Universe('portfolio', [_ib_definition()]))
        monkeypatch.setattr(cs, 'DILL_STRICT_MODE', True)
        cs.set_dill_whitelist(['builtins.dict'])
        try:
            _assert_same_universe(accessor.get('portfolio'), Universe('portfolio', [_ib_definition()]))
        finally:
            cs.set_dill_whitelist(None)
