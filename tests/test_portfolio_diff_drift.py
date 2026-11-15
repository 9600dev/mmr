"""`portfolio_diff` must report a move that arrives in small steps.

AUDIT_2026-11-14 B5.8. Every call compared the portfolio with the PREVIOUS
call's snapshot and then overwrote that snapshot, so a position sliding 0.4% per
monitoring cycle was "unchanged" for ever: 100.00 -> 81.84 over 50 cycles
reported nothing. The comparison baseline for a symbol now advances only when
that symbol was reported (new or changed).
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from ib_async import Contract

from trader.data.data_access import PortfolioSummary
from trader.sdk import MMR

TOP_LEVEL_KEYS = {'changed', 'new', 'removed', 'unchanged_count', 'prev_timestamp', 'timestamp'}
CHANGED_KEYS = {'symbol', 'value', 'prev_value', 'value_change', 'daily_pnl', 'position',
                'prev_position'}


class Book:
    """A fake trader_service portfolio whose prices the test moves."""

    def __init__(self, **positions):
        # symbol -> [quantity, price]
        self.positions = {s: list(v) for s, v in positions.items()}

    def get_portfolio_summary(self):
        out = []
        for i, (symbol, (qty, price)) in enumerate(sorted(self.positions.items()), start=1):
            contract = Contract(conId=i, symbol=symbol, localSymbol=symbol, secType='STK',
                                currency='USD')
            out.append(PortfolioSummary(
                contract=contract, position=qty, marketPrice=price, marketValue=qty * price,
                averageCost=100.0, unrealizedPNL=0.0, realizedPNL=0.0, account='U1',
                dailyPNL=qty * (price - 100.0)))
        return out

    def scale(self, symbol, factor):
        self.positions[symbol][1] *= factor


@pytest.fixture
def make_mmr(tmp_path):
    def _make(book):
        mmr = MMR.__new__(MMR)
        mmr._client = SimpleNamespace(is_setup=True, rpc=lambda **kwargs: book)
        mmr._position_map, mmr._contract_map = {}, {}
        mmr._container = MagicMock()
        mmr._container.config.return_value = {'duckdb_path': str(tmp_path / 'diff.duckdb')}
        return mmr
    return _make


def test_drift_below_the_threshold_per_call_is_reported_once_it_accumulates(make_mmr):
    book = Book(XYZ=(100, 100.0))
    mmr = make_mmr(book)

    first = mmr.portfolio_diff()
    assert [n['symbol'] for n in first['new']] == ['XYZ']

    book.scale('XYZ', 0.996)                       # -0.4%: below the 0.5% threshold
    second = mmr.portfolio_diff()
    assert second['changed'] == [] and second['unchanged_count'] == 1

    book.scale('XYZ', 0.996)                       # -0.8% from the last REPORTED value
    third = mmr.portfolio_diff()
    assert third['unchanged_count'] == 0
    assert len(third['changed']) == 1
    change = third['changed'][0]
    assert change['symbol'] == 'XYZ'
    assert change['prev_value'] == pytest.approx(10_000.0)     # not the 9,960 of call two
    assert change['value'] == pytest.approx(9_920.16)
    assert change['value_change'] == pytest.approx(-79.84)


def test_a_reported_change_becomes_the_new_baseline(make_mmr):
    book = Book(XYZ=(100, 100.0))
    mmr = make_mmr(book)
    mmr.portfolio_diff()
    book.scale('XYZ', 0.99)
    assert len(mmr.portfolio_diff()['changed']) == 1
    again = mmr.portfolio_diff()                    # nothing moved since it was reported
    assert again['changed'] == [] and again['unchanged_count'] == 1


def test_a_steady_slide_keeps_being_reported(make_mmr):
    # The audit's reproduction: -0.4% per cycle for 50 cycles.
    book = Book(XYZ=(100, 100.0))
    mmr = make_mmr(book)
    mmr.portfolio_diff()
    reported, reported_change = 0, 0.0
    for _ in range(50):
        book.scale('XYZ', 0.996)
        diff = mmr.portfolio_diff()
        reported += len(diff['changed'])
        reported_change += sum(c['value_change'] for c in diff['changed'])
    final_value = 100 * book.positions['XYZ'][1]
    assert reported >= 20                            # was 0
    # What was reported accounts for the whole move, less at most one
    # sub-threshold step still pending.
    assert reported_change == pytest.approx(final_value - 10_000.0, abs=0.005 * 10_000.0)


def test_a_quiet_name_is_not_rebaselined_because_another_name_moved(make_mmr):
    # A whole-snapshot baseline that advances whenever ANYTHING is reported
    # would still hide the drifter: something always moves in a real book.
    book = Book(LOUD=(10, 100.0), QUIET=(100, 100.0))
    mmr = make_mmr(book)
    mmr.portfolio_diff()

    book.scale('LOUD', 1.02)
    book.scale('QUIET', 0.997)
    first = mmr.portfolio_diff()
    assert [c['symbol'] for c in first['changed']] == ['LOUD']
    assert first['unchanged_count'] == 1

    book.scale('LOUD', 1.02)
    book.scale('QUIET', 0.997)
    second = mmr.portfolio_diff()
    by_symbol = {c['symbol']: c for c in second['changed']}
    assert set(by_symbol) == {'LOUD', 'QUIET'}
    assert by_symbol['QUIET']['prev_value'] == pytest.approx(10_000.0)
    assert by_symbol['LOUD']['prev_value'] == pytest.approx(1_020.0)   # its own last report


def test_quantity_change_is_reported_and_rebaselined(make_mmr):
    book = Book(XYZ=(100, 100.0))
    mmr = make_mmr(book)
    mmr.portfolio_diff()
    book.positions['XYZ'][0] = 101
    diff = mmr.portfolio_diff()
    assert diff['changed'][0]['prev_position'] == 100 and diff['changed'][0]['position'] == 101
    assert mmr.portfolio_diff()['changed'] == []


def test_a_value_appearing_from_zero_is_a_change_not_a_permanent_unchanged(make_mmr):
    # A zero baseline made the percentage 0 by definition; with a baseline that
    # only moves when something is reported, that would never recover.
    book = Book(XYZ=(100, 0.0))
    mmr = make_mmr(book)
    mmr.portfolio_diff()
    book.positions['XYZ'][1] = 50.0
    diff = mmr.portfolio_diff()
    assert [c['symbol'] for c in diff['changed']] == ['XYZ']
    assert diff['changed'][0]['prev_value'] == 0 and diff['changed'][0]['value'] == 5_000.0


def test_json_shape_is_kept(make_mmr):
    book = Book(GONE=(5, 100.0), XYZ=(100, 100.0))
    mmr = make_mmr(book)
    first = mmr.portfolio_diff()
    assert set(first) == TOP_LEVEL_KEYS
    assert first['prev_timestamp'] is None
    assert set(first['new'][0]) == {'symbol', 'value', 'daily_pnl', 'position'}

    del book.positions['GONE']
    book.scale('XYZ', 0.9)
    book.positions['NEW'] = [1, 10.0]
    second = mmr.portfolio_diff()
    assert set(second) == TOP_LEVEL_KEYS
    assert second['prev_timestamp'] == first['timestamp']
    assert CHANGED_KEYS <= set(second['changed'][0])
    assert set(second['removed'][0]) == {'symbol', 'value', 'daily_pnl', 'position'}
    assert set(second['new'][0]) == {'symbol', 'value', 'daily_pnl', 'position'}
    # The one addition: since when the reported change is measured.
    assert second['changed'][0]['baseline_timestamp'] == first['timestamp']


def test_changed_entry_says_since_when_it_is_measured(make_mmr):
    book = Book(XYZ=(100, 100.0))
    mmr = make_mmr(book)
    first = mmr.portfolio_diff()
    book.scale('XYZ', 0.996)
    second = mmr.portfolio_diff()
    book.scale('XYZ', 0.996)
    third = mmr.portfolio_diff()
    # prev_timestamp is the previous CALL; the change is measured from the
    # baseline, which is two calls old here.
    assert third['prev_timestamp'] == second['timestamp']
    assert third['changed'][0]['baseline_timestamp'] == first['timestamp']


def test_removed_position_reports_its_latest_daily_pnl(make_mmr):
    book = Book(XYZ=(100, 100.0))
    mmr = make_mmr(book)
    mmr.portfolio_diff()
    book.scale('XYZ', 0.998)                        # unchanged, but daily PnL moved
    mmr.portfolio_diff()
    del book.positions['XYZ']
    removed = mmr.portfolio_diff()['removed']
    assert removed[0]['daily_pnl'] == pytest.approx(-20.0)


def test_snapshot_written_by_the_previous_version_is_read(make_mmr, tmp_path):
    # Stored records without a per-symbol baseline timestamp (written before
    # this change) are a valid baseline, dated by the snapshot's own timestamp.
    from trader.data.duckdb_store import DuckDBObjectStore
    DuckDBObjectStore(str(tmp_path / 'diff.duckdb')).write('_portfolio_snapshot', {
        'positions': {'XYZ': {'value': 10_000.0, 'daily_pnl': 0.0, 'position': 100.0}},
        'timestamp': '2026-11-13 10:00:00',
    })
    book = Book(XYZ=(100, 99.0))
    diff = make_mmr(book).portfolio_diff()
    assert diff['prev_timestamp'] == '2026-11-13 10:00:00'
    assert diff['changed'][0]['prev_value'] == 10_000.0
    assert diff['changed'][0]['baseline_timestamp'] == '2026-11-13 10:00:00'
