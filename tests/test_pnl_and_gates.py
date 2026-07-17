"""Tests for the per-strategy PnL ledger, fill tagging, the stale-bar gate,
and the preflight summary (Cluster B slice shipped 2026-07-16).

The load-bearing invariants:
- fills are attributable per strategy ONLY because approve derives orderRef
  from proposal.metadata['strategy'] — a regression there silently lands
  every fill under 'proposal' and empties `mmr strategies pnl`;
- the stale-bar gate refuses OPENS on old bars but must NEVER block a close;
- pairing is long-only and conservative: a SELL with no attributed BUY is
  counted, never guessed into PnL.
"""
import datetime as dt
from types import SimpleNamespace

import pandas as pd
import pytest

from trader.data.event_store import (
    EventStore,
    EventType,
    TradingEvent,
    pair_fills_long_only,
)
from trader.objects import Action
from trader.strategy.auto_executor import (
    SignalWork,
    bar_age_seconds,
    decide_signal,
)


# ---------------------------------------------------------------------------
# pair_fills_long_only — pure pairing
# ---------------------------------------------------------------------------

def _fill(strategy, conid, action, qty, price, ts=None):
    return (strategy, conid, action, qty, price, ts or dt.datetime(2026, 7, 16, 10, 0))


class TestPairFills:
    def test_single_round_trip(self):
        closed, open_lots, unmatched = pair_fills_long_only([
            _fill('orb_a', 1, 'BUY', 10, 100.0),
            _fill('orb_a', 1, 'SELL', 10, 110.0),
        ])
        assert len(closed) == 1
        assert closed[0]['pnl'] == pytest.approx(100.0)
        assert open_lots == []
        assert unmatched == 0

    def test_open_lot_reported_not_realized(self):
        closed, open_lots, unmatched = pair_fills_long_only([
            _fill('orb_b', 2, 'BUY', 5, 50.0),
        ])
        assert closed == []
        assert open_lots == [{'strategy': 'orb_b', 'conid': 2, 'quantity': 5.0,
                              'entry_price': 50.0}]

    def test_unmatched_sell_counted_never_guessed(self):
        closed, open_lots, unmatched = pair_fills_long_only([
            _fill('orb_c', 3, 'SELL', 10, 100.0),
        ])
        assert closed == []
        assert unmatched == 1

    def test_partial_close_leaves_remainder(self):
        closed, open_lots, _ = pair_fills_long_only([
            _fill('orb_a', 1, 'BUY', 10, 100.0),
            _fill('orb_a', 1, 'SELL', 4, 105.0),
        ])
        assert closed[0]['quantity'] == pytest.approx(4.0)
        assert closed[0]['pnl'] == pytest.approx(20.0)
        assert open_lots[0]['quantity'] == pytest.approx(6.0)

    def test_buy_extension_volume_weights_entry(self):
        closed, _, _ = pair_fills_long_only([
            _fill('orb_a', 1, 'BUY', 10, 100.0),
            _fill('orb_a', 1, 'BUY', 10, 110.0),   # avg entry 105
            _fill('orb_a', 1, 'SELL', 20, 106.0),
        ])
        assert closed[0]['pnl'] == pytest.approx(20.0)   # (106-105) * 20

    def test_sell_capped_at_lot_counts_once(self):
        # SELL larger than the attributed lot realizes only the lot.
        closed, open_lots, unmatched = pair_fills_long_only([
            _fill('orb_a', 1, 'BUY', 10, 100.0),
            _fill('orb_a', 1, 'SELL', 15, 110.0),
        ])
        assert closed[0]['quantity'] == pytest.approx(10.0)
        assert open_lots == []
        assert unmatched == 0

    def test_strategies_and_conids_isolated(self):
        closed, _, _ = pair_fills_long_only([
            _fill('orb_a', 1, 'BUY', 10, 100.0),
            _fill('orb_b', 1, 'BUY', 10, 200.0),
            _fill('orb_a', 2, 'BUY', 10, 300.0),
            _fill('orb_a', 1, 'SELL', 10, 101.0),
        ])
        assert len(closed) == 1
        assert closed[0]['strategy'] == 'orb_a' and closed[0]['conid'] == 1


# ---------------------------------------------------------------------------
# EventStore.realized_pnl_by_strategy — aggregation over a real store
# ---------------------------------------------------------------------------

class TestRealizedPnlByStrategy:
    def _append_fill(self, store, strategy, conid, action, qty, price, ts):
        store.append(TradingEvent(
            event_type=EventType.ORDER_FILLED, timestamp=ts,
            strategy_name=strategy, conid=conid, symbol='X',
            action=action, quantity=qty, price=price))

    def test_aggregation(self, tmp_path):
        store = EventStore(str(tmp_path / 'pnl.duckdb'))
        today = dt.datetime.now().replace(hour=10, minute=0)
        yesterday = today - dt.timedelta(days=1)

        self._append_fill(store, 'orb_a', 1, 'BUY', 10, 100.0, yesterday)
        self._append_fill(store, 'orb_a', 1, 'SELL', 10, 105.0, yesterday)  # +50 yday
        self._append_fill(store, 'orb_a', 1, 'BUY', 10, 100.0, today)
        self._append_fill(store, 'orb_a', 1, 'SELL', 10, 110.0, today)      # +100 today
        self._append_fill(store, 'orb_b', 2, 'BUY', 5, 50.0, today)         # open lot
        self._append_fill(store, 'proposal', 3, 'BUY', 1, 10.0, today)      # manual tag

        report = store.realized_pnl_by_strategy()
        a = report['strategies']['orb_a']
        assert a['realized_total'] == pytest.approx(150.0)
        assert a['realized_today'] == pytest.approx(100.0)
        assert a['closed_trades'] == 2 and a['wins'] == 2
        b = report['strategies']['orb_b']
        assert b['open_lots'] == [{'conid': 2, 'quantity': 5.0, 'entry_price': 50.0}]
        # manual tag present in the raw aggregation (the CLI filters it)
        assert 'proposal' in report['strategies']
        assert 'proposal' in EventStore.NON_STRATEGY_TAGS

    def test_empty_store(self, tmp_path):
        store = EventStore(str(tmp_path / 'empty.duckdb'))
        report = store.realized_pnl_by_strategy()
        assert report['strategies'] == {}
        assert report['unmatched_sells'] == 0


# ---------------------------------------------------------------------------
# Fill tagging — approve derives orderRef from proposal metadata
# ---------------------------------------------------------------------------

class TestFillTagging:
    def _approve(self, tmp_duckdb_path, metadata):
        from trader.common.reactivex import SuccessFail
        from trader.data.proposal_store import ProposalStore
        from trader.trading.proposal import ExecutionSpec, TradeProposal
        from test_propose_approve_integration import _StubRPCClient, _StubTrade, _make_mmr

        store = ProposalStore(tmp_duckdb_path)
        pid = store.add(TradeProposal(
            symbol='AMD', action='BUY', quantity=10.0,
            execution=ExecutionSpec(order_type='MARKET'),
            reasoning='tagging test', metadata=metadata))
        rpc = _StubRPCClient(
            place_expressive_order_result=SuccessFail.success(obj=[_StubTrade(orderId=1)]))
        mmr = _make_mmr(tmp_duckdb_path, rpc)
        result = mmr.approve(pid)
        assert result.is_success()
        placed = [c for c in rpc.calls if c['method'] == 'place_expressive_order']
        assert len(placed) == 1
        return placed[0]['kwargs']

    def test_strategy_metadata_tags_order(self, tmp_duckdb_path):
        kwargs = self._approve(tmp_duckdb_path,
                               {'auto_executed': True, 'strategy': 'orb_test'})
        assert kwargs['algo_name'] == 'orb_test'

    def test_manual_proposal_stays_proposal(self, tmp_duckdb_path):
        kwargs = self._approve(tmp_duckdb_path, {})
        assert kwargs['algo_name'] == 'proposal'


# ---------------------------------------------------------------------------
# Stale-bar gate
# ---------------------------------------------------------------------------

def _work(action, bar_size_seconds=60.0, **kw):
    return SignalWork(
        strategy_name='s', conid=1, action=action,
        bar_ts=pd.Timestamp('2026-07-16 10:00:00'),
        auto_execute=True, state_running=True,
        bar_size_seconds=bar_size_seconds, **kw)


def _decide(work, bar_age=None, multiple=3.0, held=0.0, paper=True, armed=False):
    return decide_signal(
        work, kill_switch=False, paper_trading=paper, held_qty=held,
        already_executed_bar=False, cooldown_active=False,
        bar_age_seconds=bar_age, stale_bar_multiple=multiple,
        live_armed=armed)


class TestStaleBarGate:
    def test_fresh_buy_opens(self):
        assert _decide(_work(Action.BUY), bar_age=90.0).kind == 'open'

    def test_stale_buy_skipped(self):
        d = _decide(_work(Action.BUY), bar_age=200.0)   # > 3 x 60s
        assert d.kind == 'skip'
        assert 'stale_bar' in d.reason

    def test_stale_sell_still_closes(self):
        # NEVER gate exits: a stale close still reduces risk.
        d = _decide(_work(Action.SELL), bar_age=10_000.0, held=5.0)
        assert d.kind == 'close'

    def test_unknown_bar_size_disables_gate(self):
        assert _decide(_work(Action.BUY, bar_size_seconds=0.0), bar_age=10_000.0).kind == 'open'

    def test_unknown_age_disables_gate(self):
        assert _decide(_work(Action.BUY), bar_age=None).kind == 'open'

    def test_multiple_is_tunable(self):
        assert _decide(_work(Action.BUY), bar_age=200.0, multiple=10.0).kind == 'open'
        assert _decide(_work(Action.BUY), bar_age=200.0, multiple=2.0).kind == 'skip'


class TestLiveDoubleArm:
    """Real-money auto-execution needs BOTH trading_mode=live AND the strict
    MMR_AUTO_EXECUTE_LIVE=1 knob (horserank's L2 analog). Code default is
    DISARMED; a regression here arms the whole book off a single --live flag."""

    def test_live_unarmed_buy_refused(self):
        d = _decide(_work(Action.BUY), paper=False, armed=False)
        assert d.kind == 'skip'
        assert 'MMR_AUTO_EXECUTE_LIVE' in d.reason

    def test_live_armed_buy_opens(self):
        assert _decide(_work(Action.BUY), paper=False, armed=True).kind == 'open'

    def test_paper_unaffected_by_arming(self):
        # Paper mode trades with or without the knob — zero cost today.
        assert _decide(_work(Action.BUY), paper=True, armed=False).kind == 'open'

    def test_live_unarmed_close_still_works(self):
        # NEVER gate exits: disarming with live positions open must not
        # strand them unmanaged.
        d = _decide(_work(Action.SELL), paper=False, armed=False, held=5.0)
        assert d.kind == 'close'

    def test_default_is_disarmed(self):
        # Omitting live_armed entirely must fail CLOSED in live mode.
        d = decide_signal(
            _work(Action.BUY), kill_switch=False, paper_trading=False,
            held_qty=0.0, already_executed_bar=False, cooldown_active=False)
        assert d.kind == 'skip'

    def test_env_parsing_is_strict_one(self, tmp_path, monkeypatch):
        from trader.strategy.auto_executor import AutoExecutor
        ex = AutoExecutor(duckdb_path=str(tmp_path / 'arm.duckdb'), paper_trading=True)
        for value, expected in [('1', True), ('true', False), ('yes', False),
                                ('True', False), ('0', False), ('', False)]:
            monkeypatch.setenv(AutoExecutor.LIVE_ARM_ENV, value)
            assert ex.live_armed is expected, f'{value!r} -> {ex.live_armed}'
        monkeypatch.delenv(AutoExecutor.LIVE_ARM_ENV)
        assert ex.live_armed is False


class TestBarAgeSeconds:
    NOW = dt.datetime(2026, 7, 16, 15, 0, tzinfo=dt.timezone.utc)

    def test_aware(self):
        ts = pd.Timestamp('2026-07-16 14:59:00', tz='UTC')
        assert bar_age_seconds(ts, self.NOW) == pytest.approx(60.0)

    def test_naive_is_utc(self):
        ts = pd.Timestamp('2026-07-16 14:58:00')
        assert bar_age_seconds(ts, self.NOW) == pytest.approx(120.0)

    def test_garbage_returns_none(self):
        assert bar_age_seconds('nope', self.NOW) is None
        assert bar_age_seconds(None, self.NOW) is None


# ---------------------------------------------------------------------------
# Preflight summary
# ---------------------------------------------------------------------------

class TestPreflightSummary:
    def test_ok_line(self):
        from trader.mmr_cli import _preflight_summary
        ok, line = _preflight_summary([
            {'check': 'a', 'status': 'PASS', 'detail': ''},
            {'check': 'b', 'status': 'SKIP', 'detail': ''},
        ])
        assert ok and line.startswith('PREFLIGHT OK')

    def test_warn_named_in_line(self):
        from trader.mmr_cli import _preflight_summary
        ok, line = _preflight_summary([
            {'check': 'a', 'status': 'PASS', 'detail': ''},
            {'check': 'roster', 'status': 'WARN', 'detail': 'x'},
        ])
        assert ok and 'roster' in line

    def test_fail_line_carries_detail(self):
        from trader.mmr_cli import _preflight_summary
        ok, line = _preflight_summary([
            {'check': 'ib_socket', 'status': 'FAIL', 'detail': 'round-trip failed'},
        ])
        assert not ok
        assert line.startswith('PREFLIGHT FAIL')
        assert 'ib_socket: round-trip failed' in line
