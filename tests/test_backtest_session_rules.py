"""The backtester applies the executor's regular-session rules
(docs/AUDIT_2026-11-14.md, B1.11 / B3.5).

Live, an OPEN whose order could not execute in the venue's regular session is
refused, a market SELL sent outside it waits at the broker for the next open,
and a close-by time moves earlier on a day the venue closes early. A replay
that fills an after-hours signal at the next extended-hours bar's open is
validating a different trading process from the one deployed, so with
``BacktestConfig.session_calendars`` set the replay does the same three
things. Without it nothing changes (``tests/test_backtester_pin.py`` pins
that bit for bit).

Everything here runs on in-memory frames.
"""

from __future__ import annotations

import datetime as dt
import logging
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

import trader.mmr_cli as cli
from trader.data import market_session
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.backtester import BacktestConfig, Backtester, _bar_interval_seconds
from trader.strategy.parameters import apply_param_overrides
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState

NY = 'America/New_York'
SYDNEY = 'Australia/Sydney'


class _Tick:
    def __init__(self, frames):
        self._frames = frames

    def read(self, conid, date_range=None):
        return self._frames[int(conid)].copy()


class _Store:
    def __init__(self, frames):
        self._frames = frames

    def get_tickdata(self, bar_size):
        return _Tick(self._frames)


def _minutes(days, start='04:00', end='20:00', tz=NY, out_tz=NY):
    """1-min bars for each day's ``start``..``end`` wall time in ``tz``. The
    open of every bar is unique (a ramp), so a fill names the bar it came from."""
    parts = [pd.date_range(f'{day} {start}', f'{day} {end}', freq='1min', inclusive='left', tz=tz)
             for day in days]
    idx = parts[0].append(parts[1:]).tz_convert(out_tz)
    ramp = 100.0 + pd.Series(range(len(idx)), index=idx) * 0.001
    return pd.DataFrame({'open': ramp, 'high': ramp + 0.5, 'low': ramp - 0.5, 'close': ramp,
                         'volume': 1e4}, index=idx).rename_axis('date')


class _At(Strategy):
    """BUY / SELL on the bars whose wall time (``CLOCK`` zone) it is told."""

    buys: tuple = ()
    sells: tuple = ()
    close_by = None
    max_hold = None
    clock = NY

    def on_bar(self, prices, state, index):
        stamp = prices.index[index]
        if stamp.tz is not None:
            stamp = stamp.tz_convert(self.clock)
        key = stamp.strftime('%Y-%m-%d %H:%M')
        if key in self.buys:
            return Signal(source_name='at', action=Action.BUY, probability=0.9, risk=0.1,
                          close_by_time=self.close_by, max_hold_bars=self.max_hold)
        if key in self.sells:
            return Signal(source_name='at', action=Action.SELL, probability=0.9, risk=0.1)
        return None


def _run(frame, *, buys=(), sells=(), calendars=None, close_by=None, max_hold=None, clock=NY,
         params=None, bar_size=BarSize.Mins1, **config):
    strategy = _At()
    strategy.buys, strategy.sells = tuple(buys), tuple(sells)
    strategy.close_by, strategy.max_hold, strategy.clock = close_by, max_hold, clock
    strategy.install(StrategyContext(
        name='at', bar_size=bar_size, conids=[1], universe=None, historical_days_prior=0,
        paper_only=False, storage=None, universe_accessor=UniverseAccessor.__new__(UniverseAccessor),
        logger=logging.getLogger('session-rules')))
    strategy.state = StrategyState.RUNNING
    if params:
        apply_param_overrides(strategy, params)
    base = dict(start_date=dt.datetime(2020, 1, 1), end_date=dt.datetime(2030, 1, 1),
                bar_size=bar_size, slippage_bps=0.0, commission_per_share=0.0,
                execution_mode='live', trade_notional=50_000.0, cooldown_seconds=0,
                session_calendars=calendars)
    base.update(config)
    result = Backtester(_Store({1: frame}), BacktestConfig(**base)).run(strategy, [1])
    fills = []
    for trade in result.trades:
        stamp = pd.Timestamp(trade.timestamp)
        if stamp.tz is not None:
            stamp = stamp.tz_convert(clock)
        fills.append((str(trade.action), stamp.strftime('%Y-%m-%d %H:%M')))
    return fills, result


def _fills(frame, **kw):
    return _run(frame, **kw)[0]


NYSE = {1: 'NYSE'}
WED = '2026-11-25'            # an ordinary session
THANKSGIVING = '2026-11-26'   # no session
HALF = '2026-11-27'           # closes 13:00


@pytest.fixture(autouse=True)
def _cold_session_caches():
    with market_session._lock:
        market_session._regular_cache.clear()
        market_session._early_shift_cache.clear()
    yield


class TestOpensNeedTheRegularSession:
    @pytest.mark.parametrize('bar, fill', [
        (f'{WED} 09:29', f'{WED} 09:30'),        # completes AT the open
        (f'{WED} 11:00', f'{WED} 11:01'),
        (f'{WED} 15:58', f'{WED} 15:59'),
    ])
    def test_an_order_sent_inside_it_fills_at_the_next_bar(self, bar, fill):
        assert _fills(_minutes([WED]), buys=[bar], calendars=NYSE) == [('BUY', fill)]

    @pytest.mark.parametrize('bar', [
        f'{WED} 08:00',            # pre-market
        f'{WED} 09:28',            # order at 09:29
        f'{WED} 15:59',            # order at 16:00 — the close
        f'{WED} 17:30',            # post-market
    ])
    def test_an_order_sent_outside_it_is_never_filled(self, bar):
        assert _fills(_minutes([WED, HALF]), buys=[bar], calendars=NYSE) == []

    def test_without_calendars_the_replay_is_as_it_was(self):
        """The fill a market order cannot get: the next post-market print."""
        assert _fills(_minutes([WED]), buys=[f'{WED} 17:30']) == [('BUY', f'{WED} 17:31')]
        assert _fills(_minutes([WED]), buys=[f'{WED} 15:59']) == [('BUY', f'{WED} 16:00')]

    def test_an_early_close_is_the_close(self):
        frame = _minutes([HALF])
        assert _fills(frame, buys=[f'{HALF} 12:58'], calendars=NYSE) == [('BUY', f'{HALF} 12:59')]
        assert _fills(frame, buys=[f'{HALF} 12:59'], calendars=NYSE) == []
        assert _fills(frame, buys=[f'{HALF} 13:30'], calendars=NYSE) == []
        # ...where a hard-coded 16:00 session would have taken it.
        assert _fills(frame, buys=[f'{HALF} 13:30']) == [('BUY', f'{HALF} 13:31')]

    def test_the_signal_is_still_recorded(self):
        _, result = _run(_minutes([WED]), buys=[f'{WED} 17:30'], calendars=NYSE)
        assert [str(event.action) for event in result.signals] == ['BUY']
        assert result.total_trades == 0

    def test_it_agrees_with_the_executors_rule_bar_for_bar(self):
        """One function behind both. Every minute of a half day."""
        frame = _minutes([HALF])
        for stamp in frame.index[::7]:
            key = stamp.strftime('%Y-%m-%d %H:%M')
            executor_takes_it = market_session.intraday_open_refusal(stamp, 60.0, 'SMART', 'NYSE', 'STK') is None
            filled = bool(_fills(frame, buys=[key], calendars=NYSE))
            # the day's last bar has no next bar to fill at
            assert filled == (executor_takes_it and stamp != frame.index[-1]), key

    def test_a_pyramid_add_is_an_open(self):
        frame = _minutes([WED])
        fills = _fills(frame, buys=[f'{WED} 10:00', f'{WED} 11:00', f'{WED} 16:30'], calendars=NYSE,
                       execution_mode='pyramid_fixed', pyramid_max_adds=3)
        assert fills == [('BUY', f'{WED} 10:01'), ('BUY', f'{WED} 11:01')]

    def test_the_rule_does_not_depend_on_the_execution_mode(self):
        fills = _fills(_minutes([WED]), buys=[f'{WED} 17:30'], calendars=NYSE, execution_mode='accumulate')
        assert fills == []

    def test_the_legacy_same_close_policy_is_judged_at_the_bars_end_too(self):
        frame = _minutes([WED])
        assert _fills(frame, buys=[f'{WED} 15:58'], calendars=NYSE, fill_policy='same_close') == [
            ('BUY', f'{WED} 15:58')]
        assert _fills(frame, buys=[f'{WED} 15:59'], calendars=NYSE, fill_policy='same_close') == []

    def test_a_five_minute_bar_carries_its_own_interval(self):
        idx = pd.date_range(f'{WED} 09:30', f'{WED} 16:30', freq='5min', inclusive='left', tz=NY)
        frame = pd.DataFrame({'open': 100.0, 'high': 100.5, 'low': 99.5, 'close': 100.0, 'volume': 1e4},
                             index=idx).rename_axis('date')
        kw = dict(calendars=NYSE, bar_size=BarSize.Mins5)
        assert _fills(frame, buys=[f'{WED} 15:50'], **kw) == [('BUY', f'{WED} 15:55')]
        assert _fills(frame, buys=[f'{WED} 15:55'], **kw) == []          # completes at 16:00


class TestWhichInstrumentsTheRulesApplyTo:
    @pytest.mark.parametrize('calendars', [None, {}, {1: None}, {1: ''}, {2: 'NYSE'}])
    def test_no_calendar_for_the_instrument_means_no_rules(self, calendars):
        fills = _fills(_minutes([WED]), buys=[f'{WED} 17:30'], sells=[f'{WED} 18:00'], calendars=calendars)
        assert fills == [('BUY', f'{WED} 17:31'), ('SELL', f'{WED} 18:01')]

    def test_the_venue_is_the_instruments_own(self):
        """Sydney's 10:30 is the evening before in New York. The store hands
        every venue's history back New York-indexed."""
        frame = _minutes(['2026-11-25'], '10:00', '16:10', tz=SYDNEY)
        assert str(frame.index.tz) == NY
        kw = dict(clock=SYDNEY, params={'SESSION_TZ': SYDNEY})
        assert _fills(frame, buys=['2026-11-25 10:30'], calendars={1: 'ASX'}, **kw) == [
            ('BUY', '2026-11-25 10:31')]
        assert _fills(frame, buys=['2026-11-25 10:30'], calendars={1: 'NYSE'}, **kw) == []
        # ASX runs to the 16:10 closing auction: 16:08 trades, 16:09 completes at the close.
        assert _fills(frame, buys=['2026-11-25 16:08'], calendars={1: 'ASX'}, **kw) == [
            ('BUY', '2026-11-25 16:09')]
        assert _fills(frame, buys=['2026-11-25 16:09'], calendars={1: 'ASX'}, **kw) == []

    def test_a_frame_without_a_zone_is_venue_wall_time(self):
        frame = _minutes([WED])
        frame.index = frame.index.tz_localize(None)
        assert _fills(frame, buys=[f'{WED} 15:58'], calendars=NYSE) == [('BUY', f'{WED} 15:59')]
        assert _fills(frame, buys=[f'{WED} 15:59'], calendars=NYSE) == []

    def test_a_utc_indexed_frame_is_the_same_instants(self):
        frame = _minutes([WED], out_tz='UTC')
        assert _fills(frame, buys=[f'{WED} 15:58'], calendars=NYSE) == [('BUY', f'{WED} 15:59')]
        assert _fills(frame, buys=[f'{WED} 15:59'], calendars=NYSE) == []

    def test_daily_bars_are_decided_after_the_close_by_design(self):
        idx = pd.bdate_range('2026-11-02', periods=10, tz=NY)
        frame = pd.DataFrame({'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.0, 'volume': 1e6},
                             index=idx).rename_axis('date')
        buy, sell = idx[2].strftime('%Y-%m-%d %H:%M'), idx[5].strftime('%Y-%m-%d %H:%M')
        with_rules = _fills(frame, buys=[buy], sells=[sell], calendars=NYSE, bar_size=BarSize.Days1)
        without = _fills(frame, buys=[buy], sells=[sell], bar_size=BarSize.Days1)
        assert with_rules == without == [('BUY', idx[3].strftime('%Y-%m-%d %H:%M')),
                                         ('SELL', idx[6].strftime('%Y-%m-%d %H:%M'))]


class TestExitsWaitForTheRegularSession:
    """IB holds a market order sent outside the regular session until the next
    one opens. That is where it fills — not at the next extended-hours print."""

    def test_an_exit_decided_in_session_fills_at_the_next_bar(self):
        fills = _fills(_minutes([WED]), buys=[f'{WED} 10:00'], sells=[f'{WED} 15:00'], calendars=NYSE)
        assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{WED} 15:01')]

    @pytest.mark.parametrize('decided', [f'{WED} 15:59', f'{WED} 16:30', f'{WED} 19:59'])
    def test_an_exit_decided_after_the_close_fills_at_the_next_open(self, decided):
        """Thanksgiving in between: no session, so the Friday open."""
        frame = _minutes([WED, HALF])
        fills, result = _run(frame, buys=[f'{WED} 10:00'], sells=[decided], calendars=NYSE)
        assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{HALF} 09:30')]
        assert result.trades[1].price == pytest.approx(float(frame.loc[pd.Timestamp(f'{HALF} 09:30', tz=NY), 'open']))

    def test_an_exit_decided_before_the_open_fills_at_the_open(self):
        frame = _minutes(['2026-11-24', WED])
        fills = _fills(frame, buys=['2026-11-24 10:00'], sells=[f'{WED} 07:00'], calendars=NYSE)
        assert fills == [('BUY', '2026-11-24 10:01'), ('SELL', f'{WED} 09:30')]

    def test_without_calendars_it_filled_in_the_post_market(self):
        fills = _fills(_minutes([WED, HALF]), buys=[f'{WED} 10:00'], sells=[f'{WED} 16:30'])
        assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{WED} 16:31')]

    def test_bars_on_a_day_with_no_session_do_not_fill_it(self):
        """A feed that prints on the holiday changes nothing."""
        frame = _minutes([WED, THANKSGIVING, HALF])
        fills = _fills(frame, buys=[f'{WED} 10:00'], sells=[f'{WED} 16:30'], calendars=NYSE)
        assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{HALF} 09:30')]

    def test_a_time_exit_decided_after_the_close_waits_too(self):
        frame = _minutes([WED, HALF])
        fills = _fills(frame, buys=[f'{WED} 15:00'], max_hold=120, calendars=NYSE)       # decided 17:00
        assert fills == [('BUY', f'{WED} 15:01'), ('SELL', f'{HALF} 09:30')]
        assert _fills(frame, buys=[f'{WED} 15:00'], max_hold=120) == [
            ('BUY', f'{WED} 15:01'), ('SELL', f'{WED} 17:01')]

    def test_a_waiting_exit_is_not_overtaken_by_a_new_entry(self):
        """The after-hours BUY is refused; the position leaves at the open."""
        frame = _minutes([WED, HALF])
        fills = _fills(frame, buys=[f'{WED} 10:00', f'{WED} 17:00'], sells=[f'{WED} 16:30'], calendars=NYSE)
        assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{HALF} 09:30')]

    def test_an_exit_still_waiting_when_the_data_ends_never_fills(self):
        fills = _fills(_minutes([WED]), buys=[f'{WED} 10:00'], sells=[f'{WED} 16:30'], calendars=NYSE)
        assert fills == [('BUY', f'{WED} 10:01')]

    def test_a_session_that_cannot_be_read_does_not_delay_an_exit(self):
        """Unknown is not "shut". The exit fills where it always did."""
        with mock.patch.object(market_session, '_calendar', side_effect=RuntimeError('calendar unavailable')):
            fills = _fills(_minutes([WED]), sells=[f'{WED} 16:30'], calendars=NYSE,
                           buys=[])
            assert fills == []
        frame = _minutes([WED])
        real = market_session.in_regular_session

        def unknown_after_four(instant, name):
            answer = real(instant, name)
            return None if answer is False else answer

        with mock.patch('trader.simulation.backtester.in_regular_session', side_effect=unknown_after_four):
            fills = _fills(frame, buys=[f'{WED} 10:00'], sells=[f'{WED} 16:30'], calendars=NYSE)
        assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{WED} 16:31')]

    def test_a_session_that_cannot_be_read_refuses_the_open(self):
        """Fail-closed for opens, as the executor is."""
        with mock.patch.object(market_session, '_calendar', side_effect=RuntimeError('calendar unavailable')):
            assert _fills(_minutes([WED]), buys=[f'{WED} 11:00'], calendars=NYSE) == []


class TestCloseByMovesWithAnEarlyClose:
    def test_a_1545_flatten_fires_at_1245_on_a_one_oclock_close(self):
        fills = _fills(_minutes([HALF]), buys=[f'{HALF} 10:00'], close_by=dt.time(15, 45), calendars=NYSE)
        assert fills == [('BUY', f'{HALF} 10:01'), ('SELL', f'{HALF} 12:46')]

    def test_without_calendars_it_fired_in_the_post_market(self):
        fills = _fills(_minutes([HALF]), buys=[f'{HALF} 10:00'], close_by=dt.time(15, 45))
        assert fills == [('BUY', f'{HALF} 10:01'), ('SELL', f'{HALF} 15:46')]

    def test_an_ordinary_day_is_unchanged(self):
        for calendars in (None, NYSE):
            fills = _fills(_minutes([WED]), buys=[f'{WED} 10:00'], close_by=dt.time(15, 45), calendars=calendars)
            assert fills == [('BUY', f'{WED} 10:01'), ('SELL', f'{WED} 15:46')]

    def test_the_shift_is_per_day(self):
        """Opened on the half day after its (moved) cut-off... and held to an
        ordinary Monday, where the holding's own 15:45 applies again."""
        monday = '2026-11-30'
        frame = _minutes([HALF, monday])
        fills = _fills(frame, buys=[f'{HALF} 12:50'], close_by=dt.time(15, 45), calendars=NYSE)
        # 12:51 is already past the moved 12:45: flattened at once, in session.
        assert fills == [('BUY', f'{HALF} 12:51'), ('SELL', f'{HALF} 12:52')]
        fills = _fills(frame, buys=[f'{monday} 10:00'], close_by=dt.time(15, 45), calendars=NYSE)
        assert fills == [('BUY', f'{monday} 10:01'), ('SELL', f'{monday} 15:46')]

    def test_the_venues_own_early_close_is_used(self):
        """ASX ends two hours early on Christmas Eve (14:10 for 16:10)."""
        frame = _minutes(['2026-12-24'], '10:00', '14:10', tz=SYDNEY)
        fills = _fills(frame, buys=['2026-12-24 10:30'], close_by=dt.time(15, 45), calendars={1: 'ASX'},
                       clock=SYDNEY, params={'SESSION_TZ': SYDNEY})
        assert fills == [('BUY', '2026-12-24 10:31'), ('SELL', '2026-12-24 13:46')]

    def test_a_calendar_that_cannot_answer_leaves_the_stored_time_in_force(self):
        def broken(*args, **kwargs):
            raise RuntimeError('calendar unavailable')

        with mock.patch('trader.simulation.backtester.close_by_on', side_effect=broken):
            fills = _fills(_minutes([HALF, '2026-11-30']), buys=[f'{HALF} 10:00'],
                           close_by=dt.time(15, 45), calendars=NYSE)
        # 15:45 is after the 13:00 close, so the order waits for Monday's open.
        assert fills == [('BUY', f'{HALF} 10:01'), ('SELL', '2026-11-30 09:30')]


class TestBarIntervalSeconds:
    @pytest.mark.parametrize('bar_size, seconds', [
        (BarSize.Secs5, 5.0), (BarSize.Mins1, 60.0), (BarSize.Mins5, 300.0), (BarSize.Hours1, 3600.0),
        (BarSize.Days1, 86400.0), (BarSize.Weeks1, 604800.0),
    ])
    def test_fixed_length_bars(self, bar_size, seconds):
        assert _bar_interval_seconds(bar_size) == seconds

    def test_a_month_has_no_fixed_length(self):
        assert _bar_interval_seconds(BarSize.Months1) is None


# --------------------------------------------------------------------------
# The CLI supplies the calendars for live-semantics runs
# --------------------------------------------------------------------------

def _definition(conid, exchange, primary, sec_type='STK'):
    return SimpleNamespace(conId=conid, exchange=exchange, primaryExchange=primary, secType=sec_type)


class _Accessor:
    def __init__(self, definitions, fail=()):
        self.definitions = {d.conId: d for d in definitions}
        self.fail = set(fail)
        self.asked = []

    def resolve_symbol(self, symbol, exchange='', universe='', sec_type='', first_only=False):
        self.asked.append((symbol, first_only))
        if symbol in self.fail:
            raise RuntimeError('catalogue unavailable')
        found = self.definitions.get(symbol)
        return [found] if found else []


class TestSessionCalendarsFor:
    def test_each_instrument_gets_its_listing_venues_calendar(self):
        accessor = _Accessor([_definition(1, 'SMART', 'NASDAQ'), _definition(2, 'ASX', 'ASX'),
                              _definition(3, 'ARCA', 'ARCA')])
        assert cli._session_calendars_for([1, 2, 3], accessor) == {1: 'NASDAQ', 2: 'ASX', 3: 'NYSE'}
        assert accessor.asked == [(1, True), (2, True), (3, True)]

    def test_the_conid_is_looked_up_as_an_integer(self):
        """A string would be read as a ticker."""
        accessor = _Accessor([_definition(4391, 'SMART', 'NASDAQ')])
        assert cli._session_calendars_for(['4391'], accessor) == {4391: 'NASDAQ'}
        assert accessor.asked == [(4391, True)]

    def test_instruments_the_rules_do_not_apply_to_map_to_none(self):
        accessor = _Accessor([_definition(1, 'SMART', 'NASDAQ'), _definition(2, 'IDEALPRO', '', 'CASH'),
                              _definition(3, 'SMART', ''), _definition(4, 'XYZ', 'XYZ')])
        assert cli._session_calendars_for([1, 2, 3, 4, 5], accessor) == {
            1: 'NASDAQ', 2: None, 3: None, 4: None, 5: None}

    @pytest.mark.parametrize('conids', [[], None, [7], [2, 3]])
    def test_no_calendar_at_all_is_no_rules(self, conids):
        """Configured, and stamped, like a run from before the rules existed."""
        accessor = _Accessor([_definition(2, 'IDEALPRO', '', 'CASH'), _definition(3, 'SMART', '')])
        assert cli._session_calendars_for(conids, accessor) is None

    def test_a_lookup_that_fails_is_one_instrument_without_rules(self, caplog):
        accessor = _Accessor([_definition(1, 'SMART', 'NASDAQ'), _definition(2, 'ASX', 'ASX')], fail=[2])
        with caplog.at_level(logging.WARNING):
            assert cli._session_calendars_for([1, 2], accessor) == {1: 'NASDAQ', 2: None}
        assert any('conId 2' in record.getMessage() and 'catalogue unavailable' in record.getMessage()
                   for record in caplog.records)


class TestTheStampSaysWhichRulesApplied:
    def _config(self, **kw):
        base = dict(execution_mode='live', initial_capital=100_000.0)
        base.update(kw)
        return BacktestConfig(**base)

    def test_a_run_with_calendars_names_them(self):
        stamped = cli._stamp_execution_params({}, self._config(session_calendars={1: 'NASDAQ', 2: 'ASX', 3: 'NASDAQ'}))
        assert stamped['_session_calendars'] == 'ASX,NASDAQ'

    def test_an_instrument_without_one_is_named_too(self):
        stamped = cli._stamp_execution_params({}, self._config(session_calendars={1: 'NYSE', 2: None}))
        assert stamped['_session_calendars'] == 'NYSE,none'

    @pytest.mark.parametrize('calendars', [None, {}])
    def test_a_run_without_them_is_stamped_as_before(self, calendars):
        stamped = cli._stamp_execution_params({}, self._config(session_calendars=calendars))
        assert stamped == {'_execution_mode': 'live', '_trade_notional': 2_000.0}
