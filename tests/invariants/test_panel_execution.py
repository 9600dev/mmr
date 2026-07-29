"""SPEC: a panel book cannot trade on the bar it is deciding from.

`run_panel` computes target weights while observing bar t and fills them at
bar t+1's OPEN. That is the same rule `run` enforces via `fill_policy`, and it
fails the same silent way: fill at t's close instead and every strategy
acquires a free look at the price it is trading on. The equity curve stays
plausible, the trade log stays plausible, and the returns are fiction.

Unlike the per-instrument path, this one has no `fill_policy` switch to point
at — the ordering is structural, buried in the loop. So it is pinned here by
its observable consequence: the price a trade actually executes at.

The second group is arithmetic that must hold every period. If cash plus
marked positions ever stops equalling equity, every downstream statistic is
computed from a number that is not the portfolio's value, and nothing in the
output would look wrong.
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from trader.objects import BarSize
from trader.simulation.backtester import BacktestConfig, Backtester
from trader.trading.strategy import Strategy


class _FakeTick:
    def __init__(self, frames):
        self._frames = frames

    def read(self, conid, date_range=None):
        return self._frames.get(int(conid))


class _FakeStorage:
    def __init__(self, frames):
        self._tick = _FakeTick(frames)

    def get_tickdata(self, bar_size):
        return self._tick


def _frames(n=12, conids=(1, 2, 3), seed=0):
    """Bars where open and close differ sharply, so a fill at the wrong one is
    unmistakable rather than a rounding difference."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range('2024-01-01', periods=n, freq='D', tz='UTC')
    out = {}
    for k, c in enumerate(conids):
        close = 100.0 + k * 10 + np.arange(n, dtype=float)
        opn = close * 0.90            # every open is 10% below the prior close
        out[c] = pd.DataFrame({
            'open': opn, 'high': np.maximum(opn, close) + 1.0,
            'low': np.minimum(opn, close) - 1.0, 'close': close,
            'volume': rng.integers(1000, 5000, n).astype(float),
        }, index=idx)
        out[c].index.name = 'date'
    return out


class _BuyOnceAt(Strategy):
    """Goes fully long conid 1 at a chosen index, then holds."""
    def __init__(self, at=2):
        super().__init__()
        self.at = at
        self.seen_max_index = -1

    def precompute_panel(self, panel):
        self._n = len(panel)
        return {}

    def on_panel(self, panel, state, index):
        self.seen_max_index = max(self.seen_max_index, index)
        if index != self.at:
            return None
        return {1: 1.0, 2: 0.0, 3: 0.0}


def _run(strategy, frames, capital=100_000.0, slippage_bps=0.0):
    config = BacktestConfig(
        start_date=dt.datetime(2023, 12, 1), end_date=dt.datetime(2024, 3, 1),
        initial_capital=capital, bar_size=BarSize.Days1,
        slippage_bps=slippage_bps, commission_per_share=0.0)
    bt = Backtester(_FakeStorage(frames), config)
    return bt.run_panel(strategy, list(frames.keys()))


class TestFillsHappenOnTheFollowingOpen:

    def test_the_trade_price_is_the_next_bars_open(self):
        """The decisive one. Opens sit 10% below closes here, so filling at
        the decision bar's close instead would be a 10% error — far outside
        anything slippage or rounding could explain."""
        frames = _frames()
        result = _run(_BuyOnceAt(at=2), frames)
        assert result.trades, 'no trade was placed'
        t = result.trades[0]
        expected_open = float(frames[1]['open'].iloc[3])
        decision_close = float(frames[1]['close'].iloc[2])
        assert t.price == pytest.approx(expected_open), (
            f'filled at {t.price}, expected bar-3 open {expected_open}; '
            f'bar-2 close was {decision_close} — a fill there is lookahead')

    def test_the_trade_is_stamped_on_the_fill_bar_not_the_decision_bar(self):
        frames = _frames()
        result = _run(_BuyOnceAt(at=2), frames)
        assert result.trades[0].timestamp == frames[1].index[3]

    def test_the_final_bar_cannot_open_a_position(self):
        """A decision on the last bar has no following open to fill at.
        Filling it at that bar's own close would be lookahead at exactly the
        point where it most flatters the result."""
        frames = _frames(n=8)
        result = _run(_BuyOnceAt(at=7), frames)
        assert result.trades == []

    def test_the_strategy_is_never_asked_about_the_last_bar(self):
        frames = _frames(n=10)
        strat = _BuyOnceAt(at=99)
        _run(strat, frames)
        assert strat.seen_max_index == len(frames[1]) - 2


class TestTheBookAddsUp:

    def test_equity_equals_cash_plus_marked_positions(self):
        """Terminal equity must equal shares x final close, plus the cash
        truncation left behind.

        Note the share count comes from the DECISION bar's close, not from the
        fill price — see `test_sizing_uses_the_price_known_at_decision_time`.
        Computing it from the fill would require knowing tomorrow's open
        today, which is the whole thing this file exists to prevent.
        """
        frames = _frames(n=10)
        result = _run(_BuyOnceAt(at=2), frames, capital=100_000.0)
        sizing_px = float(frames[1]['close'].iloc[2])
        fill = float(frames[1]['open'].iloc[3])
        shares = float(int(100_000.0 / sizing_px))
        cash = 100_000.0 - shares * fill
        expected = cash + shares * float(frames[1]['close'].iloc[-1])
        assert float(result.equity_curve.iloc[-1]) == pytest.approx(expected)

    def test_sizing_uses_the_price_known_at_decision_time(self):
        """A weight is converted to shares using the last price the strategy
        could actually see. When the next open gaps away from it, the realised
        notional misses the target weight — and that is CORRECT. Sizing off
        the fill price would hit the weight exactly by using a price that had
        not happened yet.
        """
        frames = _frames(n=10)
        result = _run(_BuyOnceAt(at=2), frames, capital=100_000.0)
        qty = result.trades[0].quantity
        by_decision = float(int(100_000.0 / float(frames[1]['close'].iloc[2])))
        by_fill = float(int(100_000.0 / float(frames[1]['open'].iloc[3])))
        assert by_decision != by_fill, 'fixture must make the two differ'
        assert qty == pytest.approx(by_decision), (
            f'sized {qty} shares; decision-price sizing gives {by_decision}, '
            f'fill-price sizing gives {by_fill} — the latter is lookahead')

    def test_equity_starts_at_the_initial_capital(self):
        result = _run(_BuyOnceAt(at=99), _frames())
        assert float(result.equity_curve.iloc[0]) == pytest.approx(100_000.0)

    def test_doing_nothing_earns_nothing(self):
        """A strategy that never returns weights must produce a perfectly flat
        curve. Any drift means the accounting is leaking."""
        result = _run(_BuyOnceAt(at=99), _frames())
        assert result.equity_curve.nunique() == 1
        assert result.total_return == pytest.approx(0.0)


class TestNoInstructionIsNotFlatten:
    """The distinction the panel API rests on. A signal in its warm-up returns
    None every period; if that liquidated the book, no cross-sectional
    strategy could ever hold a position through its own warm-up."""

    def test_none_holds_the_existing_book(self):
        frames = _frames(n=12)
        result = _run(_BuyOnceAt(at=2), frames)
        # Bought once at index 2; every later period returns None. The
        # position must survive to the end.
        assert len(result.trades) == 1, (
            f'{len(result.trades)} trades — a None return closed the book')

    def test_an_empty_dict_is_an_instruction_to_flatten(self):
        class _BuyThenFlatten(_BuyOnceAt):
            def on_panel(self, panel, state, index):
                if index == 2:
                    return {1: 1.0}
                if index == 5:
                    return {1: 0.0}
                return None

        result = _run(_BuyThenFlatten(), _frames(n=12))
        assert len(result.trades) == 2, 'an explicit zero weight must close'
        assert result.trades[1].quantity == pytest.approx(
            result.trades[0].quantity)


class TestSlippageIsChargedAgainstYou:

    def test_a_buy_fills_above_the_open_and_a_sell_below(self):
        class _BuyThenSell(_BuyOnceAt):
            def on_panel(self, panel, state, index):
                if index == 2:
                    return {1: 1.0}
                if index == 5:
                    return {1: 0.0}
                return None

        frames = _frames(n=12)
        result = _run(_BuyThenSell(), frames, slippage_bps=100.0)
        buy, sell = result.trades[0], result.trades[1]
        assert buy.price > float(frames[1]['open'].iloc[3])
        assert sell.price < float(frames[1]['open'].iloc[6])
