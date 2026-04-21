from dataclasses import dataclass, field
from trader.common.logging_helper import setup_logging
from trader.data.data_access import TickData, TickStorage
from trader.data.event_store import EventType, TradingEvent
from trader.data.market_data import normalize_historical
from trader.data.store import DateRange
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.slippage import FixedBPS, SlippageModel
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from typing import Any, Dict, List, Optional

import datetime as dt
import importlib
import importlib.util
import inspect
import math
import numpy as np
import os
import pandas as pd
import sys


logging = setup_logging(module_name='backtester')


@dataclass
class BacktestConfig:
    start_date: dt.datetime = field(default_factory=lambda: dt.datetime.now() - dt.timedelta(days=365))
    end_date: dt.datetime = field(default_factory=dt.datetime.now)
    initial_capital: float = 100_000.0
    bar_size: BarSize = BarSize.Mins1
    slippage_bps: float = 1.0
    commission_per_share: float = 0.005
    slippage_model: Optional['SlippageModel'] = None  # overrides slippage_bps when set
    # Fill policy. ``next_open`` is realistic: a signal emitted at bar t
    # executes at bar t+1's open. ``same_close`` reproduces the (lookahead-
    # biased) legacy behavior and is only intended for regression tests.
    fill_policy: str = 'next_open'


@dataclass
class BacktestTrade:
    timestamp: dt.datetime
    conid: int
    action: Action
    quantity: float
    price: float
    commission: float
    signal_probability: float
    signal_risk: float


@dataclass
class BacktestResult:
    signals: List[TradingEvent]
    trades: List[BacktestTrade]
    equity_curve: pd.Series
    total_return: float
    sharpe_ratio: float
    max_drawdown: float
    win_rate: float
    total_trades: int
    # Extended practitioner metrics. All default to 0.0 so older code paths
    # that construct BacktestResult positionally still work.
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    profit_factor: float = 0.0
    expectancy_bps: float = 0.0
    time_in_market_pct: float = 0.0
    start_date: dt.datetime = field(default_factory=dt.datetime.now)
    end_date: dt.datetime = field(default_factory=dt.datetime.now)
    # Effective param overrides applied via ``apply_param_overrides``. Empty
    # when the run used class defaults. Populated by ``run_from_module``.
    applied_params: Dict[str, Any] = field(default_factory=dict)


def _coerce_param(raw: Any, current: Any, key: str) -> Any:
    """Coerce a raw param override (usually a CLI string) to the type of
    the existing class attribute. Bools get string-aware handling because
    ``bool("False")`` is ``True``.

    Rejects malformed inputs with a clear ``ValueError`` naming the key
    rather than failing deep inside the strategy's indicator math.
    """
    # Already native type — fast path.
    if type(current) is type(raw) or raw is None:
        return raw
    if isinstance(current, bool):
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            low = raw.strip().lower()
            if low in ('true', 'yes', '1'):
                return True
            if low in ('false', 'no', '0'):
                return False
            raise ValueError(
                f"param {key!r} expects a boolean; got {raw!r}"
            )
        return bool(raw)
    if isinstance(current, int):
        try:
            return int(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"param {key!r} expects int; got {raw!r}"
            )
    if isinstance(current, float):
        try:
            return float(raw)
        except (TypeError, ValueError):
            raise ValueError(
                f"param {key!r} expects float; got {raw!r}"
            )
    if isinstance(current, str):
        return str(raw)
    # For anything else (lists, dicts) pass through as-is; JSON sweep path
    # will have already parsed them.
    return raw


def _coerce_loose(raw: Any) -> Any:
    """Best-effort numeric coercion for lower-case ``self.params`` keys,
    where we have no type info. Tries int, then float, else returns the
    value unchanged. JSON-parsed inputs (already typed) pass straight through.
    """
    if not isinstance(raw, str):
        return raw
    try:
        return int(raw)
    except (TypeError, ValueError):
        pass
    try:
        return float(raw)
    except (TypeError, ValueError):
        pass
    low = raw.strip().lower()
    if low in ('true', 'yes'):
        return True
    if low in ('false', 'no'):
        return False
    return raw


class Backtester:
    def __init__(
        self,
        storage: TickStorage,
        config: BacktestConfig,
        risk_limits: Optional[RiskLimits] = None,
    ):
        self.storage = storage
        self.config = config
        self.risk_limits = risk_limits or RiskLimits()

        if self.config.slippage_model is None:
            self.config.slippage_model = FixedBPS(self.config.slippage_bps)

    def _load_strategy_class(self, module_path: str, class_name: str):
        filepath = os.path.abspath(os.path.expanduser(module_path))
        if not os.path.exists(filepath):
            raise FileNotFoundError(f'strategy module not found: {filepath}')

        # Sandbox: strategy modules must live inside a known safe root — either
        # the project's strategies/ directory or the tests/ tree (so pytest
        # fixtures can load their own test strategies). An arbitrary
        # /tmp/evil.py should never be executed by the backtester.
        allowed_roots = []
        try:
            cur = os.path.abspath(os.path.dirname(__file__))
            # Walk up to the project root (contains ``config_defaults/trader.yaml``)
            while cur != os.path.dirname(cur):
                if os.path.exists(os.path.join(cur, 'config_defaults', 'trader.yaml')):
                    allowed_roots.append(os.path.join(cur, 'strategies'))
                    allowed_roots.append(os.path.join(cur, 'tests'))
                    break
                cur = os.path.dirname(cur)
        except Exception:
            pass
        if os.environ.get('MMR_STRATEGIES_EXTRA_ROOT'):
            # Escape hatch for research installs that keep strategies elsewhere.
            allowed_roots.append(os.path.abspath(
                os.path.expanduser(os.environ['MMR_STRATEGIES_EXTRA_ROOT'])
            ))
        # Under pytest, test fixtures write throwaway strategy files to
        # ``tmp_path`` (system tempdir). Allow that so in-process tests can
        # exercise the loader without disabling the sandbox globally.
        if 'PYTEST_CURRENT_TEST' in os.environ:
            import tempfile as _tempfile
            allowed_roots.append(os.path.realpath(_tempfile.gettempdir()))
        if allowed_roots and not any(
            os.path.realpath(filepath) == r
            or os.path.realpath(filepath).startswith(r + os.sep)
            for r in (os.path.realpath(x) for x in allowed_roots)
        ):
            raise ValueError(
                f'strategy module {module_path!r} resolves outside allowed '
                f'roots {allowed_roots!r}; refusing to load. '
                f'Move the file under strategies/ or set '
                f'MMR_STRATEGIES_EXTRA_ROOT.'
            )

        # Namespace the module key by the class name rather than just the
        # basename, so two strategies in different dirs with the same
        # filename don't clobber each other in sys.modules.
        module_name = f'_mmr_backtest_{class_name}_{os.path.splitext(os.path.basename(filepath))[0]}'
        sys.modules.pop(module_name, None)

        spec = importlib.util.spec_from_file_location(module_name, filepath)
        if not spec or not spec.loader:
            raise ImportError(f'cannot load module from {filepath}')

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise

        class_object = getattr(module, class_name)
        if not (inspect.isclass(class_object) and issubclass(class_object, Strategy)):
            raise TypeError(f'{class_name} is not a subclass of Strategy')
        return class_object

    @staticmethod
    def _bars_per_year(bar_size: BarSize) -> float:
        """Return the approximate number of bars per year for annualizing Sharpe ratio."""
        trading_days = 252
        trading_hours = 6.5  # NYSE regular hours
        trading_minutes = trading_hours * 60  # 390
        trading_seconds = trading_minutes * 60

        mapping = {
            BarSize.Secs1: trading_days * trading_seconds,
            BarSize.Secs5: trading_days * trading_seconds / 5,
            BarSize.Secs10: trading_days * trading_seconds / 10,
            BarSize.Secs15: trading_days * trading_seconds / 15,
            BarSize.Secs30: trading_days * trading_seconds / 30,
            BarSize.Mins1: trading_days * trading_minutes,
            BarSize.Mins2: trading_days * trading_minutes / 2,
            BarSize.Mins3: trading_days * trading_minutes / 3,
            BarSize.Mins5: trading_days * trading_minutes / 5,
            BarSize.Mins10: trading_days * trading_minutes / 10,
            BarSize.Mins15: trading_days * trading_minutes / 15,
            BarSize.Mins20: trading_days * trading_minutes / 20,
            BarSize.Mins30: trading_days * trading_minutes / 30,
            BarSize.Hours1: trading_days * trading_hours,
            BarSize.Hours2: trading_days * trading_hours / 2,
            BarSize.Hours3: trading_days * trading_hours / 3,
            BarSize.Hours4: trading_days * trading_hours / 4,
            BarSize.Hours8: trading_days,  # ~1 bar per day
            BarSize.Days1: trading_days,
            BarSize.Weeks1: 52,
            BarSize.Months1: 12,
        }
        return mapping.get(bar_size, trading_days * trading_minutes)

    def run(
        self,
        strategy: Strategy,
        conids: List[int],
    ) -> BacktestResult:
        logging.info(
            f'starting backtest: {strategy.name}, conids={conids}, '
            f'{self.config.start_date} to {self.config.end_date}'
        )

        signals: List[TradingEvent] = []
        trades: List[BacktestTrade] = []
        equity_values: List[float] = []
        equity_timestamps: List[dt.datetime] = []

        cash = self.config.initial_capital
        positions: Dict[int, float] = {}  # conid -> quantity (positive=long, negative=short)
        position_entry_prices: Dict[int, float] = {}
        # Time-based exit conditions per open position. Recorded when a
        # BUY carrying ``max_hold_bars`` / ``close_by_time`` fills; checked
        # at the top of each subsequent bar; cleared when the position
        # closes (organically or via the synthesized SELL).
        #   conid -> {
        #     'entry_bar_index': int,
        #     'max_hold_bars': Optional[int],
        #     'close_by_time': Optional[dt.time],
        #     'pending_exit': bool,  # True once the synthetic SELL has been
        #                            # queued, so we don't re-trigger before
        #                            # the fill lands at next bar's open.
        #   }
        exit_conditions: Dict[int, Dict[str, Any]] = {}

        date_range = DateRange(start=self.config.start_date, end=self.config.end_date)

        # Load all historical data for each conid
        all_data: Dict[int, pd.DataFrame] = {}
        for conid in conids:
            tickdata = self.storage.get_tickdata(self.config.bar_size)
            try:
                data = tickdata.read(conid, date_range=date_range)
                if data is not None and len(data) > 0:
                    normalized = normalize_historical(data)
                    # Drop rows with NaN close prices (e.g. future dates from API)
                    normalized = normalized.dropna(subset=['close'])
                    if len(normalized) > 0:
                        all_data[conid] = normalized
                    logging.info(f'loaded {len(normalized)} bars for conid {conid}')
                else:
                    logging.warning(f'no data found for conid {conid}')
            except Exception as e:
                logging.warning(f'error loading data for conid {conid}: {e}')

        if not all_data:
            raise ValueError('no historical data available for any conids')

        # Merge all data into a single timeline sorted by timestamp
        combined = pd.concat(
            [df.assign(conid=conid) for conid, df in all_data.items()],
            axis=0
        ).sort_index()

        # Walk forward bar-by-bar, building an expanding window for each conid.
        # To avoid lookahead bias: when a strategy emits a signal while
        # observing bar t, we queue it for execution at bar t+1's open price.
        # Under ``fill_policy == 'same_close'`` we preserve the legacy (biased)
        # behavior for regression tests.
        accumulated: Dict[int, pd.DataFrame] = {}
        last_prices: Dict[int, float] = {}
        # Pending signals: list of (signal, conid, signal_timestamp)
        pending_signals: List[tuple] = []

        # Track how many bars we held at least one open position. Populated
        # inside the main loop; used at the end to compute time_in_market_pct.
        bars_in_market = 0

        # Precompute hook — strategies that override ``precompute`` get their
        # indicator work done ONCE per conid here instead of on every bar.
        # This collapses the vectorbt / numba path from O(N²) to O(N).
        precompute_state: Dict[int, Dict[str, Any]] = {}
        for conid, full_df in all_data.items():
            try:
                precompute_state[conid] = strategy.precompute(full_df) or {}
            except Exception as ex:
                logging.warning(
                    'strategy.precompute() failed for conid %s: %s — '
                    'falling back to on_prices dispatch', conid, ex,
                )
                precompute_state[conid] = {}

        # Per-conid running index of how many of its bars we've consumed.
        # Used to call ``on_bar(full_prices, state, index)`` in O(1).
        bar_index: Dict[int, int] = {conid: -1 for conid in all_data}

        def _execute_signal(signal, conid, signal_ts, bar_ts, bar_row, fill_basis):
            nonlocal cash
            signal_event = TradingEvent(
                event_type=EventType.SIGNAL,
                timestamp=signal_ts if isinstance(signal_ts, dt.datetime) else dt.datetime.now(),
                strategy_name=signal.source_name,
                conid=conid,
                action=str(signal.action),
                signal_probability=signal.probability,
                signal_risk=signal.risk,
            )
            signals.append(signal_event)

            if fill_basis <= 0:
                return

            quantity = signal.quantity if signal.quantity > 0 else 0
            if quantity == 0:
                if signal.action == Action.SELL:
                    # Exit signals default to "close the whole position" —
                    # NOT 10% of cash (which is nonsense for a sell and
                    # rounds to 0 shares once cash is drained by multiple
                    # accumulating BUYs, silently dropping the exit).
                    held = positions.get(conid, 0)
                    quantity = held if held > 0 else 0
                else:
                    quantity = math.floor((cash * 0.1) / fill_basis) if fill_basis > 0 else 0
            if quantity <= 0:
                return

            fill_price = self.config.slippage_model.calculate(fill_basis, quantity, signal.action, bar_row)
            commission = quantity * self.config.commission_per_share

            if signal.action == Action.BUY:
                cost = quantity * fill_price + commission
                if cost > cash:
                    return
                cash -= cost
                positions[conid] = positions.get(conid, 0) + quantity
                position_entry_prices[conid] = fill_price
                # Record time-based exit conditions if the signal carries
                # them. Latest-BUY-wins: if the strategy adds to a position
                # with a different max_hold_bars, the new rule applies from
                # this bar onward (intuitive: "my new thesis is for this
                # bar's entry"). Clears any prior pending_exit flag since
                # adding to a position supersedes the queued close.
                if (getattr(signal, 'max_hold_bars', None) is not None
                        or getattr(signal, 'close_by_time', None) is not None):
                    exit_conditions[conid] = {
                        'entry_bar_index': bar_index.get(conid, 0),
                        'max_hold_bars': signal.max_hold_bars,
                        'close_by_time': signal.close_by_time,
                        'pending_exit': False,
                    }
                elif conid in exit_conditions:
                    # Adding to a position that previously had exit rules,
                    # via a new BUY without them. Drop the old rules — the
                    # strategy clearly changed its mind about the timer.
                    exit_conditions.pop(conid, None)
            elif signal.action == Action.SELL:
                held = positions.get(conid, 0)
                if held <= 0:
                    return
                sell_qty = min(quantity, held)
                commission = sell_qty * self.config.commission_per_share
                proceeds = sell_qty * fill_price - commission
                cash += proceeds
                positions[conid] = positions.get(conid, 0) - sell_qty
                quantity = sell_qty
                if positions[conid] == 0:
                    positions.pop(conid, None)
                    position_entry_prices.pop(conid, None)
                    exit_conditions.pop(conid, None)

            trades.append(BacktestTrade(
                timestamp=bar_ts if isinstance(bar_ts, dt.datetime) else dt.datetime.now(),
                conid=conid,
                action=signal.action,
                quantity=quantity,
                price=fill_price,
                commission=commission,
                signal_probability=signal.probability,
                signal_risk=signal.risk,
            ))

        for timestamp, group in combined.groupby(combined.index):
            # Snapshot per-conid bars at this timestamp
            bars_this_ts: Dict[int, pd.DataFrame] = {}
            for conid in group['conid'].unique():
                bars_this_ts[conid] = group[group['conid'] == conid].drop(columns=['conid'])

            # 1. Execute any signals queued from the previous bar at THIS bar's open
            if self.config.fill_policy == 'next_open' and pending_signals:
                still_pending = []
                for signal, conid, signal_ts in pending_signals:
                    bar = bars_this_ts.get(conid)
                    if bar is None or bar.empty:
                        # No bar for this conid yet — keep waiting (e.g. new listing)
                        still_pending.append((signal, conid, signal_ts))
                        continue
                    open_price = float(bar['open'].iloc[0])
                    _execute_signal(signal, conid, signal_ts, timestamp, bar.iloc[-1], open_price)
                pending_signals = still_pending

            # 2. Accumulate this bar into the expanding window + update last prices
            for conid, bar in bars_this_ts.items():
                if conid not in accumulated:
                    accumulated[conid] = bar
                else:
                    accumulated[conid] = pd.concat([accumulated[conid], bar], axis=0)
                last_prices[conid] = float(bar['close'].iloc[-1])
                bar_index[conid] = bar_index.get(conid, -1) + 1

            # 2b. Check time-based exit conditions for every open position.
            # Fires synthetic SELL signals when ``max_hold_bars`` elapses
            # or ``close_by_time`` is reached. Queued under the same
            # fill policy as strategy-emitted signals (next_open by
            # default) so fills are consistent.
            for conid in list(exit_conditions.keys()):
                if positions.get(conid, 0) <= 0:
                    exit_conditions.pop(conid, None)
                    continue
                cond = exit_conditions[conid]
                if cond.get('pending_exit'):
                    continue  # synthesized SELL already queued
                idx = bar_index.get(conid, -1)
                bar_for_conid = bars_this_ts.get(conid)
                if bar_for_conid is None or bar_for_conid.empty:
                    continue
                bar_ts_here = bar_for_conid.index[-1]
                triggered = False
                reason = ''
                if cond.get('max_hold_bars') is not None:
                    bars_held = idx - cond['entry_bar_index']
                    if bars_held >= cond['max_hold_bars']:
                        triggered = True
                        reason = f'max_hold_bars={cond["max_hold_bars"]}'
                if not triggered and cond.get('close_by_time') is not None:
                    t_of_day = (
                        bar_ts_here.time() if hasattr(bar_ts_here, 'time')
                        else None
                    )
                    if t_of_day is not None and t_of_day >= cond['close_by_time']:
                        triggered = True
                        reason = f'close_by_time={cond["close_by_time"]}'
                if not triggered:
                    continue

                synthetic = Signal(
                    source_name=f'__time_exit__[{reason}]',
                    action=Action.SELL,
                    probability=1.0,
                    risk=0.0,
                    quantity=positions[conid],
                )
                cond['pending_exit'] = True  # suppress re-trigger
                if self.config.fill_policy == 'same_close':
                    _execute_signal(
                        synthetic, conid, timestamp, timestamp,
                        accumulated[conid].iloc[-1],
                        last_prices.get(conid, 0.0),
                    )
                else:
                    pending_signals.append((synthetic, conid, timestamp))

            # 3. Run the strategy on each conid at its current bar.
            #    on_bar is the fast path (reads precomputed state by index);
            #    its default impl falls back to on_prices(accumulated) so
            #    strategies that only implement the legacy API still work.
            for conid in list(bars_this_ts.keys()):
                idx = bar_index.get(conid, -1)
                if idx < 0:
                    continue
                full_prices = all_data[conid]
                state = precompute_state.get(conid, {})
                signal = strategy.on_bar(full_prices, state, idx)
                if not signal:
                    continue

                if self.config.fill_policy == 'same_close':
                    # Legacy lookahead-biased path: fill at this bar's close
                    _execute_signal(
                        signal, conid, timestamp, timestamp,
                        accumulated[conid].iloc[-1],
                        last_prices.get(conid, 0.0),
                    )
                else:
                    # Realistic path: queue for next bar's open
                    pending_signals.append((signal, conid, timestamp))

            # 4. Track equity (cash + mark-to-market positions)
            portfolio_value = cash
            for conid, qty in positions.items():
                portfolio_value += qty * last_prices.get(conid, 0)

            equity_values.append(portfolio_value)
            equity_timestamps.append(
                timestamp if isinstance(timestamp, dt.datetime) else dt.datetime.now()
            )

            # Time-in-market: count bars where at least one position is open.
            if positions:
                bars_in_market += 1

        # Build equity curve
        equity_curve = pd.Series(equity_values, index=equity_timestamps)

        # Calculate performance metrics
        total_return = (equity_curve.iloc[-1] / self.config.initial_capital - 1) if len(equity_curve) > 0 else 0.0

        # Sharpe ratio (annualized) — scale factor depends on bar size
        if len(equity_curve) > 1:
            returns = equity_curve.pct_change().dropna()
            if len(returns) > 0 and returns.std() > 0:
                bars_per_year = self._bars_per_year(self.config.bar_size)
                sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(bars_per_year)
            else:
                sharpe_ratio = 0.0
        else:
            sharpe_ratio = 0.0

        # Max drawdown
        if len(equity_curve) > 0:
            running_max = equity_curve.cummax()
            drawdown = (equity_curve - running_max) / running_max
            max_drawdown = float(drawdown.min())
        else:
            max_drawdown = 0.0

        # Win rate + per-round-trip P&L tracking. Each SELL closes part or
        # all of an open position; we compute its P&L from the weighted
        # average entry price and feed those P&Ls into profit_factor and
        # expectancy_bps below.
        avg_entry: Dict[int, float] = {}   # conid -> weighted avg entry price
        avg_qty: Dict[int, float] = {}     # conid -> total held quantity
        winning_trades = 0
        sell_trades = 0
        round_trip_pnl: List[float] = []           # dollar P&L per SELL
        round_trip_return_pct: List[float] = []    # P&L / notional per SELL
        for trade in trades:
            if trade.action == Action.BUY:
                prev_qty = avg_qty.get(trade.conid, 0.0)
                prev_cost = avg_entry.get(trade.conid, 0.0) * prev_qty
                new_qty = prev_qty + trade.quantity
                if new_qty > 0:
                    avg_entry[trade.conid] = (prev_cost + trade.price * trade.quantity) / new_qty
                    avg_qty[trade.conid] = new_qty
            elif trade.action == Action.SELL:
                sell_trades += 1
                entry_price = avg_entry.get(trade.conid, 0.0)
                # Round-trip P&L and return-on-notional for this SELL. Commission
                # was already deducted from the equity curve at fill time; we
                # subtract the sell-side commission here for a trade-level view.
                pnl = (trade.price - entry_price) * trade.quantity - trade.commission
                round_trip_pnl.append(pnl)
                if entry_price > 0:
                    notional = entry_price * trade.quantity
                    round_trip_return_pct.append(pnl / notional if notional > 0 else 0.0)
                if entry_price > 0 and trade.price > entry_price:
                    winning_trades += 1
                # Reduce tracked quantity
                remaining = avg_qty.get(trade.conid, 0.0) - trade.quantity
                if remaining <= 0:
                    avg_entry.pop(trade.conid, None)
                    avg_qty.pop(trade.conid, None)
                else:
                    avg_qty[trade.conid] = remaining

        win_rate = winning_trades / sell_trades if sell_trades > 0 else 0.0

        # ----- Extended practitioner metrics -----

        # Sortino: like Sharpe but only penalizes downside volatility. A
        # large number is less deceiving on asymmetric P&L than Sharpe.
        sortino_ratio = 0.0
        if len(equity_curve) > 1:
            returns = equity_curve.pct_change().dropna()
            negative = returns[returns < 0]
            if len(negative) > 0 and negative.std() > 0:
                bars_per_year = self._bars_per_year(self.config.bar_size)
                sortino_ratio = float((returns.mean() / negative.std()) * np.sqrt(bars_per_year))

        # Calmar: raw total_return / |max_drawdown|. Not annualized — raw
        # form is comparable across short and long backtests. "How many
        # dollars of edge per dollar of worst drawdown."
        calmar_ratio = 0.0
        if max_drawdown < 0:
            calmar_ratio = float(total_return / abs(max_drawdown))

        # Profit factor: gross wins / |gross losses|. Edge indicator that's
        # robust to trade count — >1 is profitable, >2 is robust.
        gross_wins = sum(p for p in round_trip_pnl if p > 0)
        gross_losses = sum(p for p in round_trip_pnl if p < 0)
        if gross_losses < 0:
            profit_factor = float(gross_wins / abs(gross_losses))
        elif gross_wins > 0:
            profit_factor = float('inf')  # all winners — rare but real
        else:
            profit_factor = 0.0

        # Expectancy per trade, in basis points of entry notional. This is
        # what actually has to survive slippage/commissions in production —
        # a strategy with +0.3 bps expectancy dies to 1 bps of real-world
        # slippage. Reported per round-trip (SELL-closing-position).
        if round_trip_return_pct:
            expectancy_bps = float(np.mean(round_trip_return_pct) * 10_000)
        else:
            expectancy_bps = 0.0

        # Time-in-market: fraction of bars during which at least one
        # position was open. Distinguishes always-on index-likes from
        # selective strategies.
        if len(equity_curve) > 0:
            time_in_market_pct = float(bars_in_market / len(equity_curve))
        else:
            time_in_market_pct = 0.0

        result = BacktestResult(
            signals=signals,
            trades=trades,
            equity_curve=equity_curve,
            total_return=total_return,
            sharpe_ratio=sharpe_ratio,
            max_drawdown=max_drawdown,
            win_rate=win_rate,
            total_trades=len(trades),
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            profit_factor=profit_factor,
            expectancy_bps=expectancy_bps,
            time_in_market_pct=time_in_market_pct,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
        )

        logging.info(
            f'backtest complete: {len(trades)} trades, '
            f'return={total_return:.2%}, sharpe={sharpe_ratio:.2f}, '
            f'sortino={sortino_ratio:.2f}, calmar={calmar_ratio:.2f}, '
            f'pf={profit_factor:.2f}, expectancy={expectancy_bps:.1f}bps, '
            f'time_in_mkt={time_in_market_pct:.1%}, '
            f'max_dd={max_drawdown:.2%}, win_rate={win_rate:.2%}'
        )

        return result

    @staticmethod
    def apply_param_overrides(
        instance: Strategy,
        params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Override strategy parameters. Call **after** ``install(context)``
        so the strategy has a writable ``params`` dict on its context.

        Two tunable idioms are handled:

        1. **Class attribute** (``EMA_PERIOD = 20``) — read as
           ``self.EMA_PERIOD``. Python MRO means
           ``setattr(instance, name, value)`` shadows the class attribute
           without mutating the class itself, so parallel sweeps don't
           collide on each other.
        2. **params dict** (``self.params.get('roc_period', 10)``) — older
           strategies that read from the runtime-provided params mapping.
           We write to ``instance._context.params`` directly since the
           ``Strategy.params`` property is read-only.

        Upper-case keys that aren't recognised class attributes raise
        ``ValueError`` (typos like ``EMAPERIOD`` would otherwise silently
        no-op). Lower-case keys are free-form — they always land in the
        context's params dict.

        Returns the effective overrides (post-coercion) for the caller to
        persist to ``BacktestRecord.params``.
        """
        if not params:
            return {}

        context = getattr(instance, '_context', None)

        applied: Dict[str, Any] = {}
        for key, raw in params.items():
            is_class_attr = (
                hasattr(type(instance), key)
                and key.isupper()
            )
            if is_class_attr:
                current = getattr(instance, key)
                coerced = _coerce_param(raw, current, key)
                setattr(instance, key, coerced)
                if context is not None:
                    context.params[key] = coerced
                applied[key] = coerced
            elif key.isupper():
                raise ValueError(
                    f"strategy {type(instance).__name__!r} has no "
                    f"parameter {key!r}; known class-level tunables: "
                    f"{sorted(k for k in vars(type(instance)) if k.isupper())}"
                )
            else:
                coerced = _coerce_loose(raw)
                if context is not None:
                    context.params[key] = coerced
                else:
                    # No context yet — stash on the instance so callers
                    # that apply overrides pre-install (e.g. tests) don't
                    # silently drop them. Strategy.__init__ seeds an empty
                    # dict on _pending_params that install() can pick up.
                    if not hasattr(instance, '_pending_params'):
                        instance._pending_params = {}
                    instance._pending_params[key] = coerced
                applied[key] = coerced
        return applied

    def run_from_module(
        self,
        module_path: str,
        class_name: str,
        conids: List[int],
        universe_accessor: Optional[UniverseAccessor] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> BacktestResult:
        class_object = self._load_strategy_class(module_path, class_name)
        instance = class_object()

        context = StrategyContext(
            name=class_name,
            bar_size=self.config.bar_size,
            conids=conids,
            universe=None,
            historical_days_prior=0,
            paper_only=False,
            storage=self.storage,
            universe_accessor=universe_accessor or UniverseAccessor.__new__(UniverseAccessor),
            logger=logging,
            module=module_path,
            class_name=class_name,
        )
        instance.install(context)
        instance.state = StrategyState.RUNNING

        # Overrides go on AFTER install so we can write to the context's
        # params dict. For class-attr overrides we also setattr on the
        # instance, so downstream ``self.EMA_PERIOD`` reads get the new
        # value regardless of which idiom the strategy uses.
        applied_params = self.apply_param_overrides(instance, params)

        result = self.run(instance, conids)
        # Attach effective param overrides so the CLI can persist them on
        # the BacktestRecord. The backtester itself doesn't know about
        # storage, so we stash on the result object.
        result.applied_params = applied_params
        return result
