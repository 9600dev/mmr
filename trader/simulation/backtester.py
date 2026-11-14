from dataclasses import dataclass, field
from trader.common.logging_helper import setup_logging
from trader.data.data_access import TickData, TickStorage
from trader.data.event_store import EventType, TradingEvent
from trader.data.market_data import normalize_historical
from trader.strategy.parameters import _coerce_param, _coerce_loose, session_bar_ts
from trader.data.store import DateRange
from trader.data.universe import UniverseAccessor
from trader.objects import Action, BarSize
from trader.simulation.slippage import FixedBPS, SlippageModel
from trader.trading.risk_gate import RiskGate, RiskLimits
from trader.trading.strategy import Signal, Strategy, StrategyContext, StrategyState
from typing import Any, Dict, FrozenSet, List, NamedTuple, Optional, Sequence

from collections.abc import Iterable
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
    # Execution semantics. ``accumulate`` (legacy): repeat BUYs pyramid into
    # the position and each BUY sizes to 10% of CURRENT cash (compounding).
    # ``live`` matches the AutoExecutor that trades these strategies for
    # real: a BUY while the conid's lot is open is refused (no pyramiding),
    # sizing is a fixed notional per trade (non-compounding), and re-opens
    # within ``cooldown_seconds`` of the last fill are refused. Validating
    # under ``accumulate`` and deploying under live semantics produces
    # incomparable return distributions — trend-stacking is a different
    # trading process. ``pyramid_fixed`` is the decomposition mode between
    # them: repeat BUYs stack like ``accumulate``, but every lot is the
    # fixed ``trade_notional`` and the cooldown applies — isolating
    # "add to winners" (pyramiding) from "bet bigger as equity grows"
    # (compounding), so you can tell which amplifier a legacy result
    # actually came from.
    execution_mode: str = 'accumulate'
    # Annualised borrow cost charged on SHORT positions, in basis points.
    # Default 0 so existing results are unchanged, but any long/short result
    # quoted without it is optimistic: borrowing stock is not free, and the
    # fee is charged on the value of the short every day it is held. Liquid US
    # large-caps run roughly 25-75bps/yr; hard-to-borrow names are multiples
    # of that, which is itself a reason a cross-sectional short leg concentrated
    # in losers costs more than the average suggests.
    short_borrow_bps_annual: float = 0.0
    # Cross-sectional books only: minimum change, as a fraction of the target
    # position, that justifies rebalancing a name it is already holding.
    # 0 = rebalance to target exactly, which is what produced 62x annual
    # turnover and gave up two thirds of the signal to costs. Entries and
    # exits are never suppressed.
    panel_no_trade_band: float = 0.0
    # Cross-sectional books only: cap on gross exposure (sum of |weight|).
    # 1.0 = fully invested, 2.0 = 2x levered. Scaling is DOWN only — an
    # under-invested book is a choice the strategy made, not a budget to fill.
    panel_max_gross: float = 1.0
    # hillclimb(C01): changed because the lowfreq harness needs an honest exit
    # for names that stop printing (design decision D07). Cross-sectional books
    # only, OFF (None) by default: a mapping conid -> timestamp of that name's
    # LAST stored print in the COMPLETE dataset (the caller derives it from the
    # full history, never from the window being run - a name merely missing
    # the last session of a slice is not a stopped name). A holding whose bar
    # is its terminal print is sold at that close - slippage and commission
    # charged, the fill logged like any other trade and counted in
    # ``diagnostics['panel_forced_exits']`` / ``['panel_forced_exit_value']``.
    # Without it a delisted or storage-cutoff name sits in the book forever at
    # a stale mark and can never be sold (no later open exists to fill
    # against). No open is ever fabricated. hillclimb(C02): a value may also
    # be an ITERABLE of timestamps - one per stopped episode - for callers
    # applying D07 without hindsight (trader/lowfreq_live/ledger.py); the
    # exit fires on whichever of them the name is held at.
    panel_terminal_prints: Optional[Dict[Any, Any]] = None
    # Per-trade notional in ``live`` mode. None -> 2% of initial_capital
    # (mirrors the PositionSizer's base_position default). Live sizing also
    # ATR-scales each trade (~0.4-1.5x); a fixed notional keeps per-trade
    # stats (PF, win rate, expectancy) exact and only approximates the
    # aggregate weighting.
    trade_notional: Optional[float] = None
    # ``live`` mode: minimum seconds between fills before a re-open is
    # allowed for a conid (AutoExecutor default 300). Evaluated on bar
    # timestamps — the backtest analog of the executor's wall clock.
    cooldown_seconds: int = 300
    # ``pyramid_fixed`` mode: maximum ADDS after the initial entry (None =
    # unbounded). 3 means a stack tops out at 4 lots — mirror of the live
    # executor's per-strategy ``pyramid_max_adds`` bound.
    pyramid_max_adds: Optional[int] = None
    # Measurement window, half-open ``[measure_from, measure_to)``. Both None
    # (the default) = the whole run is reported, as it always was.
    #
    # ``start_date`` is where DATA begins; a strategy that needs history must
    # be given bars before the period it is being judged on, and those bars
    # are not part of the judgement. With a window set the run is unchanged —
    # the strategy still sees and trades every bar, so indicators AND the book
    # are warm at the window start — but every reported number describes the
    # window only: the return is measured from the last mark before it, the
    # equity curve / fills / signals are sliced to it, and a round trip counts
    # only when it was both opened and closed inside it. Without this,
    # walk-forward reported each test run's warm-up — which lies inside the
    # training window its parameters were just chosen on — as out-of-sample
    # (audit 2026-11-14, B4.1). A naive bound is read on the frame's own clock.
    measure_from: Optional[dt.datetime] = None
    measure_to: Optional[dt.datetime] = None


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
    # Execution diagnostics that are not performance metrics. run_panel
    # records how often the D06 cash guard scaled a BUY fill
    # (``panel_scaled_buy_fills`` etc.); a run with a measurement window
    # records what the window excluded (``warmup_bars``, ``warmup_fills``,
    # ``window_bars_in_market`` ...). Otherwise empty for the per-instrument
    # path.
    diagnostics: Dict[str, Any] = field(default_factory=dict)


_TRADING_DAYS_PER_YEAR = 252


def _on_clock(value: Any, index: pd.Index) -> Optional[pd.Timestamp]:
    """``value`` as a Timestamp comparable with ``index``.

    A naive bound takes the index's zone: "from 2024-03-01" means midnight on
    the frame's own clock. An aware bound is converted. Against a naive index
    an aware bound keeps its wall time.
    """
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if not isinstance(ts, pd.Timestamp):          # NaT: a bound that is no instant
        raise ValueError(f'measurement bound is not a timestamp: {value!r}')
    tz = getattr(index, 'tz', None)
    if tz is None:
        return ts.tz_localize(None) if ts.tzinfo is not None else ts
    if ts.tzinfo is None:
        return ts.tz_localize(tz, ambiguous=True, nonexistent='shift_forward')
    return ts.tz_convert(tz)


class _Window(NamedTuple):
    """The part of a run that is REPORTED (``BacktestConfig.measure_from``)."""
    lo: Optional[pd.Timestamp]
    hi: Optional[pd.Timestamp]
    mask: np.ndarray        # per equity point: inside [lo, hi)
    base_equity: float      # what the window started from
    carried: bool           # True when bars precede the window: base is a MARK, not initial capital
    warmup_bars: int

    def contains(self, ts: Any) -> bool:
        if ts is None:
            return False
        t = pd.Timestamp(ts)
        return ((self.lo is None or t >= self.lo)
                and (self.hi is None or t < self.hi))


class _BarTimeline:
    """Per-timestamp iteration over the merged multi-conid frame, resolved once.

    Reproduces ``combined.groupby(combined.index)`` exactly — groups in sorted
    key order, NaN/NaT keys dropped, rows within a group in their original
    order, conids in first-appearance order — but as integer positions into
    ``combined`` instead of per-timestamp sub-DataFrames. Scalar reads go
    through numpy arrays; the bar-row Series (what a slippage model receives)
    is built only on demand, from the same frame minus the ``conid`` column,
    so its values, dtype and name are what the old ``bar.iloc[-1]`` produced.
    """

    def __init__(self, combined: pd.DataFrame):
        self._combined = combined
        self._index = combined.index
        self._bars: Optional[pd.DataFrame] = None   # combined minus 'conid', lazily
        conid_col = combined['conid'].to_numpy()
        # Columns may legitimately be absent from a sparse source; the old
        # loop raised KeyError only when it actually read one, so do we.
        self._open = combined['open'].to_numpy() if 'open' in combined.columns else None
        self._close = combined['close'].to_numpy() if 'close' in combined.columns else None

        codes, uniques = pd.factorize(combined.index, sort=True)
        codes = np.asarray(codes)
        order = np.argsort(codes, kind='stable')     # stable => original row order kept
        sorted_codes = codes[order]
        valid = sorted_codes >= 0                     # -1 = NaN/NaT key (groupby drops it)
        order, sorted_codes = order[valid], sorted_codes[valid]
        if len(order):
            starts = np.flatnonzero(np.r_[True, sorted_codes[1:] != sorted_codes[:-1]])
            ends = np.r_[starts[1:], len(order)]
        else:
            starts = ends = np.array([], dtype=np.intp)
        self._groups: List[tuple] = []
        order_list = order.tolist()
        for s, e, code in zip(starts.tolist(), ends.tolist(), sorted_codes[starts].tolist()):
            spans: Dict[Any, List[int]] = {}
            for p in order_list[s:e]:
                c = conid_col[p]
                span = spans.get(c)
                if span is None:
                    spans[c] = [p, p]
                else:
                    span[1] = p
            self._groups.append((uniques[code], {c: (v[0], v[1]) for c, v in spans.items()}))

    def __iter__(self):
        return iter(self._groups)

    def __len__(self) -> int:
        return len(self._groups)

    def open_at(self, pos: int) -> float:
        if self._open is None:
            raise KeyError('open')
        return float(self._open[pos])

    def close_at(self, pos: int) -> float:
        if self._close is None:
            raise KeyError('close')
        return float(self._close[pos])

    def index_at(self, pos: int):
        return self._index[pos]

    def row(self, pos: int) -> pd.Series:
        if self._bars is None:
            self._bars = self._combined.drop(columns=['conid'])
        return self._bars.iloc[pos]


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

    @staticmethod
    def _observed_bars_per_year(index: Any, bar_size: BarSize,
                                session_tz: Optional[str] = None) -> float:
        """Bars per year of the curve in hand, for annualising Sharpe/Sortino.

        ``_bars_per_year`` answers from the bar SIZE and assumes a 6.5-hour
        session: 98,280 one-minute bars a year. A stored US one-minute series
        carries extended hours — 04:00-20:00, 960 bars a day — so its bar
        returns are annualised with a factor 2.46x too small and every Sharpe
        and Sortino comes out understated by sqrt(390/960) = 0.64 (audit
        2026-11-14, B4.4). The number of return observations in a year is a
        property of the series, not of its label, so it is counted: 252 x the
        median number of bars per session day. The median, not the mean,
        because the first and last day of a window are usually partial, half
        days exist, and a stray weekend quote bar is one "day" of one bar.

        Unchanged where the old factor was right: a curve that really has 390
        one-minute bars a day still gets exactly 98,280, and bar sizes of a
        day or more keep the nominal mapping (one bar per session is what it
        already says; weeks and months cannot be counted per day at all).

        ``session_tz`` groups bars by the venue's own date. Stored frames are
        New York-indexed for every venue, and an ASX session straddles New
        York midnight, which would split each session across two dates.

        Still an approximation: 252 sessions a year is assumed, so a 7-day
        market is understated by sqrt(365/252).
        """
        nominal = Backtester._bars_per_year(bar_size)
        if nominal <= _TRADING_DAYS_PER_YEAR:
            return nominal
        if not isinstance(index, pd.DatetimeIndex) or len(index) == 0:
            return nominal
        try:
            local = (index.tz_convert(session_tz)
                     if session_tz and index.tz is not None else index)
            _, counts = np.unique(local.normalize().asi8, return_counts=True)
        except Exception:
            return nominal
        per_session = float(np.median(counts))
        if not per_session > 0:
            return nominal
        return float(_TRADING_DAYS_PER_YEAR * per_session)

    def _window(self, full_curve: pd.Series) -> Optional[_Window]:
        """Resolve ``measure_from`` / ``measure_to`` against a finished curve."""
        if self.config.measure_from is None and self.config.measure_to is None:
            return None
        index = full_curve.index
        lo = _on_clock(self.config.measure_from, index)
        hi = _on_clock(self.config.measure_to, index)
        mask = np.ones(len(index), dtype=bool)
        before = np.zeros(len(index), dtype=bool)
        if lo is not None:
            before = np.asarray(index < lo)
            mask &= ~before
        if hi is not None:
            mask &= np.asarray(index < hi)
        n_before = int(np.count_nonzero(before))
        # The window starts from the last mark BEFORE it, so its first bar's
        # move is measured. With nothing before it, it starts from the capital
        # the run started with — the same convention as an unwindowed run.
        base = (float(full_curve.to_numpy(dtype=float)[before][-1]) if n_before
                else float(self.config.initial_capital))
        return _Window(lo, hi, mask, base, bool(n_before), n_before)

    @staticmethod
    def _bar_returns(full_curve: pd.Series, window: Optional[_Window]) -> pd.Series:
        """Bar-to-bar returns of the reported part of the curve.

        Differenced on the FULL curve and then sliced, so the first bar inside
        a window is measured against the last warm-up mark rather than dropped.
        """
        returns = full_curve.pct_change()
        if window is not None:
            returns = returns[window.mask]
        return returns.dropna()

    @staticmethod
    def _downside_deviation(returns: pd.Series) -> float:
        """Sortino's denominator: RMS of ``min(r, 0)`` over ALL returns.

        Target 0, N = every observation. One definition for both execution
        paths: the panel path used the standard deviation of the negative
        returns alone — it subtracts the negatives' own mean (wrong target)
        and divides by their count (wrong N) — so the same return series
        scored a different Sortino depending on which loop produced it
        (audit 2026-11-14, B4.7c).
        """
        if len(returns) == 0:
            return 0.0
        downside = returns.clip(upper=0.0)
        return float(np.sqrt((downside ** 2).mean()))

    @staticmethod
    def _window_diagnostics(window: _Window, trades: List[BacktestTrade],
                            in_market: Sequence[bool]) -> Dict[str, Any]:
        """What a measurement window excluded, so a windowed result says so."""
        flags = np.asarray(in_market, dtype=bool)
        held = (int(np.count_nonzero(flags[window.mask]))
                if len(flags) == len(window.mask) else None)
        return {
            'measure_from': None if window.lo is None else str(window.lo),
            'measure_to': None if window.hi is None else str(window.hi),
            'window_bars': int(np.count_nonzero(window.mask)),
            'window_bars_in_market': held,           # bars inside the window with a position open
            'window_base_equity': window.base_equity,
            'warmup_bars': window.warmup_bars,
            'warmup_fills': sum(1 for t in trades
                                if window.lo is not None
                                and pd.Timestamp(t.timestamp) < window.lo),
        }

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
        # live execution mode: last fill (open OR close) per conid, for the
        # cooldown gate — mirrors AutoExecutor._last_exec.
        last_fill_ts: Dict[int, Any] = {}
        # pyramid_fixed mode: open lots per conid, for the pyramid_max_adds
        # bound — mirrors auto_exec_positions.lots.
        lot_counts: Dict[int, int] = {}
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

        # Walk forward bar-by-bar. To avoid lookahead bias: when a strategy
        # emits a signal while observing bar t, we queue it for execution at
        # bar t+1's open price. Under ``fill_policy == 'same_close'`` we
        # preserve the legacy (biased) behavior for regression tests.
        #
        # Performance: the loop used to ``groupby`` the combined frame, build
        # a per-conid sub-DataFrame for every timestamp and ``pd.concat`` it
        # onto an expanding per-conid window — O(N) pandas calls per bar and an
        # O(N²) copy overall (~26s for a 35k-bar 1-min run, none of it the
        # strategy). The expanding window was only ever read for its last row.
        # Everything is now resolved ONCE up front into numpy arrays/positions
        # into ``combined``; the bar-row Series a fill hands the slippage model
        # is materialised lazily, only when a fill actually happens. Results
        # are bit-identical (pinned by tests/test_backtester_pin.py).
        timeline = _BarTimeline(combined)
        last_prices: Dict[int, float] = {}
        # Pending signals: list of (signal, conid, signal_timestamp)
        pending_signals: List[tuple] = []

        # Per bar: was at least one position open? Populated inside the main
        # loop; used at the end to compute time_in_market_pct (a list rather
        # than a count so a measurement window can count its own bars).
        in_market: List[bool] = []

        # The strategy's runtime params, for the session clock that
        # ``close_by_time`` is compared on (SESSION_TZ; default New York).
        session_params = getattr(strategy, 'params', None)
        if not isinstance(session_params, dict):
            session_params = None

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

            mode = self.config.execution_mode
            fixed_sizing = mode in ('live', 'pyramid_fixed')
            if fixed_sizing and signal.action == Action.BUY:
                # Mirror AutoExecutor.decide_signal: in ``live`` mode a BUY
                # while this conid's lot is open is refused (no pyramiding);
                # ``pyramid_fixed`` allows the add — that difference is the
                # whole point of the decomposition mode.
                if mode == 'live' and positions.get(conid, 0) > 0:
                    return
                # pyramid_fixed: the stack is bounded — with max_adds=N a
                # position tops out at N+1 lots (mirrors the live executor).
                if (mode == 'pyramid_fixed'
                        and self.config.pyramid_max_adds is not None
                        and positions.get(conid, 0) > 0
                        and lot_counts.get(conid, 0) > self.config.pyramid_max_adds):
                    return
                # Re-opens/adds inside the cooldown window are refused
                # (evaluated on bar time; the executor uses wall clock).
                prev_ts = last_fill_ts.get(conid)
                if (prev_ts is not None and self.config.cooldown_seconds > 0
                        and hasattr(bar_ts, 'timestamp') and hasattr(prev_ts, 'timestamp')
                        and (bar_ts.timestamp() - prev_ts.timestamp())
                        < self.config.cooldown_seconds):
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
                elif fixed_sizing:
                    # Fixed notional per lot, non-compounding — matches the
                    # live PositionSizer's base (2% of equity) rather than
                    # the legacy 10%-of-current-cash compounding.
                    notional = (self.config.trade_notional
                                if self.config.trade_notional
                                else self.config.initial_capital * 0.02)
                    quantity = math.floor(notional / fill_basis)
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
                lot_counts[conid] = lot_counts.get(conid, 0) + 1
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
                    lot_counts.pop(conid, None)

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
            last_fill_ts[conid] = bar_ts

        for timestamp, bars_this_ts in timeline:
            # ``bars_this_ts``: conid -> (first_pos, last_pos) of that conid's
            # row(s) at this timestamp, in first-appearance order. A conid
            # normally has one row per timestamp; duplicate-timestamp rows
            # form ONE bar (open from the first row, close/bar row from the
            # last) exactly as the old per-timestamp sub-DataFrame did.

            # 1. Execute any signals queued from the previous bar at THIS bar's open
            if self.config.fill_policy == 'next_open' and pending_signals:
                still_pending = []
                for signal, conid, signal_ts in pending_signals:
                    span = bars_this_ts.get(conid)
                    if span is None:
                        # No bar for this conid yet — keep waiting (e.g. new listing)
                        still_pending.append((signal, conid, signal_ts))
                        continue
                    open_price = timeline.open_at(span[0])
                    _execute_signal(signal, conid, signal_ts, timestamp,
                                    timeline.row(span[1]), open_price)
                pending_signals = still_pending

            # 2. Update last prices + per-conid bar index for this bar
            for conid, span in bars_this_ts.items():
                last_prices[conid] = timeline.close_at(span[1])
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
                span_for_conid = bars_this_ts.get(conid)
                if span_for_conid is None:
                    continue
                bar_ts_here = timeline.index_at(span_for_conid[1])
                triggered = False
                reason = ''
                if cond.get('max_hold_bars') is not None:
                    bars_held = idx - cond['entry_bar_index']
                    if bars_held >= cond['max_hold_bars']:
                        triggered = True
                        reason = f'max_hold_bars={cond["max_hold_bars"]}'
                if not triggered and cond.get('close_by_time') is not None:
                    # ``close_by_time`` is session wall time. The bar stamp is
                    # NOT: stored frames come back New York-indexed for every
                    # venue, so an ASX 15:45 flatten used to fire at 15:45 New
                    # York — one bar after a Sydney-morning entry — while the
                    # live runtime converts to SESSION_TZ first. Same
                    # conversion here, or the backtest validates a different
                    # strategy from the one deployed (audit 2026-11-14, B4.2).
                    session_ts = session_bar_ts(
                        bar_ts_here, session_params, naive_is_utc=False)
                    t_of_day = (
                        session_ts.time() if hasattr(session_ts, 'time')
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
                        timeline.row(span_for_conid[1]),
                        last_prices.get(conid, 0.0),
                    )
                else:
                    pending_signals.append((synthetic, conid, timestamp))

            # 3. Run the strategy on each conid at its current bar.
            #    on_bar is the fast path (reads precomputed state by index);
            #    its default impl falls back to on_prices(prices[:index+1]) so
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
                        timeline.row(bars_this_ts[conid][1]),
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

            # Time-in-market: bars where at least one position is open.
            in_market.append(bool(positions))

        # Build equity curve
        full_curve = pd.Series(equity_values, index=equity_timestamps)

        # What is REPORTED. By default the whole run; with a measurement
        # window (``measure_from`` / ``measure_to``) only the window — the
        # run above is identical either way, so the book and the indicators
        # are warm at the window start, but nothing before it is counted.
        window = self._window(full_curve)
        equity_curve = full_curve
        base_equity = self.config.initial_capital
        all_trades = trades
        if window is not None:
            equity_curve = full_curve[window.mask]
            base_equity = window.base_equity
            trades = [t for t in all_trades if window.contains(t.timestamp)]
            signals = [s for s in signals if window.contains(s.timestamp)]
            bars_in_market = int(np.count_nonzero(
                np.asarray(in_market, dtype=bool)[window.mask]))
        else:
            bars_in_market = sum(in_market)

        # Calculate performance metrics
        total_return = (equity_curve.iloc[-1] / base_equity - 1) if len(equity_curve) > 0 else 0.0

        # Sharpe ratio (annualized) — scaled by the number of bars a year of
        # THIS curve holds, not by what the bar size would imply for a
        # 6.5-hour session (see _observed_bars_per_year).
        returns = self._bar_returns(full_curve, window)
        bars_per_year = self._observed_bars_per_year(
            equity_curve.index, self.config.bar_size,
            (session_params or {}).get('SESSION_TZ'))
        if len(returns) > 0 and returns.std() > 0:
            sharpe_ratio = (returns.mean() / returns.std()) * np.sqrt(bars_per_year)
        else:
            sharpe_ratio = 0.0

        # Max drawdown. Inside a window it is measured from the mark the
        # window started at, so a fall on its first bar counts.
        if len(equity_curve) > 0:
            dd_curve = equity_curve
            if window is not None and window.carried:
                dd_curve = pd.Series(
                    np.r_[base_equity, equity_curve.to_numpy(dtype=float)])
            running_max = dd_curve.cummax()
            drawdown = (dd_curve - running_max) / running_max
            max_drawdown = float(drawdown.min())
        else:
            max_drawdown = 0.0

        # Win rate + per-round-trip P&L tracking. Each SELL closes part or
        # all of an open position; we compute its P&L from the weighted
        # average entry price and feed those P&Ls into profit_factor and
        # expectancy_bps below.
        avg_entry: Dict[int, float] = {}   # conid -> weighted avg entry price
        avg_qty: Dict[int, float] = {}     # conid -> total held quantity
        avg_buy_comm: Dict[int, float] = {}  # conid -> weighted buy commission PER SHARE
        opened_at: Dict[int, Any] = {}     # conid -> stamp of the BUY that opened the position from flat
        winning_trades = 0
        sell_trades = 0
        round_trip_pnl: List[float] = []           # dollar P&L per SELL
        round_trip_return_pct: List[float] = []    # P&L / notional per SELL
        # Pairing walks EVERY fill, warm-up included — a SELL can only be
        # priced against the entry it closes — but a round trip is COUNTED
        # only when the position was opened inside the measurement window and
        # closed inside it. One opened during warm-up was opened on bars the
        # window does not judge; its mark-to-market inside the window is in
        # the equity curve, where it belongs, and nowhere else.
        for trade in all_trades:
            if trade.action == Action.BUY:
                prev_qty = avg_qty.get(trade.conid, 0.0)
                prev_cost = avg_entry.get(trade.conid, 0.0) * prev_qty
                prev_comm = avg_buy_comm.get(trade.conid, 0.0) * prev_qty
                new_qty = prev_qty + trade.quantity
                if new_qty > 0:
                    if trade.conid not in avg_qty:
                        opened_at[trade.conid] = trade.timestamp
                    avg_entry[trade.conid] = (prev_cost + trade.price * trade.quantity) / new_qty
                    avg_qty[trade.conid] = new_qty
                    # Carry buy-side commission as a per-share figure so a later
                    # partial sell can attribute its share of entry cost.
                    avg_buy_comm[trade.conid] = (prev_comm + trade.commission) / new_qty
            elif trade.action == Action.SELL:
                counted = window is None or (
                    window.contains(trade.timestamp)
                    and window.contains(opened_at.get(trade.conid)))
                entry_price = avg_entry.get(trade.conid, 0.0)
                if counted:
                    sell_trades += 1
                    # Round-trip P&L: subtract BOTH sides' commissions. The old code
                    # only netted the sell-side commission, overstating expectancy,
                    # profit factor and win rate (the buy-side fee was ignored).
                    buy_comm_alloc = avg_buy_comm.get(trade.conid, 0.0) * trade.quantity
                    pnl = (trade.price - entry_price) * trade.quantity - trade.commission - buy_comm_alloc
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
                    avg_buy_comm.pop(trade.conid, None)
                    opened_at.pop(trade.conid, None)
                else:
                    avg_qty[trade.conid] = remaining

        win_rate = winning_trades / sell_trades if sell_trades > 0 else 0.0

        # ----- Extended practitioner metrics -----

        # Sortino: like Sharpe but only penalizes downside volatility. A
        # large number is less deceiving on asymmetric P&L than Sharpe.
        sortino_ratio = 0.0
        if len(returns) > 0:
            # Proper downside deviation: RMS of min(r, 0) over ALL returns
            # (target = 0). The old code used the std of only the negative
            # subsample — which subtracts the negatives' own mean (wrong target)
            # and divides by the count of negatives (wrong N), inflating Sortino.
            downside_dev = self._downside_deviation(returns)
            if downside_dev > 0:
                sortino_ratio = float((returns.mean() / downside_dev) * np.sqrt(bars_per_year))

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
        if window is not None:
            result.diagnostics.update(
                self._window_diagnostics(window, all_trades, in_market))

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
    def apply_param_overrides(instance: Strategy, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply the same effective parameters as the live runtime."""
        from trader.strategy.parameters import apply_param_overrides
        return apply_param_overrides(instance, params)

    def _summarise(
        self,
        equity_curve: 'pd.Series',
        trades: List[BacktestTrade],
        signals: List[TradingEvent],
        positions: Dict[int, float],
        in_market: Sequence[bool] = (),
    ) -> BacktestResult:
        """Metrics from an equity curve plus a trade log.

        Trade-based statistics (win rate, profit factor, expectancy) come from
        FIFO round-trip pairing rather than counting legs: in a rebalanced book
        a single "trade" is a partial adjustment, and calling each leg a win or
        a loss would score a trim of a winner as a separate winning trade.

        Honours the measurement window exactly as ``run`` does: the curve and
        the fills are sliced to it, the return starts from the last mark
        before it, and a lot counts only when it was opened and closed inside.
        """
        import numpy as np
        from trader.simulation.backtest_stats import round_trip_lots

        cap = self.config.initial_capital
        full_curve, all_trades = equity_curve, trades
        window = self._window(full_curve)
        base = cap
        if window is not None:
            equity_curve = full_curve[window.mask]
            base = window.base_equity
            trades = [t for t in all_trades if window.contains(t.timestamp)]
        if len(equity_curve) == 0:
            empty = BacktestResult([], trades, equity_curve, 0.0, 0.0, 0.0,
                                   0.0, 0)
            if window is not None:
                empty.diagnostics.update(
                    self._window_diagnostics(window, all_trades, in_market))
            return empty

        total_return = float(equity_curve.iloc[-1] / base - 1.0)
        rets = self._bar_returns(full_curve, window)
        bars_per_year = self._observed_bars_per_year(
            equity_curve.index, self.config.bar_size)
        ann = math.sqrt(bars_per_year)

        sharpe = sortino = 0.0
        if len(rets) > 1 and rets.std(ddof=1) > 0:
            sharpe = float(rets.mean() / rets.std(ddof=1) * ann)
        # Same Sortino as ``run`` — see _downside_deviation.
        downside_dev = self._downside_deviation(rets)
        if downside_dev > 0:
            sortino = float(rets.mean() / downside_dev * ann)

        dd_curve = equity_curve
        if window is not None and window.carried:
            dd_curve = pd.Series(np.r_[base, equity_curve.to_numpy(dtype=float)])
        running_max = dd_curve.cummax()
        dd = (dd_curve - running_max) / running_max
        max_dd = float(dd.min()) if len(dd) else 0.0
        years = max(len(equity_curve) / bars_per_year, 1e-9)
        calmar = float((total_return / years) / abs(max_dd)) if max_dd < 0 else 0.0

        # Pairing walks every fill, warm-up included; ``_ts`` rides along so a
        # lot can be kept or dropped by when it was opened and closed.
        lots = round_trip_lots([{
            'timestamp': str(t.timestamp), 'conid': t.conid,
            'quantity': t.quantity, 'price': t.price,
            'commission': t.commission, 'action': str(t.action),
            '_ts': t.timestamp,
        } for t in all_trades])
        pnls = [pnl for pnl, opened, closed in lots
                if window is None or (window.contains(opened['_ts'])
                                      and window.contains(closed['_ts']))]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        win_rate = (len(wins) / len(pnls)) if pnls else 0.0
        gross_win, gross_loss = sum(wins), abs(sum(losses))
        profit_factor = (gross_win / gross_loss) if gross_loss > 0 else (
            float('inf') if gross_win > 0 else 0.0)
        expectancy_bps = ((float(np.mean(pnls)) / cap) * 10_000.0) if pnls else 0.0

        result = BacktestResult(
            signals=signals, trades=trades, equity_curve=equity_curve,
            total_return=total_return, sharpe_ratio=sharpe,
            max_drawdown=max_dd, win_rate=win_rate, total_trades=len(pnls),
            sortino_ratio=sortino, calmar_ratio=calmar,
            profit_factor=profit_factor, expectancy_bps=expectancy_bps,
            time_in_market_pct=1.0 if positions else 0.0,
            start_date=self.config.start_date, end_date=self.config.end_date,
        )
        if window is not None:
            result.diagnostics.update(
                self._window_diagnostics(window, all_trades, in_market))
        return result

    def run_panel(
        self,
        strategy: Strategy,
        conids: List[int],
        frames: Optional[Dict[Any, 'pd.DataFrame']] = None,
    ) -> BacktestResult:
        """Cross-sectional backtest: the strategy sees every instrument at once.

        Differs from `run` in what is being simulated. `run` walks one
        instrument's bars and reacts to Signals; this holds a BOOK of target
        weights over the whole universe and rebalances it. A signal that is
        only meaningful in comparison — "this name is cheap relative to its
        peers" — cannot be expressed in the per-instrument loop at all.

        Lookahead is handled the same way and for the same reason: weights
        computed while observing bar t are filled at bar t+1's OPEN. The
        strategy may see t's close, because that is public at that moment, but
        it cannot trade on it.
        """
        import numpy as np
        from trader.simulation.panel import (
            apply_no_trade_band, normalise_weights, rebalance_orders,
            target_positions)

        # `frames` may be injected so a caller can supply a panel that is not
        # conid-keyed in tick_data - specifically the point-in-time universe,
        # which must be ticker-keyed because delisted instruments have no conId
        # and never will. Injecting rather than reimplementing keeps this one
        # execution loop as the only one: a second copy of fill-at-next-open,
        # marking and cost accounting is exactly the divergence that produced
        # the flip residual.
        date_range = DateRange(start=self.config.start_date,
                               end=self.config.end_date)
        if frames is None:
            frames = self._load_panel_frames(conids, date_range)
        return self._run_panel_frames(strategy, frames)

    def _load_panel_frames(self, conids, date_range):
        frames: Dict[Any, pd.DataFrame] = {}
        tickdata = self.storage.get_tickdata(self.config.bar_size)
        for conid in conids:
            try:
                data = tickdata.read(conid, date_range=date_range)
            except Exception as exc:                      # noqa: BLE001
                logging.warning('panel: conid %s unreadable: %s', conid, exc)
                continue
            if data is None or len(data) == 0:
                continue
            norm = normalize_historical(data).dropna(subset=['close'])
            if len(norm):
                frames[conid] = norm
        return frames

    def _run_panel_frames(self, strategy: Strategy,
                          frames: Dict[Any, 'pd.DataFrame']) -> BacktestResult:
        import numpy as np
        from trader.simulation.panel import (
            apply_no_trade_band, normalise_weights, rebalance_orders,
            target_positions)
        if len(frames) < 2:
            raise ValueError(
                f'panel backtest needs at least 2 instruments with data, '
                f'got {len(frames)}')

        # Wide panel: columns are (field, conid) so panel['close'] is a
        # (time x instrument) frame and a cross-sectional indicator is one
        # vectorised expression.
        fields = ['open', 'high', 'low', 'close', 'volume']
        panel = pd.concat(
            {f: pd.DataFrame({c: df[f] for c, df in frames.items()
                              if f in df.columns})
             for f in fields}, axis=1).sort_index()
        closes, opens = panel['close'], panel['open']
        n = len(panel)
        logging.info('panel: %d instruments x %d periods', len(frames), n)

        state = strategy.precompute_panel(panel)

        cash = self.config.initial_capital
        positions: Dict[int, float] = {}
        # Last finite close per conid, so a missing bar does not erase a
        # holding's value from equity.
        last_price: Dict[int, float] = {}
        pending: Optional[Dict[int, float]] = None
        trades: List[BacktestTrade] = []
        equity_values: List[float] = []
        equity_timestamps: List[Any] = []
        in_market: List[bool] = []       # per bar: was any position open at the mark?
        commission = self.config.commission_per_share
        slip = self.config.slippage_bps / 10_000.0
        borrow_per_period = (
            (self.config.short_borrow_bps_annual / 10_000.0)
            / max(self._bars_per_year(self.config.bar_size), 1.0))
        scaled_buy_fills = dropped_buy_fills = scaled_rebalances = 0   # D06 diagnostics
        min_cash = cash
        # hillclimb(C01): D07 - each name's terminal print as supplied by the
        # caller from the COMPLETE dataset; a fact about the data, never read
        # by the strategy and never inferred from this window's frames.
        forced_exits = 0
        forced_exit_value = 0.0
        # hillclimb(C02): changed because the live paper ledger
        # (trader/lowfreq_live/ledger.py) applies D07 WITHOUT hindsight: it
        # recognises a stopped name after a run of silent sessions, and a
        # name can stop, re-enter through a later rebalance and stop again,
        # so one conid may carry SEVERAL terminal prints. A value is one
        # timestamp-like (the harness's hindsight rule, unchanged) or an
        # iterable of them; the exit fires on whichever of them the name is
        # HELD at, and never on one it is not.
        terminal: Dict[Any, FrozenSet[Any]] = {}      # frozenset of pd.Timestamp per conid
        for conid, t in (self.config.panel_terminal_prints or {}).items():
            if isinstance(t, (str, bytes)) or not isinstance(t, Iterable):
                terminal[conid] = frozenset([pd.Timestamp(t)])
            else:
                terminal[conid] = frozenset(pd.Timestamp(x) for x in t)

        def _execute(ts, conid, delta, fill):
            nonlocal cash, min_cash
            cost = abs(delta) * commission
            cash -= delta * fill + cost
            min_cash = min(min_cash, cash)          # after EVERY fill, not per bar
            positions[conid] = positions.get(conid, 0.0) + delta
            if abs(positions[conid]) < 1e-9:
                positions.pop(conid, None)
            trades.append(BacktestTrade(
                timestamp=ts, conid=conid,
                action=Action.BUY if delta > 0 else Action.SELL,
                quantity=abs(delta), price=fill, commission=cost,
                # A rebalance carries no per-name conviction: the weight came
                # from a rank, not from a probability. Recording a fabricated
                # 1.0 would make these trades look like high-confidence calls
                # in any downstream analysis that reads the field.
                signal_probability=0.0, signal_risk=0.0))

        for i in range(n):
            ts = panel.index[i]
            row_open = opens.iloc[i]
            row_close = closes.iloc[i]

            # 1. Fill what was decided while observing the PREVIOUS bar, at
            #    this bar's open. This ordering is the no-lookahead rule.
            if pending:
                # D06: a fill may never take cash below zero. The rebalance's
                # SELLS settle first (short-sale proceeds count as cash, as
                # before) and its BUYS are scaled pro rata to the cash then
                # available, truncated to whole shares so the scaled book
                # never costs more than the budget. Without this a book sized
                # off a holding it could not sell (a name that stopped
                # printing, marked at a stale carry-forward) bought the full
                # target anyway and ran on implicit, cost-free leverage: the
                # covered-EW PIT benchmark sat at -$3.2M cash on 99% of fill
                # dates (docs/RESEARCH_LOG.md, 2026-09-29). Scaled fills are
                # counted in ``BacktestResult.diagnostics`` so a result that
                # leaned on the guard says so.
                fills = []
                for conid, delta in pending.items():
                    px = row_open.get(conid)
                    if px is None or not np.isfinite(px) or px <= 0:
                        continue        # cannot fill what cannot be priced
                    fill = px * (1 + slip) if delta > 0 else px * (1 - slip)
                    fills.append((conid, delta, fill))
                # Sells are EXECUTED and logged first, whatever order the
                # strategy listed them in, so cash after every single fill
                # is what a real account would show.
                for conid, delta, fill in fills:
                    if delta < 0:
                        _execute(ts, conid, delta, fill)
                buys = [(c, d, f) for c, d, f in fills if d > 0]
                buy_cost = sum(d * f + abs(d) * commission for _, d, f in buys)
                scale = 1.0
                if buy_cost > 0 and buy_cost > cash:
                    scale = max(cash, 0.0) / buy_cost
                    scaled_rebalances += 1
                for conid, delta, fill in buys:
                    if scale < 1.0:
                        delta = float(math.floor(delta * scale))
                        scaled_buy_fills += 1
                        if delta <= 0:
                            dropped_buy_fills += 1
                            continue    # budget exhausted: nothing to buy
                    _execute(ts, conid, delta, fill)
                pending = None

            # 2. Mark the book at this bar's close, carrying the last known
            #    price forward for anything that did not print today.
            #
            #    Skipping unpriceable holdings instead makes a position's value
            #    VANISH from equity on any day its bar is missing, and reappear
            #    the next — which on a 120-name panel produced a -96% drawdown
            #    that never happened. A position is worth what it was last
            #    worth; the absence of a print is not the absence of the asset.
            for conid in row_close.index:
                px = row_close.get(conid)
                if px is not None and np.isfinite(px) and px > 0:
                    last_price[conid] = float(px)
            # hillclimb(C01): D07 forced exit at the terminal print. Runs
            # BEFORE the mark so the equity curve shows the book that exists.
            if terminal:
                now = pd.Timestamp(ts)
                for conid in [c for c, q in positions.items()
                              if abs(q) > 1e-9 and now in terminal.get(c, frozenset())
                              and c in last_price]:
                    qty = positions[conid]
                    px = last_price[conid]
                    fill = px * (1 - slip) if qty > 0 else px * (1 + slip)
                    forced_exits += 1
                    forced_exit_value += abs(qty) * px
                    _execute(ts, conid, -qty, fill)
            # Borrow accrues on shorts BEFORE marking, so the fee is paid out
            # of cash rather than quietly reducing the position's value.
            if borrow_per_period > 0:
                for conid, qty in positions.items():
                    if qty >= 0:
                        continue
                    px = last_price.get(conid)
                    if px is not None and px > 0:
                        cash -= abs(qty) * px * borrow_per_period

            min_cash = min(min_cash, cash)
            mark = cash
            for conid, qty in positions.items():
                px = last_price.get(conid)
                if px is not None and px > 0:
                    mark += qty * px
            equity_values.append(mark)
            equity_timestamps.append(ts)
            in_market.append(bool(positions))

            if i >= n - 1:
                break                    # nothing left to fill into

            # 3. Ask for target weights, observing up to and including i.
            weights = strategy.on_panel(panel, state, i)
            if weights is None:
                continue                 # no instruction != flatten
            weights = normalise_weights(weights, max_gross=self.config.panel_max_gross)
            prices = dict(last_price)
            target = target_positions(weights, mark, prices)
            if self.config.panel_no_trade_band > 0:
                target = apply_no_trade_band(
                    positions, target, self.config.panel_no_trade_band,
                    prices=prices)
            orders = rebalance_orders(positions, target, min_shares=0.0)
            pending = orders or None

        equity_curve = pd.Series(equity_values, index=pd.Index(equity_timestamps))
        result = self._summarise(equity_curve, trades, [], positions, in_market)
        result.diagnostics.update({
            'panel_scaled_buy_fills': scaled_buy_fills,      # BUY fills reduced by the D06 cash guard
            'panel_dropped_buy_fills': dropped_buy_fills,    # ... of which reduced to nothing
            'panel_scaled_rebalances': scaled_rebalances,    # fill dates on which the guard acted
            'panel_min_cash': float(min_cash),               # lowest cash after any fill or bar
            'panel_forced_exits': forced_exits,              # D07: holdings sold at their last stored print
            'panel_forced_exit_value': float(forced_exit_value),   # ... gross value at that print
        })
        return result

    def run_from_module(
        self,
        module_path: str,
        class_name: str,
        conids: List[int],
        universe_accessor: Optional[UniverseAccessor] = None,
        params: Optional[Dict[str, Any]] = None,
        panel: bool = False,
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

        # Panel mode holds a BOOK of weights over the whole universe; the
        # default walks each instrument's bars reacting to Signals. They are
        # different simulations, not two settings of one, so the caller says
        # which rather than the runner guessing from the strategy's methods.
        result = (self.run_panel(instance, conids) if panel
                  else self.run(instance, conids))
        # Attach effective param overrides so the CLI can persist them on
        # the BacktestRecord. The backtester itself doesn't know about
        # storage, so we stash on the result object.
        result.applied_params = applied_params
        return result
