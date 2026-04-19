# Strategy Writing Guide

## Strategy ABC

Strategies subclass `trader.trading.strategy.Strategy` and implement **at least one** of two dispatch APIs:

- **`on_prices(prices)`** — the simple, legacy API. Gets the accumulated window every bar. Fine for pandas `.rolling()`-based indicators; fine for anything that doesn't recompute heavy state. Works for both live trading and backtesting.
- **`precompute(prices) + on_bar(prices, state, index)`** — the **fast path**. `precompute` runs **once** with the full history; `on_bar` runs per bar and reads indicator values by index in O(1). Use this for vectorbt, numba, scipy, or any indicator where rebuilding on every bar would be O(N²) over a backtest.

The backtester's default `on_bar` calls `on_prices(prices.iloc[:index+1])`, so a strategy that only implements `on_prices` works unchanged. The performance win only materializes when you opt in.

## Which API to use?

| Using | Indicator library | Pick |
|---|---|---|
| Pandas `.rolling()` / `.ewm()` / arithmetic on columns | — | `on_prices` — simple and fast enough |
| vectorbt (`vbt.MA`, `vbt.MACD`, `vbt.BBANDS`) | vectorbt / numba | **`precompute` + `on_bar`** (see below) |
| scipy signal processing / statsmodels | slow | **`precompute` + `on_bar`** |
| Custom stateful logic that depends on prior bars | — | either works; `on_bar` has less per-call overhead |

**Concrete numbers**: SMICrossOver on 30d × 1-min (~11,700 bars) went from hanging >4 minutes under `on_prices` to **~1 second** under `precompute` + `on_bar` — the same numerical result, ~240× faster, because vectorbt's `vbt.MA.run()` was being reinstantiated on every bar.

## Fast-path example

```python
from typing import Any, Dict, Optional
import numpy as np
import pandas as pd
import vectorbt as vbt
from trader.trading.strategy import Signal, Strategy
from trader.objects import Action

class FastMacdBB(Strategy):
    """MACD + Bollinger Bands confluence — uses precompute for O(N) total work."""

    def precompute(self, prices: pd.DataFrame) -> Dict[str, Any]:
        close = prices['close']
        macd = vbt.MACD.run(close, fast_window=12, slow_window=26, signal_window=9)
        bb = vbt.BBANDS.run(close, window=20, alpha=2)
        # Materialize to numpy so on_bar does O(1) array access, not vectorbt wrapper re-entry
        return {
            'hist':  np.asarray(macd.hist).ravel(),
            'upper': np.asarray(bb.upper).ravel(),
            'lower': np.asarray(bb.lower).ravel(),
            'close': close.to_numpy(),
        }

    def on_bar(self, prices: pd.DataFrame, state: Dict[str, Any], index: int) -> Optional[Signal]:
        if not state or index < 26: return None
        hist = state['hist']
        if np.isnan(hist[index]) or np.isnan(hist[index-1]): return None
        if hist[index] > 0 and hist[index-1] <= 0:
            return Signal(source_name=self.name, action=Action.BUY, probability=0.6, risk=0.4)
        return None

    # on_prices is still needed for LIVE trading (the live runtime calls on_prices, not on_bar).
    # The backtester uses on_bar when precompute is overridden.
    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        # Same logic, operating on the tail of the window — slower but that's OK
        # in live trading where we get one new tick at a time, not a big backfill.
        ...
```

## Declaring tunable parameters (for sweeps)

Declare tunable parameters as **upper-case class attributes** at the class body. That makes them discoverable by `strategies inspect` and overrideable by `mmr backtest --param KEY=VALUE`, `mmr bt-sweep --grid '{"KEY":[...]}'`, and `mmr sweep run nightly.yaml` — all without touching the class.

```python
class OpeningRangeBreakout(Strategy):
    RANGE_MINUTES = 30       # int; coerced from CLI string "15" → 15
    VOLUME_MULT = 1.3        # float
    RTH_OPEN_MIN = 570       # 09:30 ET in minutes since midnight
    MIN_BARS = 40

    def precompute(self, prices):
        # Read as self.RANGE_MINUTES — Python MRO means overrides at the
        # instance level (set by the backtester's apply_param_overrides)
        # shadow the class attribute transparently.
        ...
```

Overrides via:
- **CLI**: `mmr backtest --param RANGE_MINUTES=15 --param VOLUME_MULT=1.5`
- **JSON**: `mmr backtest --params '{"RANGE_MINUTES":15,"VOLUME_MULT":1.5}'`
- **Sweep grid**: `mmr bt-sweep --grid '{"RANGE_MINUTES":[15,30,45],"VOLUME_MULT":[1.2,1.3,1.5]}'`
- **YAML manifest**: `param_grid: {RANGE_MINUTES: [15, 30, 45]}` inside a `mmr sweep run` manifest.

Type coercion is automatic based on the class attribute's current type (ints become ints, floats become floats, booleans handle `"true"`/`"false"` correctly rather than `bool("False") == True`). Typos on upper-case keys raise `ValueError` listing every valid tunable — they don't silently no-op.

Legacy `self.params.get('key', default)` still works — lower-case dict keys land in `StrategyContext.params` and can be swept with the same flags. But prefer class attributes for new strategies: `strategies inspect` surfaces them with their declared defaults, the type is inferred from the value, and parallel sweeps don't collide (each backtest subprocess has its own instance, `setattr`'d with its own overrides).

## The lookahead contract of `precompute`

`precompute` sees the FULL OHLCV history — this is convenient but dangerous. Your returned state must be aligned 1:1 with `prices` such that position `i` depends only on bars `[0..i]`. Pandas `.rolling()`, `.ewm()`, and all standard vectorbt indicators satisfy this by construction. **What you must NOT do**:

- `close.shift(-1)` — pulls tomorrow's close
- `close.rolling(20, center=True).mean()` — centered rolling peeks forward
- `(close - close.mean()) / close.std()` — the mean/std span the full series including the future
- `MinMaxScaler().fit_transform(close)` — same problem: scaling factor is global
- any label/signal derived from a known future outcome
- fitting an HMM / change-point / regime model on the complete history (fit on an expanding window instead)

Getting this wrong injects lookahead bias that looks like a spectacularly profitable strategy.

### Detecting lookahead before it ships

Use ``trader.simulation.lookahead_check.assert_no_lookahead`` in your strategy's test file — it's a one-line guard that runs `precompute` on the full series and on progressively-truncated copies, then asserts values at past indices don't change when future bars are hidden:

```python
from trader.simulation.lookahead_check import assert_no_lookahead

def test_my_strategy_no_lookahead():
    prices = _synthetic_ohlcv(n=400)  # enough bars for indicators to warm up
    assert_no_lookahead(MyStrategy(), prices)
```

A leak surfaces as `LookaheadDetected` with the key, index, and both values so you can pinpoint the offending line. Catches the common real-world mistakes — full-series normalization, `shift(-N)`, centered rolling, `fit_transform` on the complete history, Savitzky-Golay / bidirectional filters. Does not catch adversarially-hidden leaks; it's a safety net, not a proof.

## Legacy on_prices API

Strategies can still subclass `Strategy` and implement only `on_prices()`:

```python
from trader.trading.strategy import Signal, Strategy
from trader.objects import Action
from typing import Optional
import pandas as pd

class MyStrategy(Strategy):
    def __init__(self):
        super().__init__()

    def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
        # Return a Signal to trade, or None to do nothing
        return None
```

## Signal Dataclass

```python
@dataclass
class Signal:
    source_name: str        # Strategy name (use self.name)
    action: Action          # Action.BUY or Action.SELL
    probability: float      # Confidence 0.0-1.0
    risk: float             # Risk level 0.0-1.0
    conid: int = 0          # Contract ID (0 = use context conid)
    quantity: float = 0.0   # Shares (0 = auto-size by backtester/runtime)
    date_time: datetime     # Auto-set to now()
    metadata: Dict = {}     # Arbitrary key-value data
```

## StrategyContext

After `install()`, access via `self.ctx`:

| Attribute | Type | Description |
|-----------|------|-------------|
| `self.name` | str | Strategy name from config |
| `self.bar_size` | BarSize | Bar size enum |
| `self.conids` | List[int] | Contract IDs assigned |
| `self.universe` | str | Universe name (if used instead of conids) |
| `self.historical_days_prior` | int | Days of history to preload |
| `self.paper` | bool | Paper trading mode |
| `self.storage` | TickStorage | Access to DuckDB historical data |
| `self.logging` | Logger | Per-strategy logger |

## on_prices() Contract

The `on_prices(prices: pd.DataFrame)` method is called on every new bar with an **accumulated** DataFrame — all bars seen so far for that conid:

**DataFrame columns** (DatetimeIndex named `date`):
- `open` — bar open price
- `high` — bar high price
- `low` — bar low price
- `close` — bar close price
- `volume` — bar volume
- `vwap` — VWAP (mapped from IB historical `average`, IB live `vwap`, Massive `vwap`)
- `bar_count` — number of trades in bar (NaN for live ticks)
- `bid` — best bid (NaN in backtest)
- `ask` — best ask (NaN in backtest)
- `last` — last trade price (equals `close` in backtest)
- `last_size` — last trade size (NaN in backtest)

**Return value**: `Signal` to generate a trade, `None` to do nothing.

**Important**: The DataFrame grows over time. For live trading with `1 min` bars and 90 days history, the initial call has ~90*390=35,100 rows. Guard against insufficient data:

```python
def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
    if len(prices) < 50:
        return None
    # ... your logic
```

## Common Patterns

### Moving Average Crossover

```python
def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
    if len(prices) < 50:
        return None

    close = prices['close']
    fast = close.rolling(10).mean()
    slow = close.rolling(50).mean()

    # Cross above: buy
    if fast.iloc[-1] > slow.iloc[-1] and fast.iloc[-2] <= slow.iloc[-2]:
        return Signal(source_name=self.name, action=Action.BUY, probability=0.7, risk=0.3)

    # Cross below: sell
    if fast.iloc[-1] < slow.iloc[-1] and fast.iloc[-2] >= slow.iloc[-2]:
        return Signal(source_name=self.name, action=Action.SELL, probability=0.7, risk=0.3)

    return None
```

### RSI

```python
def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
    if len(prices) < 20:
        return None

    close = prices['close']
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))

    if rsi.iloc[-1] < 30:
        return Signal(source_name=self.name, action=Action.BUY, probability=0.6, risk=0.4)
    if rsi.iloc[-1] > 70:
        return Signal(source_name=self.name, action=Action.SELL, probability=0.6, risk=0.4)

    return None
```

### Bollinger Band Breakout

```python
def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
    if len(prices) < 30:
        return None

    close = prices['close']
    sma = close.rolling(20).mean()
    std = close.rolling(20).std()
    upper = sma + 2 * std
    lower = sma - 2 * std

    if close.iloc[-1] < lower.iloc[-1] and close.iloc[-2] >= lower.iloc[-2]:
        return Signal(source_name=self.name, action=Action.BUY, probability=0.65, risk=0.35)
    if close.iloc[-1] > upper.iloc[-1] and close.iloc[-2] <= upper.iloc[-2]:
        return Signal(source_name=self.name, action=Action.SELL, probability=0.65, risk=0.35)

    return None
```

### EMA with Trend Filter

```python
def on_prices(self, prices: pd.DataFrame) -> Optional[Signal]:
    if len(prices) < 200:
        return None

    close = prices['close']
    ema_short = close.ewm(span=12).mean()
    ema_long = close.ewm(span=26).mean()
    trend = close.rolling(200).mean()

    # Only buy in uptrend, only sell in downtrend
    in_uptrend = close.iloc[-1] > trend.iloc[-1]

    if in_uptrend and ema_short.iloc[-1] > ema_long.iloc[-1] and ema_short.iloc[-2] <= ema_long.iloc[-2]:
        return Signal(source_name=self.name, action=Action.BUY, probability=0.7, risk=0.3)
    if not in_uptrend and ema_short.iloc[-1] < ema_long.iloc[-1] and ema_short.iloc[-2] >= ema_long.iloc[-2]:
        return Signal(source_name=self.name, action=Action.SELL, probability=0.7, risk=0.3)

    return None
```

## Backtest Results

### Fill policy (important for interpreting numbers)

Backtests default to `fill_policy='next_open'`: a signal emitted while observing bar `t` fills at bar `t+1`'s **open**, not bar `t`'s close. This eliminates lookahead bias — your strategy can see bar `t`'s close (public info at that moment) but can't trade at it. Numbers from older MMR versions that used the legacy "fill at close of triggering bar" behaviour will look rosier than the realistic pipeline.

If you're comparing against an older run or intentionally want the legacy behaviour (not recommended), `BacktestConfig(fill_policy='same_close')` reproduces it. The skill helpers always use the default.

When backtesting via CLI, the JSON summary contains the full practitioner-metric set:

```json
{
  "data": {
    "summary": {
      "total_return": 0.05,
      "sharpe_ratio": 1.2,
      "sortino_ratio": 1.8,
      "calmar_ratio": 0.6,
      "profit_factor": 1.45,
      "expectancy_bps": 12.3,
      "max_drawdown": -0.08,
      "time_in_market_pct": 0.35,
      "win_rate": 0.55,
      "total_trades": 42,
      "start_date": "2025-03-10",
      "end_date": "2026-03-10",
      "final_equity": 105000.0
    },
    "trades": [
      {"timestamp": "...", "conid": 265598, "action": "BUY", "quantity": 10, "price": 175.50, ...}
    ]
  }
}
```

### Headline metrics to evaluate

| Metric | Good threshold | What it tells you |
|--------|----------------|-------------------|
| `total_return` | > 5% over the period | Necessary but nowhere-near-sufficient. Compare to buy-and-hold. |
| `sharpe_ratio` | > 1.0 decent, > 2.0 good | Annualised. Treats upside vol as bad; misleading on asymmetric P&L. |
| `sortino_ratio` | > 1.5 decent, > 2.5 good | Like Sharpe but downside-only — better for asymmetric strategies. |
| `calmar_ratio` | > 1.0 | Raw (not annualised): return / |max_dd|. "Edge per dollar of worst loss." |
| `profit_factor` | > 1.5 decent, > 2.0 robust | Gross wins / gross losses. The single number most practitioners read first. |
| `expectancy_bps` | > 10 bps | Must clear real-world slippage + commissions with margin. |
| `max_drawdown` | > −10% | How bad the worst peak-to-trough was. |
| `win_rate` | Meaningless alone | Pair with profit_factor. 40% wins is fine at 3:1 payoff. |
| `time_in_market_pct` | 20–60% typical | Low = selective, 100% = index-like. Context, not quality. |
| `total_trades` | ≥ 30 for reliability | Anything under ~30 makes the other metrics statistical noise. |

### Statistical-confidence tests (via `backtests_show`)

Headline metrics describe what happened. These describe whether it was **real** — i.e. whether the observed edge is statistically distinguishable from noise. Available on any run saved with trade + equity data (the `--save-trades` CLI default, `backtest` helper defaults to on).

Call `MMRHelpers.backtests_show(run_id)` to get a `statistical_confidence` block with five tests:

**1. Probabilistic Sharpe Ratio (`probabilistic_sharpe`)**
Probability that the **true** Sharpe of the process exceeds 0, given observed sample size AND actual skew/kurtosis (López de Prado 2012). The single highest-signal "is it real?" number. Green ≥ 0.95, yellow ≥ 0.80, red below. A raw Sharpe of 3 on 30 bars and on 3000 bars give very different PSRs.

**2. t-test of mean per-trade P&L (`t_stat` + `p_value`)**
Classical two-sided test of H₀: mean per-trade P&L = 0. `p_value < 0.05` → edge is unlikely to be random-walk noise.

**3. Bootstrap 95% CIs (`return_ci_lo/hi`, `sharpe_ci_lo/hi`)**
Nonparametric resampling (5000 replicates). The narrower the CI, the more stable the estimate. If `return_ci_lo > 0`, the edge is statistically significant; if the CI straddles zero, it isn't.

**4. P&L distribution shape (`pnl_skew`, `pnl_excess_kurtosis`)**
Negative skew + high excess kurtosis (> 5) is the classic "picking up pennies in front of a steamroller" signature that Sharpe misses entirely. A backtest with Sharpe 3 but skew −8 and kurt +80 has hidden blow-up risk — treat it as a red flag regardless of the headline numbers.

**5. Longest losing streak vs Monte Carlo expectation (`losing_streak_actual`, `losing_streak_mc_95`)**
The actual longest consecutive losing run compared against the 95th percentile you'd expect if the same P&Ls were randomly reordered. If `actual > mc_95`, losses **cluster** — a sign of auto-correlated P&L or regime-dependent edge that won't survive the next regime shift.

### Decision rule

Before paper-trading a strategy:
1. `profit_factor > 1.5` AND `sortino > 1.5` AND `expectancy_bps` clears your real-world costs with margin (ideally > 10 bps).
2. `probabilistic_sharpe > 0.95` — the edge is real with high confidence.
3. Bootstrap `return_ci_lo > 0` — the edge isn't CI-straddling zero.
4. Distribution: `pnl_skew > -0.5` AND `pnl_excess_kurtosis < 5` — no hidden blow-up risk.
5. `losing_streak_actual ≤ losing_streak_mc_95` — losses aren't clustering.

Any single red flag above is enough reason to re-investigate before risking real capital. A strategy that clears all five is a genuine candidate.
