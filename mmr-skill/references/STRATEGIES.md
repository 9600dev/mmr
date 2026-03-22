# Strategy Writing Guide

## Strategy ABC

All strategies must subclass `trader.trading.strategy.Strategy` and implement `on_prices()`:

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

When backtesting via CLI, the JSON result contains:

```json
{
  "data": {
    "summary": {
      "total_return": 0.05,
      "sharpe_ratio": 1.2,
      "max_drawdown": -0.08,
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

Key metrics to evaluate:
- **sharpe_ratio > 1.0**: Decent risk-adjusted returns
- **max_drawdown > -0.20**: Acceptable for most strategies
- **win_rate > 0.50**: More winning than losing trades
- **total_trades**: Enough trades for statistical significance
