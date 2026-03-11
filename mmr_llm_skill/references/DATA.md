# Data Sources & Schema Reference

## DuckDB Schema

All historical data is stored in a single DuckDB file (path from `duckdb_path` in trader.yaml).

### `tick_data` Table

```sql
CREATE TABLE tick_data (
    symbol VARCHAR NOT NULL,        -- conId as string (e.g. "265598")
    date TIMESTAMP NOT NULL,        -- bar timestamp (timezone-aware)
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    average DOUBLE,                 -- VWAP
    bar_count INTEGER,              -- number of trades in bar
    bar_size VARCHAR,               -- e.g. "1 day", "1 min"
    what_to_show INTEGER            -- WhatToShow enum value
)
```

**Key**: Data is keyed by `(symbol, date)`. The `symbol` field is the conId as a string. Different bar sizes for the same conId are stored in the same table with different `bar_size` values.

### `object_store` Table

Used for serialized Python objects (universes, security definitions, etc.):

```sql
CREATE TABLE object_store (
    key VARCHAR PRIMARY KEY,
    value BLOB                      -- dill-serialized Python object
)
```

## Valid Bar Sizes (BarSize Enum)

| Bar Size | String | Use Case |
|----------|--------|----------|
| 1 secs | `"1 secs"` | Ultra high-frequency |
| 5 secs | `"5 secs"` | High-frequency |
| 10 secs | `"10 secs"` | |
| 15 secs | `"15 secs"` | |
| 30 secs | `"30 secs"` | |
| 1 min | `"1 min"` | Intraday strategies (default for live) |
| 2 mins | `"2 mins"` | |
| 3 mins | `"3 mins"` | |
| 5 mins | `"5 mins"` | |
| 10 mins | `"10 mins"` | |
| 15 mins | `"15 mins"` | |
| 20 mins | `"20 mins"` | |
| 30 mins | `"30 mins"` | |
| 1 hour | `"1 hour"` | Swing trading |
| 2 hours | `"2 hours"` | |
| 3 hours | `"3 hours"` | |
| 4 hours | `"4 hours"` | |
| 8 hours | `"8 hours"` | |
| 1 day | `"1 day"` | Daily strategies, common for backtesting |
| 1 week | `"1 week"` | Weekly analysis |
| 1 month | `"1 month"` | Monthly analysis |

## Data Sources

### Massive.com

- **API key**: Set `massive_api_key` in `~/.config/mmr/trader.yaml`
- **Data**: US stocks, ETFs, indices, forex, crypto
- **Bar sizes**: All sizes supported
- **History depth**: Up to 20+ years for daily, less for intraday
- **No service needed**: Direct API calls, data stored in local DuckDB
- **CLI**: `mmr data download AAPL --bar-size "1 day" --days 365`

### Interactive Brokers (IB)

- **Requirements**: IB Gateway running + `trader_service` or `data_service`
- **Data**: All IB-tradeable instruments
- **Bar sizes**: All sizes supported
- **History depth**: Varies by instrument and bar size (IB limits apply)
- **CLI**: `mmr history ib --symbol AAPL --bar_size "1 min" --prev_days 5`

## TickStorage Read Patterns

```python
from trader.container import Container
from trader.data.data_access import TickStorage
from trader.data.store import DateRange
from trader.objects import BarSize
import datetime as dt

container = Container.instance()
cfg = container.config()
storage = TickStorage(cfg['duckdb_path'])

# Get TickData for a specific bar size
tickdata = storage.get_tickdata(BarSize.Day1)

# Read data for a conId
date_range = DateRange(
    start=dt.datetime.now() - dt.timedelta(days=30),
    end=dt.datetime.now()
)
df = tickdata.read(265598, date_range=date_range)  # 265598 = AAPL

# List available bar sizes
bar_sizes = storage.list_libraries()  # ["1 day", "1 min", ...]

# List symbols stored for a bar size
symbols = tickdata.list_symbols()  # ["265598", "4391", ...]

# Date summary for a conId
min_date, max_date = tickdata.date_summary(265598)
```

## Common ConIds

| Symbol | ConId | Description |
|--------|-------|-------------|
| AAPL | 265598 | Apple Inc. |
| AMD | 4391 | Advanced Micro Devices |
| MSFT | 272093 | Microsoft Corp. |
| NVDA | 4815747 | NVIDIA Corp. |
| GOOGL | 208813720 | Alphabet Inc. |
| AMZN | 3691937 | Amazon.com |
| META | 107113386 | Meta Platforms |
| TSLA | 76792991 | Tesla Inc. |
| SPY | 756733 | S&P 500 ETF |
| QQQ | 320227571 | Nasdaq-100 ETF |

To find a conId: `mmr --json resolve SYMBOL`

## Date Range Handling

The `DateRange` dataclass:
```python
@dataclass
class DateRange:
    start: Union[datetime, date]
    end: Union[datetime, date]
```

- Dates are stored in UTC in DuckDB
- On read, they're converted to `America/New_York` timezone
- When querying, provide naive datetimes (treated as local) or timezone-aware datetimes
