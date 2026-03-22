# Trading Loop Configuration

All configuration is set via `TradingLoop.config` before starting the loop.

## Timing

| Key | Default | Description |
|-----|---------|-------------|
| `scan_interval_seconds` | `120` | Seconds between cycles. Market open + volatile → 120s. Calm → 300s. Closed → 3600s. |

## Scanning

| Key | Default | Description |
|-----|---------|-------------|
| `scan_presets` | `["momentum", "mean-reversion", "breakout", "volatile", "gap-down"]` | Presets to rotate through. Each cycle uses the next preset in the list. |
| `scan_num` | `10` | Number of ideas to request per scan. |
| `max_proposals_per_cycle` | `2` | Cap on proposals created per cycle. Prevents flooding. |

## Market Targeting

| Key | Default | Description |
|-----|---------|-------------|
| `location` | `""` | IB location code for international markets (e.g. `"STK.AU.ASX"`). Empty = use Massive/US default. |
| `tickers` | `[]` | Explicit tickers for IB-path scanning. Required when `location` is set. |
| `exchange` | `""` | Exchange hint for proposals (e.g. `"ASX"`). |
| `currency` | `""` | Currency hint for proposals (e.g. `"AUD"`). |

## Thresholds

| Key | Default | Description |
|-----|---------|-------------|
| `position_move_pct` | `0.015` | A position must move >1.5% daily to trigger deeper analysis. |
| `daily_pnl_alert_pct` | `0.02` | Portfolio daily loss >2% triggers an alert. |
| `risk_hhi_warning` | `0.15` | HHI above 0.15 triggers a concentration warning. |

## Safety

| Key | Default | Description |
|-----|---------|-------------|
| `auto_approve` | `false` | If true, auto-approves proposals below a confidence threshold. **Use with extreme caution.** |

## Context Management

| Key | Default | Description |
|-----|---------|-------------|
| `compact_after_cycle` | `true` | Run `compact("drop-helpers-results")` after each DIGEST phase. Keeps context at ~12K tokens. |

## Example Configurations

### ASX Portfolio Monitoring (Current Setup)
```python
TradingLoop.config.update({
    "scan_interval_seconds": 120,
    "location": "STK.AU.ASX",
    "tickers": ["BHP", "CBA", "CSL", "WDS", "RIO", "FMG", "NAB", "ANZ", "WBC", "TLS"],
    "exchange": "ASX",
    "currency": "AUD",
    "scan_presets": ["momentum", "mean-reversion", "volatile"],
})
await TradingLoop.start()
```

### US Market Day Trading
```python
TradingLoop.config.update({
    "scan_interval_seconds": 60,
    "scan_presets": ["momentum", "gap-up", "breakout", "volatile"],
    "scan_num": 20,
    "max_proposals_per_cycle": 3,
})
await TradingLoop.start()
```

### Conservative Long-Term Monitoring
```python
TradingLoop.config.update({
    "scan_interval_seconds": 900,  # 15 minutes
    "scan_presets": ["mean-reversion"],
    "max_proposals_per_cycle": 1,
    "position_move_pct": 0.03,  # only flag 3%+ moves
})
await TradingLoop.start()
```
