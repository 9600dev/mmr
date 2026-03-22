"""Trading ideas scanner — discover, enrich, filter, score, and rank day-trading candidates.

Uses Massive.com REST API for snapshots and server-side technical indicators.
No trader_service needed — only requires massive_api_key.

When ``--location`` is provided, ``IBIdeaScanner`` uses IB's scanner API +
``get_snapshot()`` + ``reqHistoricalData`` for international markets.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Dataclasses
# ------------------------------------------------------------------

@dataclass
class ScanFilter:
    min_price: float = 1.0
    max_price: float = 10000.0
    min_volume: int = 100_000
    min_change_pct: Optional[float] = None
    max_change_pct: Optional[float] = None
    min_relative_volume: float = 0.0
    max_spread_pct: float = 5.0  # filter out illiquid instruments


@dataclass
class ScanPreset:
    name: str
    description: str
    filters: ScanFilter
    indicators: List[str]
    score_fn: str  # function name in _SCORE_FUNCTIONS
    use_market_scan: bool = False  # True = scan full market via snapshot_all


# ------------------------------------------------------------------
# Preset definitions
# ------------------------------------------------------------------

PRESETS: Dict[str, ScanPreset] = {
    'momentum': ScanPreset(
        name='momentum',
        description='High momentum stocks with strong volume and price action',
        filters=ScanFilter(min_price=5.0, min_volume=500_000, min_change_pct=2.0),
        indicators=['rsi', 'ema_9'],
        score_fn='_score_momentum',
    ),
    'gap-up': ScanPreset(
        name='gap-up',
        description='Stocks gapping up significantly at open',
        filters=ScanFilter(min_price=5.0, min_volume=300_000, min_change_pct=3.0),
        indicators=['rsi'],
        score_fn='_score_gap_up',
    ),
    'gap-down': ScanPreset(
        name='gap-down',
        description='Stocks gapping down — potential reversal or short candidates',
        filters=ScanFilter(min_price=5.0, min_volume=300_000, max_change_pct=-3.0),
        indicators=['rsi', 'sma_20'],
        score_fn='_score_gap_down',
        use_market_scan=True,
    ),
    'mean-reversion': ScanPreset(
        name='mean-reversion',
        description='Oversold stocks near support — bounce candidates',
        filters=ScanFilter(min_price=5.0, min_volume=200_000, max_change_pct=0.0),
        indicators=['rsi', 'sma_20', 'sma_50'],
        score_fn='_score_mean_reversion',
        use_market_scan=True,
    ),
    'breakout': ScanPreset(
        name='breakout',
        description='Stocks breaking above key moving averages with volume',
        filters=ScanFilter(min_price=5.0, min_volume=500_000, min_change_pct=1.0),
        indicators=['ema_9', 'sma_20'],
        score_fn='_score_breakout',
    ),
    'volatile': ScanPreset(
        name='volatile',
        description='High intraday range and volume — scalping candidates',
        filters=ScanFilter(min_price=2.0, min_volume=300_000),
        indicators=['rsi'],
        score_fn='_score_volatile',
        use_market_scan=True,
    ),
}

# Preset → IB scanner scan-code mapping
PRESET_SCAN_CODES: Dict[str, str] = {
    'momentum': 'TOP_PERC_GAIN',
    'gap-up': 'HIGH_OPEN_GAP',
    'gap-down': 'LOW_OPEN_GAP',
    'mean-reversion': 'TOP_PERC_LOSE',
    'breakout': 'TOP_PERC_GAIN',
    'volatile': 'HIGH_VS_13W_HL',
}


def list_presets() -> pd.DataFrame:
    """Return a DataFrame describing all available presets."""
    rows = []
    for p in PRESETS.values():
        f = p.filters
        filter_parts = []
        if f.min_price > 1.0:
            filter_parts.append(f'price>={f.min_price}')
        if f.min_volume > 0:
            filter_parts.append(f'vol>={f.min_volume:,}')
        if f.min_change_pct is not None:
            filter_parts.append(f'chg>={f.min_change_pct}%')
        if f.max_change_pct is not None:
            filter_parts.append(f'chg<={f.max_change_pct}%')
        rows.append({
            'preset': p.name,
            'description': p.description,
            'filters': ', '.join(filter_parts),
            'indicators': ', '.join(p.indicators),
        })
    return pd.DataFrame(rows)


# ------------------------------------------------------------------
# Module-level scoring functions (shared by IdeaScanner & IBIdeaScanner)
# ------------------------------------------------------------------

def _score_momentum(c: Dict) -> tuple:
    """Score based on change%, relative volume, RSI headroom, and EMA position."""
    score = 0.0
    # Change contribution (0-30 points)
    score += min(abs(c.get('change_pct', 0)), 15) * 2
    # Relative volume (0-25 points)
    score += min(c.get('rel_vol', 0), 5) * 5
    # RSI headroom: higher RSI = less room but shows momentum
    rsi = c.get('rsi')
    if rsi is not None:
        if 50 < rsi < 80:
            score += (rsi - 50) * 0.5  # 0-15 points
        elif rsi >= 80:
            score += 10  # extended, still momentum
    # Price above EMA9
    ema_9 = c.get('ema_9')
    if ema_9 is not None and ema_9 > 0 and c['price'] > ema_9:
        score += 10

    signal = 'BUY' if score >= 40 else ('WATCH' if score >= 20 else 'WATCH')
    return (min(score, 100), signal)


def _score_gap_up(c: Dict) -> tuple:
    """Score based on gap size and relative volume."""
    score = 0.0
    # Gap contribution (0-40 points)
    gap = abs(c.get('gap_pct', 0))
    score += min(gap, 20) * 2
    # Relative volume (0-30 points)
    score += min(c.get('rel_vol', 0), 6) * 5
    # Change contribution (0-20 points)
    score += min(abs(c.get('change_pct', 0)), 10) * 2
    # RSI — not too overbought
    rsi = c.get('rsi')
    if rsi is not None and rsi < 70:
        score += 10

    signal = 'BUY' if score >= 40 else 'WATCH'
    return (min(score, 100), signal)


def _score_gap_down(c: Dict) -> tuple:
    """Score based on gap size, RSI oversold level, and SMA distance."""
    score = 0.0
    # Gap size (0-40 points)
    gap = abs(c.get('gap_pct', 0))
    score += min(gap, 20) * 2
    # RSI oversold bonus (0-30 points)
    rsi = c.get('rsi')
    if rsi is not None and rsi < 40:
        score += (40 - rsi) * 0.75
    # Price below SMA20
    sma_20 = c.get('sma_20')
    if sma_20 is not None and sma_20 > 0 and c['price'] < sma_20:
        distance_pct = ((sma_20 - c['price']) / sma_20) * 100
        score += min(distance_pct, 10) * 2
    # Relative volume
    score += min(c.get('rel_vol', 0), 4) * 2.5

    signal = 'SELL' if score >= 40 else 'WATCH'
    return (min(score, 100), signal)


def _score_mean_reversion(c: Dict) -> tuple:
    """Score based on RSI oversold level and distance below moving averages."""
    score = 0.0
    # RSI oversold (0-30 points)
    rsi = c.get('rsi')
    if rsi is not None:
        if rsi < 30:
            score += (30 - rsi) * 1.0
        elif rsi < 40:
            score += (40 - rsi) * 0.5

    # Distance below SMA20 (0-25 points)
    sma_20 = c.get('sma_20')
    if sma_20 is not None and sma_20 > 0 and c['price'] < sma_20:
        distance_pct = ((sma_20 - c['price']) / sma_20) * 100
        score += min(distance_pct, 10) * 2.5

    # Distance below SMA50 (0-25 points)
    sma_50 = c.get('sma_50')
    if sma_50 is not None and sma_50 > 0 and c['price'] < sma_50:
        distance_pct = ((sma_50 - c['price']) / sma_50) * 100
        score += min(distance_pct, 10) * 2.5

    # Relative volume (0-20 points)
    score += min(c.get('rel_vol', 0), 4) * 5

    signal = 'BUY' if score >= 35 else 'WATCH'
    return (min(score, 100), signal)


def _score_breakout(c: Dict) -> tuple:
    """Score based on price vs VWAP, EMA/SMA alignment, and volume."""
    score = 0.0
    price = c['price']

    # Price above VWAP (0-15 points)
    vwap = c.get('vwap', 0)
    if vwap > 0 and price > vwap:
        score += 15

    # Price > EMA9 > SMA20 alignment (0-30 points)
    ema_9 = c.get('ema_9')
    sma_20 = c.get('sma_20')
    if ema_9 is not None and sma_20 is not None:
        if price > ema_9 > sma_20:
            score += 30
        elif price > ema_9:
            score += 15
        elif price > sma_20:
            score += 10

    # Relative volume (0-30 points)
    score += min(c.get('rel_vol', 0), 6) * 5

    # Change contribution (0-15 points)
    score += min(abs(c.get('change_pct', 0)), 5) * 3

    signal = 'BUY' if score >= 45 else 'WATCH'
    return (min(score, 100), signal)


def _score_volatile(c: Dict) -> tuple:
    """Score based on intraday range, relative volume, and absolute change."""
    score = 0.0
    # Range % (0-40 points)
    score += min(c.get('range_pct', 0), 20) * 2
    # Relative volume (0-25 points)
    score += min(c.get('rel_vol', 0), 5) * 5
    # Absolute change % (0-25 points)
    score += min(abs(c.get('change_pct', 0)), 10) * 2.5
    # RSI extremes bonus
    rsi = c.get('rsi')
    if rsi is not None and (rsi > 70 or rsi < 30):
        score += 10

    signal = 'BUY' if c.get('change_pct', 0) < -3 else ('SELL' if c.get('change_pct', 0) > 3 else 'WATCH')
    return (min(score, 100), signal)


# Lookup dict for scoring functions by name
_SCORE_FUNCTIONS: Dict[str, Callable] = {
    '_score_momentum': _score_momentum,
    '_score_gap_up': _score_gap_up,
    '_score_gap_down': _score_gap_down,
    '_score_mean_reversion': _score_mean_reversion,
    '_score_breakout': _score_breakout,
    '_score_volatile': _score_volatile,
}


# ------------------------------------------------------------------
# Module-level filtering and formatting (shared by both scanners)
# ------------------------------------------------------------------

def apply_filters(candidates: List[Dict], filters: ScanFilter,
                  trading_filter=None) -> List[Dict]:
    """Apply ScanFilter criteria to candidate list.

    Parameters
    ----------
    trading_filter : TradingFilter, optional
        If provided, also apply denylist/allowlist/exchange/sec_type checks.
    """
    result = []
    for c in candidates:
        # Trading filter check (denylist, allowlist, etc.)
        if trading_filter:
            allowed, _ = trading_filter.is_allowed(c['ticker'], price=c.get('price', 0))
            if not allowed:
                continue
        if c['price'] < filters.min_price or c['price'] > filters.max_price:
            continue
        if c['volume'] > 0 and c['volume'] < filters.min_volume:
            continue
        if filters.min_change_pct is not None and c['change_pct'] < filters.min_change_pct:
            continue
        if filters.max_change_pct is not None and c['change_pct'] > filters.max_change_pct:
            continue
        if c['rel_vol'] < filters.min_relative_volume:
            continue
        if c.get('spread_pct', 0) > filters.max_spread_pct > 0:
            continue
        result.append(c)
    return result


def to_dataframe(candidates: List[Dict], fundamentals: bool = False, news: bool = False) -> pd.DataFrame:
    """Convert scored candidates to a DataFrame with consistent column order."""
    if not candidates:
        return pd.DataFrame()

    # Base columns always present
    base_cols = ['ticker', 'name', 'price', 'change_pct', 'volume', 'gap_pct',
                 'rel_vol', 'range_pct', 'score', 'signal']
    # Indicator columns that may be present
    indicator_cols = ['rsi', 'ema_9', 'sma_20', 'sma_50']
    # Fundamental columns (in display order)
    fundamental_cols = ['pe_ratio', 'pb_ratio', 'ps_ratio', 'debt_equity',
                       'roe', 'roa', 'div_yield', 'ev_ebitda',
                       'mkt_cap', 'eps', 'fcf']
    # News columns
    news_cols = ['sentiment', 'headline', 'catalyst', 'news_date']

    df = pd.DataFrame(candidates)

    # Order columns: base first, then indicators, then fundamentals, then news, then extras
    ordered = [c for c in base_cols if c in df.columns]
    ordered += [c for c in indicator_cols if c in df.columns]
    if fundamentals:
        ordered += [c for c in fundamental_cols if c in df.columns]
    if news:
        ordered += [c for c in news_cols if c in df.columns]
    ordered += [c for c in df.columns if c not in ordered]

    df = df[ordered]

    # Round indicator columns
    for col in indicator_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    return df.reset_index(drop=True)


def merge_filters(scan_preset: ScanPreset, custom_filters: Optional[Dict[str, Any]] = None) -> ScanFilter:
    """Create a ScanFilter from a preset, optionally overriding with custom values."""
    filters = ScanFilter(
        min_price=scan_preset.filters.min_price,
        max_price=scan_preset.filters.max_price,
        min_volume=scan_preset.filters.min_volume,
        min_change_pct=scan_preset.filters.min_change_pct,
        max_change_pct=scan_preset.filters.max_change_pct,
        min_relative_volume=scan_preset.filters.min_relative_volume,
    )
    if custom_filters:
        for k, v in custom_filters.items():
            if hasattr(filters, k) and v is not None:
                setattr(filters, k, v)
    return filters


# ------------------------------------------------------------------
# Local indicator computation (pure pandas, no API)
# ------------------------------------------------------------------

def compute_rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Compute RSI from a list of closing prices."""
    if len(closes) < period + 1:
        return None
    deltas = pd.Series(closes).diff()
    gain = deltas.clip(lower=0).rolling(period).mean()
    loss = (-deltas.clip(upper=0)).rolling(period).mean()
    rs = gain / loss
    rsi = 100 - 100 / (1 + rs)
    val = rsi.iloc[-1]
    if pd.isna(val):
        return None
    return round(float(val), 2)


def compute_ema(closes: List[float], window: int) -> Optional[float]:
    """Compute EMA from a list of closing prices."""
    if len(closes) < window:
        return None
    val = pd.Series(closes).ewm(span=window, adjust=False).mean().iloc[-1]
    if pd.isna(val):
        return None
    return round(float(val), 2)


def compute_sma(closes: List[float], window: int) -> Optional[float]:
    """Compute SMA from a list of closing prices."""
    if len(closes) < window:
        return None
    val = pd.Series(closes).rolling(window).mean().iloc[-1]
    if pd.isna(val):
        return None
    return round(float(val), 2)


# ------------------------------------------------------------------
# IdeaScanner (Massive.com-backed, US markets)
# ------------------------------------------------------------------

class IdeaScanner:
    """Discover → Enrich → Filter → Score → Rank pipeline for trading ideas."""

    def __init__(self, massive_client):
        self._client = massive_client

    def scan(
        self,
        preset: str = 'momentum',
        source: str = 'movers',
        tickers: Optional[List[str]] = None,
        universe_symbols: Optional[List[str]] = None,
        top_n: int = 15,
        custom_filters: Optional[Dict[str, Any]] = None,
        fundamentals: bool = False,
        news: bool = False,
        names: bool = False,
    ) -> pd.DataFrame:
        """Run the full scan pipeline.

        Parameters
        ----------
        preset : str
            Preset name (momentum, gap-up, gap-down, mean-reversion, breakout, volatile).
        source : str
            'movers' (default), 'tickers', or 'universe'.
        tickers : list of str, optional
            Explicit ticker list (when source='tickers').
        universe_symbols : list of str, optional
            Symbol list from a universe (when source='universe').
        top_n : int
            Max results to return.
        custom_filters : dict, optional
            Override preset filter values (e.g. {'min_price': 10}).
        fundamentals : bool
            If True, enrich results with financial ratios (PE, D/E, ROE, etc.).
        news : bool
            If True, enrich results with latest news headline and sentiment.

        Returns
        -------
        pd.DataFrame
            Ranked ideas with columns: ticker, price, change_pct, volume,
            gap_pct, rel_vol, range_pct, score, signal, plus indicator columns.
        """
        scan_preset = PRESETS.get(preset)
        if not scan_preset:
            raise ValueError(f'Unknown preset: {preset}. Available: {", ".join(PRESETS.keys())}')

        # Merge custom filters with preset defaults
        filters = merge_filters(scan_preset, custom_filters)

        # 1. Discover candidates
        # For market-scan presets using movers source, upgrade to full market scan
        effective_source = source
        if source == 'movers' and scan_preset.use_market_scan:
            effective_source = 'market'
        snapshots = self._discover(effective_source, tickers, universe_symbols)
        if not snapshots:
            return pd.DataFrame()

        # 2. Build candidate dicts from snapshots
        candidates = self._build_candidates(snapshots)
        if not candidates:
            return pd.DataFrame()

        # 3. Filter
        candidates = self._apply_filters(candidates, filters)
        if not candidates:
            return pd.DataFrame()

        # 4. Pre-score without indicators to rank candidates locally.
        #    This lets us cap indicator API calls for market-wide scans.
        score_fn = _SCORE_FUNCTIONS[scan_preset.score_fn]
        for c in candidates:
            score, signal = score_fn(c)
            c['_pre_score'] = score

        # Cap indicator fetching: take the best candidates by pre-score,
        # up to 3x top_n (or all if pool is small), to limit API calls.
        indicator_cap = max(top_n * 3, 30)
        if len(candidates) > indicator_cap:
            candidates.sort(key=lambda c: c['_pre_score'], reverse=True)
            candidates = candidates[:indicator_cap]

        # 5. Fetch indicators for survivors
        ticker_list = [c['ticker'] for c in candidates]
        indicators = self._fetch_indicators(ticker_list, scan_preset.indicators)

        # Merge indicators into candidates
        for c in candidates:
            t = c['ticker']
            if t in indicators:
                c.update(indicators[t])

        # 6. Re-score with indicator data
        for c in candidates:
            score, signal = score_fn(c)
            c['score'] = round(score, 1)
            c['signal'] = signal
            c.pop('_pre_score', None)

        # 7. Sort + top_n
        candidates.sort(key=lambda c: c['score'], reverse=True)
        candidates = candidates[:top_n]

        # 8. Optionally fetch company names
        if names and candidates:
            name_data = self._fetch_names([c['ticker'] for c in candidates])
            for c in candidates:
                c['name'] = name_data.get(c['ticker'], '')

        # 9. Optionally enrich with financial ratios
        if fundamentals and candidates:
            fund_data = self._fetch_fundamentals([c['ticker'] for c in candidates])
            for c in candidates:
                if c['ticker'] in fund_data:
                    c.update(fund_data[c['ticker']])

        # 10. Optionally enrich with news
        if news and candidates:
            news_data = self._fetch_news([c['ticker'] for c in candidates])
            for c in candidates:
                if c['ticker'] in news_data:
                    c.update(news_data[c['ticker']])

        return self._to_dataframe(candidates, fundamentals=fundamentals, news=news)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _discover(
        self,
        source: str,
        tickers: Optional[List[str]],
        universe_symbols: Optional[List[str]],
    ) -> list:
        """Fetch raw snapshots from Massive.com."""
        if source == 'tickers' and tickers:
            return list(self._client.get_snapshot_all(
                market_type='stocks', tickers=tickers,
            ))
        elif source == 'universe' and universe_symbols:
            return list(self._client.get_snapshot_all(
                market_type='stocks', tickers=universe_symbols,
            ))
        elif source == 'market':
            # Full market scan — returns 10K+ snapshots. Filtering happens
            # in _build_candidates and _apply_filters before any indicator
            # API calls, so this is efficient (1 API call, local filtering).
            return list(self._client.get_snapshot_all(
                market_type='stocks',
            ))
        else:
            # Movers: fetch both gainers and losers for a broader pool
            gainers = list(self._client.get_snapshot_direction(
                market_type='stocks', direction='gainers',
            ))
            losers = list(self._client.get_snapshot_direction(
                market_type='stocks', direction='losers',
            ))
            return gainers + losers

    # ------------------------------------------------------------------
    # Candidate building
    # ------------------------------------------------------------------

    def _build_candidates(self, snapshots: list) -> List[Dict[str, Any]]:
        """Extract structured data from TickerSnapshot objects."""
        candidates = []
        seen = set()
        for snap in snapshots:
            ticker = snap.ticker or ''
            if not ticker or ticker in seen:
                continue
            # Skip warrants, units, rights, preferred stocks
            # (e.g. AEVAW, SRTAW, KKRpD, ACHR.U)
            if any(ticker.endswith(s) for s in ('W', 'WS', '.U', '.R')):
                continue
            if 'p' in ticker and ticker != ticker.upper():
                # Preferred stock: contains lowercase 'p' (e.g. KKRpD)
                continue
            seen.add(ticker)

            day = snap.day
            prev_day = snap.prev_day
            if not day:
                continue

            price = getattr(day, 'close', None) or 0.0
            volume = getattr(day, 'volume', None) or 0
            day_open = getattr(day, 'open', None) or 0.0
            day_high = getattr(day, 'high', None) or 0.0
            day_low = getattr(day, 'low', None) or 0.0
            vwap = getattr(day, 'vwap', None) or 0.0

            change_pct = snap.todays_change_percent or 0.0

            # Gap: (open - prev_close) / prev_close
            prev_close = 0.0
            prev_volume = 0
            if prev_day:
                prev_close = getattr(prev_day, 'close', None) or 0.0
                prev_volume = getattr(prev_day, 'volume', None) or 0

            gap_pct = 0.0
            if prev_close > 0:
                gap_pct = ((day_open - prev_close) / prev_close) * 100.0

            # Relative volume
            rel_vol = 0.0
            if prev_volume > 0:
                rel_vol = volume / prev_volume

            # Intraday range %
            range_pct = 0.0
            if day_low > 0:
                range_pct = ((day_high - day_low) / day_low) * 100.0

            # Spread — Massive API uses bid_price/ask_price fields
            spread_pct = 0.0
            if snap.last_quote and price > 0:
                bid = (getattr(snap.last_quote, 'bid_price', None)
                       or getattr(snap.last_quote, 'bid', None) or 0.0)
                ask = (getattr(snap.last_quote, 'ask_price', None)
                       or getattr(snap.last_quote, 'ask', None) or 0.0)
                if bid > 0 and ask > 0:
                    spread_pct = ((ask - bid) / price) * 100.0

            candidates.append({
                'ticker': ticker,
                'price': round(price, 2),
                'change_pct': round(change_pct, 2),
                'volume': int(volume),
                'gap_pct': round(gap_pct, 2),
                'rel_vol': round(rel_vol, 2),
                'range_pct': round(range_pct, 2),
                'spread_pct': round(spread_pct, 3),
                'vwap': round(vwap, 2),
            })

        return candidates

    # ------------------------------------------------------------------
    # Filtering (delegates to module-level)
    # ------------------------------------------------------------------

    def _apply_filters(self, candidates: List[Dict], filters: ScanFilter) -> List[Dict]:
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        return apply_filters(candidates, filters, trading_filter=tf if not tf.is_empty() else None)

    # ------------------------------------------------------------------
    # Indicator fetching (parallel)
    # ------------------------------------------------------------------

    def _fetch_indicators(
        self,
        tickers: List[str],
        needed: List[str],
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """Fetch technical indicators in parallel via ThreadPoolExecutor."""
        if not needed or not tickers:
            return {}

        results: Dict[str, Dict[str, Optional[float]]] = {t: {} for t in tickers}

        def fetch_one(ticker: str, indicator: str) -> tuple:
            """Returns (ticker, indicator_name, value)."""
            try:
                if indicator == 'rsi':
                    res = self._client.get_rsi(
                        ticker, timespan='day', window=14, limit=1,
                    )
                    vals = list(res.values) if hasattr(res, 'values') else []
                    return (ticker, 'rsi', vals[0].value if vals else None)
                elif indicator == 'ema_9':
                    res = self._client.get_ema(
                        ticker, timespan='day', window=9, limit=1,
                    )
                    vals = list(res.values) if hasattr(res, 'values') else []
                    return (ticker, 'ema_9', vals[0].value if vals else None)
                elif indicator == 'sma_20':
                    res = self._client.get_sma(
                        ticker, timespan='day', window=20, limit=1,
                    )
                    vals = list(res.values) if hasattr(res, 'values') else []
                    return (ticker, 'sma_20', vals[0].value if vals else None)
                elif indicator == 'sma_50':
                    res = self._client.get_sma(
                        ticker, timespan='day', window=50, limit=1,
                    )
                    vals = list(res.values) if hasattr(res, 'values') else []
                    return (ticker, 'sma_50', vals[0].value if vals else None)
                else:
                    return (ticker, indicator, None)
            except Exception as e:
                logger.debug('indicator fetch failed: %s %s: %s', ticker, indicator, e)
                return (ticker, indicator, None)

        tasks = [(t, ind) for t in tickers for ind in needed]
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(fetch_one, t, ind): (t, ind) for t, ind in tasks}
            for future in as_completed(futures):
                ticker, indicator, value = future.result()
                results[ticker][indicator] = value

        return results

    # ------------------------------------------------------------------
    # Fundamentals fetching (parallel)
    # ------------------------------------------------------------------

    # Fields to extract from FinancialRatio objects
    _FUNDAMENTAL_FIELDS = [
        ('price_to_earnings', 'pe_ratio'),
        ('price_to_book', 'pb_ratio'),
        ('price_to_sales', 'ps_ratio'),
        ('debt_to_equity', 'debt_equity'),
        ('return_on_equity', 'roe'),
        ('return_on_assets', 'roa'),
        ('dividend_yield', 'div_yield'),
        ('ev_to_ebitda', 'ev_ebitda'),
        ('market_cap', 'mkt_cap'),
        ('earnings_per_share', 'eps'),
        ('free_cash_flow', 'fcf'),
    ]

    def _fetch_names(
        self,
        tickers: List[str],
    ) -> Dict[str, str]:
        """Fetch company names in parallel via get_ticker_details."""
        if not tickers:
            return {}

        results: Dict[str, str] = {}

        def fetch_one(ticker: str) -> tuple:
            try:
                d = self._client.get_ticker_details(ticker)
                return (ticker, d.name or '')
            except Exception:
                return (ticker, '')

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(fetch_one, t): t for t in tickers}
            for future in as_completed(futures):
                ticker, name = future.result()
                if name:
                    results[ticker] = name

        return results

    def _fetch_fundamentals(
        self,
        tickers: List[str],
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """Fetch financial ratios (TTM) in parallel for each ticker."""
        if not tickers:
            return {}

        results: Dict[str, Dict[str, Optional[float]]] = {}

        def fetch_one(ticker: str) -> tuple:
            try:
                ratios = list(self._client.list_financials_ratios(
                    ticker=ticker, limit=1,
                ))
                if not ratios:
                    return (ticker, {})
                r = ratios[0]
                data = {}
                for api_field, col_name in self._FUNDAMENTAL_FIELDS:
                    val = getattr(r, api_field, None)
                    if val is not None:
                        data[col_name] = round(float(val), 2) if col_name not in ('mkt_cap', 'fcf') else val
                    else:
                        data[col_name] = None
                return (ticker, data)
            except Exception as e:
                logger.debug('fundamentals fetch failed: %s: %s', ticker, e)
                return (ticker, {})

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(fetch_one, t): t for t in tickers}
            for future in as_completed(futures):
                ticker, data = future.result()
                if data:
                    results[ticker] = data

        return results

    # ------------------------------------------------------------------
    # News fetching (parallel)
    # ------------------------------------------------------------------

    def _fetch_news(
        self,
        tickers: List[str],
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """Fetch latest news headline + sentiment in parallel for each ticker."""
        if not tickers:
            return {}

        results: Dict[str, Dict[str, Optional[str]]] = {}

        def fetch_one(ticker: str) -> tuple:
            try:
                articles = list(self._client.list_ticker_news(
                    ticker=ticker, limit=1,
                ))
                if not articles:
                    return (ticker, {})
                a = articles[0]
                # Extract sentiment for this specific ticker from insights
                sentiment = ''
                sentiment_reason = ''
                if a.insights:
                    for i in a.insights:
                        if getattr(i, 'ticker', '') == ticker:
                            sentiment = getattr(i, 'sentiment', '') or ''
                            sentiment_reason = getattr(i, 'sentiment_reasoning', '') or ''
                            break
                    # If no ticker-specific insight, take the first one
                    if not sentiment and a.insights:
                        sentiment = getattr(a.insights[0], 'sentiment', '') or ''
                        sentiment_reason = getattr(a.insights[0], 'sentiment_reasoning', '') or ''
                title = a.title or ''
                # Truncate long titles
                if len(title) > 120:
                    title = title[:117] + '...'
                # Truncate long sentiment reasons
                if len(sentiment_reason) > 200:
                    sentiment_reason = sentiment_reason[:197] + '...'
                published = (getattr(a, 'published_utc', '') or '')[:10]
                return (ticker, {
                    'headline': title,
                    'news_date': published,
                    'sentiment': sentiment,
                    'catalyst': sentiment_reason,
                })
            except Exception as e:
                logger.debug('news fetch failed: %s: %s', ticker, e)
                return (ticker, {})

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(fetch_one, t): t for t in tickers}
            for future in as_completed(futures):
                ticker, data = future.result()
                if data:
                    results[ticker] = data

        return results

    # ------------------------------------------------------------------
    # Scoring (delegates to module-level functions)
    # ------------------------------------------------------------------

    def _score_momentum(self, c: Dict) -> tuple:
        return _score_momentum(c)

    def _score_gap_up(self, c: Dict) -> tuple:
        return _score_gap_up(c)

    def _score_gap_down(self, c: Dict) -> tuple:
        return _score_gap_down(c)

    def _score_mean_reversion(self, c: Dict) -> tuple:
        return _score_mean_reversion(c)

    def _score_breakout(self, c: Dict) -> tuple:
        return _score_breakout(c)

    def _score_volatile(self, c: Dict) -> tuple:
        return _score_volatile(c)

    # ------------------------------------------------------------------
    # Output formatting (delegates to module-level)
    # ------------------------------------------------------------------

    def _to_dataframe(self, candidates: List[Dict], fundamentals: bool = False, news: bool = False) -> pd.DataFrame:
        return to_dataframe(candidates, fundamentals=fundamentals, news=news)


# ------------------------------------------------------------------
# IBIdeaScanner (IB-backed, international markets)
# ------------------------------------------------------------------

def parse_report_snapshot(xml_str: str) -> Dict[str, Any]:
    """Parse IB ReportSnapshot XML into a flat dict of financial ratios.

    Returns keys matching the Massive path: pe_ratio, pb_ratio, debt_equity,
    roe, roa, div_yield, mkt_cap, eps, etc.
    """
    import xml.etree.ElementTree as ET

    data: Dict[str, Any] = {}
    if not xml_str:
        return data

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return data

    # IB ReportSnapshot uses <Ratio FieldName="...">value</Ratio> inside
    # <Group ID="..."> under <Ratios>.
    field_map = {
        'APENORM': 'pe_ratio',
        'TTMPRFCFPS': 'ps_ratio',
        'PRICE2BK': 'pb_ratio',
        'QTOTD2EQ': 'debt_equity',
        'TTMROEPCT': 'roe',
        'TTMROAPCT': 'roa',
        'YIELD': 'div_yield',
        'EV2EBITDA': 'ev_ebitda',
        'MKTCAP': 'mkt_cap',
        'TTMEPSXCLX': 'eps',
        'AFETEFCFPS': 'fcf',
    }

    for ratio in root.iter('Ratio'):
        field_name = ratio.get('FieldName', '')
        if field_name in field_map and ratio.text:
            try:
                val = float(ratio.text)
                col = field_map[field_name]
                if col in ('mkt_cap', 'fcf'):
                    data[col] = val
                else:
                    data[col] = round(val, 2)
            except (ValueError, TypeError):
                pass

    return data


class IBIdeaScanner:
    """IB-backed idea scanner for international markets.

    Uses IB's scanner API for discovery, ``get_snapshot()`` for price data,
    and ``reqHistoricalData`` for indicator computation (RSI/EMA/SMA).
    Fundamentals via ``reqFundamentalData``, news via ``reqHistoricalNews``.

    When explicit tickers or a universe is provided, symbol resolution via IB
    is used instead of the scanner API (which may not support all locations).

    The scoring/filtering/formatting logic is shared with :class:`IdeaScanner`.
    """

    def __init__(self, rpc_client):
        self._rpc = rpc_client

    def scan(
        self,
        preset: str = 'momentum',
        location: str = 'STK.AU.ASX',
        top_n: int = 15,
        custom_filters: Optional[Dict[str, Any]] = None,
        fundamentals: bool = False,
        news: bool = False,
        tickers: Optional[List[str]] = None,
        universe_symbols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Run the IB-backed scan pipeline.

        Parameters
        ----------
        preset : str
            Scoring preset name.
        location : str
            IB market location code (e.g. STK.AU.ASX, STK.CA, STK.HK.SEHK).
        top_n : int
            Max results to return.
        custom_filters : dict, optional
            Override preset filter values.
        fundamentals : bool
            If True, enrich with financial ratios via IB reqFundamentalData.
        news : bool
            If True, enrich with news headlines via IB reqHistoricalNews.
        tickers : list of str, optional
            Explicit ticker list. Resolved via IB (bypasses scanner).
        universe_symbols : list of str, optional
            Symbol list from a universe. Resolved via IB (bypasses scanner).
        """
        # Check location against trading filters before doing any work
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        if not tf.is_empty():
            allowed, reason = tf.is_allowed('', location=location)
            if not allowed:
                logger.warning('Trading filter blocked location %s: %s', location, reason)
                return pd.DataFrame()

        from ib_async.contract import Contract
        from trader.messaging.clientserver import consume

        scan_preset = PRESETS.get(preset)
        if not scan_preset:
            raise ValueError(f'Unknown preset: {preset}. Available: {", ".join(PRESETS.keys())}')

        filters = merge_filters(scan_preset, custom_filters)
        score_fn = _SCORE_FUNCTIONS[scan_preset.score_fn]

        # 1. Discover candidates
        symbols_to_resolve = tickers or universe_symbols
        if symbols_to_resolve:
            # When user provides explicit symbols, don't filter on change_pct
            # (the preset's min_change_pct is for scanner-based discovery)
            if custom_filters is None or 'min_change_pct' not in custom_filters:
                filters.min_change_pct = None
            if custom_filters is None or 'max_change_pct' not in custom_filters:
                filters.max_change_pct = None
        if symbols_to_resolve:
            # Resolve explicit symbols via IB (bypasses scanner API)
            contracts, conid_map = self._resolve_symbols(
                symbols_to_resolve, location, consume,
            )
            if not contracts:
                logger.warning('No symbols could be resolved via IB')
                return pd.DataFrame()
        else:
            # Use IB scanner for discovery
            scan_code = PRESET_SCAN_CODES.get(preset, 'TOP_PERC_GAIN')
            scanner_results = consume(
                self._rpc.rpc(return_type=list[dict]).scanner_data(
                    scan_code=scan_code,
                    location_code=location,
                    num_rows=top_n * 3,
                )
            )
            if not scanner_results:
                logger.warning(
                    'IB scanner returned no results for location=%s scan_code=%s. '
                    'This usually means the scanner does not support this location, '
                    'or your IB account lacks market data subscriptions for it. '
                    'Try using --tickers or --universe to specify symbols explicitly.',
                    location, scan_code,
                )
                return pd.DataFrame()

            # Build Contract objects from scanner results
            contracts = []
            conid_map: Dict[str, int] = {}  # symbol → conId for news lookup
            for r in scanner_results:
                raw_exchange = r.get('exchange', '') or exchange_hint or 'SMART'
                primary = raw_exchange if raw_exchange != 'SMART' else ''
                c = Contract(
                    conId=r['conId'],
                    symbol=r['symbol'],
                    secType=r.get('secType', 'STK'),
                    exchange='SMART',
                    primaryExchange=primary,
                    currency=r.get('currency', ''),
                )
                contracts.append(c)
                conid_map[r['symbol']] = r['conId']

        # 3. Get snapshots via RPC → IB
        snapshots = consume(
            self._rpc.rpc(return_type=list[dict]).get_snapshots_batch(contracts, True)
        )

        # 4. Get history for prev_close/prev_volume + local indicator computation
        #    Limit history fetches to top candidates
        history_map: Dict[str, list[dict]] = {}
        history_limit = min(len(contracts), top_n * 2)
        for contract in contracts[:history_limit]:
            try:
                bars = consume(
                    self._rpc.rpc(return_type=list[dict]).get_history_bars(contract, '60 D', '1 day')
                )
                if bars:
                    history_map[contract.symbol] = bars
            except Exception:
                logger.debug('History fetch failed for %s', contract.symbol)

        # 5. Build candidates (same dict format as Massive path)
        candidates = self._build_candidates(snapshots, history_map)
        if not candidates:
            return pd.DataFrame()

        # 6. Filter
        from trader.trading.trading_filter import TradingFilter
        tf = TradingFilter.load()
        candidates = apply_filters(candidates, filters,
                                   trading_filter=tf if not tf.is_empty() else None)
        if not candidates:
            return pd.DataFrame()

        # 7. Compute indicators locally from history bars
        for c in candidates:
            symbol = c['ticker']
            bars = history_map.get(symbol, [])
            if bars:
                closes = [b['close'] for b in bars if b.get('close') is not None]
                for ind in scan_preset.indicators:
                    if ind == 'rsi':
                        c['rsi'] = compute_rsi(closes)
                    elif ind == 'ema_9':
                        c['ema_9'] = compute_ema(closes, 9)
                    elif ind == 'sma_20':
                        c['sma_20'] = compute_sma(closes, 20)
                    elif ind == 'sma_50':
                        c['sma_50'] = compute_sma(closes, 50)

        # 8. Score
        for c in candidates:
            score, signal = score_fn(c)
            c['score'] = round(score, 1)
            c['signal'] = signal

        # 9. Sort + top_n
        candidates.sort(key=lambda c: c['score'], reverse=True)
        candidates = candidates[:top_n]

        # 10. Optionally enrich with fundamentals (IB ReportSnapshot)
        if fundamentals and candidates:
            fund_data = self._fetch_fundamentals(
                contracts, candidates, consume,
            )
            for c in candidates:
                if c['ticker'] in fund_data:
                    c.update(fund_data[c['ticker']])

        # 11. Optionally enrich with news (IB reqHistoricalNews)
        if news and candidates:
            news_data = self._fetch_news(
                candidates, conid_map, consume,
            )
            for c in candidates:
                if c['ticker'] in news_data:
                    c.update(news_data[c['ticker']])

        return to_dataframe(candidates, fundamentals=fundamentals, news=news)

    # ------------------------------------------------------------------
    # Symbol resolution (alternative to scanner discovery)
    # ------------------------------------------------------------------

    def _resolve_symbols(
        self,
        symbols: List[str],
        location: str,
        consume,
    ) -> tuple:
        """Resolve explicit symbol names to IB Contracts via resolve_contract RPC.

        Builds a partial Contract with the exchange extracted from the location
        code (e.g. STK.AU.ASX → exchange=ASX) and uses IB's reqContractDetails
        to get the full contract. This avoids the SMART exchange fallback that
        would return US ADRs instead of local listings.

        Returns (contracts, conid_map) tuple.
        """
        from ib_async.contract import Contract

        # Extract exchange hint from location code (e.g. STK.AU.ASX → ASX)
        exchange_hint = ''
        parts = location.split('.')
        if len(parts) >= 3:
            exchange_hint = parts[2]  # e.g. ASX, SEHK, TSE

        contracts = []
        conid_map: Dict[str, int] = {}
        for sym in symbols:
            try:
                # Build a partial contract with the right exchange
                partial = Contract(
                    symbol=sym,
                    secType='STK',
                    exchange=exchange_hint or 'SMART',
                )
                defs = consume(
                    self._rpc.rpc(return_type=list).resolve_contract(partial)
                )
                if not defs:
                    logger.debug('Could not resolve symbol: %s (exchange=%s)', sym, exchange_hint)
                    continue
                # Pick the first matching definition
                d = defs[0]
                # Handle both SecurityDefinition objects and dicts
                if hasattr(d, 'conId'):
                    primary = d.primaryExchange or exchange_hint or ''
                    c = Contract(
                        conId=d.conId,
                        symbol=d.symbol,
                        secType='STK',
                        exchange='SMART',
                        primaryExchange=primary,
                        currency=d.currency or '',
                    )
                    contracts.append(c)
                    conid_map[d.symbol] = d.conId
                elif isinstance(d, dict):
                    primary = d.get('primaryExchange', '') or exchange_hint or ''
                    c = Contract(
                        conId=d.get('conId', 0),
                        symbol=d.get('symbol', sym),
                        secType='STK',
                        exchange='SMART',
                        primaryExchange=primary,
                        currency=d.get('currency', ''),
                    )
                    contracts.append(c)
                    conid_map[c.symbol] = c.conId
            except Exception:
                logger.debug('Failed to resolve symbol: %s', sym)
        return contracts, conid_map

    @staticmethod
    def _build_candidates(
        snapshots: list[dict],
        history_map: Dict[str, list[dict]],
    ) -> List[Dict[str, Any]]:
        """Build candidate dicts from IB snapshot data + history bars."""
        candidates = []
        seen = set()
        for snap in snapshots:
            symbol = snap.get('symbol', '')
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)

            price = snap.get('last') or snap.get('close') or 0.0
            if not price or price != price:  # NaN check
                price = snap.get('bid') or snap.get('ask') or 0.0
            if not price or price != price:
                continue

            volume = snap.get('volume') or 0
            day_open = snap.get('open') or 0.0
            day_high = snap.get('high') or 0.0
            day_low = snap.get('low') or 0.0

            # Derive prev_close and prev_volume from history bars
            bars = history_map.get(symbol, [])
            prev_close = 0.0
            prev_volume = 0
            if len(bars) >= 2:
                prev_bar = bars[-2]
                prev_close = prev_bar.get('close', 0.0) or 0.0
                prev_volume = prev_bar.get('volume', 0) or 0

            # change_pct
            change_pct = 0.0
            if prev_close > 0:
                change_pct = ((price - prev_close) / prev_close) * 100.0

            # gap_pct
            gap_pct = 0.0
            if prev_close > 0 and day_open > 0:
                gap_pct = ((day_open - prev_close) / prev_close) * 100.0

            # relative volume
            rel_vol = 0.0
            if prev_volume > 0 and volume > 0:
                rel_vol = volume / prev_volume

            # intraday range %
            range_pct = 0.0
            if day_low > 0 and day_high > 0:
                range_pct = ((day_high - day_low) / day_low) * 100.0

            # spread
            spread_pct = 0.0
            bid = snap.get('bid') or 0.0
            ask = snap.get('ask') or 0.0
            if bid > 0 and ask > 0 and price > 0:
                spread_pct = ((ask - bid) / price) * 100.0

            candidates.append({
                'ticker': symbol,
                'price': round(float(price), 2),
                'change_pct': round(change_pct, 2),
                'volume': int(volume) if volume and volume == volume else 0,
                'gap_pct': round(gap_pct, 2),
                'rel_vol': round(rel_vol, 2),
                'range_pct': round(range_pct, 2),
                'spread_pct': round(spread_pct, 3),
                'vwap': 0.0,
                'exchange': snap.get('exchange', ''),
                'currency': snap.get('currency', ''),
            })

        return candidates

    # ------------------------------------------------------------------
    # Fundamentals (IB ReportSnapshot)
    # ------------------------------------------------------------------

    def _fetch_fundamentals(
        self,
        contracts: list,
        candidates: List[Dict],
        consume,
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch fundamental data via IB reqFundamentalData for each candidate."""
        # Build symbol → contract lookup from the full contract list
        contract_by_symbol: Dict[str, Any] = {}
        for c in contracts:
            contract_by_symbol[c.symbol] = c

        results: Dict[str, Dict[str, Any]] = {}
        for cand in candidates:
            symbol = cand['ticker']
            contract = contract_by_symbol.get(symbol)
            if not contract:
                continue
            try:
                xml_str = consume(
                    self._rpc.rpc(return_type=str).get_fundamental_data(contract, 'ReportSnapshot')
                )
                data = parse_report_snapshot(xml_str)
                if data:
                    results[symbol] = data
            except Exception:
                logger.debug('Fundamentals fetch failed for %s', symbol)

        return results

    # ------------------------------------------------------------------
    # News (IB reqHistoricalNews)
    # ------------------------------------------------------------------

    def _fetch_news(
        self,
        candidates: List[Dict],
        conid_map: Dict[str, int],
        consume,
    ) -> Dict[str, Dict[str, Optional[str]]]:
        """Fetch latest news headlines via IB reqHistoricalNews for each candidate."""
        results: Dict[str, Dict[str, Optional[str]]] = {}
        for cand in candidates:
            symbol = cand['ticker']
            conId = conid_map.get(symbol)
            if not conId:
                continue
            try:
                headlines = consume(
                    self._rpc.rpc(return_type=list[dict]).get_news_headlines(conId, '', 1)
                )
                if headlines:
                    h = headlines[0]
                    title = h.get('headline', '')
                    # IB headlines include metadata prefix like
                    # {A:800015:L:en}Actual headline text
                    # Strip it to get clean headline
                    if title.startswith('{') and '}' in title:
                        title = title[title.index('}') + 1:]
                    title = title.strip()
                    if len(title) > 120:
                        title = title[:117] + '...'
                    news_date = h.get('time', '')[:10]
                    results[symbol] = {
                        'headline': title,
                        'news_date': news_date,
                    }
            except Exception:
                logger.debug('News fetch failed for %s', symbol)

        return results
