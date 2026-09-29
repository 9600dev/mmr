"""Panel loaders for the low-frequency harness (D03: research DBs only, read-only).

``load_etf_panel`` reads ``etf_daily_bars`` and returns TOTAL-RETURN open/close
frames (``tr_open``/``tr_close``): time x ticker, NaN before a ticker's first
bar, so no ticker is usable before it existed (D04). ``td_adj_close`` is never
read - it is a cross-check column known to be wrong on split funds.

``load_equity_panel`` wraps ``scripts/pit_panel.load_pit_panel`` (point-in-time
membership, delisted names included) and ``scripts/fundamental_signals.build
(pit=True)`` (as-of EDGAR-acceptance fundamentals), both bound to the research
config and forced read-only.
"""
from __future__ import annotations

import datetime as dt
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional, Sequence

import numpy as np
import pandas as pd

from trader.research.lowfreq import config as _cfg

DateLike = "str | dt.date | dt.datetime | pd.Timestamp"


def _ts(d) -> pd.Timestamp:
    return pd.Timestamp(d).normalize().tz_localize(None) if pd.Timestamp(d).tzinfo is None \
        else pd.Timestamp(d).tz_convert(None).normalize()


@dataclass
class PricePanel:
    """Aligned total-return frames, time x ticker. ``fields`` may carry extra
    per-name frames (high/low/volume for ETFs, as-of fundamentals for equities)."""
    open: pd.DataFrame
    close: pd.DataFrame
    fields: Dict[str, pd.DataFrame] = field(default_factory=dict)
    signals: Dict[str, pd.DataFrame] = field(default_factory=dict)
    universe: str = ''

    @property
    def tickers(self) -> list:
        return list(self.close.columns)

    def sliced(self, start, end_exclusive) -> 'PricePanel':
        s, e = _ts(start), _ts(end_exclusive)
        mask = (self.close.index >= s) & (self.close.index < e)
        return PricePanel(
            open=self.open.loc[mask], close=self.close.loc[mask],
            fields={k: v.loc[mask] for k, v in self.fields.items()},
            signals={k: v.loc[mask] for k, v in self.signals.items()},
            universe=self.universe)

    def frames(self) -> Dict[str, pd.DataFrame]:
        """Per-ticker OHLCV frames for ``Backtester.run_panel``. Rows before a
        ticker's first bar are dropped so the panel's NaN is a missing bar, not
        a zero price."""
        out: Dict[str, pd.DataFrame] = {}
        high = self.fields.get('high')
        low = self.fields.get('low')
        vol = self.fields.get('volume')
        for t in self.close.columns:
            close = self.close[t]
            keep = close.notna()
            if not keep.any():
                continue
            df = pd.DataFrame({
                'open': self.open[t][keep].astype(float),
                'high': (high[t][keep] if high is not None else close[keep]).astype(float),
                'low': (low[t][keep] if low is not None else close[keep]).astype(float),
                'close': close[keep].astype(float),
                'volume': (vol[t][keep] if vol is not None else pd.Series(0.0, index=close[keep].index)).astype(float),
            })
            df.index.name = 'date'
            out[t] = df
        return out


def load_etf_panel(tickers: Optional[Sequence[str]] = None, start=None, end=None) -> PricePanel:
    """Total-return ETF panel from ``etf_daily_bars`` in the research history DB.

    ``start``/``end`` are inclusive dates (None = whole table). High/low are
    scaled by the same total-return factor as the close so a strategy reading
    them sees one consistent price basis; raw volume is carried through.
    """
    paths = _cfg.research_db_paths()
    con = _cfg.connect_read_only(paths['history'])
    try:
        where, params = [], []
        if start is not None:
            where.append('day >= ?'); params.append(_ts(start).date())
        if end is not None:
            where.append('day <= ?'); params.append(_ts(end).date())
        if tickers:
            where.append('ticker IN (' + ','.join('?' * len(tickers)) + ')')
            params.extend(list(tickers))
        sql = ('SELECT day, ticker, tr_open, tr_close, high, low, close, volume '
               'FROM etf_daily_bars')
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        df = con.execute(sql + ' ORDER BY day, ticker', params).fetchdf()
    finally:
        con.close()
    if df is None or len(df) == 0:
        raise ValueError('etf_daily_bars returned no rows for the requested window/tickers')
    if tickers:
        missing = sorted(set(tickers) - set(df['ticker'].unique()))
        if missing:
            raise ValueError(f'etf_daily_bars has no rows for {missing}')
    df['day'] = pd.to_datetime(df['day'])
    df = df.dropna(subset=['tr_close', 'tr_open'])
    df = df[(df['tr_close'] > 0) & (df['tr_open'] > 0)]
    factor = df['tr_close'] / df['close']
    df = df.assign(tr_high=df['high'] * factor, tr_low=df['low'] * factor)

    def piv(col):
        return df.pivot_table(index='day', columns='ticker', values=col, aggfunc='last').sort_index()

    close = piv('tr_close')
    cols = list(tickers) if tickers else list(close.columns)
    close = close.reindex(columns=cols)
    opn = piv('tr_open').reindex(index=close.index, columns=cols)
    fields = {k: piv(v).reindex(index=close.index, columns=cols)
              for k, v in (('high', 'tr_high'), ('low', 'tr_low'), ('volume', 'volume'))}
    return PricePanel(open=opn, close=close, fields=fields, universe='etf')


def load_equity_panel(start=None, end=None, top_n: int = 500) -> PricePanel:
    """Point-in-time equity panel plus as-of fundamentals (D05).

    Prices/membership: ``scripts/pit_panel.load_pit_panel(top_n, start)``.
    Fundamentals: ``scripts/fundamental_signals.build(pit=True, with_frames=True)``
    - the six ratio signals (earnings_yield, book_to_price, gross_profitability,
    cash_flow_yield, sales_to_price, low_accruals) as ``signals`` and the raw
    as-of frames (shares_diluted, revenue_ttm, ...) as ``fields``. ``build``
    always evaluates the top-500 universe from 2016-08-01; for another
    ``top_n`` the frames are reindexed onto the requested membership, so names
    outside the top 500 carry NaN fundamentals.
    """
    scripts_dir = str(_cfg.repo_root() / 'scripts')
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    import pit_panel                    # noqa: E402  (scripts/pit_panel.py)
    import fundamental_signals          # noqa: E402  (scripts/fundamental_signals.py)

    start_str = _ts(start).strftime('%Y-%m-%d') if start is not None else '2016-08-01'
    with _cfg.bound_container(), _cfg.read_only_duckdb_store():
        px, signals, frames = fundamental_signals.build(pit=True, with_frames=True)
        if top_n != 500 or start_str != '2016-08-01':
            close, opens = pit_panel.load_pit_panel(top_n, start_str, verbose=False)
        else:
            close, opens = px, None
            _, opens = pit_panel.load_pit_panel(500, start_str, verbose=False)
    if close is None or len(close) == 0:
        raise ValueError('pit_daily_bars returned no rows')
    close = close.copy(); close.index = pd.to_datetime(close.index)
    opens = opens.reindex(index=close.index, columns=close.columns)
    opens.index = close.index
    s = _ts(start) if start is not None else close.index[0]
    e = _ts(end) if end is not None else close.index[-1]
    mask = (close.index >= s) & (close.index <= e)
    close, opens = close.loc[mask], opens.loc[mask]

    def align(fr: pd.DataFrame) -> pd.DataFrame:
        fr = fr.copy(); fr.index = pd.to_datetime(fr.index)
        return fr.reindex(index=close.index, columns=close.columns)

    return PricePanel(open=opens, close=close,
                      fields={k: align(v) for k, v in frames.items()},
                      signals={k: align(v) for k, v in signals.items()},
                      universe='equity')
