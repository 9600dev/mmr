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
import importlib
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Sequence, cast

import numpy as np
import pandas as pd

from trader.research.lowfreq import config as _cfg

def _ts(d: Any) -> pd.Timestamp:
    """A tz-naive midnight Timestamp. Refuses NaT rather than propagating it:
    a window boundary that is not a date would silently select nothing."""
    t = cast(pd.Timestamp, pd.Timestamp(d))
    if pd.isna(t):
        raise ValueError(f'not a date: {d!r}')
    if t.tzinfo is not None:
        t = t.tz_convert(None)
    return t.normalize()


def to_date(d: Any) -> dt.date:
    """Any date-like -> ``datetime.date`` (typed; NaT refused)."""
    return _ts(d).date()


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
    df = df.assign(tr_high=df['high'] * factor, tr_low=df['low'] * factor,
                   dollar_volume=df['volume'] * df['close'])

    def piv(col):
        return df.pivot_table(index='day', columns='ticker', values=col, aggfunc='last').sort_index()

    close = piv('tr_close')
    cols = list(tickers) if tickers else list(close.columns)
    close = close.reindex(columns=cols)
    opn = piv('tr_open').reindex(index=close.index, columns=cols)
    fields = {k: piv(v).reindex(index=close.index, columns=cols)
              for k, v in (('high', 'tr_high'), ('low', 'tr_low'), ('volume', 'volume'),
                           ('dollar_volume', 'dollar_volume'))}
    return PricePanel(open=opn, close=close, fields=fields, universe='etf')


def load_equity_panel(start=None, end=None, top_n: int = 500) -> PricePanel:
    """Point-in-time equity panel plus as-of fundamentals (D05).

    Membership: ``scripts/pit_panel.load_pit_panel(top_n, start)`` (top-N by
    dollar volume, decided monthly with the PRIOR month's rank, delisted names
    included). Fundamentals: ``scripts/fundamental_signals.build(pit=True,
    with_frames=True)`` - the six ratio signals (earnings_yield, book_to_price,
    gross_profitability, cash_flow_yield, sales_to_price, low_accruals) as
    ``signals`` and the raw as-of frames (shares_diluted, revenue_ttm, ...) as
    ``fields``. ``build`` always evaluates the top-500 universe from 2016-08-01;
    for another ``top_n`` the frames are reindexed onto the requested
    membership, so names outside the top 500 carry NaN fundamentals.

    PRICES PERSIST AFTER A NAME LEAVES MEMBERSHIP. ``close``/``open`` carry the
    stored price of every name that was ever a member in the window for as long
    as ``pit_daily_bars`` stores it (top 800 by dollar volume), and
    ``fields['member']`` is the point-in-time membership mask. A strategy may
    only OPEN a position in a member (weight > 0 while ``member`` is True); the
    harness refuses a BUY fill in a non-member. Why: a membership-filtered
    price panel made a name's series vanish the month it left the top 500, so
    its sell could never fill - in the covered-EW benchmark 511 of 857 holdings
    were frozen at stale prices worth 102% of reported equity, with cash at
    -$3.2M on 99% of fill dates (2026-09-29 diagnostic). Names that also fall
    below the storage cutoff or delist still freeze at their last price; the
    harness reports that residual (``frozen_value_fraction_end``) rather than
    hiding it.
    """
    scripts_dir = str(_cfg.repo_root() / 'scripts')
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    pit_panel = importlib.import_module('pit_panel')                    # scripts/pit_panel.py
    fundamental_signals = importlib.import_module('fundamental_signals')  # scripts/fundamental_signals.py

    # Membership for a month is decided from the PRIOR month's ranks, so the
    # point-in-time loader must see ~45 days before the requested start or the
    # first requested month comes back with no members (the window silently
    # began a month late). Load early, slice to the inclusive window below.
    s_req = _ts(start) if start is not None else pd.Timestamp('2016-08-01')
    pit_start = (s_req - pd.Timedelta(days=PIT_MEMBERSHIP_LEAD_DAYS)).strftime('%Y-%m-%d')
    with _cfg.bound_container(), _cfg.read_only_duckdb_store():
        px, signals, frames = fundamental_signals.build(pit=True, with_frames=True)
        member_close, _ = pit_panel.load_pit_panel(top_n, pit_start, verbose=False)
    if member_close is None or len(member_close) == 0:
        raise ValueError('pit_daily_bars returned no rows')
    member_close = member_close.copy()
    member_close.index = pd.to_datetime(member_close.index)
    s = _ts(start) if start is not None else member_close.index[0]
    e = _ts(end) if end is not None else member_close.index[-1]
    member_close = member_close.loc[(member_close.index >= s) & (member_close.index <= e)]
    member_close = member_close.loc[:, member_close.notna().any(axis=0)]   # ever a member in the window
    if member_close.shape[1] == 0:
        raise ValueError(f'no point-in-time members between {s.date()} and {e.date()}')
    tickers = list(member_close.columns)
    full = _pit_price_frames(s, e)

    def align(fr: pd.DataFrame) -> pd.DataFrame:
        fr = fr.copy()
        fr.index = pd.to_datetime(fr.index)
        return fr.reindex(index=member_close.index, columns=tickers)

    close = align(full['close'])
    opens = align(full['open'])
    # A member day must have a stored price; the mask can never exceed the data.
    member = member_close.notna() & close.notna()
    # EXIT AT THE LAST STORED PRICE. When a name stops printing (fell below the
    # storage cutoff, or delisted) its last close stands for up to EXIT_GRACE_BARS
    # more sessions - as the mark AND as the fill price - so the sell a strategy
    # issues at its next rebalance can fill, instead of the position sitting in
    # the book forever at a stale mark that inflates equity and sizes new buys
    # (implicit leverage). Same assumption scripts/size_tilt_backtest.py made;
    # for a name that actually went to zero it is optimistic, which is why the
    # harness also reports the frozen fraction that remains.
    stale = close.isna() & close.ffill(limit=EXIT_GRACE_BARS).notna()
    close = close.ffill(limit=EXIT_GRACE_BARS)
    opens = opens.where(~stale, close)
    fields = {k: align(v) for k, v in frames.items()}
    fields['member'] = member
    fields['dollar_volume'] = align(full['dollar_volume'])
    return PricePanel(open=opens, close=close, fields=fields,
                      signals={k: align(v) for k, v in signals.items()},
                      universe='equity')


EXIT_GRACE_BARS = 30      # > one month of sessions: a monthly rebalance always falls inside
PIT_MEMBERSHIP_LEAD_DAYS = 45   # prior-month ranks needed to decide the first requested month


def _pit_price_frames(start: pd.Timestamp, end: pd.Timestamp) -> Dict[str, pd.DataFrame]:
    """close / open / dollar_volume of every stored PIT name, read-only, with the
    same sanity filters ``pit_panel.load_pit_panel`` applies (exchange TEST
    tickers dropped, absurd prices blanked)."""
    from trader.data.bar_quality import EXCHANGE_TEST_TICKERS
    con = _cfg.connect_read_only(_cfg.research_db_paths()['history'])
    try:
        df = con.execute('SELECT day, ticker, open, close, dollar_volume FROM pit_daily_bars '
                         'WHERE day >= ? AND day <= ?', [start.date(), end.date()]).fetchdf()
    finally:
        con.close()
    if df is None or len(df) == 0:
        raise ValueError(f'pit_daily_bars has no rows between {start.date()} and {end.date()}')
    df = df[~df['ticker'].isin(EXCHANGE_TEST_TICKERS)]
    bad = ~((df['close'] > 0.01) & (df['close'] < 100_000))
    df.loc[bad, ['open', 'close']] = np.nan
    df['day'] = pd.to_datetime(df['day'])
    return {k: df.pivot_table(index='day', columns='ticker', values=k, aggfunc='last').sort_index()
            for k in ('close', 'open', 'dollar_volume')}
