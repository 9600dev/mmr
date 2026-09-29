#!/usr/bin/env python3
"""Fetch a cross-asset ETF daily panel with TOTAL-RETURN prices (research only).

WHY
    Every low-frequency result so far was measured on single stocks. A
    cross-asset book (bonds, commodities, international, sectors) is genuinely
    different information, but only if its prices are TOTAL return: TLT pays
    ~3-4%/yr in distributions, so a price-only series understates a bond
    sleeve by that much every year and makes any trend/allocation result that
    holds bonds wrong in the most important direction.

WHAT IT WRITES
    Table ``etf_daily_bars`` in the HISTORY DuckDB named by ``--db`` (never the
    live container volume; the ShadowedDatabase guard still applies):

        day, ticker, open, high, low, close   raw split-adjusted OHLC (Massive)
        volume, dividend                       cash on the ex-date, split-adjusted
        tr_close, tr_open                      total-return index / matching open
        td_adj_close                           TwelveData adjust=all close (cross-check)

    ``tr_close`` is built forward: TR_t = TR_{t-1} * (close_t + div_t) / close_{t-1},
    starting at the first close. ``tr_open = open * tr_close / close`` so an
    open-fill backtest sees the same adjustment as the close.

WHY THE PANEL STARTS IN 2007
    Measured 2026-09-28: Massive's dividend endpoint is complete (12/yr for
    the monthly payers) from about 2007-04 on and EMPTY before it for the
    bond funds, and TwelveData's adjust=all silently drops distributions (its
    adjusted return equals the raw price return on many ex-dates). Inferring
    the missing ones from the TwelveData/Massive difference was tried and
    rejected: it "found" 22 distributions for GLD, which pays none — close-
    convention noise, not dividends. So the TR index uses Massive cash
    dividends only, over the span where they are complete.

CROSS-CHECK
    Daily TR returns from Massive are compared with TwelveData's dividend-and-
    split-adjusted closes over their common dates. The per-ticker agreement
    (share of days within 5bps, CAGR gap) is printed and written to
    ``--report``; a ticker whose two sources disagree by more than 0.5%/yr in
    CAGR is FLAGGED, not silently kept.

    What the flags meant when first run (2026-09-28, 49 tickers, 12 flagged):
    every flag was TwelveData's error, not Massive's. (a) Funds that split
    2:1 on 2025-12-05 (XLK XLE XLY XLB XLU): TwelveData runs 1-3.4%/yr HIGH
    on every pre-split ex-date and agrees exactly after the split — it divides
    the raw dividend by the split-adjusted price, double-counting the yield.
    (b) TLT IEF IEI EMB EWG: TwelveData runs 2-4%/yr LOW before ~2013 and
    agrees after — its early distribution history is missing. Massive's cash
    amounts are raw (XLU 2025-09-22 paid 0.568 at a raw close of 85.70) and
    are scaled by later splits here. A future flag should be read the same
    way: find the year the gap lives in before trusting either side.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402
import requests      # noqa: E402

DEFAULT_TICKERS = (
    # US equity
    'SPY QQQ IWM MDY DIA VTI '
    # sectors
    'XLK XLF XLE XLV XLY XLP XLI XLB XLU XLRE XLC '
    # international equity
    'EFA EEM VGK EWJ FXI EWZ EWC EWA EWG EWU '
    # rates / credit
    'SHY IEI IEF TLT TIP LQD HYG AGG EMB BIL MUB '
    # real assets
    'GLD SLV DBC USO UNG VNQ GDX DBA '
    # currency
    'UUP FXE FXY'
).split()


def _keys():
    from trader.config import MMRConfig
    from trader.container import default_config_path
    cfg = MMRConfig.from_yaml(os.environ.get('TRADER_CONFIG') or default_config_path())
    return cfg.massive.api_key, cfg.twelvedata.api_key


def fetch_massive(client, ticker: str, start: str, end: str) -> pd.DataFrame:
    aggs = list(client.list_aggs(ticker, 1, 'day', start, end, limit=50000, adjusted=True))
    rows = []
    for a in aggs:
        if a.timestamp is None:
            continue
        # daily aggregates are stamped at 00:00 America/New_York in UTC ms
        day = (pd.Timestamp(a.timestamp, unit='ms', tz='UTC')
               .tz_convert('America/New_York').date())
        rows.append({'day': day, 'open': a.open, 'high': a.high, 'low': a.low,
                     'close': a.close, 'volume': a.volume})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates('day', keep='last').set_index('day').sort_index()

    # Splits: Massive's adjusted=True closes are split-adjusted to TODAY, but
    # dividend cash amounts are as paid. Scale each dividend by the product of
    # every split ratio AFTER its ex-date so it is in the same units.
    splits = []
    try:
        for s in client.list_splits(ticker=ticker, limit=1000):
            if s.execution_date and s.split_from and s.split_to:
                splits.append((dt.date.fromisoformat(s.execution_date),
                               float(s.split_from) / float(s.split_to)))
    except Exception as ex:  # noqa: BLE001 — reported, not swallowed
        print(f'  {ticker}: splits unavailable ({ex}); dividends unscaled', flush=True)
    div = pd.Series(0.0, index=df.index)
    for d in client.list_dividends(ticker=ticker, limit=1000):
        if not d.ex_dividend_date or d.cash_amount is None:
            continue
        ex = dt.date.fromisoformat(d.ex_dividend_date)
        if ex not in div.index:
            continue
        factor = 1.0
        for sd, ratio in splits:
            if sd > ex:
                factor *= ratio
        div[ex] += float(d.cash_amount) * factor
    df['dividend'] = div
    return build_tr(df)


def build_tr(df: pd.DataFrame) -> pd.DataFrame:
    tr = [float(df['close'].iloc[0])]
    closes = df['close'].to_numpy(dtype=float)
    divs = df['dividend'].to_numpy(dtype=float)
    for i in range(1, len(df)):
        tr.append(tr[-1] * (closes[i] + divs[i]) / closes[i - 1])
    df['tr_close'] = tr
    df['tr_open'] = df['open'] * df['tr_close'] / df['close']
    return df


def fetch_twelvedata(key: str, ticker: str, start: str) -> pd.Series:
    """adjust=all closes, paged backwards (5000-row cap per call)."""
    out = {}
    end = None
    for _ in range(10):
        params = dict(symbol=ticker, interval='1day', start_date=start,
                      outputsize=5000, apikey=key, adjust='all', order='DESC')
        if end:
            params['end_date'] = end
        j = requests.get('https://api.twelvedata.com/time_series', params=params,
                         timeout=60).json()
        if j.get('status') != 'ok':
            if out:
                break
            print(f'  {ticker}: twelvedata {j.get("message", j)}', flush=True)
            return pd.Series(dtype=float)
        vals = j.get('values', [])
        for v in vals:
            out[dt.date.fromisoformat(v['datetime'][:10])] = float(v['close'])
        if len(vals) < 5000:
            break
        end = (min(out) - dt.timedelta(days=1)).isoformat()
        time.sleep(0.5)
    return pd.Series(out).sort_index()


def _cadence(df: pd.DataFrame) -> dict:
    """Distributions per calendar year — a gap in a monthly payer shows here."""
    paid = df[df['dividend'] > 0]
    return {str(y): int(n) for y, n in
            pd.Series(1, index=pd.to_datetime(paid.index)).groupby(lambda d: d.year).sum().items()}


def cross_check(df: pd.DataFrame, td: pd.Series) -> dict:
    common = df.index.intersection(td.index)
    if len(common) < 250:
        return {'common_days': len(common), 'flag': 'insufficient overlap'}
    a = df.loc[common, 'tr_close'].pct_change().dropna()
    b = td.loc[common].pct_change().dropna()
    diff = (a - b).abs()
    yrs = len(common) / 252.0
    ca = (df.loc[common[-1], 'tr_close'] / df.loc[common[0], 'tr_close']) ** (1 / yrs) - 1
    cb = (td.loc[common[-1]] / td.loc[common[0]]) ** (1 / yrs) - 1
    gap = float(ca - cb)
    return {'common_days': len(common),
            'share_within_5bps': float((diff <= 0.0005).mean()),
            'max_abs_daily_diff': float(diff.max()),
            'cagr_massive_tr': float(ca), 'cagr_twelvedata_adj': float(cb),
            'cagr_gap': gap,
            'flag': 'CAGR gap > 0.5%/yr' if abs(gap) > 0.005 else None}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--db', required=True, help='history DuckDB to write (research copy)')
    ap.add_argument('--tickers', nargs='*', default=list(DEFAULT_TICKERS))
    ap.add_argument('--start', default='2007-06-01')
    ap.add_argument('--end', default=dt.date.today().isoformat())
    ap.add_argument('--report', default='')
    args = ap.parse_args()

    from massive import RESTClient
    from trader.data.duckdb_store import DuckDBConnection
    mkey, tkey = _keys()
    client = RESTClient(api_key=mkey)
    db = DuckDBConnection(os.path.expanduser(args.db))
    db.execute("""CREATE TABLE IF NOT EXISTS etf_daily_bars (
        day DATE NOT NULL, ticker VARCHAR NOT NULL,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume DOUBLE,
        dividend DOUBLE,
        tr_close DOUBLE, tr_open DOUBLE, td_adj_close DOUBLE,
        PRIMARY KEY (day, ticker))""")

    report = {}
    for t in args.tickers:
        df = fetch_massive(client, t, args.start, args.end)
        if df.empty:
            print(f'{t}: NO DATA from Massive', flush=True)
            report[t] = {'flag': 'no data'}
            continue
        td = fetch_twelvedata(tkey, t, args.start)
        df['td_adj_close'] = td.reindex(df.index)
        chk = cross_check(df, td) if not td.empty else {'flag': 'no twelvedata'}
        chk.update({'first_day': str(df.index[0]), 'last_day': str(df.index[-1]),
                    'rows': len(df), 'n_dividends': int((df['dividend'] > 0).sum()),
                    'dividends_per_year': _cadence(df)})
        report[t] = chk
        out = df.reset_index()
        out['ticker'] = t
        cols = ['day', 'ticker', 'open', 'high', 'low', 'close', 'volume',
                'dividend', 'tr_close', 'tr_open', 'td_adj_close']
        frame = out[cols]

        def _write(conn, frame=frame, t=t):
            conn.execute('DELETE FROM etf_daily_bars WHERE ticker = ?', [t])
            conn.register('etf_frame', frame)
            conn.execute(f'INSERT INTO etf_daily_bars SELECT {", ".join(cols)} FROM etf_frame')
            conn.unregister('etf_frame')
        db.execute_atomic(_write)
        print(f'{t}: {len(df)} rows {df.index[0]}..{df.index[-1]} divs={chk["n_dividends"]} '
              f'within5bps={chk.get("share_within_5bps", float("nan")):.3f} '
              f'cagr_gap={chk.get("cagr_gap", float("nan")):+.4f} {chk.get("flag") or ""}',
              flush=True)
        time.sleep(0.3)
    if args.report:
        with open(os.path.expanduser(args.report), 'w') as f:
            json.dump(report, f, indent=2, default=str)
    flagged = [t for t, v in report.items() if v.get('flag')]
    print(f'\n{len(report)} tickers, flagged: {flagged}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
