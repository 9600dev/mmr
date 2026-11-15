#!/usr/bin/env python3
"""Build fundamental signals as-of each date, and measure whether they predict.

THE AS-OF RULE
    At each date D, a name's fundamentals are the most recent filing ACCEPTED
    before D's close (16:00 ET). Not the most recent period - the most recently
    KNOWN one. 58.5% of filings in this store land at or after 16:00 ET, so
    more than half are unavailable on their own filing date, and a date-only
    source would trade them a day early.

THE LEAK CHECK
    Fundamentals are where a positive result is most likely to be fake, because
    the lookahead is subtle and produces exactly the clean strong signal that is
    easy to believe. So every signal is also measured with acceptance instants
    shifted FORWARD by a week: a real signal degrades under that shift, a
    leaked one barely moves, because a leak does not depend on when the news
    actually arrived.

WHAT THE LEAK CHECK CANNOT SEE (2026-11-15)
    A delay probe moves WHEN a number is known. It says nothing about whether
    the number is right, or on the same basis as the price beside it. Four
    defects in the stored rows passed it for three months; `prepare_rows`
    repairs them and docs/RESEARCH_LOG.md (2026-11-15) has the measurements:
      notices        rows dated to an NT 10-K / NT 10-Q (a notice that the
                     report will be late) put the numbers before they were filed
      annual_shares  the retired vendor's fiscal-Q4 "share count" is
                     annual average - mean(Q1..Q3): not a share count
      split_basis    as-reported shares x split-ADJUSTED prices understate the
                     market cap by every later split - lookahead toward the
                     names that went on to split
      units          ~1% of quarterly share counts are off by a power of 1000
      periods        a trailing-twelve-month sum over "the last four rows",
                     which are not four consecutive quarters when one is
                     missing or one was restated
    `MMR_FUNDAMENTALS_CORRECTIONS` (comma-separated, or `none`) selects a
    subset - the decomposition instrument for those measurements, not a
    setting: everything else runs with all of them.
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np      # noqa: E402
import pandas as pd     # noqa: E402


def session_close_utc(days) -> pd.DatetimeIndex:
    """The NYSE close of each of ``days``, as UTC instants.

    Read from the exchange calendar (pandas_market_calendars XNYS), so it is
    daylight-saving aware (20:00 UTC in summer, 21:00 in winter) AND honours
    early closes (13:00 New York on the day after Thanksgiving, Christmas
    Eve, ...). A day the calendar has no session for (synthetic bars) gets
    16:00 America/New_York. This is the instant a filing must have been
    ACCEPTED strictly before to be usable at that session's close
    (``searchsorted`` with ``side='left'`` below).
    """
    import pandas_market_calendars as mcal
    d = pd.DatetimeIndex(pd.to_datetime(days))
    if d.tz is not None:
        d = d.tz_convert(None)
    d = d.normalize()
    regular = pd.Series((d + pd.Timedelta(hours=16)).tz_localize('America/New_York').tz_convert('UTC'), index=d)
    if len(d):
        sched = mcal.get_calendar('XNYS').schedule(d.min().strftime('%Y-%m-%d'), d.max().strftime('%Y-%m-%d'))
        closes = pd.Series(pd.DatetimeIndex(sched['market_close']).tz_convert('UTC'),
                           index=pd.DatetimeIndex(sched.index).normalize())
        common = d.intersection(closes.index)
        if len(common):
            regular.loc[common] = closes.loc[common].to_numpy()
    return pd.DatetimeIndex(regular)


CORRECTIONS = ('notices', 'annual_shares', 'split_basis', 'units', 'periods')
# Consecutive fiscal quarters end this many days apart: 13 weeks for most, 12
# or 16 for a filer on a 16/12/12/12-week year (KR), 14 in a 53-week year.
QUARTER_MIN_DAYS = 70
QUARTER_MAX_DAYS = 120
FLOW_FIELDS = ('revenue', 'net_income', 'gross_profit', 'cash_from_operations')


def enabled_corrections() -> frozenset:
    """Every correction, unless ``MMR_FUNDAMENTALS_CORRECTIONS`` names a subset
    (``none`` for the rows exactly as stored). An unknown name raises: a typo
    must not quietly measure something else."""
    raw = os.environ.get('MMR_FUNDAMENTALS_CORRECTIONS')
    if raw is None:
        return frozenset(CORRECTIONS)
    names = {part.strip() for part in raw.split(',') if part.strip()}
    unknown = names - set(CORRECTIONS) - {'none'}
    if unknown:
        raise ValueError(f'MMR_FUNDAMENTALS_CORRECTIONS names unknown correction(s) {sorted(unknown)}; '
                         f'known: {", ".join(CORRECTIONS)}, none')
    return frozenset(names - {'none'})


def prepare_rows(rows: pd.DataFrame, key: str, *, events=None, through=None,
                 unreflected=frozenset(), corrections=frozenset(CORRECTIONS)) -> pd.DataFrame:
    """The stored fundamentals rows, made fit to build as-of frames from.

    ``rows`` needs ``key``, ``ticker``, ``period_end``, ``known_at``,
    ``form_type``, ``shares_diluted`` and the flow fields; an optional
    ``source`` column says which vendor endpoint a row came from (absent or
    NULL = the retired one). ``events`` is ``share_basis.split_events(...)``,
    ``through`` the last day the prices are adjusted to, ``unreflected`` the
    ``(ticker, execution_date)`` splits the stored prices do NOT carry.
    Returns the rows sorted by ``(key, known_at)`` with ``shares_diluted``
    replaced by the share count usable at that row and a ``<flow>_ttm``
    column per flow field. See the module docstring for what each correction
    is and ``trader/data/share_basis.py`` for why.
    """
    from trader.data import share_basis

    rows = rows.copy()
    if 'notices' in corrections:
        rows = rows[~rows['form_type'].map(share_basis.is_late_filing_notice)]
    rows = rows.sort_values([key, 'known_at']).reset_index(drop=True)
    shares = pd.to_numeric(rows['shares_diluted'], errors='coerce').astype(float)

    if 'annual_shares' in corrections:
        legacy = (rows['source'].isna() | (rows['source'].astype(str).str.strip() == '')
                  if 'source' in rows.columns else pd.Series(True, index=rows.index))
        shares = shares.mask(rows['form_type'].map(share_basis.is_annual_report) & legacy)

    if 'split_basis' in corrections:
        if events is None:
            raise RuntimeError(
                'share counts cannot be put on the price basis without the split history: the '
                "'corporate_splits' table is missing from the history database. Run "
                'scripts/fetch_splits.py, or name the corrections to apply in '
                'MMR_FUNDAMENTALS_CORRECTIONS (see the module docstring).')
        # The day the stored count is on the basis of. A legacy row: the
        # FILING day (not the possibly delayed day it is treated as known). A
        # row from the replacement endpoint: the day it was FETCHED, because
        # that endpoint serves counts already adjusted to the day it is asked.
        accepted = rows['accepted_at'] if 'accepted_at' in rows.columns else rows['known_at']
        filed = pd.to_datetime(accepted, utc=True).dt.tz_convert('America/New_York').dt.date
        if 'source' in rows.columns and 'fetched_at' in rows.columns:
            from trader.data.statements_v1 import SOURCE as _V1
            fetched = pd.to_datetime(rows['fetched_at'], utc=True).dt.tz_convert('America/New_York').dt.date
            current = (rows['source'].astype(str) == _V1) & pd.notna(rows['fetched_at'])
            filed = filed.where(~current, fetched)
        factors = []
        for ticker, day in zip(rows['ticker'].astype(str), filed):
            carried = [(d, r) for d, r in events.get(ticker, ()) if (ticker, d) not in unreflected]
            factors.append(share_basis.split_factor_after(carried, day, through) if carried else 1.0)
        shares = shares * pd.Series(factors, index=rows.index, dtype=float)

    if 'units' in corrections:
        cleaned = shares.copy()
        for _, index in rows.groupby(key, sort=False).groups.items():
            values = share_basis.plausible_share_counts(
                [None if pd.isna(v) else float(v) for v in shares.loc[index]])
            cleaned.loc[index] = [float('nan') if v is None else v for v in values]
        shares = cleaned

    rows['shares_diluted'] = shares
    if corrections & {'annual_shares', 'units'}:
        # The count usable at a row is the last one believed: a 10-K carries
        # the preceding 10-Q's, a rejected figure the one before it.
        rows['shares_diluted'] = rows.groupby(key, sort=False)['shares_diluted'].ffill()

    # Trailing-twelve-month sums for FLOW items; balance-sheet items are
    # point-in-time and must not be summed.
    if 'periods' in corrections:
        for f, sums in _trailing_sums(rows, key).items():
            rows[f + '_ttm'] = sums
    else:
        for f in FLOW_FIELDS:
            rows[f + '_ttm'] = (rows.groupby(key, sort=False)[f]
                                .rolling(4, min_periods=4).sum()
                                .reset_index(level=0, drop=True))
    return rows


def _trailing_sums(rows: pd.DataFrame, key: str) -> dict:
    """``{flow: array}``: at each row, the sum of the flow over the four most
    recent DISTINCT fiscal quarters known by then - each at its latest known
    version - or NaN unless those four are consecutive quarters.

    "The last four rows" is not that. A quarter the store never got makes
    four rows span fifteen months, and a restatement stored beside the
    original counts one quarter twice. ``rows`` must be in ``(key, known_at)``
    order with a fresh positional index.
    """
    ends = pd.to_datetime(rows['period_end']).to_numpy(dtype='datetime64[D]')
    values = {f: pd.to_numeric(rows[f], errors='coerce').to_numpy(dtype=float) for f in FLOW_FIELDS}
    out = {f: np.full(len(rows), np.nan) for f in FLOW_FIELDS}
    for positions in rows.groupby(key, sort=False).indices.values():
        latest = {}                                   # period end -> position of its newest version
        for pos in positions:
            latest[ends[pos]] = pos
            recent = sorted(latest)[-4:]
            if len(recent) < 4:
                continue
            steps = np.diff(np.array(recent)).astype(int)
            if not all(QUARTER_MIN_DAYS <= step <= QUARTER_MAX_DAYS for step in steps):
                continue
            for f in FLOW_FIELDS:
                out[f][pos] = sum(values[f][latest[end]] for end in recent)
    return out


def price_basis_day(hist_db):
    """The day the stored point-in-time prices are split-adjusted THROUGH, or
    None when the store does not say.

    ``pit_daily_bars`` rows are adjusted as of the day they were fetched, and
    that day is not the last stored day: a research snapshot cut at
    2024-06-28 still carries prices fetched - and adjusted - in July 2026.
    ``pit_price_basis`` records it (written by the build, and by
    ``scripts/pit_split_adjust.py`` each time it makes the table uniform).
    """
    try:
        row = hist_db.execute('SELECT max(adjusted_through) FROM pit_price_basis', fetch='one')
    except Exception as ex:
        if 'pit_price_basis' in str(ex):
            return None
        raise
    day = row[0] if row else None
    if day is None:
        return None
    return day.date() if hasattr(day, 'date') else day


def refused_tickers(hist_db) -> frozenset:
    """Tickers whose stored prices ``scripts/pit_split_adjust.py`` could not
    put on the common split basis (``pit_split_refusals``): prices that carry
    a split nobody lists any more, a restructuring that is not a split. Their
    price is on an unknown basis, so no share factor can make it a market cap.
    Empty when the table does not exist (nothing has been refused)."""
    try:
        rows = hist_db.execute('SELECT DISTINCT ticker FROM pit_split_refusals', fetch='all')
    except Exception as ex:
        if 'pit_split_refusals' in str(ex):
            return frozenset()
        raise
    return frozenset(str(row[0]) for row in rows or [])


def without_refused(frame: pd.DataFrame, refused) -> pd.DataFrame:
    """``frame`` with the columns of refused tickers set to NaN (unknown), so
    they drop out of every ratio and of coverage rather than being ranked on a
    market cap that is not one."""
    columns = [c for c in frame.columns if str(c) in refused]
    if not columns:
        return frame
    out = frame.copy()
    out[columns] = np.nan
    return out


def _split_inputs(hist_db, tickers, pit: bool, px: pd.DataFrame, key: str, rows: pd.DataFrame):
    """``(events, through, unreflected)`` for ``prepare_rows`` — or
    ``(None, None, frozenset())`` when there is no ``corporate_splits`` table.

    ``through`` is the day the prices are adjusted to. For the point-in-time
    panel the store must SAY (``price_basis_day``): it is not the last stored
    day - a snapshot cut in 2024 carries prices adjusted in 2026 - and guessing
    it leaves out every split in between, which is most of the correction,
    silently. A store that does not say is refused. The present-day panel is
    re-fetched whole and is adjusted to its own last day.

    A split is ``unreflected`` when the stored prices show the raw jump on its
    execution day (the vendor's adjusted series missed it): multiplying the
    share count by it would then adjust one side only. Unknown counts as
    reflected, which is what an adjusted series is 98% of the time.
    """
    from trader.data import share_basis

    try:
        frame = hist_db.execute(
            'SELECT ticker, execution_date, split_from, split_to FROM corporate_splits', fetch='df')
    except Exception as ex:
        if 'corporate_splits' in str(ex):
            return None, None, frozenset()
        raise
    wanted = set(tickers)
    events = {t: ev for t, ev in share_basis.split_events(
        (frame if frame is not None else pd.DataFrame()).to_dict('records')).items() if t in wanted}
    days = pd.DatetimeIndex(pd.to_datetime(px.index))
    last = days.max().date() if len(days) else None
    first = days.min().date() if len(days) else None
    if pit:
        through = price_basis_day(hist_db)
        if through is None:
            raise RuntimeError(
                'the history database does not say what day pit_daily_bars is split-adjusted through '
                "(table 'pit_price_basis' is missing or empty), so share counts cannot be put on the "
                'price basis. scripts/pit_split_adjust.py --apply records it; for a frozen snapshot, '
                'record the day its prices were fetched.')
    else:
        through = last

    closes = {}
    if pit and events:
        # The panel carries a price only on member days; the check needs the
        # stored series itself.
        bars = hist_db.execute_atomic(lambda conn: conn.execute(
            'SELECT ticker, day, close FROM pit_daily_bars WHERE ticker IN (SELECT unnest(?)) ORDER BY ticker, day',
            [sorted(events)]).fetchdf())
        for ticker, grp in bars.groupby('ticker'):
            closes[ticker] = pd.Series(grp['close'].to_numpy(dtype=float),
                                       index=pd.DatetimeIndex(pd.to_datetime(grp['day'])))
    elif events:
        by_ticker = dict(zip(rows['ticker'].astype(str), rows[key].astype(str)))
        for ticker in events:
            column = by_ticker.get(ticker)
            if column in px.columns:
                series = px[column].dropna()
                closes[ticker] = pd.Series(series.to_numpy(dtype=float),
                                           index=pd.DatetimeIndex(pd.to_datetime(series.index)).tz_localize(None))

    unreflected = set()
    for ticker, ticker_events in events.items():
        series = closes.get(ticker)
        if series is None or not len(series):
            continue
        for day, ratio in ticker_events:
            if first is None or last is None or not (first < day <= last):
                continue                      # no stored price on both sides: cannot be told
            stamp = pd.Timestamp(day)
            earlier = series[series.index < stamp]
            on = series[series.index == stamp]
            if not len(earlier) or not len(on):
                continue
            if share_basis.split_reflected_in_prices(float(earlier.iloc[-1]), float(on.iloc[0]), ratio) is False:
                unreflected.add((ticker, day))
    return events, through, frozenset(unreflected)


def build(delay_days: int = 0, pit: bool = False, with_frames: bool = False):
    """(price panel, {signal name: as-of frame}) with acceptance delayed by
    ``delay_days`` - 0 is the honest read, positive is the leak probe.

    ``pit=True`` swaps the present-day conid-keyed universe for the
    point-in-time panel (`pit_panel.load_pit_panel`) and joins fundamentals by
    TICKER, because delisted members have no conId and never will. This is the
    re-run the 2026-07-29 finding called for: "the size effect here is
    survivorship" was measured on a universe of names that grew into today's
    liquid list, so the honest universe is the only place the question can be
    answered. Known residual: a ticker recycled by a later listing merges two
    companies' filings into one column — the same residual pit_daily_bars
    carries, accepted rather than hidden.
    """
    from trader.container import Container
    from trader.data.duckdb_store import DuckDBConnection

    if pit:
        from pit_panel import load_pit_panel
        px, _ = load_pit_panel(500, verbose=False)
        key = 'ticker'
    else:
        from signal_scan import load_panel
        px = load_panel(500)
        key = 'conid'
    cfg = Container.instance().config()
    db = DuckDBConnection(cfg.get('duckdb_path', ''))
    # SELECT *: a `source` column exists only in a store written since the
    # statements endpoint was replaced; its absence means every row is legacy.
    rows = db.execute('SELECT * FROM fundamentals ORDER BY conid, accepted_at', fetch='df')
    if rows is None or len(rows) == 0:
        return px, {}
    rows = rows[rows[key].astype(str) != '']

    rows['known_at'] = (pd.to_datetime(rows['accepted_at'], utc=True)
                        + pd.Timedelta(days=delay_days))
    corrections = enabled_corrections()
    events, through, unreflected = None, None, frozenset()
    refused = frozenset()
    if 'split_basis' in corrections:
        hist_db = DuckDBConnection(cfg.get('history_duckdb_path', '') or cfg.get('duckdb_path', ''))
        events, through, unreflected = _split_inputs(
            hist_db, set(rows['ticker'].astype(str)), pit, px, key, rows)
        if pit:
            refused = refused_tickers(hist_db)
    rows = prepare_rows(rows, key, events=events, through=through,
                        unreflected=unreflected, corrections=corrections)

    # hillclimb(C01): changed because 16:00 ET is 20:00 UTC under EDT and 21:00
    # UTC under EST; a fixed 21:00 UTC treated a filing accepted at 20:30 UTC on
    # a summer day as known at that day's close, one session early.
    idx = session_close_utc(px.index)
    out = {}
    fields = ['net_income_ttm', 'stockholders_equity', 'gross_profit_ttm',
              'total_assets', 'cash_from_operations_ttm', 'shares_diluted',
              'revenue_ttm']
    frames = {f: pd.DataFrame(index=px.index, columns=px.columns, dtype=float)
              for f in fields}
    for keyval, grp in rows.groupby(key):
        col = str(keyval)
        if col not in px.columns:
            continue
        g = grp.dropna(subset=['known_at']).sort_values('known_at')
        # searchsorted with side='left' gives the count of filings STRICTLY
        # before each date, which is the as-of rule: a filing accepted at the
        # instant asked about was not actionable at it.
        pos = np.searchsorted(g['known_at'].values, idx.values, side='left') - 1
        valid = pos >= 0
        for f in fields:
            vals = np.full(len(idx), np.nan)
            vals[valid] = g[f].to_numpy()[pos[valid]]
            frames[f][col] = vals

    # A ticker whose prices could not be put on the common split basis has no
    # market cap here, whatever its share count says.
    frames['shares_diluted'] = without_refused(frames['shares_diluted'], refused)
    mcap = px * frames['shares_diluted']
    out['earnings_yield'] = frames['net_income_ttm'] / mcap
    out['book_to_price'] = frames['stockholders_equity'] / mcap
    out['gross_profitability'] = frames['gross_profit_ttm'] / frames['total_assets']
    out['cash_flow_yield'] = frames['cash_from_operations_ttm'] / mcap
    out['sales_to_price'] = frames['revenue_ttm'] / mcap
    # Accruals: earnings not backed by cash. Negated so high = attractive,
    # matching every other signal's orientation.
    out['low_accruals'] = -((frames['net_income_ttm']
                             - frames['cash_from_operations_ttm'])
                            / frames['total_assets'])
    for k in out:
        out[k] = out[k].replace([np.inf, -np.inf], np.nan)
    if with_frames:
        return px, out, frames
    return px, out


def _freeze_per_name(frame):
    """Each column becomes its FIRST observed value, held wherever the
    original was observed — the same knowledge mask, the information removed.
    This is the decomposition instrument from the 2026-07-29 standing rule: if
    a ratio does not beat its own frozen-numerator variant, the numerator was
    subtracting, and what you measured is the denominator."""
    first = frame.apply(lambda col: col.dropna().iloc[0]
                        if col.notna().any() else np.nan)
    # NaN * 0 stays NaN, an observed cell becomes 0 + its column's constant —
    # the knowledge mask survives, the variation does not.
    return frame * 0 + first


def run_decomposition(pit: bool, horizons):
    """sales_to_price against its own parts, per the standing rule: freeze
    the numerator, freeze the denominator's slow half (shares), and score
    1/price and 1/marketcap alone — every variant masked to the ratio's OWN
    observed cells so all rows score the same observations."""
    from trader.simulation.signal_eval import evaluate

    px, sigs, frames = build(0, pit=pit, with_frames=True)
    mcap = px * frames['shares_diluted']
    ratio = sigs['sales_to_price']
    mask = ratio.notna()

    variants = {
        'sales_to_price (as measured)': ratio,
        'revenue FROZEN per name': _freeze_per_name(frames['revenue_ttm']) / mcap,
        'shares FROZEN per name': frames['revenue_ttm'] / (px * _freeze_per_name(frames['shares_diluted'])),
        '1/price alone': 1.0 / px,
        '1/marketcap alone': 1.0 / mcap,
    }
    print(f"\n=== DECOMPOSITION ({'point-in-time' if pit else 'present-day'} "
          f"universe; all variants scored on sales_to_price's cells) ===")
    print(f"{'variant':<32}{'h':>4}{'mean IC':>10}{'t(NW)':>8}")
    print('-' * 54)
    for name, v in variants.items():
        v = v.replace([np.inf, -np.inf], np.nan).where(mask)
        for h in horizons:
            r = evaluate(v, px, horizon=h)
            if r['mean_ic'] is None:
                print(f'{name:<32}{h:>4}   (not computable)'); continue
            t = r['ic_t_stat']
            print(f"{name:<32}{h:>4}{r['mean_ic']:>10.4f}"
                  f"{(t if t is not None else float('nan')):>8.2f}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--horizons', type=int, nargs='+', default=[21, 63])
    ap.add_argument('--pit', action='store_true',
                    help='score on the point-in-time universe (ticker-keyed, '
                         'delisted members included) instead of the '
                         'present-day conid universe')
    ap.add_argument('--decompose', action='store_true',
                    help='run the standing-rule ratio decomposition '
                         '(sales_to_price vs its frozen parts and 1/mcap) '
                         'instead of the signal table')
    args = ap.parse_args()
    from trader.simulation.signal_eval import evaluate

    if args.decompose:
        run_decomposition(args.pit, args.horizons)
        return 0

    for delay, label in ((0, 'AS-OF (honest)'), (7, 'DELAYED +7d (leak probe)')):
        px, sigs = build(delay, pit=args.pit)
        if not sigs:
            print('no fundamentals', file=sys.stderr); return 1
        rng = np.random.default_rng(0)
        sigs['random_control'] = pd.DataFrame(
            rng.normal(size=px.shape), index=px.index, columns=px.columns)
        print(f'\n=== {label} ===', flush=True)
        print(f"{'signal':<22}{'h':>4}{'mean IC':>10}{'t(NW)':>8}{'coverage':>10}")
        print('-' * 54)
        for name, s in sigs.items():
            cov = float(s.notna().sum().sum()) / (s.shape[0] * s.shape[1])
            for h in args.horizons:
                r = evaluate(s, px, horizon=h)
                if r['mean_ic'] is None:
                    print(f'{name:<22}{h:>4}   (not computable)'); continue
                t = r['ic_t_stat']
                flag = ' *' if t is not None and abs(t) >= 2 else ''
                print(f"{name:<22}{h:>4}{r['mean_ic']:>10.4f}"
                      f"{(t if t is not None else float('nan')):>8.2f}"
                      f"{100*cov:>9.0f}%{flag}", flush=True)
    print('\n* = |t(NW)| >= 2. A real signal DEGRADES under the +7d delay; a')
    print('  leaked one barely moves, because a leak does not depend on when')
    print('  the news actually arrived.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
