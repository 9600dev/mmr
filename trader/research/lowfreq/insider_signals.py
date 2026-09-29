"""Form 4 insider-transaction signals for the low-frequency INSIDER family
(component C04; design record D11, D14).

Everything here is BACKWARD-LOOKING by construction so that the harness's
fresh-prefix lookahead probe (``engine.lookahead_check``) holds:

* an event enters the daily frames on the FIRST panel session >= its
  ``usable_from`` (D14: the close of the first session strictly after the
  Form 4 filing date, asserted again here, never earlier);
* every derived quantity at row t reads rows <= t only (rolling sums,
  rolling maxima, a routine classification that looks at PRIOR years);
* the per-process memo hands back the SAME numbers a fresh computation on
  the prefix would produce (pinned in ``tests/research/test_insider_signals.py``).

Events come from C03's loader ``trader.data.insider.load_insider_events``
(read-only, research DBs only, D03) and are cached once per process; tests
inject a frame with ``set_events_override`` instead of a database.

Insider scope (pre-registered in ``research/lowfreq/insider/PREREG.md``):
open-market purchases (code P) and sales (code S) by OFFICERS or DIRECTORS;
rows whose only relationship is ten-percent owner are excluded from every
signal (funds and holding companies, not the managers whose information the
hypotheses are about). Joint filings list one row per reporting owner: dollar
values are weighted by ``1/n_owners`` and a filing's owner set is collapsed to
one buyer (its lowest ``owner_cik``) when insiders are counted.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, cast

import numpy as np
import pandas as pd

CODES: Tuple[str, ...] = ('P', 'S')
HISTORY_START = '2014-01-01'            # first quarter of the SEC data sets (DATA.md)
FAR_FUTURE = '2100-01-01'
ROUTINE_YEARS = 3                       # Cohen, Malloy & Pomorski (2012): same calendar month, 3 prior years
CONVICTION_MIN = 0.10                   # a purchase that is >= 10% of the insider's post-trade holding
MAX_PRICE = 100_000.0                   # DATA.md: mis-keyed prices above this are excluded from value sums
MAX_VALUE = 1e10

#: Daily measures produced by ``daily_frames`` (dollar values are 1/n_owners weighted).
FILTERS: Tuple[str, ...] = ('', '_opp', '_officer', '_director', '_conv')
MEASURES: Tuple[str, ...] = tuple(f'p_filings{s}' for s in FILTERS) + tuple(f'p_value{s}' for s in FILTERS) + ('s_value',)

_events_override: Optional[pd.DataFrame] = None
_events_cache: Dict[str, pd.DataFrame] = {}
_memo: Dict[Tuple[Any, ...], Tuple[pd.DatetimeIndex, List[str], Dict[str, pd.DataFrame]]] = {}


# --------------------------------------------------------------------------- events
def set_events_override(events: Optional[pd.DataFrame]) -> None:
    """Tests: use ``events`` (interface columns of ``insider_events``) instead of
    the research database. ``None`` restores database loading. Clears the memo."""
    global _events_override
    _events_override = None if events is None else events.copy()
    _events_cache.clear()
    _memo.clear()


def clear_caches() -> None:
    _events_cache.clear()
    _memo.clear()


def raw_events() -> pd.DataFrame:
    """Every effective Form 4 P/S event the research database holds (from
    ``HISTORY_START``), loaded ONCE per process; or the test override."""
    if _events_override is not None:
        return _events_override
    if 'db' not in _events_cache:
        from trader.data.insider import load_insider_events   # D03: read-only research DB
        _events_cache['db'] = load_insider_events(HISTORY_START, FAR_FUTURE, codes=list(CODES))
    return _events_cache['db']


def prepared_events() -> pd.DataFrame:
    """``prepare_events(raw_events())``, cached per process."""
    key = 'prepared_override' if _events_override is not None else 'prepared_db'
    if key not in _events_cache:
        _events_cache[key] = prepare_events(raw_events())
    return _events_cache[key]


def prepare_events(ev: pd.DataFrame) -> pd.DataFrame:
    """Clean and annotate an ``insider_events`` frame (pure).

    Keeps P/S rows with a ticker; re-asserts D14 (``usable_from`` strictly
    after ``filing_date``); adds ``owner_weight`` (1/n_owners), ``value_w``
    (0 where the price is missing or mis-keyed), ``is_od`` (officer or
    director), ``ten_pct_only``, ``group_key`` (a filing's lowest owner cik),
    ``holding_increase`` (shares / shares_after for purchases) and ``routine``
    (``routine_flags``).
    """
    df = ev.copy()
    df = df[df['code'].isin(list(CODES)) & df['ticker'].notna()].copy()
    for c in ('filing_date', 'usable_from', 'trans_date'):
        df[c] = pd.to_datetime(df[c])
    if len(df):
        late = df['usable_from'] <= df['filing_date']
        if bool(late.any()):
            raise ValueError(f'D14 violated: {int(late.sum())} events usable on or before their filing date')
    n_owners = pd.to_numeric(df['n_owners'], errors='coerce').fillna(1).clip(lower=1).astype(float)
    df['owner_weight'] = 1.0 / n_owners
    price = pd.to_numeric(df['price'], errors='coerce').astype(float)
    value = pd.to_numeric(df['value_usd'], errors='coerce').astype(float)
    valid = value.notna() & np.isfinite(value) & (value > 0) & (price <= MAX_PRICE) & (value <= MAX_VALUE)
    df['value_w'] = value.where(valid, 0.0) * df['owner_weight']
    for c in ('is_officer', 'is_director', 'is_ten_pct'):
        df[c] = df[c].fillna(False).astype(bool)
    df['is_od'] = df['is_officer'] | df['is_director']
    df['ten_pct_only'] = df['is_ten_pct'] & ~df['is_od']
    df['owner_cik'] = pd.to_numeric(df['owner_cik'], errors='coerce')
    df['group_key'] = df.groupby('accession')['owner_cik'].transform('min')
    df['group_key'] = df['group_key'].fillna(df['owner_cik']).fillna(-1).astype('int64')
    shares = pd.to_numeric(df['shares'], errors='coerce').astype(float)
    after = pd.to_numeric(df['shares_after'], errors='coerce').astype(float)
    inc = (shares / after).where((after > 0) & (shares > 0))
    df['holding_increase'] = inc.replace([np.inf, -np.inf], np.nan)
    df['routine'] = routine_flags(df)
    return df.reset_index(drop=True)


def routine_flags(df: pd.DataFrame, years: int = ROUTINE_YEARS) -> pd.Series:
    """Cohen, Malloy & Pomorski (2012) routine trader, per (ticker, owner):
    a purchase is ROUTINE when the same owner bought the same issuer in the
    same calendar month (by trade month) in each of the ``years`` PRIOR
    years, counting only prior purchases whose ``usable_from`` is at or
    before the classified purchase's own ``usable_from`` - what was KNOWN at
    that close (D14). A late or backdated filing therefore cannot make an
    earlier purchase routine, and the flag depends only on events usable at
    or before the event's own session, so it is identical on any prefix.
    Sales and non-purchases are False.
    """
    out = pd.Series(False, index=df.index)
    p = df[df['code'] == 'P']
    if len(p) == 0:
        return out
    td = pd.to_datetime(p['trans_date'])
    ym = (td.dt.year * 12 + td.dt.month).astype('int64')
    owner = pd.to_numeric(p['owner_cik'], errors='coerce').fillna(-1).astype('int64')
    usable = pd.to_datetime(p['usable_from']).to_numpy(dtype='datetime64[ns]')
    keys = pd.DataFrame({'ticker': p['ticker'].astype(str).to_numpy(), 'owner': owner.to_numpy(),
                         'ym': ym.to_numpy(), 'usable': usable})
    # the FIRST session a purchase in (ticker, owner, trade month) became usable
    first_usable = keys.groupby(['ticker', 'owner', 'ym'])['usable'].min()
    known: Dict[Tuple[str, int, int], np.datetime64] = {
        (str(t), int(o), int(m)): np.datetime64(u) for (t, o, m), u in first_usable.items()}

    def is_routine(t: str, o: int, m: int, u: np.datetime64) -> bool:
        for k in range(1, years + 1):
            prior = known.get((t, o, m - 12 * k))
            if prior is None or prior > u:
                return False
        return True

    flags = np.fromiter((is_routine(str(t), int(o), int(m), u)
                         for t, o, m, u in zip(keys['ticker'], keys['owner'], keys['ym'], keys['usable'])),
                        dtype=bool, count=len(keys))
    out.loc[p.index] = flags
    return out


# --------------------------------------------------------------------------- daily frames
def _positions(usable_from: pd.Series, index: pd.DatetimeIndex) -> np.ndarray:
    """Row position of the FIRST session >= usable_from (D14: never earlier);
    ``len(index)`` for events after the last session."""
    vals = pd.to_datetime(usable_from).to_numpy(dtype='datetime64[ns]')
    return cast(np.ndarray, index.to_numpy(dtype='datetime64[ns]').searchsorted(vals, side='left'))


def events_in_window(prepared: pd.DataFrame, index: pd.DatetimeIndex, tickers: Sequence[str]) -> pd.DataFrame:
    """Events of ``tickers`` usable inside ``[index[0], index[-1]]``, with a
    ``pos`` column = the session they enter the frames on."""
    if len(index) == 0:
        return prepared.iloc[0:0].assign(pos=np.array([], dtype=int))
    tick = set(str(t) for t in tickers)
    ev = prepared[prepared['ticker'].isin(tick)]
    ev = ev[(ev['usable_from'] >= index[0]) & (ev['usable_from'] <= index[-1])].copy()
    ev['pos'] = _positions(ev['usable_from'], index)
    return ev[ev['pos'] < len(index)]


def _pivot_sum(ev: pd.DataFrame, mask: pd.Series, col: str, index: pd.DatetimeIndex,
               tickers: Sequence[str]) -> pd.DataFrame:
    sub = ev[mask]
    if len(sub) == 0:
        return pd.DataFrame(0.0, index=index, columns=list(tickers))
    g = sub.groupby(['pos', 'ticker'])[col].sum().unstack('ticker')
    out = pd.DataFrame(0.0, index=index, columns=list(tickers))
    g.index = index[g.index.to_numpy(dtype=int)]
    out.loc[g.index, [c for c in g.columns if c in out.columns]] = g[[c for c in g.columns if c in out.columns]]
    return out.fillna(0.0)


def _pivot_filings(ev: pd.DataFrame, mask: pd.Series, index: pd.DatetimeIndex,
                   tickers: Sequence[str]) -> pd.DataFrame:
    """Distinct FILINGS (accessions) per session and ticker - a joint filing
    with several reporting owners is one filing, a filing with several
    purchase lines is one filing."""
    sub = ev[mask]
    out = pd.DataFrame(0.0, index=index, columns=list(tickers))
    if len(sub) == 0:
        return out
    g = sub.groupby(['pos', 'ticker'])['accession'].nunique().unstack('ticker')
    g.index = index[g.index.to_numpy(dtype=int)]
    cols = [c for c in g.columns if c in out.columns]
    out.loc[g.index, cols] = g[cols].astype(float)
    return out.fillna(0.0)


def daily_frames(prepared: pd.DataFrame, index: pd.DatetimeIndex, tickers: Sequence[str]) -> Dict[str, pd.DataFrame]:
    """The ``MEASURES`` as dense (session x ticker) frames, zero where nothing
    became usable that session (pure; row t holds only events whose
    ``usable_from`` maps to session t). ``p_filings*`` count filings,
    ``p_value*``/``s_value`` sum 1/n_owners-weighted dollars; the suffix
    names the filter (``_opp`` non-routine, ``_officer``, ``_director``
    = director and not officer, ``_conv`` holding increase >= 10%)."""
    idx = pd.DatetimeIndex(index)
    ev = events_in_window(prepared, idx, tickers)
    is_p = (ev['code'] == 'P') & ev['is_od']
    is_s = (ev['code'] == 'S') & ev['is_od']
    filters = {
        '': is_p,
        '_opp': is_p & ~ev['routine'].astype(bool),
        '_officer': is_p & ev['is_officer'],
        '_director': is_p & ev['is_director'] & ~ev['is_officer'],
        '_conv': is_p & (ev['holding_increase'] >= CONVICTION_MIN),
    }
    out: Dict[str, pd.DataFrame] = {}
    for suffix, mask in filters.items():
        out['p_filings' + suffix] = _pivot_filings(ev, mask, idx, tickers)
        out['p_value' + suffix] = _pivot_sum(ev, mask, 'value_w', idx, tickers)
    out['s_value'] = _pivot_sum(ev, is_s, 'value_w', idx, tickers)
    return out


def distinct_buyers(prepared: pd.DataFrame, index: pd.DatetimeIndex, tickers: Sequence[str],
                    window: int) -> pd.DataFrame:
    """Number of DISTINCT officer/director buyers (joint filings collapsed to
    their ``group_key``) whose purchase became usable within the trailing
    ``window`` sessions ending at each row (inclusive). Backward-looking:
    a rolling maximum of each buyer's daily indicator, summed per ticker."""
    idx = pd.DatetimeIndex(index)
    cols = list(tickers)
    ev = events_in_window(prepared, idx, cols)
    ev = ev[(ev['code'] == 'P') & ev['is_od']]
    out = pd.DataFrame(0.0, index=idx, columns=cols)
    if len(ev) == 0 or window <= 0:
        return out
    pairs = ev[['pos', 'ticker', 'group_key']].drop_duplicates()
    pair_index = pd.MultiIndex.from_frame(pairs[['ticker', 'group_key']].drop_duplicates())
    col_pos = {k: i for i, k in enumerate(pair_index)}
    arr = np.zeros((len(idx), len(pair_index)), dtype='float32')
    for pos, t, g in zip(pairs['pos'].to_numpy(), pairs['ticker'], pairs['group_key']):
        arr[int(pos), col_pos[(t, g)]] = 1.0
    ind = pd.DataFrame(arr, index=idx, columns=pair_index)
    active = ind.rolling(window, min_periods=1).max()
    per_ticker = active.T.groupby(level=0).sum().T
    out.loc[:, [c for c in per_ticker.columns if c in out.columns]] = per_ticker[[c for c in per_ticker.columns if c in out.columns]]
    return out.fillna(0.0)


# --------------------------------------------------------------------------- memo
Builder = Callable[[pd.DatetimeIndex, List[str]], Dict[str, pd.DataFrame]]


def memoised(key: Tuple[Any, ...], index: pd.DatetimeIndex, tickers: Sequence[str], build: Builder) -> Dict[str, pd.DataFrame]:
    """``build(index, tickers)`` served from a per-process memo when a
    PREFIX of an already-built index (same first session) with a SUBSET of its
    tickers is asked for. Valid only because every builder here is
    backward-looking and per-ticker independent: the first n rows of a column
    are identical however many later rows or other columns exist - the
    property the fresh-prefix test pins. Anything else is rebuilt (and kept
    when it is the longer index)."""
    idx = pd.DatetimeIndex(index)
    cols = [str(t) for t in tickers]
    full_key = key + (idx[0] if len(idx) else None, 'override' if _events_override is not None else 'db')
    hit = _memo.get(full_key)
    if hit is not None:
        m_idx, m_cols, m_frames = hit
        if len(idx) <= len(m_idx) and m_idx[:len(idx)].equals(idx) and set(cols) <= set(m_cols):
            return {k: v.iloc[:len(idx)].reindex(columns=cols) for k, v in m_frames.items()}
    frames = build(idx, cols)
    if hit is None or len(idx) > len(hit[0]):
        _memo[full_key] = (idx, cols, frames)
    return frames


def signal_frames(index: pd.DatetimeIndex, tickers: Sequence[str], buyer_window: int) -> Dict[str, pd.DataFrame]:
    """``daily_frames`` plus ``buyers_<window>`` for the panel's sessions and
    tickers, memoised per process."""
    def build(idx: pd.DatetimeIndex, cols: List[str]) -> Dict[str, pd.DataFrame]:
        prepared = prepared_events()
        out = daily_frames(prepared, idx, cols)
        out[f'buyers_{buyer_window}'] = distinct_buyers(prepared, idx, cols, buyer_window)
        return out
    return memoised(('signal_frames', int(buyer_window)), index, tickers, build)


def trailing_sum(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    """Sum over the trailing ``window`` sessions (inclusive of the row)."""
    return frame.rolling(int(window), min_periods=1).sum()


def to_events_frame(rows: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    """Test helper: dict rows -> an ``insider_events``-shaped frame with the
    interface columns present (missing ones defaulted)."""
    from trader.data.insider import EVENT_COLUMNS, EXTRA_COLUMNS
    df = pd.DataFrame(list(rows))
    defaults: Dict[str, Any] = {'cik': 1, 'code': 'P', 'acquired': True, 'is_director': False,
                                'is_officer': True, 'officer_title': None, 'is_ten_pct': False,
                                'n_owners': 1, 'document_type': '4', 'direct_indirect': 'D',
                                'security_title': 'Common Stock', 'trans_sk': 0, 'symbol_as_filed': None}
    for c in list(EVENT_COLUMNS) + list(EXTRA_COLUMNS):
        if c not in df.columns:
            df[c] = defaults.get(c, np.nan)
    if 'value_usd' in df.columns:
        df['value_usd'] = df['value_usd'].where(df['value_usd'].notna(), df['shares'] * df['price'])
    if 'trans_date' not in df.columns or df['trans_date'].isna().all():
        df['trans_date'] = pd.to_datetime(df['filing_date']) - pd.Timedelta(days=1)
    if 'accession' not in df.columns or df['accession'].isna().any():
        df['accession'] = [f'acc-{i}' for i in range(len(df))]
    if 'shares_after' not in df.columns:
        df['shares_after'] = df['shares'] * 5.0
    df['trans_date'] = pd.to_datetime(df['trans_date'])
    return df


def next_session(day: Any, sessions: pd.DatetimeIndex) -> dt.date:
    """Test helper: the first session strictly after ``day``."""
    d = pd.Timestamp(day)
    later = sessions[sessions > d]
    if len(later) == 0:
        raise ValueError(f'no session after {d.date()}')
    return cast(pd.Timestamp, later[0]).date()
