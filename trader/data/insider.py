"""SEC Form 3/4/5 insider transactions: read-only loader and the pure helpers
shared with ``scripts/fetch_insider_form345.py`` (design record D10).

WHAT THE DATA IS
    The SEC publishes every Section 16 filing as quarterly tab-separated data
    sets ("Insider Transactions Data Sets"). ``scripts/fetch_insider_form345.py``
    ingests them into ``insider.duckdb`` - one file per research directory,
    sitting NEXT TO the history database named by ``LOWFREQ_RESEARCH_CONFIG``
    (D03: the harness reads only the databases of its research directory).
    This module never writes; the database is opened ``read_only=True``.

WHEN AN EVENT BECOMES USABLE (D10)
    A Form 4 is frequently filed after the close, and the data set carries a
    filing DATE, not a time. So an event is usable only from the close of the
    first trading session strictly AFTER its ``filing_date``: ``usable_from``
    is that session. A filing on Friday is usable from Monday's close; one on
    the day before a holiday, from the session after the holiday. A backtest
    that marks at the close of ``usable_from`` and fills at the next open
    (``Backtester.run_panel``) therefore never acts on information it could
    not have had. The loader asserts ``usable_from > filing_date`` on every
    row it returns rather than trusting the ingest.

AMENDMENTS (D10)
    A ``4/A`` replaces the transactions of the filing it amends rather than
    adding to them. The ingest links every amendment to its original through
    the data set's ``DATE_OF_ORIG_SUB`` (same issuer, same form, same original
    filing date, a shared reporting owner) and marks the original - and any
    earlier amendment of it - ``superseded_by`` the latest amendment. The
    loader returns only effective (non-superseded) filings unless asked.
    Supersession is recomputed for the truncated dev copy on its own rows, so
    an amendment filed after the cutoff cannot retroactively erase an original
    that was the best information available before it.

TICKERS
    The as-filed ``ISSUERTRADINGSYMBOL`` is normalised to the PIT universe's
    Polygon-style spelling (``BRK.B``); filings that declare no symbol
    (``NONE``, ``N/A``, blank) inherit the same issuer CIK's most recent earlier
    symbol (else its earliest later one). ``symbol_as_filed`` keeps the raw
    string. The issuer CIK, not the ticker, is the stable identity.
"""
from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple, cast

import duckdb
import numpy as np
import pandas as pd

# The research config (D03). Duplicated from trader.research.lowfreq.config on
# purpose: importing that package pulls in the whole evaluation harness.
ENV_CONFIG = 'LOWFREQ_RESEARCH_CONFIG'
DEFAULT_CONFIG = '~/.local/share/mmr/research_dev/trader_research.yaml'
INSIDER_DB_NAME = 'insider.duckdb'

#: The interface columns (design record, interface ``insider_events``), in order.
EVENT_COLUMNS: Tuple[str, ...] = (
    'ticker', 'cik', 'filing_date', 'usable_from', 'trans_date', 'code', 'shares',
    'price', 'value_usd', 'acquired', 'owner_cik', 'is_director', 'is_officer',
    'officer_title', 'is_ten_pct', 'shares_after',
)
#: Provenance columns appended after the interface columns.
EXTRA_COLUMNS: Tuple[str, ...] = (
    'accession', 'trans_sk', 'n_owners', 'document_type', 'symbol_as_filed',
    'direct_indirect', 'security_title',
)

#: Values of ISSUERTRADINGSYMBOL that declare "no symbol" rather than naming one.
NO_SYMBOL_VALUES = frozenset({
    '', 'NONE', 'N/A', 'NA', 'N.A.', 'N.A', 'NULL', 'NOT APPLICABLE', 'NOTAPPLICABLE',
    'NOT LISTED', 'UNLISTED', 'PRIVATE', 'TBD', 'NO', 'NON', 'NONE.', 'N/A.', '-', '--',
    'NOT PUBLIC', 'NOTPUBLIC', 'NOT TRADED', 'NO SYMBOL', 'NOSYMBOL', 'NOT AVAILABLE',
    'XXX', 'XXXX', 'ZZZZ', '0', '00000',
})
_SYMBOL_RE = re.compile(r'^[A-Z][A-Z0-9]{0,5}(\.[A-Z0-9]{1,4})?$')
_SPLIT_RE = re.compile(r'\s*(?:[,;|]|\band\b|\bor\b|\s/\s)\s*', re.IGNORECASE)
_EXCHANGE_PREFIX_RE = re.compile(
    r'^(?:NYSE\s*(?:MKT|AMERICAN|ARCA)?|NASDAQ(?:\s*[A-Z]+)?|NASD|AMEX|OTC(?:\s*(?:BB|QB|QX|PINK))?|OTCBB|OTCQB|OTCQX|'
    r'PINK(?:\s*SHEETS)?|TSX(?:\s*V)?|CBOE|BATS|TICKER|SYMBOL)\s*[:\-]?\s*')
_OTC_SUFFIX_RE = re.compile(r'(?:\.(?:OB|PK|OTC|QB|QX|BB)|\((?:OB|PK|OTC|OTCBB|OTCQB|OTCQX)\))$')


def normalize_symbol(raw: Any) -> Optional[str]:
    """As-filed ``ISSUERTRADINGSYMBOL`` -> PIT-style ticker, or ``None``.

    ``None`` means the filing does not name a usable symbol (blank, ``NONE``,
    prose, a non-ticker string). A list (``"ISCA, ISCB"``) yields its first
    entry; an exchange prefix (``NYSE: KRC``) and an OTC suffix (``NWIN.OB``)
    are stripped; class separators ``-``/``/``/space become the PIT dot
    (``BRK-B`` -> ``BRK.B``).
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    if not s:
        return None
    s = _OTC_SUFFIX_RE.sub('', s).strip(' "\'$*().:[]{}')
    if not s or s in NO_SYMBOL_VALUES:
        return None
    s = _EXCHANGE_PREFIX_RE.sub('', s)
    first = _SPLIT_RE.split(s, maxsplit=1)[0].strip()
    first = first.strip(' "\'$*().:[]{}')
    first = _OTC_SUFFIX_RE.sub('', first)
    if not first or first in NO_SYMBOL_VALUES:
        return None
    first = re.sub(r'[-/ ]+', '.', first)
    first = re.sub(r'\.{2,}', '.', first).strip('.')
    if not _SYMBOL_RE.match(first):
        return None
    return first


def parse_relationship(relationship: Any) -> Tuple[bool, bool, bool, bool]:
    """``RPTOWNER_RELATIONSHIP`` -> (is_director, is_officer, is_ten_pct, is_other).

    The field is a concatenation such as ``Director,Officer`` or
    ``TenPercentOwnerOther`` (the data set does not always separate the
    tokens), so membership is tested by substring on the upper-cased value.
    """
    r = '' if relationship is None else str(relationship).upper().replace(' ', '')
    return ('DIRECTOR' in r, 'OFFICER' in r, 'TENPERCENTOWNER' in r, 'OTHER' in r)


def nyse_sessions(start: Any, end: Any) -> np.ndarray:
    """XNYS session dates in ``[start, end]`` as ``datetime64[D]`` (sorted)."""
    import exchange_calendars as xcals  # runtime dependency (see CLAUDE.md)

    s = cast(pd.Timestamp, pd.Timestamp(start)).normalize()
    e = cast(pd.Timestamp, pd.Timestamp(end)).normalize()
    cal = xcals.get_calendar('XNYS', start=s, end=e)
    return cast(np.ndarray, cal.sessions.values.astype('datetime64[D]'))


def usable_from_sessions(filing_dates: pd.Series, sessions: np.ndarray) -> pd.Series:
    """D10: the first session STRICTLY after each filing date.

    ``sessions`` must be sorted ``datetime64[D]`` and extend past the last
    filing date, otherwise this raises rather than returning a truncated or
    clamped answer (a clamped ``usable_from`` would make a late filing look
    usable early).
    """
    if len(sessions) == 0:
        raise ValueError('empty session calendar')
    fd = pd.to_datetime(filing_dates)
    if fd.isna().any():
        raise ValueError('filing_dates contains NaT')
    days = fd.values.astype('datetime64[D]')
    idx = np.searchsorted(sessions, days, side='right')
    if len(days) and int(idx.max()) >= len(sessions):
        raise ValueError(
            f'session calendar ends {sessions[-1]} but a filing on {days[idx.argmax()]} '
            f'needs the following session')
    out = pd.Series(pd.to_datetime(sessions[idx]), index=filing_dates.index, name='usable_from')
    assert bool((out.values.astype('datetime64[D]') > days).all())
    return out


def research_config_path() -> Path:
    return Path(os.path.expanduser(os.environ.get(ENV_CONFIG) or DEFAULT_CONFIG))


def insider_db_path(config_path: Optional[os.PathLike | str] = None) -> Path:
    """``insider.duckdb`` next to the history DB named by the research config.

    ``config_path`` overrides ``LOWFREQ_RESEARCH_CONFIG``. Fails loudly when the
    config is missing or names no ``history_duckdb_path`` (D03).
    """
    import yaml

    path = Path(os.path.expanduser(str(config_path))) if config_path is not None else research_config_path()
    if not path.exists():
        raise FileNotFoundError(f'{ENV_CONFIG} points at {path}, which does not exist (D03)')
    with open(path, 'r') as fh:
        cfg = yaml.safe_load(fh) or {}
    if not isinstance(cfg, dict) or not cfg.get('history_duckdb_path'):
        raise ValueError(f'{path} names no history_duckdb_path; cannot locate {INSIDER_DB_NAME}')
    hist = Path(os.path.expanduser(str(cfg['history_duckdb_path'])))
    return hist.parent / INSIDER_DB_NAME


def connect_read_only(path: Path) -> duckdb.DuckDBPyConnection:
    """Open the insider DB read-only. A missing file is an error, never created."""
    if not path.exists():
        raise FileNotFoundError(
            f'insider database {path} does not exist; build it with '
            f'scripts/fetch_insider_form345.py build (it sits next to the history DB of {ENV_CONFIG})')
    return duckdb.connect(str(path), read_only=True)


def _date(d: Any) -> dt.date:
    t = cast(pd.Timestamp, pd.Timestamp(d))
    if pd.isna(t):
        raise ValueError(f'not a date: {d!r}')
    return t.date()


_EVENT_SQL = """
SELECT s.ticker,
       s.issuer_cik                       AS cik,
       s.filing_date,
       s.usable_from,
       t.trans_date,
       t.trans_code                       AS code,
       t.shares,
       t.price,
       CASE WHEN t.price IS NULL THEN NULL ELSE t.shares * t.price END AS value_usd,
       (t.acquired_disposed = 'A')        AS acquired,
       o.owner_cik,
       o.is_director,
       o.is_officer,
       o.officer_title,
       o.is_ten_pct,
       t.shares_after,
       s.accession,
       t.trans_sk,
       o.n_owners,
       s.document_type,
       s.symbol_as_filed,
       t.direct_indirect,
       t.security_title
FROM form345_nonderiv_trans t
JOIN form345_submissions s ON s.accession = t.accession
JOIN (SELECT accession, owner_cik, is_director, is_officer, officer_title, is_ten_pct,
             count(*) OVER (PARTITION BY accession) AS n_owners
      FROM form345_owners) o ON o.accession = t.accession
WHERE {where}
ORDER BY s.usable_from, s.ticker, s.accession, t.trans_sk, o.owner_cik
"""


def load_insider_events(start: Any, end: Any, codes: Optional[Iterable[str]] = None, *,
                        forms: Sequence[str] = ('4',),
                        date_field: str = 'usable_from',
                        include_superseded: bool = False,
                        tickers: Optional[Iterable[str]] = None,
                        db_path: Optional[os.PathLike | str] = None) -> pd.DataFrame:
    """Non-derivative insider transactions usable in ``[start, end]`` (inclusive).

    One row per (transaction, reporting owner): a joint filing by N owners
    yields N rows for the same transaction, distinguishable by ``accession`` +
    ``trans_sk`` and weightable by ``1/n_owners``. Columns: ``EVENT_COLUMNS``
    then ``EXTRA_COLUMNS``.

    ``codes`` filters ``TRANS_CODE`` (``P`` purchase, ``S`` sale, ``A`` grant,
    ``M`` exercise, ``F`` tax withholding, ``G`` gift, ...). ``forms`` selects
    the base form (default Form 4 only; ``('3','4','5')`` for all; a ``4/A``
    counts as form 4). ``date_field`` chooses which date the window applies to
    (``usable_from`` by default - what BECOMES usable in the window - or
    ``filing_date`` / ``trans_date``). Superseded filings (D10) are excluded
    unless ``include_superseded``. Reads ``db_path`` or ``insider_db_path()``
    read-only.
    """
    if date_field not in ('usable_from', 'filing_date', 'trans_date'):
        raise ValueError(f'date_field must be usable_from, filing_date or trans_date, not {date_field!r}')
    s_date, e_date = _date(start), _date(end)
    if e_date < s_date:
        raise ValueError(f'end {e_date} precedes start {s_date}')
    col = {'usable_from': 's.usable_from', 'filing_date': 's.filing_date', 'trans_date': 't.trans_date'}[date_field]
    where: List[str] = [f'{col} >= ?', f'{col} <= ?']
    params: List[Any] = [s_date, e_date]
    forms_list = [str(f) for f in forms]
    if not forms_list:
        raise ValueError('forms must name at least one base form')
    where.append('s.base_form IN (' + ','.join('?' * len(forms_list)) + ')')
    params.extend(forms_list)
    if codes is not None:
        code_list = sorted({str(c).upper() for c in codes})
        if not code_list:
            raise ValueError('codes is empty; pass None for all codes')
        where.append('t.trans_code IN (' + ','.join('?' * len(code_list)) + ')')
        params.extend(code_list)
    if tickers is not None:
        tick_list = sorted({str(t).upper() for t in tickers})
        if not tick_list:
            raise ValueError('tickers is empty; pass None for all tickers')
        where.append('s.ticker IN (' + ','.join('?' * len(tick_list)) + ')')
        params.extend(tick_list)
    if not include_superseded:
        where.append('s.superseded_by IS NULL')
    path = Path(os.path.expanduser(str(db_path))) if db_path is not None else insider_db_path()
    con = connect_read_only(path)
    try:
        df = con.execute(_EVENT_SQL.format(where=' AND '.join(where)), params).fetchdf()
    finally:
        con.close()
    return _finish(df)


def _finish(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(EVENT_COLUMNS) + list(EXTRA_COLUMNS)
    df = df.reindex(columns=cols)
    for c in ('filing_date', 'usable_from', 'trans_date'):
        df[c] = pd.to_datetime(df[c])
    for c in ('acquired', 'is_director', 'is_officer', 'is_ten_pct'):
        df[c] = df[c].fillna(False).astype(bool)
    for c in ('shares', 'price', 'value_usd', 'shares_after'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype(float)
    for c in ('cik', 'owner_cik', 'trans_sk', 'n_owners'):
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
    df['code'] = df['code'].astype(object)
    if len(df):
        # D10, checked on every read rather than trusted from the ingest.
        late = df['usable_from'] <= df['filing_date']
        if bool(late.any()):
            bad = df.loc[late, ['accession', 'filing_date', 'usable_from']].head(3).to_dict('records')
            raise ValueError(f'usable_from not after filing_date (D10) for {int(late.sum())} rows, e.g. {bad}')
    return df.reset_index(drop=True)


def summarise_events(df: pd.DataFrame) -> Mapping[str, Any]:
    """Small diagnostic block for reports/tests: rows, filings, names, code counts."""
    if len(df) == 0:
        return {'rows': 0, 'filings': 0, 'tickers': 0, 'codes': {}}
    return {
        'rows': int(len(df)),
        'filings': int(df['accession'].nunique()),
        'tickers': int(df['ticker'].dropna().nunique()),
        'codes': {str(k): int(v) for k, v in df['code'].value_counts().items()},
        'first_usable': str(df['usable_from'].min().date()),
        'last_usable': str(df['usable_from'].max().date()),
    }
