#!/usr/bin/env python3
"""Ingest the SEC's quarterly Insider Transactions Data Sets (Forms 3/4/5).

    uv run python scripts/fetch_insider_form345.py build            # download 2014q1..latest, build full + dev DBs
    uv run python scripts/fetch_insider_form345.py build --no-download --force
    uv run python scripts/fetch_insider_form345.py check --db ~/.local/share/mmr/research_dev/insider.duckdb --cutoff 2024-06-30
    uv run python scripts/fetch_insider_form345.py report            # markdown statistics for research/lowfreq/insider/DATA.md

Source: https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/<YYYY>q<N>_form345.zip
(one zip per FILING quarter; tab-separated SUBMISSION, REPORTINGOWNER,
NONDERIV_TRANS, DERIV_TRANS, ... "as filed", amendments included). The SEC
asks for a descriptive User-Agent and at most 10 requests/second; this
script sends ``--user-agent`` (default names this project and a contact) and
spaces requests >= 0.2 s apart.

Outputs (D03 layout - one insider DB per research directory, next to its
history DB):
  * FULL history  -> ~/.local/share/mmr/research_full/insider.duckdb
  * DEV copy      -> ~/.local/share/mmr/research_dev/insider.duckdb, truncated to
    FILING_DATE <= 2024-06-30 with supersession (D10) recomputed on its own rows.

Idempotent: each quarter is recorded with its zip's SHA-256 in
``form345_quarters``; an unchanged quarter is skipped, a changed one (or
``--force``) is deleted and re-inserted, and every derived column
(``superseded_by``, ``ticker``, ``usable_from``) is recomputed from scratch at
the end. Re-running never duplicates a row.

Design record: D10 (usable_from = first session after the filing date;
4/A replaces its original), D03 (research DBs, read-only for consumers).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import duckdb
import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from trader.data.insider import (  # noqa: E402
    INSIDER_DB_NAME, normalize_symbol, nyse_sessions, parse_relationship, usable_from_sessions,
)

BASE_URL = 'https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/'
FIRST_QUARTER = '2014q1'
DEFAULT_CACHE = '~/.local/share/mmr/insider_form345'
DEFAULT_FULL_DB = '~/.local/share/mmr/research_full/' + INSIDER_DB_NAME
DEFAULT_DEV_DB = '~/.local/share/mmr/research_dev/' + INSIDER_DB_NAME
DEFAULT_DEV_CUTOFF = '2024-06-30'
DEFAULT_USER_AGENT = 'MMR Research joelpob@gmail.com'   # SEC format: '<name> <email>' - parentheses get the Archives 403 'Undeclared Automated Tool'
MIN_REQUEST_INTERVAL_S = 0.2          # <= 5 requests/second, under the SEC's 10/s
TABLES = ('SUBMISSION', 'REPORTINGOWNER', 'NONDERIV_TRANS', 'DERIV_TRANS')

_last_request_at = 0.0


# --------------------------------------------------------------------------- quarters

def quarter_range(first: str, last: str) -> List[str]:
    """``['2014q1', '2014q2', ..., last]`` inclusive."""
    def parse(q: str) -> Tuple[int, int]:
        q = q.lower().strip()
        if len(q) != 6 or q[4] != 'q' or not q[:4].isdigit() or q[5] not in '1234':
            raise ValueError(f'quarter must look like 2014q1, not {q!r}')
        return int(q[:4]), int(q[5])
    y0, n0 = parse(first)
    y1, n1 = parse(last)
    out: List[str] = []
    y, n = y0, n0
    while (y, n) <= (y1, n1):
        out.append(f'{y}q{n}')
        n += 1
        if n == 5:
            y, n = y + 1, 1
    return out


def quarter_of(day: dt.date) -> str:
    return f'{day.year}q{(day.month - 1) // 3 + 1}'


def previous_quarter(q: str) -> str:
    y, n = int(q[:4]), int(q[5])
    return f'{y - 1}q4' if n == 1 else f'{y}q{n - 1}'


def zip_name(quarter: str) -> str:
    return f'{quarter}_form345.zip'


# --------------------------------------------------------------------------- download

def _throttle() -> None:
    global _last_request_at
    wait = MIN_REQUEST_INTERVAL_S - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def _request(url: str, user_agent: str, method: str = 'GET', timeout: float = 300.0) -> Tuple[int, bytes]:
    """One throttled HTTP request. Returns (status, body); 404 is a status, not an exception."""
    _throttle()
    req = urllib.request.Request(url, method=method, headers={
        'User-Agent': user_agent, 'Accept-Encoding': 'identity', 'Host': 'www.sec.gov'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return int(resp.status), (b'' if method == 'HEAD' else resp.read())
    except urllib.error.HTTPError as exc:
        return int(exc.code), b''


def latest_published_quarter(user_agent: str, today: Optional[dt.date] = None, probes: int = 6) -> str:
    """Walk back from the current quarter until a zip answers 200 (HEAD requests)."""
    q = quarter_of(today or dt.date.today())
    for _ in range(probes):
        status, _body = _request(BASE_URL + zip_name(q), user_agent, method='HEAD', timeout=60)
        if status == 200:
            return q
        q = previous_quarter(q)
    raise RuntimeError(f'no published quarter found within {probes} probes back from {quarter_of(today or dt.date.today())}')


def _zip_ok(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            return zf.testzip() is None and 'SUBMISSION.tsv' in names and 'NONDERIV_TRANS.tsv' in names
    except (zipfile.BadZipFile, OSError):
        return False


def download_quarter(quarter: str, cache_dir: Path, user_agent: str, retries: int = 4) -> Path:
    """Fetch one quarterly zip into ``cache_dir`` (cached copies that pass an
    integrity check are reused). Retries on 403/429/5xx with backoff."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / zip_name(quarter)
    if path.exists() and _zip_ok(path):
        return path
    url = BASE_URL + zip_name(quarter)
    delay = 2.0
    for attempt in range(1, retries + 1):
        status, body = _request(url, user_agent)
        if status == 200:
            tmp = path.with_suffix('.zip.part')
            tmp.write_bytes(body)
            if not _zip_ok(tmp):
                tmp.unlink(missing_ok=True)
                raise RuntimeError(f'{url}: downloaded file is not a valid form345 zip')
            os.replace(tmp, path)
            return path
        if status == 404:
            raise FileNotFoundError(f'{url}: 404 (quarter not published)')
        if attempt == retries:
            raise RuntimeError(f'{url}: HTTP {status} after {retries} attempts')
        print(f'  {quarter}: HTTP {status}, retrying in {delay:.0f}s', file=sys.stderr)
        time.sleep(delay)
        delay *= 2
    raise AssertionError('unreachable')


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- parse

def _read_tsv(zf: zipfile.ZipFile, name: str) -> pd.DataFrame:
    with zf.open(name) as fh:
        raw = fh.read()
    return pd.read_csv(io.BytesIO(raw), sep='\t', dtype=str, keep_default_na=False,
                       na_filter=False, quoting=3, encoding='utf-8', encoding_errors='replace',
                       on_bad_lines='warn', engine='c')


def _dates(s: pd.Series) -> pd.Series:
    """``DD-MON-YYYY`` -> date (NaT for blank/malformed)."""
    out = pd.to_datetime(s.str.strip(), format='%d-%b-%Y', errors='coerce')
    return out.dt.date.where(out.notna(), None)


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.replace(',', '', regex=False).str.strip(), errors='coerce')


def _cik(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.strip(), errors='coerce').astype('Int64')


def _text(s: pd.Series) -> pd.Series:
    t = s.str.strip()
    return t.where(t != '', None)


def parse_zip(path: Path, quarter: str) -> Dict[str, pd.DataFrame]:
    """The four tables of one quarterly zip, typed for the schema below."""
    with zipfile.ZipFile(path) as zf:
        names = set(zf.namelist())
        sub = _read_tsv(zf, 'SUBMISSION.tsv')
        own = _read_tsv(zf, 'REPORTINGOWNER.tsv')
        ndt = _read_tsv(zf, 'NONDERIV_TRANS.tsv')
        drt = _read_tsv(zf, 'DERIV_TRANS.tsv') if 'DERIV_TRANS.tsv' in names else pd.DataFrame()

    doc = sub['DOCUMENT_TYPE'].str.strip().str.upper()
    symbol_raw = sub['ISSUERTRADINGSYMBOL'].str.strip()
    ticker = symbol_raw.map(normalize_symbol)
    submissions = pd.DataFrame({
        'accession': sub['ACCESSION_NUMBER'].str.strip(),
        'quarter': quarter,
        'filing_date': _dates(sub['FILING_DATE']),
        'period_of_report': _dates(sub['PERIOD_OF_REPORT']),
        'date_of_orig_sub': _dates(sub['DATE_OF_ORIG_SUB']),
        'document_type': doc,
        'base_form': doc.str.replace('/A', '', regex=False).str.strip(),
        'is_amendment': doc.str.endswith('/A'),
        'issuer_cik': _cik(sub['ISSUERCIK']),
        'issuer_name': _text(sub['ISSUERNAME']),
        'symbol_as_filed': symbol_raw.where(symbol_raw != '', None),
        'ticker': ticker.where(ticker.notna(), None),
        'ticker_source': np.where(ticker.notna(), 'filed', None),
        'usable_from': None,
        'superseded_by': None,
    })
    submissions = submissions[submissions['filing_date'].notna() & submissions['accession'].ne('')]

    rel = own['RPTOWNER_RELATIONSHIP']
    flags = pd.DataFrame([parse_relationship(r) for r in rel],
                         columns=['is_director', 'is_officer', 'is_ten_pct', 'is_other'], index=own.index)
    owners = pd.DataFrame({
        'accession': own['ACCESSION_NUMBER'].str.strip(),
        'owner_cik': _cik(own['RPTOWNERCIK']),
        'owner_name': _text(own['RPTOWNERNAME']),
        'relationship': _text(rel),
        'is_director': flags['is_director'].astype(bool),
        'is_officer': flags['is_officer'].astype(bool),
        'is_ten_pct': flags['is_ten_pct'].astype(bool),
        'is_other': flags['is_other'].astype(bool),
        'officer_title': _text(own['RPTOWNER_TITLE']),
        'other_text': _text(own['RPTOWNER_TXT']),
        'quarter': quarter,
    })
    owners = owners[owners['owner_cik'].notna()].drop_duplicates(['accession', 'owner_cik'], keep='first')

    def trans_frame(df: pd.DataFrame, sk_col: str, deriv: bool) -> pd.DataFrame:
        if len(df) == 0:
            return pd.DataFrame()
        out = pd.DataFrame({
            'accession': df['ACCESSION_NUMBER'].str.strip(),
            'trans_sk': pd.to_numeric(df[sk_col], errors='coerce').astype('Int64'),
            'security_title': _text(df['SECURITY_TITLE']),
            'trans_date': _dates(df['TRANS_DATE']),
            'deemed_execution_date': _dates(df['DEEMED_EXECUTION_DATE']),
            'trans_form_type': _text(df['TRANS_FORM_TYPE']),
            'trans_code': df['TRANS_CODE'].str.strip().str.upper().where(df['TRANS_CODE'].str.strip() != '', None),
            'equity_swap_involved': df['EQUITY_SWAP_INVOLVED'].str.strip().isin(['1', 'true', 'TRUE', 'True']),
            'trans_timeliness': _text(df['TRANS_TIMELINESS']),
            'shares': _num(df['TRANS_SHARES']),
            'price': _num(df['TRANS_PRICEPERSHARE']),
            'acquired_disposed': df['TRANS_ACQUIRED_DISP_CD'].str.strip().str.upper().where(
                df['TRANS_ACQUIRED_DISP_CD'].str.strip() != '', None),
            'shares_after': _num(df['SHRS_OWND_FOLWNG_TRANS']),
            'value_after': _num(df['VALU_OWND_FOLWNG_TRANS']),
            'direct_indirect': _text(df['DIRECT_INDIRECT_OWNERSHIP']),
            'nature_of_ownership': _text(df['NATURE_OF_OWNERSHIP']),
            'quarter': quarter,
        })
        if deriv:
            out['conv_exercise_price'] = _num(df['CONV_EXERCISE_PRICE'])
            out['exercise_date'] = _dates(df['EXCERCISE_DATE'])
            out['expiration_date'] = _dates(df['EXPIRATION_DATE'])
            out['underlying_title'] = _text(df['UNDLYNG_SEC_TITLE'])
            out['underlying_shares'] = _num(df['UNDLYNG_SEC_SHARES'])
            out['total_value'] = _num(df['TRANS_TOTAL_VALUE'])
        out = out[out['trans_sk'].notna() & out['accession'].ne('')]
        return out.drop_duplicates(['accession', 'trans_sk'], keep='first')

    return {
        'submissions': submissions,
        'owners': owners,
        'nonderiv': trans_frame(ndt, 'NONDERIV_TRANS_SK', deriv=False),
        'deriv': trans_frame(drt, 'DERIV_TRANS_SK', deriv=True),
    }


# --------------------------------------------------------------------------- schema

DDL = [
    """CREATE TABLE IF NOT EXISTS form345_quarters(
        quarter VARCHAR PRIMARY KEY, zip_sha256 VARCHAR, zip_bytes BIGINT, source_url VARCHAR,
        n_submissions BIGINT, n_owners BIGINT, n_nonderiv BIGINT, n_deriv BIGINT,
        n_dup_accessions BIGINT, loaded_at TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS form345_submissions(
        accession VARCHAR PRIMARY KEY, quarter VARCHAR, filing_date DATE, period_of_report DATE,
        date_of_orig_sub DATE, document_type VARCHAR, base_form VARCHAR, is_amendment BOOLEAN,
        issuer_cik BIGINT, issuer_name VARCHAR, symbol_as_filed VARCHAR, ticker VARCHAR,
        ticker_source VARCHAR, usable_from DATE, superseded_by VARCHAR,
        amends_accession VARCHAR, link_tier SMALLINT)""",
    """CREATE TABLE IF NOT EXISTS form345_owners(
        accession VARCHAR, owner_cik BIGINT, owner_name VARCHAR, relationship VARCHAR,
        is_director BOOLEAN, is_officer BOOLEAN, is_ten_pct BOOLEAN, is_other BOOLEAN,
        officer_title VARCHAR, other_text VARCHAR, quarter VARCHAR, PRIMARY KEY(accession, owner_cik))""",
    """CREATE TABLE IF NOT EXISTS form345_nonderiv_trans(
        accession VARCHAR, trans_sk BIGINT, security_title VARCHAR, trans_date DATE,
        deemed_execution_date DATE, trans_form_type VARCHAR, trans_code VARCHAR,
        equity_swap_involved BOOLEAN, trans_timeliness VARCHAR, shares DOUBLE, price DOUBLE,
        acquired_disposed VARCHAR, shares_after DOUBLE, value_after DOUBLE, direct_indirect VARCHAR,
        nature_of_ownership VARCHAR, quarter VARCHAR, PRIMARY KEY(accession, trans_sk))""",
    """CREATE TABLE IF NOT EXISTS form345_deriv_trans(
        accession VARCHAR, trans_sk BIGINT, security_title VARCHAR, trans_date DATE,
        deemed_execution_date DATE, trans_form_type VARCHAR, trans_code VARCHAR,
        equity_swap_involved BOOLEAN, trans_timeliness VARCHAR, shares DOUBLE, price DOUBLE,
        acquired_disposed VARCHAR, shares_after DOUBLE, value_after DOUBLE, direct_indirect VARCHAR,
        nature_of_ownership VARCHAR, quarter VARCHAR, conv_exercise_price DOUBLE, exercise_date DATE,
        expiration_date DATE, underlying_title VARCHAR, underlying_shares DOUBLE, total_value DOUBLE,
        PRIMARY KEY(accession, trans_sk))""",
    """CREATE TABLE IF NOT EXISTS form345_build_info(key VARCHAR PRIMARY KEY, value VARCHAR)""",
]

_TABLE_COLUMNS = {
    'form345_submissions': ['accession', 'quarter', 'filing_date', 'period_of_report', 'date_of_orig_sub',
                            'document_type', 'base_form', 'is_amendment', 'issuer_cik', 'issuer_name',
                            'symbol_as_filed', 'ticker', 'ticker_source', 'usable_from', 'superseded_by'],
    'form345_owners': ['accession', 'owner_cik', 'owner_name', 'relationship', 'is_director', 'is_officer',
                       'is_ten_pct', 'is_other', 'officer_title', 'other_text', 'quarter'],
    'form345_nonderiv_trans': ['accession', 'trans_sk', 'security_title', 'trans_date', 'deemed_execution_date',
                               'trans_form_type', 'trans_code', 'equity_swap_involved', 'trans_timeliness',
                               'shares', 'price', 'acquired_disposed', 'shares_after', 'value_after',
                               'direct_indirect', 'nature_of_ownership', 'quarter'],
    'form345_deriv_trans': ['accession', 'trans_sk', 'security_title', 'trans_date', 'deemed_execution_date',
                            'trans_form_type', 'trans_code', 'equity_swap_involved', 'trans_timeliness',
                            'shares', 'price', 'acquired_disposed', 'shares_after', 'value_after',
                            'direct_indirect', 'nature_of_ownership', 'quarter', 'conv_exercise_price',
                            'exercise_date', 'expiration_date', 'underlying_title', 'underlying_shares',
                            'total_value'],
}


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    for stmt in DDL:
        con.execute(stmt)
    # Databases built before the link outcome was persisted (2026-09-29) gain the columns in
    # place; apply_supersession fills them on every finalize, so no re-ingest is needed.
    con.execute('ALTER TABLE form345_submissions ADD COLUMN IF NOT EXISTS amends_accession VARCHAR')
    con.execute('ALTER TABLE form345_submissions ADD COLUMN IF NOT EXISTS link_tier SMALLINT')


def _insert(con: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    cols = _TABLE_COLUMNS[table]
    if len(df) == 0:
        return 0
    frame = df.reindex(columns=cols)
    con.register('_incoming', frame)
    try:
        con.execute(f'INSERT INTO {table} ({", ".join(cols)}) SELECT {", ".join(cols)} FROM _incoming')
    finally:
        con.unregister('_incoming')
    return int(len(frame))


def ingest_quarter(con: duckdb.DuckDBPyConnection, quarter: str, frames: Dict[str, pd.DataFrame],
                   zip_sha: str, zip_bytes: int, source_url: str) -> Dict[str, int]:
    """Replace ``quarter``'s rows with ``frames`` inside one transaction.

    An accession already present from ANOTHER quarter (the SEC occasionally
    re-disseminates a filing) is kept where it first appeared; the count is
    recorded as ``n_dup_accessions``.
    """
    con.execute('BEGIN')
    try:
        for table in ('form345_nonderiv_trans', 'form345_deriv_trans', 'form345_owners', 'form345_submissions'):
            con.execute(f'DELETE FROM {table} WHERE quarter = ?', [quarter])
        con.execute('DELETE FROM form345_quarters WHERE quarter = ?', [quarter])
        subs = frames['submissions']
        existing = con.execute('SELECT accession FROM form345_submissions').fetchdf()['accession']
        dup_mask = subs['accession'].isin(set(existing))
        n_dup = int(dup_mask.sum())
        subs = subs[~dup_mask]
        keep = set(subs['accession'])
        counts = {
            'n_submissions': _insert(con, 'form345_submissions', subs),
            'n_owners': _insert(con, 'form345_owners', frames['owners'][frames['owners']['accession'].isin(keep)]),
            'n_nonderiv': _insert(con, 'form345_nonderiv_trans',
                                  frames['nonderiv'][frames['nonderiv']['accession'].isin(keep)] if len(frames['nonderiv']) else frames['nonderiv']),
            'n_deriv': _insert(con, 'form345_deriv_trans',
                               frames['deriv'][frames['deriv']['accession'].isin(keep)] if len(frames['deriv']) else frames['deriv']),
            'n_dup_accessions': n_dup,
        }
        con.execute(
            'INSERT INTO form345_quarters VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [quarter, zip_sha, zip_bytes, source_url, counts['n_submissions'], counts['n_owners'],
             counts['n_nonderiv'], counts['n_deriv'], n_dup, dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)])
        con.execute('COMMIT')
    except Exception:
        con.execute('ROLLBACK')
        raise
    return counts


# --------------------------------------------------------------------------- derived columns

SUPERSESSION_SQL = """
-- D10: an amendment replaces the filing it amends. Six matching tiers, each
-- tried only when the previous one found no candidate; within a tier the
-- candidates are ranked JOINTLY (same period of report +2, an original rather
-- than an amendment +1) and only the top-scored one(s) are linked. Ranking
-- rather than filtering matters because the two preferences can disagree
-- (observed: TPL 0001207097-22-000001, whose true target is a same-period
-- earlier AMENDMENT beside two different-period originals filed the same day -
-- independent filters linked nothing). Ties at the top score are all linked
-- (ambiguous; counted).
--   tier 1  DATE: same issuer and base form, FILING_DATE = the amendment's
--           DATE_OF_ORIG_SUB, a shared reporting owner. Filers name either the
--           original's filing date or the PREVIOUS amendment's, so an earlier
--           amendment is a legitimate target and chains are walked to their
--           root (GROUP_SQL). A same-day ORIGINAL is a candidate whatever its
--           accession (observed: SWM 0001062993-22-012373 amends -012378, filed
--           the same day with a HIGHER accession); a same-day AMENDMENT target
--           must sort before the amendment so two same-day amendments cannot
--           each replace the other.
--   tier 2  CONTENT: no tier-1 candidate (DATE_OF_ORIG_SUB is often the
--           transaction date instead); same issuer/base form/shared owner,
--           filed within the 45 days up to the amendment, IDENTICAL
--           non-derivative transaction list. Replacing an identical filing
--           only removes a double count, so this tier cannot be wrong about
--           the numbers.
--   tier 3  PERIOD: neither; same issuer/base form/shared owner, an ORIGINAL
--           with the same PERIOD_OF_REPORT filed within the 45 days up to the
--           amendment - linked only when it is UNIQUE (an ambiguous tier-3
--           match is left unlinked and counted).
--   tier 4  OWNER-FREE: none of the above matched because the amendment
--           corrects the reporting owner's identity (observed: CYAN
--           0001010549-14-000096 moves a gift from an individual's CIK to a
--           foundation's; issuers' own CIKs filed as the owner and later fixed
--           are the common shape). Same issuer/base form, FILING_DATE = the
--           named DATE_OF_ORIG_SUB, same period, and an IDENTICAL extended
--           transaction list (date, code, shares, price, A/D, shares owned
--           after; NOT direct/indirect, which the CYAN correction flips from
--           "I, by the foundation" to "D") - linked only when UNIQUE. Different
--           people with identical grants filed the same day are refused
--           (ambiguous, counted); different content is never linked without a
--           shared owner - the "same period, different content" candidates
--           are other officers' filings.
--   tier 5  OWNER-FREE, CORE CONTENT: an owner correction that also corrects
--           the holdings (observed: BATL 0001209191-21-056275, "inadvertently
--           reported under reporting person Grant Evans", shares owned after
--           9,551 -> 11,601, so tier 4's extended signature cannot match).
--           Same issuer/base form, FILING_DATE = the named DATE_OF_ORIG_SUB,
--           same period, IDENTICAL core transaction list (date, code, shares,
--           price, A/D), UNIQUE, and two guards measured on the full history:
--           (a) both accessions come from the SAME SUBMITTER (accession
--           prefix) - the party that misattributed the filing is the one that
--           fixes it; two insiders each self-filing an identical grant (ABCB
--           0001356555-16-000004 vs 0001239875-16-000012) are not a
--           correction; (b) the amending owner has NO earlier filing of their
--           own with the same period - an officer re-filing a months-old
--           report (POA / clerical amendments: LSCC 0000855658-22-000265, OKE
--           0001741956-22-000014) beside a colleague's identical withholding
--           is amending their own filing, not the colleague's. Refused
--           candidates are counted, never linked.
--   tier 6  OWNER + PERIOD, NO WINDOW: tier 3 beyond its 45 days. Same
--           issuer/base form/shared owner, a UNIQUE original with the same
--           PERIOD_OF_REPORT filed any time earlier. Corrections of months-old
--           filings are ordinary (observed: LSCC 0000855658-22-000265 re-files
--           its 2022-05-17 original 157 days later to attach a power of
--           attorney; OKE 0001741956-22-000014 corrects a withholding reported
--           308 days earlier; a sample of 14 such pairs on the full history was
--           all same-owner restatements or corrections by their footnotes). The
--           period of report is the filing's earliest transaction date, so two
--           filings by one owner sharing it are the same report unless the
--           owner filed twice for that period - then the match is ambiguous and
--           refused (counted).
CREATE OR REPLACE TEMP TABLE _sig AS
SELECT accession,
       string_agg(coalesce(trans_date::VARCHAR, '') || '|' || coalesce(trans_code, '') || '|' ||
                  coalesce(shares::VARCHAR, '') || '|' || coalesce(price::VARCHAR, '') || '|' ||
                  coalesce(acquired_disposed, ''), ';'
                  ORDER BY trans_date, trans_code, shares, price, acquired_disposed) AS signature
FROM form345_nonderiv_trans GROUP BY accession;

CREATE OR REPLACE TEMP TABLE _sigx AS
SELECT accession,
       string_agg(coalesce(trans_date::VARCHAR, '') || '|' || coalesce(trans_code, '') || '|' ||
                  coalesce(shares::VARCHAR, '') || '|' || coalesce(price::VARCHAR, '') || '|' ||
                  coalesce(acquired_disposed, '') || '|' || coalesce(shares_after::VARCHAR, ''), ';'
                  ORDER BY trans_date, trans_code, shares, price, acquired_disposed, shares_after) AS signature
FROM form345_nonderiv_trans GROUP BY accession;

CREATE OR REPLACE TEMP TABLE _links AS
WITH am AS (
    SELECT accession, issuer_cik, base_form, date_of_orig_sub, period_of_report, filing_date
    FROM form345_submissions WHERE is_amendment
), t1 AS (
    SELECT a.accession AS amend_acc, o.accession AS target_acc, o.is_amendment AS target_is_amend,
           (o.period_of_report IS NOT DISTINCT FROM a.period_of_report) AS same_period, 1 AS tier
    FROM am a
    JOIN form345_submissions o
      ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form
     AND o.filing_date = a.date_of_orig_sub AND o.accession <> a.accession
     AND (o.filing_date < a.filing_date
          OR (o.filing_date = a.filing_date AND (NOT o.is_amendment OR o.accession < a.accession)))
    WHERE a.date_of_orig_sub IS NOT NULL
      AND EXISTS (SELECT 1 FROM form345_owners x JOIN form345_owners y ON y.owner_cik = x.owner_cik
                  WHERE x.accession = a.accession AND y.accession = o.accession)
), t2 AS (
    SELECT a.accession AS amend_acc, o.accession AS target_acc, o.is_amendment AS target_is_amend,
           (o.period_of_report IS NOT DISTINCT FROM a.period_of_report) AS same_period, 2 AS tier
    FROM am a
    JOIN _sig sa ON sa.accession = a.accession
    JOIN form345_submissions o
      ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
     AND o.filing_date <= a.filing_date AND o.filing_date >= a.filing_date - INTERVAL 45 DAY
     AND (o.filing_date < a.filing_date OR NOT o.is_amendment OR o.accession < a.accession)
    JOIN _sig so ON so.accession = o.accession AND so.signature = sa.signature
    WHERE NOT EXISTS (SELECT 1 FROM t1 WHERE t1.amend_acc = a.accession)
      AND EXISTS (SELECT 1 FROM form345_owners x JOIN form345_owners y ON y.owner_cik = x.owner_cik
                  WHERE x.accession = a.accession AND y.accession = o.accession)
), t3 AS (
    SELECT a.accession AS amend_acc, o.accession AS target_acc, o.is_amendment AS target_is_amend,
           TRUE AS same_period, 3 AS tier
    FROM am a
    JOIN form345_submissions o
      ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
     AND NOT o.is_amendment AND o.period_of_report = a.period_of_report
     AND o.filing_date <= a.filing_date AND o.filing_date >= a.filing_date - INTERVAL 45 DAY
    WHERE a.period_of_report IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM t1 WHERE t1.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t2 WHERE t2.amend_acc = a.accession)
      AND EXISTS (SELECT 1 FROM form345_owners x JOIN form345_owners y ON y.owner_cik = x.owner_cik
                  WHERE x.accession = a.accession AND y.accession = o.accession)
    QUALIFY count(*) OVER (PARTITION BY a.accession) = 1
), t4 AS (
    SELECT a.accession AS amend_acc, o.accession AS target_acc, o.is_amendment AS target_is_amend,
           TRUE AS same_period, 4 AS tier
    FROM am a
    JOIN _sigx sa ON sa.accession = a.accession
    JOIN form345_submissions o
      ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
     AND o.filing_date = a.date_of_orig_sub
     AND o.period_of_report IS NOT DISTINCT FROM a.period_of_report
     AND (o.filing_date < a.filing_date
          OR (o.filing_date = a.filing_date AND (NOT o.is_amendment OR o.accession < a.accession)))
    JOIN _sigx so ON so.accession = o.accession AND so.signature = sa.signature
    WHERE a.date_of_orig_sub IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM t1 WHERE t1.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t2 WHERE t2.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t3 WHERE t3.amend_acc = a.accession)
    QUALIFY count(*) OVER (PARTITION BY a.accession) = 1
), t5 AS (
    SELECT a.accession AS amend_acc, o.accession AS target_acc, o.is_amendment AS target_is_amend,
           TRUE AS same_period, 5 AS tier
    FROM am a
    JOIN _sig sa ON sa.accession = a.accession
    JOIN form345_submissions o
      ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
     AND o.filing_date = a.date_of_orig_sub
     AND o.period_of_report IS NOT DISTINCT FROM a.period_of_report
     AND split_part(o.accession, '-', 1) = split_part(a.accession, '-', 1)
     AND (o.filing_date < a.filing_date
          OR (o.filing_date = a.filing_date AND (NOT o.is_amendment OR o.accession < a.accession)))
    JOIN _sig so ON so.accession = o.accession AND so.signature = sa.signature
    WHERE a.date_of_orig_sub IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM t1 WHERE t1.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t2 WHERE t2.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t3 WHERE t3.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t4 WHERE t4.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM form345_submissions p
                      JOIN form345_owners po ON po.accession = p.accession
                      JOIN form345_owners ao ON ao.owner_cik = po.owner_cik AND ao.accession = a.accession
                      WHERE p.issuer_cik = a.issuer_cik AND p.base_form = a.base_form AND p.accession <> a.accession
                        AND p.period_of_report IS NOT DISTINCT FROM a.period_of_report
                        AND (p.filing_date < a.filing_date
                             OR (p.filing_date = a.filing_date AND p.accession < a.accession)))
    QUALIFY count(*) OVER (PARTITION BY a.accession) = 1
), t6 AS (
    SELECT a.accession AS amend_acc, o.accession AS target_acc, o.is_amendment AS target_is_amend,
           TRUE AS same_period, 6 AS tier
    FROM am a
    JOIN form345_submissions o
      ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
     AND NOT o.is_amendment AND o.period_of_report = a.period_of_report
     AND o.filing_date <= a.filing_date
    WHERE a.period_of_report IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM t1 WHERE t1.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t2 WHERE t2.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t3 WHERE t3.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t4 WHERE t4.amend_acc = a.accession)
      AND NOT EXISTS (SELECT 1 FROM t5 WHERE t5.amend_acc = a.accession)
      AND EXISTS (SELECT 1 FROM form345_owners x JOIN form345_owners y ON y.owner_cik = x.owner_cik
                  WHERE x.accession = a.accession AND y.accession = o.accession)
    QUALIFY count(*) OVER (PARTITION BY a.accession) = 1
), cand AS (
    SELECT * FROM t1 UNION ALL SELECT * FROM t2 UNION ALL SELECT * FROM t3 UNION ALL SELECT * FROM t4
    UNION ALL SELECT * FROM t5 UNION ALL SELECT * FROM t6
), scored AS (
    SELECT *, (CASE WHEN same_period THEN 2 ELSE 0 END) + (CASE WHEN target_is_amend THEN 0 ELSE 1 END) AS score
    FROM cand
), best AS (
    SELECT *, max(score) OVER (PARTITION BY amend_acc) AS best_score FROM scored
)
SELECT amend_acc, target_acc, target_is_amend, score, tier,
       count(*) OVER (PARTITION BY amend_acc) AS n_targets
FROM best WHERE score = best_score;

-- tier-3 candidates that were NOT unique (left unlinked), for the diagnostics
CREATE OR REPLACE TEMP TABLE _tier3_ambiguous AS
WITH am AS (SELECT accession, issuer_cik, base_form, period_of_report, filing_date FROM form345_submissions WHERE is_amendment)
SELECT a.accession AS amend_acc, count(*) AS n
FROM am a
JOIN form345_submissions o
  ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
 AND NOT o.is_amendment AND o.period_of_report = a.period_of_report
 AND o.filing_date <= a.filing_date AND o.filing_date >= a.filing_date - INTERVAL 45 DAY
WHERE NOT EXISTS (SELECT 1 FROM _links l WHERE l.amend_acc = a.accession)
  AND EXISTS (SELECT 1 FROM form345_owners x JOIN form345_owners y ON y.owner_cik = x.owner_cik
              WHERE x.accession = a.accession AND y.accession = o.accession)
GROUP BY 1 HAVING count(*) > 1;

-- tier-4 candidates that were NOT unique (identical filings by several owners), for the diagnostics
CREATE OR REPLACE TEMP TABLE _tier4_ambiguous AS
WITH am AS (SELECT accession, issuer_cik, base_form, date_of_orig_sub, period_of_report, filing_date FROM form345_submissions WHERE is_amendment)
SELECT a.accession AS amend_acc, count(*) AS n
FROM am a
JOIN _sigx sa ON sa.accession = a.accession
JOIN form345_submissions o
  ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
 AND o.filing_date = a.date_of_orig_sub AND o.period_of_report IS NOT DISTINCT FROM a.period_of_report
JOIN _sigx so ON so.accession = o.accession AND so.signature = sa.signature
WHERE NOT EXISTS (SELECT 1 FROM _links l WHERE l.amend_acc = a.accession)
GROUP BY 1 HAVING count(*) > 1;

-- tier-5 candidates that were refused (several same-submitter filings with the
-- identical core transaction on the named day, or a guard failed), for the diagnostics
CREATE OR REPLACE TEMP TABLE _tier5_refused AS
WITH am AS (SELECT accession, issuer_cik, base_form, date_of_orig_sub, period_of_report, filing_date FROM form345_submissions WHERE is_amendment)
SELECT a.accession AS amend_acc, count(*) AS n
FROM am a
JOIN _sig sa ON sa.accession = a.accession
JOIN form345_submissions o
  ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
 AND o.filing_date = a.date_of_orig_sub AND o.period_of_report IS NOT DISTINCT FROM a.period_of_report
 AND (o.filing_date < a.filing_date OR (o.filing_date = a.filing_date AND (NOT o.is_amendment OR o.accession < a.accession)))
JOIN _sig so ON so.accession = o.accession AND so.signature = sa.signature
WHERE NOT EXISTS (SELECT 1 FROM _links l WHERE l.amend_acc = a.accession)
GROUP BY 1;

-- tier-6 candidates that were NOT unique (the owner filed more than one original for the
-- period), for the diagnostics; those already counted as tier-3 ambiguous are excluded
CREATE OR REPLACE TEMP TABLE _tier6_ambiguous AS
WITH am AS (SELECT accession, issuer_cik, base_form, period_of_report, filing_date FROM form345_submissions WHERE is_amendment)
SELECT a.accession AS amend_acc, count(*) AS n
FROM am a
JOIN form345_submissions o
  ON o.issuer_cik = a.issuer_cik AND o.base_form = a.base_form AND o.accession <> a.accession
 AND NOT o.is_amendment AND o.period_of_report = a.period_of_report AND o.filing_date <= a.filing_date
WHERE NOT EXISTS (SELECT 1 FROM _links l WHERE l.amend_acc = a.accession)
  AND NOT EXISTS (SELECT 1 FROM _tier3_ambiguous t WHERE t.amend_acc = a.accession)
  AND EXISTS (SELECT 1 FROM form345_owners x JOIN form345_owners y ON y.owner_cik = x.owner_cik
              WHERE x.accession = a.accession AND y.accession = o.accession)
GROUP BY 1 HAVING count(*) > 1;
"""

GROUP_SQL = """
-- Walk each amendment's link chain to its root (a filing with no outgoing
-- link: the original, or an amendment whose own original is outside the
-- data). Links always point to an earlier filing date, or on the same day to
-- an original or a lower-accession amendment, so the walk is acyclic; the
-- depth cap is a guard only. The group of a root = the root plus every
-- amendment that reaches it; its EFFECTIVE member is the latest filed, an
-- amendment beating an original filed the same day, then the higher accession
-- (submission order within a filer agent's sequence).
CREATE OR REPLACE TEMP TABLE _groups AS
WITH RECURSIVE chain(start_acc, cur_acc, depth) AS (
    SELECT amend_acc, target_acc, 1 FROM _links
    UNION ALL
    SELECT c.start_acc, l.target_acc, c.depth + 1
    FROM chain c JOIN _links l ON l.amend_acc = c.cur_acc
    WHERE c.depth < 12
), roots AS (
    SELECT DISTINCT c.start_acc AS accession, c.cur_acc AS root_acc
    FROM chain c
    WHERE NOT EXISTS (SELECT 1 FROM _links l WHERE l.amend_acc = c.cur_acc)
), members AS (
    SELECT root_acc, accession FROM roots
    UNION
    SELECT root_acc, root_acc FROM roots
), ranked AS (
    SELECT m.root_acc, m.accession,
           row_number() OVER (PARTITION BY m.root_acc
                              ORDER BY s.filing_date DESC, s.is_amendment DESC, s.accession DESC) AS rn
    FROM members m JOIN form345_submissions s ON s.accession = m.accession
)
SELECT r.root_acc, r.accession, e.accession AS effective_acc
FROM ranked r JOIN ranked e ON e.root_acc = r.root_acc AND e.rn = 1;
"""


def apply_supersession(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """Recompute ``superseded_by`` from scratch (D10/D14). Returns diagnostics.

    Every amendment also records its link outcome: ``amends_accession`` (the
    filing it was paired with; for an ambiguous top-score tie, the lowest
    accession among them) and ``link_tier`` (1-6). Both stay NULL for an
    amendment no tier could pair - kept, counted and reported per D14 - so the
    residual in DATA.md is reproducible from the database with one query."""
    con.execute('UPDATE form345_submissions SET superseded_by = NULL, amends_accession = NULL, link_tier = NULL')
    con.execute(SUPERSESSION_SQL)
    con.execute(GROUP_SQL)
    con.execute("""
        UPDATE form345_submissions s SET superseded_by = g.effective_acc
        FROM _groups g WHERE s.accession = g.accession AND g.effective_acc <> s.accession""")
    con.execute("""
        UPDATE form345_submissions s SET amends_accession = l.target_acc, link_tier = l.tier
        FROM (SELECT amend_acc, min(target_acc) AS target_acc, min(tier) AS tier FROM _links GROUP BY 1) l
        WHERE s.accession = l.amend_acc""")
    n_amend = int(con.execute('SELECT count(*) FROM form345_submissions WHERE is_amendment').fetchone()[0])
    n_linked = int(con.execute('SELECT count(DISTINCT amend_acc) FROM _links').fetchone()[0])
    n_ambiguous = int(con.execute('SELECT count(DISTINCT amend_acc) FROM _links WHERE n_targets > 1').fetchone()[0])
    tiers = dict(con.execute('SELECT tier, count(DISTINCT amend_acc) FROM _links GROUP BY 1').fetchall())
    n_t3_amb = int(con.execute('SELECT count(*) FROM _tier3_ambiguous').fetchone()[0])
    n_t4_amb = int(con.execute('SELECT count(*) FROM _tier4_ambiguous').fetchone()[0])
    n_t5_ref = int(con.execute('SELECT count(*) FROM _tier5_refused').fetchone()[0])
    n_t6_amb = int(con.execute('SELECT count(*) FROM _tier6_ambiguous').fetchone()[0])
    n_superseded = int(con.execute('SELECT count(*) FROM form345_submissions WHERE superseded_by IS NOT NULL').fetchone()[0])
    n_unlinked_amend = int(con.execute("""
        SELECT count(*) FROM form345_submissions s WHERE is_amendment
        AND NOT EXISTS (SELECT 1 FROM _links l WHERE l.amend_acc = s.accession)""").fetchone()[0])
    return {'amendments': n_amend, 'amendments_linked': n_linked, 'amendments_ambiguous': n_ambiguous,
            'linked_tier1_date': int(tiers.get(1, 0)), 'linked_tier2_content': int(tiers.get(2, 0)),
            'linked_tier3_period': int(tiers.get(3, 0)), 'tier3_ambiguous_unlinked': n_t3_amb,
            'linked_tier4_owner_free': int(tiers.get(4, 0)), 'tier4_ambiguous_unlinked': n_t4_amb,
            'linked_tier5_owner_free_core': int(tiers.get(5, 0)), 'tier5_refused_unlinked': n_t5_ref,
            'linked_tier6_owner_period_nowindow': int(tiers.get(6, 0)), 'tier6_ambiguous_unlinked': n_t6_amb,
            'amendments_unlinked': n_unlinked_amend, 'filings_superseded': n_superseded}


RESIDUAL_DUPLICATES_SQL = """
-- Effective Form 4 filings (after supersession) by the same issuer and owner,
-- filed within 10 days of each other, whose non-derivative transaction lists
-- are IDENTICAL (date, code, shares, price, A/D). Pairs involving an amendment
-- are the shape a missed 4/A link leaves behind (SWM 0001062993-22-012373 was
-- one); pairs of two originals are either genuine re-filings or two identical
-- trades reported separately and are only counted.
WITH eff AS (
    SELECT accession, issuer_cik, filing_date, is_amendment
    FROM form345_submissions WHERE superseded_by IS NULL AND base_form = '4'
), sig AS (
    SELECT e.accession, e.issuer_cik, e.filing_date, e.is_amendment, o.owner_cik,
           string_agg(coalesce(t.trans_date::VARCHAR, '') || '|' || coalesce(t.trans_code, '') || '|' ||
                      coalesce(t.shares::VARCHAR, '') || '|' || coalesce(t.price::VARCHAR, '') || '|' ||
                      coalesce(t.acquired_disposed, ''), ';'
                      ORDER BY t.trans_date, t.trans_code, t.shares, t.price, t.acquired_disposed) AS signature
    FROM eff e JOIN form345_owners o USING (accession) JOIN form345_nonderiv_trans t USING (accession)
    GROUP BY 1, 2, 3, 4, 5
)
SELECT count(*) FILTER (WHERE a.is_amendment OR b.is_amendment) AS dup_pairs_amend,
       count(*) AS dup_pairs_any
FROM sig a JOIN sig b
  ON b.issuer_cik = a.issuer_cik AND b.owner_cik = a.owner_cik AND b.signature = a.signature
 AND b.accession < a.accession AND abs(a.filing_date - b.filing_date) <= 10
"""

RESIDUAL_DUPLICATES_OWNER_FREE_SQL = """
-- Same as above but ignoring the owner and using the extended signature (with
-- shares owned after): the shape an unmatched
-- owner-identity correction leaves behind (CYAN 0001010549-14-000096 was one).
WITH eff AS (
    SELECT accession, issuer_cik, filing_date, is_amendment, period_of_report
    FROM form345_submissions WHERE superseded_by IS NULL AND base_form = '4'
), sig AS (
    SELECT e.accession, e.issuer_cik, e.filing_date, e.is_amendment, e.period_of_report,
           string_agg(coalesce(t.trans_date::VARCHAR, '') || '|' || coalesce(t.trans_code, '') || '|' ||
                      coalesce(t.shares::VARCHAR, '') || '|' || coalesce(t.price::VARCHAR, '') || '|' ||
                      coalesce(t.acquired_disposed, '') || '|' || coalesce(t.shares_after::VARCHAR, ''), ';'
                      ORDER BY t.trans_date, t.trans_code, t.shares, t.price, t.acquired_disposed, t.shares_after) AS signature
    FROM eff e JOIN form345_nonderiv_trans t USING (accession)
    GROUP BY 1, 2, 3, 4, 5
)
SELECT count(*) AS dup_pairs_amend_owner_free
FROM sig a JOIN sig b
  ON b.issuer_cik = a.issuer_cik AND b.signature = a.signature
 AND b.period_of_report IS NOT DISTINCT FROM a.period_of_report
 AND b.accession < a.accession AND abs(a.filing_date - b.filing_date) <= 10
WHERE a.is_amendment OR b.is_amendment
"""


def residual_duplicates(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """How many identical effective filing pairs supersession left behind (see SQL)."""
    row = con.execute(RESIDUAL_DUPLICATES_SQL).fetchone()
    row2 = con.execute(RESIDUAL_DUPLICATES_OWNER_FREE_SQL).fetchone()
    return {'dup_pairs_amend': int(row[0] or 0), 'dup_pairs_any': int(row[1] or 0),
            'dup_pairs_amend_owner_free': int(row2[0] or 0)}


def fill_tickers(con: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """A filing with no usable symbol inherits the issuer's most recent EARLIER
    normalised symbol (what was known at the time), else its earliest later one."""
    con.execute("UPDATE form345_submissions SET ticker = NULL, ticker_source = NULL WHERE ticker_source IS DISTINCT FROM 'filed'")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _fill AS
        SELECT accession,
               coalesce(
                 last_value(ticker IGNORE NULLS) OVER (PARTITION BY issuer_cik ORDER BY filing_date, accession
                                                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                 first_value(ticker IGNORE NULLS) OVER (PARTITION BY issuer_cik ORDER BY filing_date, accession
                                                         ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)) AS fill_ticker
        FROM form345_submissions""")
    con.execute("""
        UPDATE form345_submissions s SET ticker = f.fill_ticker, ticker_source = 'issuer_fill'
        FROM _fill f WHERE s.accession = f.accession AND s.ticker IS NULL AND f.fill_ticker IS NOT NULL""")
    row = con.execute("""
        SELECT count(*) FILTER (WHERE ticker_source = 'filed'), count(*) FILTER (WHERE ticker_source = 'issuer_fill'),
               count(*) FILTER (WHERE ticker IS NULL) FROM form345_submissions""").fetchone()
    return {'ticker_filed': int(row[0]), 'ticker_issuer_fill': int(row[1]), 'ticker_missing': int(row[2])}


def fill_usable_from(con: duckdb.DuckDBPyConnection) -> int:
    """D10: ``usable_from`` = first XNYS session strictly after ``filing_date``."""
    dates = con.execute('SELECT DISTINCT filing_date FROM form345_submissions WHERE filing_date IS NOT NULL ORDER BY 1').fetchdf()
    if len(dates) == 0:
        return 0
    fd = pd.to_datetime(dates['filing_date'])
    sessions = nyse_sessions(fd.min() - pd.Timedelta(days=7), fd.max() + pd.Timedelta(days=45))
    dates['usable_from'] = usable_from_sessions(fd, sessions).dt.date
    con.register('_uf', dates)
    try:
        con.execute('UPDATE form345_submissions s SET usable_from = u.usable_from FROM _uf u WHERE s.filing_date = u.filing_date')
    finally:
        con.unregister('_uf')
    return int(len(dates))


def finalize(con: duckdb.DuckDBPyConnection, note: str = '') -> Dict[str, Any]:
    """Recompute every derived column and record build info."""
    diag: Dict[str, Any] = {}
    diag.update(apply_supersession(con))
    diag.update(residual_duplicates(con))
    diag.update(fill_tickers(con))
    diag['filing_dates'] = fill_usable_from(con)
    bad = con.execute('SELECT count(*) FROM form345_submissions WHERE usable_from IS NULL OR usable_from <= filing_date').fetchone()[0]
    if bad:
        raise RuntimeError(f'{bad} submissions have usable_from missing or not after filing_date (D10)')
    con.execute("INSERT OR REPLACE INTO form345_build_info VALUES ('finalized_at', ?)",
                [dt.datetime.now(dt.timezone.utc).isoformat()])
    con.execute("INSERT OR REPLACE INTO form345_build_info VALUES ('note', ?)", [note])
    con.execute("INSERT OR REPLACE INTO form345_build_info VALUES ('diagnostics', ?)", [json.dumps(diag)])
    return diag


# --------------------------------------------------------------------------- build / derive / check

def build_database(zips: Sequence[Tuple[str, Path]], db_path: Path, force: bool = False,
                   verbose: bool = True, note: str = '') -> Dict[str, Any]:
    """Ingest ``[(quarter, zip_path), ...]`` into ``db_path`` (created if absent),
    skipping quarters whose zip SHA-256 is already recorded unless ``force``."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        ensure_schema(con)
        known = {q: sha for q, sha in con.execute('SELECT quarter, zip_sha256 FROM form345_quarters').fetchall()}
        loaded, skipped = [], []
        for quarter, path in zips:
            sha = sha256_of(path)
            if not force and known.get(quarter) == sha:
                skipped.append(quarter)
                continue
            t0 = time.monotonic()
            frames = parse_zip(path, quarter)
            counts = ingest_quarter(con, quarter, frames, sha, path.stat().st_size, BASE_URL + zip_name(quarter))
            loaded.append(quarter)
            if verbose:
                print(f'  {quarter}: {counts["n_submissions"]:>7,} filings {counts["n_nonderiv"]:>8,} non-deriv '
                      f'{counts["n_deriv"]:>7,} deriv  dup_acc={counts["n_dup_accessions"]}  {time.monotonic() - t0:4.1f}s')
        diag = finalize(con, note=note)
        diag['quarters_loaded'] = loaded
        diag['quarters_skipped'] = skipped
        if verbose:
            print('  derived:', json.dumps({k: v for k, v in diag.items() if not k.startswith('quarters_')}))
        return diag
    finally:
        con.close()


def derive_dev_copy(full_db: Path, dev_db: Path, cutoff: dt.date, verbose: bool = True) -> Dict[str, Any]:
    """``dev_db`` = ``full_db`` restricted to FILING_DATE <= cutoff, with
    supersession/tickers/usable_from recomputed on the truncated rows (D10).
    The dev file is rebuilt from scratch (written to a temp file, then swapped in)."""
    tmp = dev_db.with_suffix('.duckdb.building')
    tmp.unlink(missing_ok=True)
    for extra in (tmp.with_suffix('.building.wal'),):
        extra.unlink(missing_ok=True)
    dev_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(tmp))
    try:
        ensure_schema(con)
        con.execute(f"ATTACH '{full_db}' AS src (READ_ONLY)")
        con.execute('INSERT INTO form345_submissions SELECT * FROM src.form345_submissions WHERE filing_date <= ?', [cutoff])
        for table in ('form345_owners', 'form345_nonderiv_trans', 'form345_deriv_trans'):
            con.execute(f"""INSERT INTO {table} SELECT * FROM src.{table} t
                            WHERE EXISTS (SELECT 1 FROM form345_submissions s WHERE s.accession = t.accession)""")
        con.execute("""INSERT INTO form345_quarters
                       SELECT q.quarter, q.zip_sha256, q.zip_bytes, q.source_url,
                              (SELECT count(*) FROM form345_submissions s WHERE s.quarter = q.quarter),
                              (SELECT count(*) FROM form345_owners s WHERE s.quarter = q.quarter),
                              (SELECT count(*) FROM form345_nonderiv_trans s WHERE s.quarter = q.quarter),
                              (SELECT count(*) FROM form345_deriv_trans s WHERE s.quarter = q.quarter),
                              q.n_dup_accessions, q.loaded_at
                       FROM src.form345_quarters q""")
        con.execute('DETACH src')
        diag = finalize(con, note=f'dev copy truncated to filing_date <= {cutoff.isoformat()} (derived from {full_db})')
        con.execute("INSERT OR REPLACE INTO form345_build_info VALUES ('dev_cutoff', ?)", [cutoff.isoformat()])
    finally:
        con.close()
    for stale in (dev_db, dev_db.with_suffix('.duckdb.wal')):
        stale.unlink(missing_ok=True)
    os.replace(tmp, dev_db)
    if verbose:
        print(f'  dev copy {dev_db}: {json.dumps(diag)}')
    return diag


def check_cutoff(db_path: Path, cutoff: dt.date) -> Dict[str, Any]:
    """Prove no row in ``db_path`` was filed after ``cutoff`` (all four tables,
    joined through the submission's filing date). Raises on any violation."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        out: Dict[str, Any] = {'db': str(db_path), 'cutoff': cutoff.isoformat()}
        out['submissions_after'] = int(con.execute(
            'SELECT count(*) FROM form345_submissions WHERE filing_date > ? OR filing_date IS NULL', [cutoff]).fetchone()[0])
        for table in ('form345_owners', 'form345_nonderiv_trans', 'form345_deriv_trans'):
            out[f'{table}_after'] = int(con.execute(f"""
                SELECT count(*) FROM {table} t LEFT JOIN form345_submissions s ON s.accession = t.accession
                WHERE s.accession IS NULL OR s.filing_date > ?""", [cutoff]).fetchone()[0])
        out['max_filing_date'] = str(con.execute('SELECT max(filing_date) FROM form345_submissions').fetchone()[0])
        out['rows'] = int(con.execute('SELECT count(*) FROM form345_submissions').fetchone()[0])
    finally:
        con.close()
    bad = {k: v for k, v in out.items() if k.endswith('_after') and v}
    if bad:
        raise AssertionError(f'{db_path} has rows filed after {cutoff}: {bad}')
    return out


# --------------------------------------------------------------------------- report

def _pit_member_months(history_db: Path, top_n: int = 500) -> pd.DataFrame:
    """(ticker, month) PIT membership: rank <= top_n at the END of the prior
    month (the rule of scripts/pit_panel.py, reproduced in SQL for the report)."""
    con = duckdb.connect(str(history_db), read_only=True)
    try:
        df = con.execute("""
            WITH last_day AS (
                SELECT ticker, date_trunc('month', day) AS m, arg_max(dv_rank, day) AS rank_end
                FROM pit_daily_bars WHERE close > 0.01 AND close < 100000 GROUP BY 1, 2)
            SELECT ticker, (m + INTERVAL 1 MONTH)::DATE AS member_month
            FROM last_day WHERE rank_end <= ?""", [top_n]).fetchdf()
    finally:
        con.close()
    return df


def report(db_path: Path, history_db: Optional[Path], start: str = '2016-09-01', end: str = '2024-06-30',
           main_db: Optional[Path] = None) -> str:
    """Markdown statistics for research/lowfreq/insider/DATA.md."""
    con = duckdb.connect(str(db_path), read_only=True)
    lines: List[str] = []
    try:
        info = dict(con.execute('SELECT key, value FROM form345_build_info').fetchall())
        lines.append(f'Database: `{db_path}`  finalized {info.get("finalized_at", "?")}')
        lines.append('')
        lines.append('### Rows per quarter (as loaded, before supersession)')
        lines.append('')
        lines.append('| quarter | filings | owners | non-deriv trans | deriv trans | 4/A | superseded |')
        lines.append('|---|---:|---:|---:|---:|---:|---:|')
        q = con.execute("""
            SELECT q.quarter, q.n_submissions, q.n_owners, q.n_nonderiv, q.n_deriv,
                   (SELECT count(*) FROM form345_submissions s WHERE s.quarter = q.quarter AND s.document_type = '4/A'),
                   (SELECT count(*) FROM form345_submissions s WHERE s.quarter = q.quarter AND s.superseded_by IS NOT NULL)
            FROM form345_quarters q ORDER BY q.quarter""").fetchall()
        for r in q:
            lines.append(f'| {r[0]} | {r[1]:,} | {r[2]:,} | {r[3]:,} | {r[4]:,} | {r[5]:,} | {r[6]:,} |')
        tot = con.execute("""SELECT count(*), sum(n_submissions), sum(n_nonderiv), sum(n_deriv) FROM form345_quarters""").fetchone()
        lines.append(f'| **{tot[0]} quarters** | **{tot[1]:,}** | | **{tot[2]:,}** | **{tot[3]:,}** | | |')
        lines.append('')
        lines.append('Derived-column diagnostics: `' + info.get('diagnostics', '{}') + '`')
        lines.append('')
        lines.append('### Open-market purchases (code P, Form 4, effective filings) per filing year')
        lines.append('')
        lines.append('| year | P transactions | P filings | issuers (CIK) | matched PIT tickers |')
        lines.append('|---|---:|---:|---:|---:|')
        pit_tickers: set = set()
        if history_db is not None and history_db.exists():
            hcon = duckdb.connect(str(history_db), read_only=True)
            try:
                pit_tickers = set(hcon.execute('SELECT DISTINCT ticker FROM pit_daily_bars').fetchdf()['ticker'])
            finally:
                hcon.close()
        p = con.execute("""
            SELECT extract(year FROM s.filing_date) AS y, count(*), count(DISTINCT s.accession),
                   count(DISTINCT s.issuer_cik), list(DISTINCT s.ticker)
            FROM form345_nonderiv_trans t JOIN form345_submissions s ON s.accession = t.accession
            WHERE t.trans_code = 'P' AND s.base_form = '4' AND s.superseded_by IS NULL
            GROUP BY 1 ORDER BY 1""").fetchall()
        for r in p:
            matched = len([x for x in (r[4] or []) if x in pit_tickers]) if pit_tickers else 0
            lines.append(f'| {int(r[0])} | {r[1]:,} | {r[2]:,} | {r[3]:,} | {matched:,} |')
        lines.append('')
        # D14: amendments no tier could pair with an original are kept and counted; report their
        # weight in the P-code purchases a strategy would read (all tickers, and PIT tickers only).
        lines.append('### Unmatched amendments (D14: kept, counted)')
        lines.append('')
        n_unm, n_unm_eff = con.execute("""
            SELECT count(*), count(*) FILTER (WHERE superseded_by IS NULL)
            FROM form345_submissions WHERE is_amendment AND link_tier IS NULL""").fetchone()
        lines.append(f'- amendments no tier could pair with an original: {n_unm:,} ({n_unm_eff:,} effective)')
        for label, tick_filter, params in (('all tickers', '', []),
                                           ('PIT tickers', 'AND s.ticker IN (SELECT unnest(?::VARCHAR[]))', [sorted(pit_tickers)])):
            if label == 'PIT tickers' and not pit_tickers:
                continue
            # Value sums exclude data-entry errors where the price field carries a dollar total
            # (DNP 0000899243-16-025693: 47,000,000 shares "at" $47,000,000): a row above $10 B or
            # $100,000 per share is not a purchase anyone made. The excluded count is reported.
            r = con.execute(f"""
                WITH eff_p AS (
                    SELECT (s.is_amendment AND s.link_tier IS NULL) AS unmatched, t.shares, t.price,
                           (t.price > 0 AND t.price <= 1e5 AND isfinite(t.price) AND isfinite(t.shares)
                            AND t.shares * t.price <= 1e10) AS valued
                    FROM form345_nonderiv_trans t JOIN form345_submissions s ON s.accession = t.accession
                    WHERE t.trans_code = 'P' AND s.base_form = '4' AND s.superseded_by IS NULL {tick_filter})
                SELECT count(*) FILTER (WHERE unmatched), count(*),
                       coalesce(sum(shares * price) FILTER (WHERE unmatched AND valued), 0),
                       coalesce(sum(shares * price) FILTER (WHERE valued), 0),
                       count(*) FILTER (WHERE NOT valued AND price > 0)
                FROM eff_p""", params).fetchone()
            lines.append(f'- {label}: their P transactions {r[0]:,} of {r[1]:,} effective Form 4 P transactions '
                         f'({100.0 * r[0] / max(r[1], 1):.2f}%), purchase value ${r[2]:,.0f} of ${r[3]:,.0f} '
                         f'({100.0 * r[2] / max(r[3], 1):.2f}%; {r[4]:,} rows above $10 B or $100,000/share excluded)')
        lines.append('')
        if history_db is not None and history_db.exists():
            mm = _pit_member_months(history_db)
            mm['member_month'] = pd.to_datetime(mm['member_month'])
            mm = mm[(mm['member_month'] >= pd.Timestamp(start)) & (mm['member_month'] <= pd.Timestamp(end))]
            f4 = con.execute("""
                SELECT DISTINCT s.ticker, date_trunc('month', s.usable_from)::DATE AS m
                FROM form345_submissions s
                WHERE s.base_form = '4' AND s.superseded_by IS NULL AND s.ticker IS NOT NULL
                  AND EXISTS (SELECT 1 FROM form345_nonderiv_trans t WHERE t.accession = s.accession)""").fetchdf()
            f4p = con.execute("""
                SELECT DISTINCT s.ticker, date_trunc('month', s.usable_from)::DATE AS m
                FROM form345_submissions s JOIN form345_nonderiv_trans t ON t.accession = s.accession
                WHERE s.base_form = '4' AND s.superseded_by IS NULL AND s.ticker IS NOT NULL AND t.trans_code = 'P'""").fetchdf()
            any_ticker = set(con.execute('SELECT DISTINCT ticker FROM form345_submissions WHERE ticker IS NOT NULL').fetchdf()['ticker'])
            key_all = set(zip(f4['ticker'], pd.to_datetime(f4['m']).dt.date))
            key_p = set(zip(f4p['ticker'], pd.to_datetime(f4p['m']).dt.date))
            mm_keys = list(zip(mm['ticker'], pd.to_datetime(mm['member_month']).dt.date))
            n = len(mm_keys)
            hit_all = sum(1 for k in mm_keys if k in key_all)
            hit_p = sum(1 for k in mm_keys if k in key_p)
            ever = mm['ticker'].isin(any_ticker)
            lines.append(f'### PIT top-500 coverage ({start} .. {end})')
            lines.append('')
            lines.append(f'- member-months: {n:,}; with >= 1 effective Form 4 non-derivative filing usable that month: '
                         f'{hit_all:,} ({100.0 * hit_all / max(n, 1):.1f}%); with >= 1 P purchase: {hit_p:,} ({100.0 * hit_p / max(n, 1):.1f}%)')
            lines.append(f'- member-months whose ticker NEVER appears as an issuer symbol in the Form 3/4/5 data: '
                         f'{int((~ever).sum()):,} ({100.0 * (~ever).mean():.1f}%) over {mm.loc[~ever, "ticker"].nunique():,} tickers')
            top_unmatched = (mm[~ever].groupby('ticker').size().sort_values(ascending=False).head(25))
            lines.append('- largest unmatched tickers by member-months: ' + ', '.join(f'{t} ({c})' for t, c in top_unmatched.items()))
            # Split the residue: a ticker with 10-K/10-Q fundamentals is an operating company whose
            # insiders MUST file Form 4 (a genuine matching failure); one without is an ETF/ADR/trust.
            if main_db is not None and main_db.exists():
                mcon = duckdb.connect(str(main_db), read_only=True)
                try:
                    fund = set(mcon.execute('SELECT DISTINCT ticker FROM fundamentals WHERE ticker IS NOT NULL').fetchdf()['ticker'])
                finally:
                    mcon.close()
                op = mm['ticker'].isin(fund)
                n_op = int(op.sum())
                hit_op = sum(1 for k, o in zip(mm_keys, op) if o and k in key_all)
                res_op = mm[~ever & op]
                lines.append(f'- operating companies only (ticker has 10-K/10-Q fundamentals): {n_op:,} member-months, '
                             f'{hit_op:,} ({100.0 * hit_op / max(n_op, 1):.1f}%) with a usable Form 4 that month; '
                             f'unmatched residue {len(res_op):,} member-months ({100.0 * len(res_op) / max(n_op, 1):.1f}%) over '
                             f'{res_op["ticker"].nunique():,} tickers: ' +
                             ', '.join(f'{t} ({c})' for t, c in res_op.groupby('ticker').size().sort_values(ascending=False).head(20).items()))
                res_no = mm[~ever & ~op]
                lines.append(f'- no EDGAR financial filings either (ETFs, ADRs, trusts - no Section 16 insiders): '
                             f'{len(res_no):,} member-months over {res_no["ticker"].nunique():,} tickers')
            lines.append('')
    finally:
        con.close()
    return '\n'.join(lines)


# --------------------------------------------------------------------------- CLI

def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest='cmd', required=True)

    b = sub.add_parser('build', help='download (unless --no-download) and build the full and dev databases')
    b.add_argument('--first', default=FIRST_QUARTER)
    b.add_argument('--last', default='latest', help='e.g. 2026q1, or "latest" (probed with HEAD requests)')
    b.add_argument('--cache-dir', default=DEFAULT_CACHE)
    b.add_argument('--full-db', default=DEFAULT_FULL_DB)
    b.add_argument('--dev-db', default=DEFAULT_DEV_DB, help='"" to skip the dev copy')
    b.add_argument('--dev-cutoff', default=DEFAULT_DEV_CUTOFF)
    b.add_argument('--user-agent', default=os.environ.get('SEC_USER_AGENT', DEFAULT_USER_AGENT))
    b.add_argument('--no-download', action='store_true', help='use only zips already in --cache-dir')
    b.add_argument('--force', action='store_true', help='re-ingest quarters whose zip hash is unchanged')

    c = sub.add_parser('check', help='exit 1 if the database has any row filed after --cutoff')
    c.add_argument('--db', default=DEFAULT_DEV_DB)
    c.add_argument('--cutoff', default=DEFAULT_DEV_CUTOFF)

    r = sub.add_parser('report', help='markdown statistics for DATA.md')
    r.add_argument('--db', default=DEFAULT_FULL_DB)
    r.add_argument('--history-db', default='~/.local/share/mmr/research_full/mmr_history.duckdb')
    r.add_argument('--main-db', default='~/.local/share/mmr/research_full/mmr.duckdb', help='for the fundamentals table (residue split)')
    r.add_argument('--start', default='2016-09-01')
    r.add_argument('--end', default='2024-06-30')

    args = ap.parse_args(argv)
    if args.cmd == 'build':
        cache = Path(os.path.expanduser(args.cache_dir))
        last = args.last
        if last == 'latest':
            if args.no_download:
                cached = sorted(p.name[:6] for p in cache.glob('*_form345.zip'))
                if not cached:
                    print(f'no cached zips in {cache}', file=sys.stderr)
                    return 2
                last = cached[-1]
            else:
                last = latest_published_quarter(args.user_agent)
        quarters = quarter_range(args.first, last)
        print(f'quarters {quarters[0]}..{quarters[-1]} ({len(quarters)}), cache {cache}')
        zips: List[Tuple[str, Path]] = []
        for qtr in quarters:
            if args.no_download:
                path = cache / zip_name(qtr)
                if not path.exists():
                    print(f'  missing {path}', file=sys.stderr)
                    return 2
            else:
                path = download_quarter(qtr, cache, args.user_agent)
            zips.append((qtr, path))
        full_db = Path(os.path.expanduser(args.full_db))
        print(f'building {full_db}')
        build_database(zips, full_db, force=args.force, note=f'full history {quarters[0]}..{quarters[-1]}')
        if args.dev_db:
            dev_db = Path(os.path.expanduser(args.dev_db))
            cutoff = dt.date.fromisoformat(args.dev_cutoff)
            print(f'deriving {dev_db} (filing_date <= {cutoff})')
            derive_dev_copy(full_db, dev_db, cutoff)
            print('check:', json.dumps(check_cutoff(dev_db, cutoff)))
        return 0
    if args.cmd == 'check':
        try:
            out = check_cutoff(Path(os.path.expanduser(args.db)), dt.date.fromisoformat(args.cutoff))
        except AssertionError as exc:
            print(f'FAIL {exc}')
            return 1
        print('OK', json.dumps(out))
        return 0
    if args.cmd == 'report':
        hist = Path(os.path.expanduser(args.history_db)) if args.history_db else None
        main_db = Path(os.path.expanduser(args.main_db)) if args.main_db else None
        print(report(Path(os.path.expanduser(args.db)), hist, start=args.start, end=args.end, main_db=main_db))
        return 0
    return 2


if __name__ == '__main__':
    sys.exit(main())
