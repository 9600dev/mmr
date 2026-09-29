"""SEC Form 3/4/5 insider data: ingest (scripts/fetch_insider_form345.py) and
loader (trader/data/insider.py), proven on a small fixture zip built in-test.

Covers the acceptance criteria of component C03: parse of SUBMISSION /
REPORTINGOWNER / NONDERIV_TRANS into insider.duckdb, 4/A replacement (D10),
idempotent re-runs, the dev copy truncated at the cutoff (with supersession
recomputed on its own rows), and ``load_insider_events`` with ``usable_from``
= first session after the filing date, read from the DB next to the history DB
named by LOWFREQ_RESEARCH_CONFIG, read-only.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import io
import os
import sys
import zipfile
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from trader.data import insider
from trader.data.insider import (
    EVENT_COLUMNS, EXTRA_COLUMNS, insider_db_path, load_insider_events, normalize_symbol,
    parse_relationship, usable_from_sessions,
)

REPO = Path(__file__).resolve().parents[1]


def _load_script():
    spec = importlib.util.spec_from_file_location('fetch_insider_form345', REPO / 'scripts' / 'fetch_insider_form345.py')
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules['fetch_insider_form345'] = mod
    spec.loader.exec_module(mod)
    return mod


F = _load_script()

# --------------------------------------------------------------------------- fixture zips

SUB_COLS = ['ACCESSION_NUMBER', 'FILING_DATE', 'PERIOD_OF_REPORT', 'DATE_OF_ORIG_SUB', 'NO_SECURITIES_OWNED',
            'NOT_SUBJECT_SEC16', 'FORM3_HOLDINGS_REPORTED', 'FORM4_TRANS_REPORTED', 'DOCUMENT_TYPE', 'ISSUERCIK',
            'ISSUERNAME', 'ISSUERTRADINGSYMBOL', 'REMARKS']
OWN_COLS = ['ACCESSION_NUMBER', 'RPTOWNERCIK', 'RPTOWNERNAME', 'RPTOWNER_RELATIONSHIP', 'RPTOWNER_TITLE', 'RPTOWNER_TXT',
            'RPTOWNER_STREET1', 'RPTOWNER_STREET2', 'RPTOWNER_CITY', 'RPTOWNER_STATE', 'RPTOWNER_ZIPCODE',
            'RPTOWNER_STATE_DESC', 'FILE_NUMBER']
NDT_COLS = ['ACCESSION_NUMBER', 'NONDERIV_TRANS_SK', 'SECURITY_TITLE', 'SECURITY_TITLE_FN', 'TRANS_DATE', 'TRANS_DATE_FN',
            'DEEMED_EXECUTION_DATE', 'DEEMED_EXECUTION_DATE_FN', 'TRANS_FORM_TYPE', 'TRANS_CODE', 'EQUITY_SWAP_INVOLVED',
            'EQUITY_SWAP_TRANS_CD_FN', 'TRANS_TIMELINESS', 'TRANS_TIMELINESS_FN', 'TRANS_SHARES', 'TRANS_SHARES_FN',
            'TRANS_PRICEPERSHARE', 'TRANS_PRICEPERSHARE_FN', 'TRANS_ACQUIRED_DISP_CD', 'TRANS_ACQUIRED_DISP_CD_FN',
            'SHRS_OWND_FOLWNG_TRANS', 'SHRS_OWND_FOLWNG_TRANS_FN', 'VALU_OWND_FOLWNG_TRANS', 'VALU_OWND_FOLWNG_TRANS_FN',
            'DIRECT_INDIRECT_OWNERSHIP', 'DIRECT_INDIRECT_OWNERSHIP_FN', 'NATURE_OF_OWNERSHIP', 'NATURE_OF_OWNERSHIP_FN']
DRT_COLS = ['ACCESSION_NUMBER', 'DERIV_TRANS_SK', 'SECURITY_TITLE', 'SECURITY_TITLE_FN', 'CONV_EXERCISE_PRICE',
            'CONV_EXERCISE_PRICE_FN', 'TRANS_DATE', 'TRANS_DATE_FN', 'DEEMED_EXECUTION_DATE', 'DEEMED_EXECUTION_DATE_FN',
            'TRANS_FORM_TYPE', 'TRANS_CODE', 'EQUITY_SWAP_INVOLVED', 'EQUITY_SWAP_TRANS_CD_FN', 'TRANS_TIMELINESS',
            'TRANS_TIMELINESS_FN', 'TRANS_SHARES', 'TRANS_SHARES_FN', 'TRANS_TOTAL_VALUE', 'TRANS_TOTAL_VALUE_FN',
            'TRANS_PRICEPERSHARE', 'TRANS_PRICEPERSHARE_FN', 'TRANS_ACQUIRED_DISP_CD', 'TRANS_ACQUIRED_DISP_CD_FN',
            'EXCERCISE_DATE', 'EXCERCISE_DATE_FN', 'EXPIRATION_DATE', 'EXPIRATION_DATE_FN', 'UNDLYNG_SEC_TITLE',
            'UNDLYNG_SEC_TITLE_FN', 'UNDLYNG_SEC_SHARES', 'UNDLYNG_SEC_SHARES_FN', 'UNDLYNG_SEC_VALUE',
            'UNDLYNG_SEC_VALUE_FN', 'SHRS_OWND_FOLWNG_TRANS', 'SHRS_OWND_FOLWNG_TRANS_FN', 'VALU_OWND_FOLWNG_TRANS',
            'VALU_OWND_FOLWNG_TRANS_FN', 'DIRECT_INDIRECT_OWNERSHIP', 'DIRECT_INDIRECT_OWNERSHIP_FN',
            'NATURE_OF_OWNERSHIP', 'NATURE_OF_OWNERSHIP_FN']


def _tsv(cols, rows):
    out = ['\t'.join(cols)]
    for r in rows:
        d = dict(r)
        out.append('\t'.join(str(d.get(c, '')) for c in cols))
    return ('\n'.join(out) + '\n').encode('utf-8')


def _sub(acc, filing, period, doc, cik, name, symbol, orig=''):
    return {'ACCESSION_NUMBER': acc, 'FILING_DATE': filing, 'PERIOD_OF_REPORT': period, 'DATE_OF_ORIG_SUB': orig,
            'DOCUMENT_TYPE': doc, 'ISSUERCIK': cik, 'ISSUERNAME': name, 'ISSUERTRADINGSYMBOL': symbol}


def _own(acc, cik, name, rel, title=''):
    return {'ACCESSION_NUMBER': acc, 'RPTOWNERCIK': cik, 'RPTOWNERNAME': name, 'RPTOWNER_RELATIONSHIP': rel,
            'RPTOWNER_TITLE': title}


def _ndt(acc, sk, date, code, shares, price, ad, after, form='4', di='D'):
    return {'ACCESSION_NUMBER': acc, 'NONDERIV_TRANS_SK': sk, 'SECURITY_TITLE': 'Common Stock', 'TRANS_DATE': date,
            'TRANS_FORM_TYPE': form, 'TRANS_CODE': code, 'EQUITY_SWAP_INVOLVED': '0', 'TRANS_SHARES': shares,
            'TRANS_PRICEPERSHARE': price, 'TRANS_ACQUIRED_DISP_CD': ad, 'SHRS_OWND_FOLWNG_TRANS': after,
            'DIRECT_INDIRECT_OWNERSHIP': di}


def _zip(path: Path, subs, owns, ndts, drts=()):
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('SUBMISSION.tsv', _tsv(SUB_COLS, subs))
        zf.writestr('REPORTINGOWNER.tsv', _tsv(OWN_COLS, owns))
        zf.writestr('NONDERIV_TRANS.tsv', _tsv(NDT_COLS, ndts))
        zf.writestr('DERIV_TRANS.tsv', _tsv(DRT_COLS, drts))
        zf.writestr('insider_transactions_readme.htm', '<html/>')
    return path


# Fixture story (all dates 2024, so the dev cutoff 2024-06-30 splits the quarters):
#   ACME (CIK 100, symbol ACME)
#     Q2  A1  Form 4 filed Fri 2024-06-14  P 1,000 @ 10.00 by CEO (owner 1)      -> usable Mon 2024-06-17
#         A2  Form 4 filed Fri 2024-06-28  S   500 @ 12.00 by director (owner 2) -> usable Mon 2024-07-01
#         A3  Form 4 filed 2024-06-21 by owner 1, symbol "NONE" (inherits ACME), A grant 200 @ 0
#     Q3  A4  4/A filed 2024-07-10 amending A1 (DATE_OF_ORIG_SUB 14-JUN-2024): P 1,200 @ 10.50
#         A5  Form 4 filed Wed 2024-07-03 (Thu 4th is a holiday) P 100 @ 11 by owner 1 -> usable Fri 2024-07-05
#         A6  Form 3 filed 2024-07-08 (no transactions)
#   BETA (CIK 200, symbol "NYSE: BETA"): Q3 B1 Form 4 filed 2024-08-01, joint filing by owners 3 and 4, P 50 @ 20
#   Form 5: Q3 C1 filed 2024-08-05 for ACME, code G
Q2_SUBS = [
    _sub('0001-24-000001', '14-JUN-2024', '12-JUN-2024', '4', '100', 'ACME CORP', 'ACME'),
    _sub('0001-24-000002', '28-JUN-2024', '26-JUN-2024', '4', '100', 'ACME CORP', 'ACME'),
    _sub('0001-24-000003', '21-JUN-2024', '19-JUN-2024', '4', '100', 'ACME CORP', 'NONE'),
]
Q2_OWNS = [
    _own('0001-24-000001', '1', 'Doe Jane', 'Officer', 'Chief Executive Officer'),
    _own('0001-24-000002', '2', 'Roe Rick', 'Director'),
    _own('0001-24-000003', '1', 'Doe Jane', 'Officer', 'Chief Executive Officer'),
]
Q2_NDTS = [
    _ndt('0001-24-000001', '11', '12-JUN-2024', 'P', '1000.0', '10.0', 'A', '5000.0'),
    _ndt('0001-24-000002', '12', '26-JUN-2024', 'S', '500.0', '12.0', 'D', '1500.0'),
    _ndt('0001-24-000003', '13', '19-JUN-2024', 'A', '200.0', '0', 'A', '5200.0'),
]
Q3_SUBS = [
    _sub('0001-24-000004', '10-JUL-2024', '12-JUN-2024', '4/A', '100', 'ACME CORP', 'ACME', orig='14-JUN-2024'),
    _sub('0001-24-000005', '03-JUL-2024', '01-JUL-2024', '4', '100', 'ACME CORP', 'ACME'),
    _sub('0001-24-000006', '08-JUL-2024', '08-JUL-2024', '3', '100', 'ACME CORP', 'ACME'),
    _sub('0002-24-000001', '01-AUG-2024', '30-JUL-2024', '4', '200', 'BETA INC', 'NYSE: BETA'),
    _sub('0003-24-000001', '05-AUG-2024', '01-AUG-2024', '5', '100', 'ACME CORP', 'ACME'),
]
Q3_OWNS = [
    _own('0001-24-000004', '1', 'Doe Jane', 'Officer', 'Chief Executive Officer'),
    _own('0001-24-000005', '1', 'Doe Jane', 'Officer', 'Chief Executive Officer'),
    _own('0001-24-000006', '5', 'New Ned', 'Director,TenPercentOwner'),
    _own('0002-24-000001', '3', 'Fund LP', 'TenPercentOwner'),
    _own('0002-24-000001', '4', 'Fund GP LLC', 'TenPercentOwnerOther'),
    _own('0003-24-000001', '2', 'Roe Rick', 'Director'),
]
Q3_NDTS = [
    _ndt('0001-24-000004', '14', '12-JUN-2024', 'P', '1200.0', '10.5', 'A', '5200.0'),
    _ndt('0001-24-000005', '15', '01-JUL-2024', 'P', '100.0', '11.0', 'A', '5300.0'),
    _ndt('0002-24-000001', '16', '30-JUL-2024', 'P', '50.0', '20.0', 'A', '1000050.0', di='I'),
    _ndt('0003-24-000001', '17', '01-AUG-2024', 'G', '10.0', '', 'D', '1490.0', form='5'),
]
CUTOFF = dt.date(2024, 6, 30)


@pytest.fixture(scope='module')
def fixture_zips(tmp_path_factory) -> list:
    d = tmp_path_factory.mktemp('form345')
    return [('2024q2', _zip(d / '2024q2_form345.zip', Q2_SUBS, Q2_OWNS, Q2_NDTS)),
            ('2024q3', _zip(d / '2024q3_form345.zip', Q3_SUBS, Q3_OWNS, Q3_NDTS))]


@pytest.fixture(scope='module')
def built(fixture_zips, tmp_path_factory):
    """Full DB, dev DB and a research config naming a history DB next to them."""
    root = tmp_path_factory.mktemp('research')
    full_dir, dev_dir = root / 'research_full', root / 'research_dev'
    full_dir.mkdir()
    dev_dir.mkdir()
    full_db = full_dir / 'insider.duckdb'
    diag = F.build_database(fixture_zips, full_db, verbose=False)
    dev_db = dev_dir / 'insider.duckdb'
    F.derive_dev_copy(full_db, dev_db, CUTOFF, verbose=False)
    for d in (full_dir, dev_dir):
        duckdb.connect(str(d / 'mmr_history.duckdb')).close()   # placeholder history DB
        (d / 'trader_research.yaml').write_text(
            f'duckdb_path: {d / "mmr.duckdb"}\nhistory_duckdb_path: {d / "mmr_history.duckdb"}\n')
    return {'full_db': full_db, 'dev_db': dev_db, 'full_cfg': full_dir / 'trader_research.yaml',
            'dev_cfg': dev_dir / 'trader_research.yaml', 'diag': diag}


def _q(db, sql, params=None):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    finally:
        con.close()


# --------------------------------------------------------------------------- pure helpers

@pytest.mark.parametrize('raw,expected', [
    ('ACME', 'ACME'), ('acme ', 'ACME'), ('NYSE: KRC', 'KRC'), ('NASDAQ:MSFT', 'MSFT'), ('BRK-B', 'BRK.B'),
    ('BRK.B', 'BRK.B'), ('ISCA, ISCB', 'ISCA'), ('NWIN(OB)', 'NWIN'), ('GOOG.OB', 'GOOG'), ('JW.A', 'JW.A'),
    ('NONE', None), ('none', None), ('(NONE)', None), ('N/A', None), ('', None), (None, None),
    ('Not applicable', None), ('12345', None), ('ZZZZ', None),
])
def test_normalize_symbol(raw, expected):
    assert normalize_symbol(raw) == expected


@pytest.mark.parametrize('rel,expected', [
    ('Officer', (False, True, False, False)), ('Director', (True, False, False, False)),
    ('Director,Officer', (True, True, False, False)), ('TenPercentOwner', (False, False, True, False)),
    ('TenPercentOwnerOther', (False, False, True, True)), ('Director,Officer,TenPercentOwner', (True, True, True, False)),
    ('Other', (False, False, False, True)), ('', (False, False, False, False)), (None, (False, False, False, False)),
])
def test_parse_relationship(rel, expected):
    assert parse_relationship(rel) == expected


def test_usable_from_is_first_session_strictly_after_filing_date():
    sessions = insider.nyse_sessions('2024-06-01', '2024-07-31')
    fd = pd.Series(pd.to_datetime(['2024-06-14', '2024-06-17', '2024-07-03', '2024-06-15', '2024-06-28']))
    got = usable_from_sessions(fd, sessions)
    # Friday -> Monday; Monday -> Tuesday; Wed 3 Jul -> Fri 5 Jul (4 Jul holiday);
    # Saturday -> Monday; Fri 28 Jun -> Mon 1 Jul (crosses the dev cutoff)
    assert [d.date().isoformat() for d in got] == ['2024-06-17', '2024-06-18', '2024-07-05', '2024-06-17', '2024-07-01']
    assert bool((got > fd).all())


def test_usable_from_refuses_calendar_that_ends_too_early():
    sessions = insider.nyse_sessions('2024-06-01', '2024-06-14')
    with pytest.raises(ValueError, match='calendar ends'):
        usable_from_sessions(pd.Series(pd.to_datetime(['2024-06-14'])), sessions)
    with pytest.raises(ValueError, match='NaT'):
        usable_from_sessions(pd.Series([pd.NaT]), sessions)


def test_quarter_range_and_helpers():
    assert F.quarter_range('2014q1', '2014q4') == ['2014q1', '2014q2', '2014q3', '2014q4']
    assert F.quarter_range('2023q3', '2024q2') == ['2023q3', '2023q4', '2024q1', '2024q2']
    assert F.quarter_of(dt.date(2026, 9, 29)) == '2026q3'
    assert F.previous_quarter('2026q1') == '2025q4'
    assert F.zip_name('2014q1') == '2014q1_form345.zip'
    assert F.BASE_URL.startswith('https://www.sec.gov/files/structureddata/data/insider-transactions-data-sets/')
    with pytest.raises(ValueError):
        F.quarter_range('2014-1', '2014q4')


def test_request_throttle_and_user_agent(monkeypatch):
    """Every request carries the descriptive User-Agent and is spaced >= the minimum interval."""
    seen = []

    class _Resp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b''

    def fake_urlopen(req, timeout=0):
        seen.append((req.get_header('User-agent'), F.time.monotonic()))
        return _Resp()

    monkeypatch.setattr(F.urllib.request, 'urlopen', fake_urlopen)
    for _ in range(3):
        F._request('https://www.sec.gov/x', 'MMR test agent (x@y)', method='HEAD')
    assert all(ua == 'MMR test agent (x@y)' for ua, _ in seen)
    gaps = np.diff([t for _, t in seen])
    assert (gaps >= F.MIN_REQUEST_INTERVAL_S * 0.95).all()
    assert F.MIN_REQUEST_INTERVAL_S >= 0.1     # <= 10 requests/second


# --------------------------------------------------------------------------- ingest

def test_parse_zip_types_and_columns(fixture_zips):
    frames = F.parse_zip(fixture_zips[1][1], '2024q3')
    subs = frames['submissions']
    assert set(['accession', 'filing_date', 'document_type', 'base_form', 'is_amendment', 'issuer_cik',
                'symbol_as_filed', 'ticker', 'date_of_orig_sub']) <= set(subs.columns)
    a4 = subs.set_index('accession').loc['0001-24-000004']
    assert a4['base_form'] == '4' and bool(a4['is_amendment']) and a4['date_of_orig_sub'] == dt.date(2024, 6, 14)
    beta = subs.set_index('accession').loc['0002-24-000001']
    assert beta['symbol_as_filed'] == 'NYSE: BETA' and beta['ticker'] == 'BETA' and int(beta['issuer_cik']) == 200
    owners = frames['owners'].set_index(['accession', 'owner_cik'])
    assert bool(owners.loc[('0001-24-000006', 5), 'is_director']) and bool(owners.loc[('0001-24-000006', 5), 'is_ten_pct'])
    assert owners.loc[('0001-24-000005', 1), 'officer_title'] == 'Chief Executive Officer'
    nd = frames['nonderiv'].set_index('trans_sk')
    assert nd.loc[16, 'shares'] == 50.0 and nd.loc[16, 'price'] == 20.0 and nd.loc[16, 'acquired_disposed'] == 'A'
    assert nd.loc[16, 'direct_indirect'] == 'I' and nd.loc[16, 'trans_date'] == dt.date(2024, 7, 30)
    assert np.isnan(nd.loc[17, 'price']) and nd.loc[17, 'trans_code'] == 'G' and nd.loc[17, 'trans_form_type'] == '5'


def test_build_loads_all_tables_with_required_fields(built):
    db = built['full_db']
    tables = {r[0] for r in _q(db, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")}
    assert {'form345_submissions', 'form345_owners', 'form345_nonderiv_trans', 'form345_deriv_trans', 'form345_quarters'} <= tables
    assert _q(db, 'SELECT count(*) FROM form345_submissions')[0][0] == 8
    assert _q(db, 'SELECT count(*) FROM form345_owners')[0][0] == 9
    assert _q(db, 'SELECT count(*) FROM form345_nonderiv_trans')[0][0] == 7
    cols = {r[0] for r in _q(db, "SELECT column_name FROM information_schema.columns WHERE table_name = 'form345_submissions'")}
    assert {'filing_date', 'accession', 'issuer_cik', 'ticker', 'symbol_as_filed', 'usable_from', 'superseded_by'} <= cols
    cols = {r[0] for r in _q(db, "SELECT column_name FROM information_schema.columns WHERE table_name = 'form345_owners'")}
    assert {'owner_cik', 'is_director', 'is_officer', 'officer_title', 'is_ten_pct'} <= cols
    cols = {r[0] for r in _q(db, "SELECT column_name FROM information_schema.columns WHERE table_name = 'form345_nonderiv_trans'")}
    assert {'trans_date', 'trans_code', 'shares', 'price', 'acquired_disposed', 'shares_after'} <= cols
    q = {r[0]: r for r in _q(db, 'SELECT quarter, n_submissions, n_nonderiv, zip_sha256 FROM form345_quarters')}
    assert q['2024q2'][1] == 3 and q['2024q3'][1] == 5 and len(q['2024q3'][3]) == 64


def test_amendment_replaces_original_d10(built):
    db = built['full_db']
    rows = dict((r[0], r[1]) for r in _q(db, 'SELECT accession, superseded_by FROM form345_submissions'))
    assert rows['0001-24-000001'] == '0001-24-000004'          # original superseded by its 4/A
    assert rows['0001-24-000004'] is None                       # the amendment is effective
    assert all(v is None for k, v in rows.items() if k not in ('0001-24-000001',))
    assert built['diag']['amendments'] == 1 and built['diag']['amendments_linked'] == 1


def test_ticker_inherited_from_issuer_when_filing_says_none(built):
    rows = {r[0]: (r[1], r[2], r[3]) for r in _q(built['full_db'],
            'SELECT accession, ticker, ticker_source, symbol_as_filed FROM form345_submissions')}
    assert rows['0001-24-000003'] == ('ACME', 'issuer_fill', 'NONE')
    assert rows['0001-24-000001'] == ('ACME', 'filed', 'ACME')
    assert rows['0002-24-000001'] == ('BETA', 'filed', 'NYSE: BETA')


def test_usable_from_stored_per_d10(built):
    rows = {r[0]: (r[1], r[2]) for r in _q(built['full_db'], 'SELECT accession, filing_date, usable_from FROM form345_submissions')}
    assert rows['0001-24-000001'] == (dt.date(2024, 6, 14), dt.date(2024, 6, 17))   # Fri -> Mon
    assert rows['0001-24-000005'] == (dt.date(2024, 7, 3), dt.date(2024, 7, 5))     # 4 July holiday
    assert rows['0001-24-000002'] == (dt.date(2024, 6, 28), dt.date(2024, 7, 1))
    assert all(u > f for f, u in rows.values())


def test_rerun_is_idempotent(fixture_zips, built, tmp_path):
    db = tmp_path / 'idem.duckdb'
    d1 = F.build_database(fixture_zips, db, verbose=False)
    counts = lambda: [_q(db, f'SELECT count(*) FROM {t}')[0][0] for t in  # noqa: E731
                      ('form345_submissions', 'form345_owners', 'form345_nonderiv_trans', 'form345_deriv_trans', 'form345_quarters')]
    first = counts()
    d2 = F.build_database(fixture_zips, db, verbose=False)              # unchanged zips -> skipped
    assert d2['quarters_skipped'] == ['2024q2', '2024q3'] and d2['quarters_loaded'] == []
    assert counts() == first
    d3 = F.build_database(fixture_zips, db, force=True, verbose=False)  # forced re-ingest -> replaced, not duplicated
    assert d3['quarters_loaded'] == ['2024q2', '2024q3']
    assert counts() == first
    assert d1['filings_superseded'] == d3['filings_superseded'] == 1
    sup = _q(db, "SELECT superseded_by FROM form345_submissions WHERE accession = '0001-24-000001'")[0][0]
    assert sup == '0001-24-000004'


def test_changed_zip_replaces_only_its_quarter(fixture_zips, tmp_path):
    db = tmp_path / 'chg.duckdb'
    F.build_database(fixture_zips, db, verbose=False)
    # republish 2024q3 with one more purchase
    q3 = _zip(tmp_path / '2024q3_form345.zip', Q3_SUBS + [_sub('0009-24-000001', '20-AUG-2024', '19-AUG-2024', '4', '100', 'ACME CORP', 'ACME')],
              Q3_OWNS + [_own('0009-24-000001', '2', 'Roe Rick', 'Director')],
              Q3_NDTS + [_ndt('0009-24-000001', '99', '19-AUG-2024', 'P', '10.0', '9.0', 'A', '1500.0')])
    d = F.build_database([fixture_zips[0], ('2024q3', q3)], db, verbose=False)
    assert d['quarters_loaded'] == ['2024q3'] and d['quarters_skipped'] == ['2024q2']
    assert _q(db, 'SELECT count(*) FROM form345_submissions')[0][0] == 9
    assert _q(db, "SELECT count(*) FROM form345_submissions WHERE quarter = '2024q2'")[0][0] == 3


# --------------------------------------------------------------------------- dev copy

def test_dev_copy_contains_no_row_filed_after_cutoff(built):
    out = F.check_cutoff(built['dev_db'], CUTOFF)
    assert out['submissions_after'] == 0 and out['form345_nonderiv_trans_after'] == 0
    assert out['max_filing_date'] == '2024-06-28' and out['rows'] == 3
    for table in ('form345_owners', 'form345_nonderiv_trans', 'form345_deriv_trans'):
        assert _q(built['dev_db'], f"""SELECT count(*) FROM {table} t JOIN form345_submissions s USING (accession)
                                       WHERE s.filing_date > DATE '2024-06-30'""")[0][0] == 0
    with pytest.raises(AssertionError, match='filed after'):
        F.check_cutoff(built['full_db'], CUTOFF)
    assert F.main(['check', '--db', str(built['dev_db']), '--cutoff', '2024-06-30']) == 0
    assert F.main(['check', '--db', str(built['full_db']), '--cutoff', '2024-06-30']) == 1


def test_dev_copy_recomputes_supersession_on_its_own_rows(built):
    """The 4/A was filed after the cutoff: in the dev copy the original is the
    best information available and must be effective (D10 applied point-in-time)."""
    assert _q(built['dev_db'], "SELECT superseded_by FROM form345_submissions WHERE accession = '0001-24-000001'")[0][0] is None
    assert _q(built['full_db'], "SELECT superseded_by FROM form345_submissions WHERE accession = '0001-24-000001'")[0][0] == '0001-24-000004'
    info = dict(_q(built['dev_db'], 'SELECT key, value FROM form345_build_info'))
    assert info['dev_cutoff'] == '2024-06-30'


@pytest.mark.skipif(not Path(os.path.expanduser('~/.local/share/mmr/research_dev/insider.duckdb')).exists(),
                    reason='real research_dev insider DB not built on this machine')
def test_real_dev_database_has_no_row_after_cutoff():
    out = F.check_cutoff(Path(os.path.expanduser('~/.local/share/mmr/research_dev/insider.duckdb')), CUTOFF)
    assert out['submissions_after'] == 0 and out['rows'] > 1_000_000


# --------------------------------------------------------------------------- loader

def test_loader_resolves_db_next_to_history_db_from_env(built, monkeypatch):
    monkeypatch.setenv(insider.ENV_CONFIG, str(built['dev_cfg']))
    assert insider_db_path() == built['dev_db']
    assert insider_db_path(built['full_cfg']) == built['full_db']
    monkeypatch.setenv(insider.ENV_CONFIG, str(built['dev_cfg'].parent / 'missing.yaml'))
    with pytest.raises(FileNotFoundError):
        insider_db_path()
    with pytest.raises(FileNotFoundError, match='does not exist'):
        load_insider_events('2024-01-01', '2024-12-31', db_path=built['dev_db'].parent / 'nope.duckdb')


def test_loader_interface_columns_and_usable_from(built, monkeypatch):
    monkeypatch.setenv(insider.ENV_CONFIG, str(built['full_cfg']))
    df = load_insider_events('2024-06-01', '2024-12-31')
    assert list(df.columns) == list(EVENT_COLUMNS) + list(EXTRA_COLUMNS)
    assert list(EVENT_COLUMNS) == ['ticker', 'cik', 'filing_date', 'usable_from', 'trans_date', 'code', 'shares', 'price',
                                   'value_usd', 'acquired', 'owner_cik', 'is_director', 'is_officer', 'officer_title',
                                   'is_ten_pct', 'shares_after']
    # Form 4 only by default, superseded original excluded, joint filing = one row per owner
    assert set(df['accession']) == {'0001-24-000002', '0001-24-000003', '0001-24-000004', '0001-24-000005', '0002-24-000001'}
    assert (df['usable_from'] > df['filing_date']).all()
    a4 = df[df['accession'] == '0001-24-000004'].iloc[0]
    assert a4['ticker'] == 'ACME' and a4['cik'] == 100 and a4['code'] == 'P' and a4['shares'] == 1200.0
    assert a4['price'] == 10.5 and a4['value_usd'] == pytest.approx(12600.0) and bool(a4['acquired'])
    assert a4['usable_from'] == pd.Timestamp('2024-07-11') and a4['filing_date'] == pd.Timestamp('2024-07-10')
    assert a4['trans_date'] == pd.Timestamp('2024-06-12') and a4['shares_after'] == 5200.0
    assert bool(a4['is_officer']) and not bool(a4['is_director']) and a4['officer_title'] == 'Chief Executive Officer'
    beta = df[df['accession'] == '0002-24-000001']
    assert len(beta) == 2 and set(beta['owner_cik']) == {3, 4} and (beta['n_owners'] == 2).all()
    assert beta['is_ten_pct'].all() and beta['ticker'].eq('BETA').all() and beta['symbol_as_filed'].eq('NYSE: BETA').all()
    a3 = df[df['accession'] == '0001-24-000003'].iloc[0]
    assert a3['ticker'] == 'ACME' and a3['code'] == 'A' and a3['value_usd'] == 0.0


def test_loader_filters(built):
    db = built['full_db']
    p = load_insider_events('2024-06-01', '2024-12-31', codes=['P'], db_path=db)
    assert set(p['code']) == {'P'} and set(p['accession']) == {'0001-24-000004', '0001-24-000005', '0002-24-000001'}
    win = load_insider_events('2024-07-01', '2024-07-05', db_path=db)          # window applies to usable_from
    assert set(win['accession']) == {'0001-24-000002', '0001-24-000005'}
    by_filing = load_insider_events('2024-07-01', '2024-07-05', date_field='filing_date', db_path=db)
    assert set(by_filing['accession']) == {'0001-24-000005'}
    all_forms = load_insider_events('2024-06-01', '2024-12-31', forms=('3', '4', '5'), db_path=db)
    assert '0003-24-000001' in set(all_forms['accession'])                     # Form 5 gift
    sup = load_insider_events('2024-06-01', '2024-12-31', include_superseded=True, db_path=db)
    assert '0001-24-000001' in set(sup['accession'])
    only_beta = load_insider_events('2024-06-01', '2024-12-31', tickers=['beta'], db_path=db)
    assert set(only_beta['ticker']) == {'BETA'}
    empty = load_insider_events('2030-01-01', '2030-12-31', db_path=db)
    assert len(empty) == 0 and list(empty.columns) == list(EVENT_COLUMNS) + list(EXTRA_COLUMNS)
    with pytest.raises(ValueError):
        load_insider_events('2024-12-31', '2024-01-01', db_path=db)
    with pytest.raises(ValueError):
        load_insider_events('2024-01-01', '2024-12-31', codes=[], db_path=db)


def test_loader_dev_copy_sees_original_not_amendment(built, monkeypatch):
    monkeypatch.setenv(insider.ENV_CONFIG, str(built['dev_cfg']))
    df = load_insider_events('2024-06-01', '2024-12-31')
    assert set(df['accession']) == {'0001-24-000001', '0001-24-000002', '0001-24-000003'}
    assert (df['filing_date'] <= pd.Timestamp('2024-06-30')).all()


def test_loader_opens_read_only(built, monkeypatch):
    opened = []
    real = duckdb.connect

    def spy(*args, **kwargs):
        opened.append(kwargs.get('read_only'))
        return real(*args, **kwargs)

    monkeypatch.setattr(insider.duckdb, 'connect', spy)
    load_insider_events('2024-06-01', '2024-12-31', db_path=built['full_db'])
    assert opened == [True]
    # and the file is untouched by a read
    before = built['full_db'].stat().st_mtime_ns
    load_insider_events('2024-06-01', '2024-12-31', db_path=built['full_db'])
    assert built['full_db'].stat().st_mtime_ns == before


# --------------------------------------------------------------------------- amendment matching regressions
#
# Patterns found in the real data on 2026-09-29 (critic round 1), each pinned here:
#   S  SWM 0001062993-22-012373 (4/A) amends 0001062993-22-012378 (4): SAME DAY, and the
#      amendment's accession is LOWER than the original's. Ordering by accession missed it.
#   T  TPL 0001207097-22-000001 (4/A) names 2021-12-30 as its original date, the filing date of
#      the PREVIOUS amendment 0001207097-21-000455 (same period), beside two different-period
#      originals filed that day. "Prefer originals" and "prefer same period" disagreed and,
#      applied as independent filters, linked nothing; both filings stayed effective.
#   Y  TPL 0001207097-22-000067 carried a period typo (2021 for 2022); its same-day 4/A has no
#      same-period candidate and must still link.
#   D  two amendments of one original filed the same later day: the higher accession wins.
#   X  two identical-period originals on one day and one amendment naming that day: ambiguous,
#      both superseded, counted.
CHAIN_SUBS = [
    # S: original filed 2024-06-20 with the HIGHER accession, amendment same day with the lower one
    _sub('0003-24-000011', '20-JUN-2024', '20-JUN-2024', '4', '300', 'GAMMA CO', 'GAMA'),
    _sub('0003-24-000010', '20-JUN-2024', '20-JUN-2024', '4/A', '300', 'GAMMA CO', 'GAMA', orig='20-JUN-2024'),
    # T: O (05-06) <- A1 (05-08, names 06-MAY) <- A2 (05-13, names 08-MAY = A1's date); decoys on 05-08
    _sub('0004-24-000020', '06-MAY-2024', '03-MAY-2024', '4', '400', 'DELTA INC', 'DLTA'),
    _sub('0004-24-000021', '08-MAY-2024', '03-MAY-2024', '4/A', '400', 'DELTA INC', 'DLTA', orig='06-MAY-2024'),
    _sub('0004-24-000022', '08-MAY-2024', '07-MAY-2024', '4', '400', 'DELTA INC', 'DLTA'),
    _sub('0004-24-000023', '08-MAY-2024', '06-MAY-2024', '4', '400', 'DELTA INC', 'DLTA'),
    _sub('0004-24-000024', '13-MAY-2024', '03-MAY-2024', '4/A', '400', 'DELTA INC', 'DLTA', orig='08-MAY-2024'),
    # Y: same-day original with a period typo (2023), amendment with the corrected period
    _sub('0005-24-000030', '17-MAY-2024', '16-MAY-2023', '4', '500', 'EPSILON LTD', 'EPSL'),
    _sub('0005-24-000031', '17-MAY-2024', '16-MAY-2024', '4/A', '500', 'EPSILON LTD', 'EPSL', orig='17-MAY-2024'),
    # D: original 2024-05-20, two amendments filed 2024-05-22 naming 20-MAY
    _sub('0006-24-000040', '20-MAY-2024', '17-MAY-2024', '4', '600', 'ZETA PLC', 'ZETA'),
    _sub('0006-24-000041', '22-MAY-2024', '17-MAY-2024', '4/A', '600', 'ZETA PLC', 'ZETA', orig='20-MAY-2024'),
    _sub('0006-24-000042', '22-MAY-2024', '17-MAY-2024', '4/A', '600', 'ZETA PLC', 'ZETA', orig='20-MAY-2024'),
    # X: two same-period originals on 2024-05-28 by one owner, one amendment on 05-30 naming 28-MAY
    _sub('0007-24-000050', '28-MAY-2024', '24-MAY-2024', '4', '700', 'ETA CORP', 'ETA'),
    _sub('0007-24-000051', '28-MAY-2024', '24-MAY-2024', '4', '700', 'ETA CORP', 'ETA'),
    _sub('0007-24-000052', '30-MAY-2024', '24-MAY-2024', '4/A', '700', 'ETA CORP', 'ETA', orig='28-MAY-2024'),
    # C2 (content tier): DATE_OF_ORIG_SUB is the TRANSACTION date (no filing that day); identical content
    _sub('0008-24-000060', '05-JUN-2024', '03-JUN-2024', '4', '800', 'THETA CO', 'THTA'),
    _sub('0008-24-000061', '07-JUN-2024', '03-JUN-2024', '4/A', '800', 'THETA CO', 'THTA', orig='03-JUN-2024'),
    # P3 (period tier): same period, different content, exactly one original within 45 days
    _sub('0009-24-000070', '10-JUN-2024', '07-JUN-2024', '4', '900', 'IOTA INC', 'IOTA'),
    _sub('0009-24-000071', '12-JUN-2024', '07-JUN-2024', '4/A', '900', 'IOTA INC', 'IOTA', orig='07-JUN-2024'),
    # P3-ambiguous: two same-period originals on different days -> tier 3 refuses, nothing superseded
    _sub('0010-24-000080', '14-JUN-2024', '13-JUN-2024', '4', '1000', 'KAPPA LTD', 'KAPA'),
    _sub('0010-24-000081', '17-JUN-2024', '13-JUN-2024', '4', '1000', 'KAPPA LTD', 'KAPA'),
    _sub('0010-24-000082', '19-JUN-2024', '13-JUN-2024', '4/A', '1000', 'KAPPA LTD', 'KAPA', orig='13-JUN-2024'),
    # O (owner-free tier 4, CYAN pattern): the 4/A corrects the reporting owner; no shared owner CIK,
    # same named date, same period, identical extended content, unique -> replaced
    _sub('0011-24-000090', '24-JUN-2024', '21-JUN-2024', '4', '1100', 'LAMBDA CORP', 'LMDA'),
    _sub('0011-24-000091', '26-JUN-2024', '21-JUN-2024', '4/A', '1100', 'LAMBDA CORP', 'LMDA', orig='24-JUN-2024'),
    # O-amb: two directors with identical grants filed 06-24, a third owner's 4/A names 06-24 -> refused
    _sub('0012-24-000100', '24-JUN-2024', '21-JUN-2024', '4', '1200', 'MU INC', 'MUU'),
    _sub('0012-24-000101', '24-JUN-2024', '21-JUN-2024', '4', '1200', 'MU INC', 'MUU'),
    _sub('0012-24-000102', '26-JUN-2024', '21-JUN-2024', '4/A', '1200', 'MU INC', 'MUU', orig='24-JUN-2024'),
    # O-diff: new owner, named date holds one same-period filing with DIFFERENT content -> never linked
    _sub('0013-24-000110', '24-JUN-2024', '21-JUN-2024', '4', '1300', 'NU PLC', 'NUU'),
    _sub('0013-24-000111', '26-JUN-2024', '21-JUN-2024', '4/A', '1300', 'NU PLC', 'NUU', orig='24-JUN-2024'),
    # B (tier 5, BATL pattern): the 4/A corrects the reporting owner AND the holdings: same submitter
    # prefix, named date, same period, identical core transaction, different shares_after -> replaced
    _sub('0014-24-000120', '24-JUN-2024', '20-JUN-2024', '4', '1400', 'XI CORP', 'XII'),
    _sub('0014-24-000121', '25-JUN-2024', '20-JUN-2024', '4/A', '1400', 'XI CORP', 'XII', orig='24-JUN-2024'),
    # B-self (ABCB pattern): two insiders each SELF-file an identical grant (submitter prefix = own CIK) -> refused
    _sub('0151-24-000001', '24-JUN-2024', '20-JUN-2024', '4', '1500', 'OMICRON INC', 'OMI'),
    _sub('0152-24-000001', '25-JUN-2024', '20-JUN-2024', '4/A', '1500', 'OMICRON INC', 'OMI', orig='24-JUN-2024'),
    # B-own (LSCC/OKE pattern): the amending owner has an OWN earlier same-period filing (outside the 45-day
    # window) and a colleague's identical withholding sits on the named day -> refused, all three effective
    _sub('0016-24-000130', '15-JAN-2024', '10-JAN-2024', '4', '1600', 'PI LTD', 'PII'),
    _sub('0016-24-000131', '24-JUN-2024', '10-JAN-2024', '4', '1600', 'PI LTD', 'PII'),
    _sub('0016-24-000132', '24-JUN-2024', '10-JAN-2024', '4/A', '1600', 'PI LTD', 'PII', orig='24-JUN-2024'),
    # B-amb: two same-submitter originals with the identical core transaction but different holdings on the
    # named day, a third owner's 4/A -> ambiguous at tier 5, refused
    _sub('0017-24-000140', '24-JUN-2024', '20-JUN-2024', '4', '1700', 'RHO CO', 'RHO'),
    _sub('0017-24-000141', '24-JUN-2024', '20-JUN-2024', '4', '1700', 'RHO CO', 'RHO'),
    _sub('0017-24-000142', '25-JUN-2024', '20-JUN-2024', '4/A', '1700', 'RHO CO', 'RHO', orig='24-JUN-2024'),
]
CHAIN_OWNS = [_own(acc, cik, name, 'Director') for acc, cik, name in [
    ('0003-24-000011', '31', 'Gamma Dir'), ('0003-24-000010', '31', 'Gamma Dir'),
    ('0004-24-000020', '41', 'Delta Dir'), ('0004-24-000021', '41', 'Delta Dir'), ('0004-24-000022', '41', 'Delta Dir'),
    ('0004-24-000023', '41', 'Delta Dir'), ('0004-24-000024', '41', 'Delta Dir'),
    ('0005-24-000030', '51', 'Eps Dir'), ('0005-24-000031', '51', 'Eps Dir'),
    ('0006-24-000040', '61', 'Zeta Dir'), ('0006-24-000041', '61', 'Zeta Dir'), ('0006-24-000042', '61', 'Zeta Dir'),
    ('0007-24-000050', '71', 'Eta Dir'), ('0007-24-000051', '71', 'Eta Dir'), ('0007-24-000052', '71', 'Eta Dir'),
    ('0008-24-000060', '81', 'Theta Dir'), ('0008-24-000061', '81', 'Theta Dir'),
    ('0009-24-000070', '91', 'Iota Dir'), ('0009-24-000071', '91', 'Iota Dir'),
    ('0010-24-000080', '101', 'Kappa Dir'), ('0010-24-000081', '101', 'Kappa Dir'), ('0010-24-000082', '101', 'Kappa Dir'),
    ('0011-24-000090', '111', 'Lambda Corp (misfiled as owner)'), ('0011-24-000091', '112', 'Lambda Dir'),
    ('0012-24-000100', '121', 'Mu Dir One'), ('0012-24-000101', '122', 'Mu Dir Two'), ('0012-24-000102', '123', 'Mu Dir Three'),
    ('0013-24-000110', '131', 'Nu Dir One'), ('0013-24-000111', '132', 'Nu Dir Two'),
    ('0014-24-000120', '141', 'Xi Officer (misattributed)'), ('0014-24-000121', '142', 'Xi Dir'),
    ('0151-24-000001', '151', 'Omicron Officer One'), ('0152-24-000001', '152', 'Omicron Officer Two'),
    ('0016-24-000130', '161', 'Pi Officer One'), ('0016-24-000131', '162', 'Pi Officer Two'), ('0016-24-000132', '161', 'Pi Officer One'),
    ('0017-24-000140', '171', 'Rho Dir One'), ('0017-24-000141', '172', 'Rho Dir Two'), ('0017-24-000142', '173', 'Rho Dir Three'),
]]
CHAIN_NDTS = [
    _ndt('0003-24-000011', '111', '20-JUN-2024', 'P', '1000.0', '27.15', 'A', '7000.0'),
    _ndt('0003-24-000010', '110', '20-JUN-2024', 'P', '1000.0', '27.15', 'A', '7000.0'),
    _ndt('0004-24-000020', '120', '03-MAY-2024', 'P', '10.0', '100.0', 'A', '110.0'),
    _ndt('0004-24-000021', '121', '03-MAY-2024', 'P', '10.0', '100.0', 'A', '110.0'),
    _ndt('0004-24-000022', '122', '07-MAY-2024', 'P', '5.0', '101.0', 'A', '115.0'),
    _ndt('0004-24-000023', '123', '06-MAY-2024', 'P', '7.0', '102.0', 'A', '117.0'),
    _ndt('0004-24-000024', '124', '03-MAY-2024', 'P', '12.0', '100.0', 'A', '112.0'),
    _ndt('0005-24-000030', '130', '16-MAY-2024', 'S', '300.0', '50.0', 'D', '700.0'),
    _ndt('0005-24-000031', '131', '16-MAY-2024', 'S', '300.0', '50.0', 'D', '700.0'),
    _ndt('0006-24-000040', '140', '17-MAY-2024', 'P', '20.0', '10.0', 'A', '20.0'),
    _ndt('0006-24-000041', '141', '17-MAY-2024', 'P', '21.0', '10.0', 'A', '21.0'),
    _ndt('0006-24-000042', '142', '17-MAY-2024', 'P', '22.0', '10.0', 'A', '22.0'),
    _ndt('0007-24-000050', '150', '24-MAY-2024', 'P', '1.0', '5.0', 'A', '1.0'),
    _ndt('0007-24-000051', '151', '24-MAY-2024', 'P', '2.0', '5.0', 'A', '3.0'),
    _ndt('0007-24-000052', '152', '24-MAY-2024', 'P', '3.0', '5.0', 'A', '3.0'),
    _ndt('0008-24-000060', '160', '03-JUN-2024', 'P', '400.0', '12.5', 'A', '400.0'),
    _ndt('0008-24-000061', '161', '03-JUN-2024', 'P', '400.0', '12.5', 'A', '400.0'),
    _ndt('0009-24-000070', '170', '07-JUN-2024', 'P', '100.0', '9.0', 'A', '100.0'),
    _ndt('0009-24-000071', '171', '07-JUN-2024', 'P', '150.0', '9.0', 'A', '150.0'),
    _ndt('0010-24-000080', '180', '13-JUN-2024', 'S', '10.0', '8.0', 'D', '90.0'),
    _ndt('0010-24-000081', '181', '13-JUN-2024', 'S', '20.0', '8.0', 'D', '70.0'),
    _ndt('0010-24-000082', '182', '13-JUN-2024', 'S', '25.0', '8.0', 'D', '65.0'),
    _ndt('0011-24-000090', '190', '21-JUN-2024', 'G', '30769.0', '5.1', 'A', '767133.0', di='I'),   # D/I flips in the fix
    _ndt('0011-24-000091', '191', '21-JUN-2024', 'G', '30769.0', '5.1', 'A', '767133.0'),
    _ndt('0012-24-000100', '200', '21-JUN-2024', 'A', '9000.0', '0', 'A', '9000.0'),
    _ndt('0012-24-000101', '201', '21-JUN-2024', 'A', '9000.0', '0', 'A', '9000.0'),
    _ndt('0012-24-000102', '202', '21-JUN-2024', 'A', '9000.0', '0', 'A', '9000.0'),
    _ndt('0013-24-000110', '210', '21-JUN-2024', 'F', '653.0', '33.69', 'D', '5000.0'),
    _ndt('0013-24-000111', '211', '21-JUN-2024', 'F', '485.0', '33.69', 'D', '4000.0'),
    _ndt('0014-24-000120', '220', '20-JUN-2024', 'P', '6201.0', '7.99', 'A', '9551.0'),    # BATL numbers
    _ndt('0014-24-000121', '221', '20-JUN-2024', 'P', '6201.0', '7.99', 'A', '11601.0'),
    _ndt('0151-24-000001', '230', '20-JUN-2024', 'A', '4930.0', '0', 'A', '62428.0'),
    _ndt('0152-24-000001', '231', '20-JUN-2024', 'A', '4930.0', '0', 'A', '65737.0'),
    _ndt('0016-24-000130', '240', '10-JAN-2024', 'F', '902.0', '49.38', 'D', '139868.0'),
    _ndt('0016-24-000131', '241', '10-JAN-2024', 'F', '902.0', '49.38', 'D', '167459.0'),
    _ndt('0016-24-000132', '242', '10-JAN-2024', 'F', '902.0', '49.38', 'D', '139868.0'),
    _ndt('0017-24-000140', '250', '20-JUN-2024', 'A', '500.0', '0', 'A', '1500.0'),
    _ndt('0017-24-000141', '251', '20-JUN-2024', 'A', '500.0', '0', 'A', '2500.0'),
    _ndt('0017-24-000142', '252', '20-JUN-2024', 'A', '500.0', '0', 'A', '3500.0'),
]


@pytest.fixture(scope='module')
def chain_db(tmp_path_factory):
    d = tmp_path_factory.mktemp('chain')
    z = _zip(d / '2024q2_form345.zip', CHAIN_SUBS, CHAIN_OWNS, CHAIN_NDTS)
    db = d / 'insider.duckdb'
    diag = F.build_database([('2024q2', z)], db, verbose=False)
    return db, diag


def _superseded(db):
    return dict(_q(db, 'SELECT accession, superseded_by FROM form345_submissions'))


def test_same_day_amendment_with_lower_accession_replaces_original(chain_db):
    """SWM pattern: the original's accession sorts AFTER the amendment's."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0003-24-000011'] == '0003-24-000010'
    assert sup['0003-24-000010'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['GAMA'], db_path=db)
    assert list(ev['accession']) == ['0003-24-000010'] and len(ev) == 1


def test_amendment_chain_through_previous_amendment_with_conflicting_preferences(chain_db):
    """TPL pattern: A2 names A1's filing date; same-period amendment beats different-period originals."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0004-24-000020'] == '0004-24-000024'      # original -> latest amendment (two hops)
    assert sup['0004-24-000021'] == '0004-24-000024'      # first amendment -> latest amendment
    assert sup['0004-24-000024'] is None
    assert sup['0004-24-000022'] is None and sup['0004-24-000023'] is None   # decoys untouched
    ev = load_insider_events('2024-05-01', '2024-05-31', tickers=['DLTA'], db_path=db)
    assert sorted(ev['accession']) == ['0004-24-000022', '0004-24-000023', '0004-24-000024']
    assert ev.loc[ev['accession'] == '0004-24-000024', 'shares'].iloc[0] == 12.0


def test_amendment_links_when_no_candidate_shares_its_period(chain_db):
    """Period typo in the original: same-day 4/A still replaces it."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0005-24-000030'] == '0005-24-000031' and sup['0005-24-000031'] is None


def test_two_same_day_amendments_latest_accession_wins(chain_db):
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0006-24-000040'] == '0006-24-000042'
    assert sup['0006-24-000041'] == '0006-24-000042'
    assert sup['0006-24-000042'] is None
    ev = load_insider_events('2024-05-01', '2024-06-30', tickers=['ZETA'], db_path=db)
    assert list(ev['shares']) == [22.0]


def test_ambiguous_amendment_supersedes_all_candidates_and_is_counted(chain_db):
    db, diag = chain_db
    sup = _superseded(db)
    assert sup['0007-24-000050'] == '0007-24-000052' and sup['0007-24-000051'] == '0007-24-000052'
    assert sup['0007-24-000052'] is None
    assert diag['amendments'] == 17 and diag['amendments_linked'] == 11 and diag['amendments_unlinked'] == 6
    assert diag['amendments_ambiguous'] == 1
    assert diag['linked_tier1_date'] == 7 and diag['linked_tier2_content'] == 1 and diag['linked_tier3_period'] == 1
    assert diag['linked_tier4_owner_free'] == 1 and diag['linked_tier5_owner_free_core'] == 1
    assert diag['tier3_ambiguous_unlinked'] == 1 and diag['tier4_ambiguous_unlinked'] == 1
    assert diag['tier5_refused_unlinked'] == 4        # O-amb, B-self, B-own, B-amb
    assert diag['filings_superseded'] == 12           # S 1 + T 2 + Y 1 + D 2 + X 2 + C2 1 + P3 1 + O 1 + B 1
    assert diag['dup_pairs_amend'] == 0          # no identical amendment/original pair left effective
    assert diag['dup_pairs_amend_owner_free'] == 2   # the refused O-amb triple: amendment x two identical originals


def test_residual_duplicate_metric_detects_a_missed_link(chain_db, tmp_path):
    """The metric that would have flagged SWM: force the S pair to both be effective and count it."""
    db, _ = chain_db
    import shutil
    copy = tmp_path / 'broken.duckdb'
    shutil.copy(db, copy)
    con = duckdb.connect(str(copy))
    try:
        con.execute('UPDATE form345_submissions SET superseded_by = NULL')
        got = F.residual_duplicates(con)
    finally:
        con.close()
    assert got['dup_pairs_amend'] >= 2 and got['dup_pairs_any'] >= got['dup_pairs_amend']
    con = duckdb.connect(str(db), read_only=True)
    try:
        assert F.residual_duplicates(con)['dup_pairs_amend'] == 0
    finally:
        con.close()


def test_content_tier_links_amendment_whose_orig_date_is_the_transaction_date(chain_db):
    """Tier 2: DATE_OF_ORIG_SUB names a day with no filing; the identical earlier filing is replaced."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0008-24-000060'] == '0008-24-000061' and sup['0008-24-000061'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['THTA'], db_path=db)
    assert list(ev['accession']) == ['0008-24-000061']


def test_period_tier_links_unique_same_period_original_only(chain_db):
    """Tier 3: same period + one original within 45 days -> replaced (content differs);
    two same-period originals -> ambiguous, refused, all three stay effective."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0009-24-000070'] == '0009-24-000071' and sup['0009-24-000071'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['IOTA'], db_path=db)
    assert list(ev['shares']) == [150.0]
    assert sup['0010-24-000080'] is None and sup['0010-24-000081'] is None and sup['0010-24-000082'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['KAPA'], db_path=db)
    assert sorted(ev['shares']) == [10.0, 20.0, 25.0]


def test_owner_free_tier_links_owner_identity_correction(chain_db):
    """Tier 4 (CYAN pattern): no shared owner CIK, but the named date holds exactly one same-period
    filing with an identical extended transaction list -> replaced. Direct/indirect ownership is
    deliberately NOT part of the comparison: the CYAN original said "I, by the foundation" and the
    amendment under the foundation's own CIK says "D"."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0011-24-000090'] == '0011-24-000091' and sup['0011-24-000091'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['LMDA'], db_path=db)
    assert list(ev['accession']) == ['0011-24-000091'] and list(ev['owner_cik']) == [112]


def test_owner_free_tier_refuses_ambiguous_and_different_content(chain_db):
    """Identical grants by two other directors on the named day -> ambiguous, refused; a lone
    same-period filing with different content -> never linked without a shared owner."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0012-24-000100'] is None and sup['0012-24-000101'] is None and sup['0012-24-000102'] is None
    assert sup['0013-24-000110'] is None and sup['0013-24-000111'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['MUU', 'NUU'], db_path=db)
    assert len(ev) == 5


def test_core_content_tier_links_owner_correction_that_also_corrects_holdings(chain_db):
    """Tier 5 (BATL 0001209191-21-056275 pattern): the 4/A moves the trade to another reporting owner
    AND corrects shares owned after, so tier 4's extended signature cannot match. Same submitter
    prefix, named date, same period, identical core transaction, unique -> replaced; the loader
    returns only the amendment, under the corrected owner with the corrected holdings."""
    db, _ = chain_db
    sup = _superseded(db)
    assert sup['0014-24-000120'] == '0014-24-000121' and sup['0014-24-000121'] is None
    ev = load_insider_events('2024-06-01', '2024-06-30', tickers=['XII'], db_path=db)
    assert list(ev['accession']) == ['0014-24-000121']
    assert list(ev['owner_cik']) == [142] and list(ev['shares_after']) == [11601.0]


def test_core_content_tier_guards_refuse_look_alikes(chain_db):
    """The core-content match alone is not evidence of a correction. Refused, all filings effective:
    B-self  two insiders each self-filing an identical grant (different submitter prefixes; ABCB shape);
    B-own   the amending owner already reported the same period themselves months earlier - the 4/A
            amends THEIR filing, not the colleague's identical one on the named day (LSCC/OKE shape);
    B-amb   two same-submitter originals with the identical core transaction on the named day."""
    db, _ = chain_db
    sup = _superseded(db)
    for acc in ('0151-24-000001', '0152-24-000001', '0016-24-000130', '0016-24-000131', '0016-24-000132',
                '0017-24-000140', '0017-24-000141', '0017-24-000142'):
        assert sup[acc] is None, acc
    ev = load_insider_events('2024-01-01', '2024-06-30', tickers=['OMI', 'PII', 'RHO'], db_path=db)
    assert len(ev) == 8


_FULL_DB = Path(os.path.expanduser('~/.local/share/mmr/research_full/insider.duckdb'))


@pytest.mark.skipif(not _FULL_DB.exists(), reason='real research_full insider DB not built on this machine')
def test_real_full_database_supersedes_the_observed_cases():
    con = duckdb.connect(str(_FULL_DB), read_only=True)
    try:
        sup = dict(con.execute("""SELECT accession, superseded_by FROM form345_submissions WHERE accession IN
            ('0001062993-22-012378', '0001062993-22-012373', '0001207097-21-000451', '0001207097-21-000455',
             '0001207097-22-000001', '0001207097-21-000456', '0001207097-21-000457',
             '0001207097-22-000067', '0001207097-22-000069',
             '0001010549-14-000083', '0001010549-14-000096',
             '0001209191-21-056028', '0001209191-21-056275', '0001437749-22-016953', '0001437749-22-029426',
             '0001090009-15-000020', '0001090009-16-000028',
             '0001239875-16-000012', '0001356555-16-000004', '0000855658-22-000264', '0000855658-22-000265',
             '0001741956-22-000011', '0001741956-22-000014')""").fetchall())
    finally:
        con.close()
    # tier 5: owner corrections that also correct the holdings, confirmed by the filings' own text
    assert sup['0001209191-21-056028'] == '0001209191-21-056275' and sup['0001209191-21-056275'] is None   # BATL
    assert sup['0001437749-22-016953'] == '0001437749-22-029426'                                          # CLRO
    assert sup['0001090009-15-000020'] == '0001090009-16-000028'                                          # SFST
    # tier-5 guards: look-alikes stay effective (two self-filers; own months-old filing being amended)
    for acc in ('0001239875-16-000012', '0001356555-16-000004',      # ABCB: identical grants, two self-filers
                '0000855658-22-000264', '0000855658-22-000265',      # LSCC: two officers' POA amendments
                '0001741956-22-000011', '0001741956-22-000014'):     # OKE: two officers' clerical amendments
        assert sup[acc] is None, acc
    batl = load_insider_events('2021-09-01', '2021-09-30', codes=['P'], tickers=['BATL'], db_path=_FULL_DB)
    assert '0001209191-21-056028' not in set(batl['accession'])
    hinds = batl[batl['accession'] == '0001209191-21-056275']
    assert len(hinds) == 1 and hinds['owner_cik'].iloc[0] == 1545974 and hinds['shares_after'].iloc[0] == 11601.0
    assert sup['0001010549-14-000083'] == '0001010549-14-000096' and sup['0001010549-14-000096'] is None   # CYAN owner fix
    assert sup['0001062993-22-012378'] == '0001062993-22-012373' and sup['0001062993-22-012373'] is None   # SWM
    assert sup['0001207097-21-000451'] == '0001207097-22-000001'                                          # TPL chain root
    assert sup['0001207097-21-000455'] == '0001207097-22-000001' and sup['0001207097-22-000001'] is None
    assert sup['0001207097-21-000456'] is None and sup['0001207097-21-000457'] is None                    # decoys
    assert sup['0001207097-22-000067'] == '0001207097-22-000069'                                          # period typo
    cyan = load_insider_events('2014-02-01', '2014-03-31', forms=('4',), tickers=['CYAN'], db_path=_FULL_DB)
    assert '0001010549-14-000083' not in set(cyan['accession']) and '0001010549-14-000096' in set(cyan['accession'])
    tpl = load_insider_events('2021-12-01', '2022-01-31', codes=['P'], tickers=['TPL'], db_path=_FULL_DB)
    assert (tpl['accession'] != '0001207097-21-000455').all()
    assert (tpl['accession'] == '0001207097-22-000001').sum() == 14      # 7 transactions x 2 reporting owners
