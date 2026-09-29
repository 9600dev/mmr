# SEC Form 3/4/5 insider-transaction data (component C03)

Built 2026-09-29 by `scripts/fetch_insider_form345.py build` from the SEC's
quarterly Insider Transactions Data Sets, 2014q1 to 2026q1 (latest published). One DuckDB file per research
directory, beside its history DB (D03):

| file | filings | non-derivative transactions | max filing date |
|---|---:|---:|---|
| `~/.local/share/mmr/research_full/insider.duckdb` | 2,522,861 | 3,756,762 | 2026-03-31 |
| `~/.local/share/mmr/research_dev/insider.duckdb` (FILING_DATE <= 2024-06-30) | 2,192,957 | derived | 2024-06-28 |

`fetch_insider_form345.py check` and `tests/test_insider_data.py::test_real_dev_database_has_no_row_after_cutoff`
prove the dev copy has no row filed after 2024-06-30 in any table.
Loader: `trader.data.insider.load_insider_events(start, end, codes=None)`: read-only, Form 4 by
default, superseded filings excluded, one row per transaction x reporting owner (`n_owners`),
`usable_from > filing_date` asserted on every row.

## What was applied (D10)

- **usable_from** = first XNYS session strictly after `FILING_DATE`: the data set carries no time
  and filings are often accepted after 16:00 ET (two of three spot checks below).
- **Amendments replace originals**, matched in five tiers, each tried only when the previous
  found nothing: (1) DATE: same issuer, base form and a shared reporting owner, `FILING_DATE` equal
  to the amendment's `DATE_OF_ORIG_SUB` (chains through earlier amendments are walked to their
  root); (2) CONTENT: shared owner, identical non-derivative transaction list within the prior
  45 days; (3) PERIOD: shared owner, a UNIQUE original with the same period of report within 45
  days; (4) OWNER-FREE: the amendment corrects the reporting owner's identity (CYAN
  `0001010549-14-000096` moves a gift from an individual's CIK to a foundation's): same named
  date, same period, IDENTICAL transaction list including shares owned after, UNIQUE; (5)
  OWNER-FREE, CORE CONTENT: an owner correction that also corrects the holdings (BATL
  `0001209191-21-056275`, "inadvertently reported under Grant Evans", shares
  after 9,551 -> 11,601): same named date and period, identical date/code/shares/price/A-D, UNIQUE,
  both accessions from the same submitter prefix, and the amending owner has no earlier filing of
  their own for that period. Guards set on the full history, where the core match alone found 10
  unique pairs: footnotes confirm six as owner corrections (BATL, CLRO, RTEC, SFST, FMCB, WCUI);
  ABCB is two self-filers with identical grants, LSCC/OKE officers re-filing their own months-old
  reports beside a colleague's identical withholding - refused. Different content without a shared owner is never linked.
  Within a tier candidates are ranked (same period +2, original +1) and the top-scored linked; a
  group is effective in its latest-filed member (then higher accession). Earlier
  rounds missed SWM `0001062993-22-012373` (same day, lower accession), TPL `0001207097-22-000001`
  (chain through an earlier amendment), CYAN and BATL, all pinned on the real database plus fourteen
  fixture patterns.
  Full history: 54,268 amendments (46,060 `4/A`); 51,006 linked (tier 1: 49,729; 2: 377; 3: 844; 4: 50; 5: 6), 627 ambiguous (all superseded), 23 tier-3, 3 tier-4 and 15 tier-5 candidates refused, 3,262 unlinked (original before 2014 or no match) and effective; 51,555 filings superseded. Residual check (effective same-issuer Form 4 pairs within 10
  days, identical transaction lists): 78 same-owner pairs involve an amendment (348 before tiers 2-5). Ignoring the owner, 1,388 such pairs remain: a sample of ten was all different directors' identical grants, hence the named-date requirement in tiers 4-5. Supersession is recomputed inside the dev
  copy on its own rows, so a post-cutoff amendment cannot erase what was the best information
  before it (`test_dev_copy_recomputes_supersession_on_its_own_rows`).
- **Re-runs are idempotent**: each quarter is keyed by its zip SHA-256; unchanged quarters are
  skipped, changed/forced ones are deleted and re-inserted in one transaction, and every derived
  column is rebuilt (`test_rerun_is_idempotent`, `test_changed_zip_replaces_only_its_quarter`).

## Ticker / CIK matching

The stable identity is the issuer CIK (`cik`). The as-filed `ISSUERTRADINGSYMBOL` is
normalised to the PIT universe's Polygon spelling (upper-case, `NYSE: KRC` -> `KRC`, OTC suffix
stripped, list -> first entry, `BRK-B` -> `BRK.B`, `NONE`/`N/A`/prose -> null). Null symbols inherit the same CIK's latest
earlier symbol (else earliest later): 2,487,375 filings carry the filed symbol, 12,687 were
filled, 22,799 (0.9%) have none. PIT matching is by normalised ticker at the
event's `usable_from`, so a reused ticker matches whoever held it then and delisted names keep
their historical symbols.

**Coverage, PIT top-500 member-months 2016-09 .. 2024-06** (membership rule of
`scripts/pit_panel.py`): 56,756 member-months; 29,248 (51.5%) have an effective Form 4
non-derivative filing usable that month, 2,738 (4.8%) an open-market purchase (P).
The unmatched residue, 13,401 member-months (23.6%) over 1,134 tickers, is almost entirely
instruments without Section 16 insiders: 12,966 member-months over 1,111 tickers have
no 10-K/10-Q fundamentals either (ETFs like XLE/TLT/GLD, ADRs, trusts). Among
operating companies (ticker in the `fundamentals` table) coverage is 27,128 of 38,001
member-months (71.4%); the residue, 435 member-months (1.1%) over 23 tickers, is mostly
grantor trusts and foreign private issuers that file no Form 4 (GLD, SLV, IAU, SHOP,
SHPG, CBOE, BF.B, GBTC ...).

### Open-market purchases (code P, Form 4, effective filings) per filing year

| year | P transactions | P filings | issuers (CIK) | matched PIT tickers |
|---|---:|---:|---:|---:|
| 2014 | 32,319 | 17,515 | 3,065 | 1,303 |
| 2015 | 39,457 | 20,440 | 3,229 | 1,537 |
| 2016 | 33,997 | 17,557 | 2,957 | 1,473 |
| 2017 | 27,522 | 15,026 | 2,683 | 1,402 |
| 2018 | 44,097 | 18,943 | 2,872 | 1,611 |
| 2019 | 38,609 | 17,249 | 2,739 | 1,531 |
| 2020 | 30,587 | 18,416 | 3,047 | 1,797 |
| 2021 | 25,558 | 14,423 | 2,785 | 1,582 |
| 2022 | 33,063 | 16,933 | 2,934 | 1,748 |
| 2023 | 27,981 | 15,307 | 2,638 | 1,474 |
| 2024 | 21,863 | 12,106 | 2,419 | 1,302 |
| 2025 | 21,638 | 12,227 | 2,512 | 1,372 |
| 2026 | 4,851 | 2,969 | 1,110 | 555 |

`matched PIT tickers` = P-purchase tickers ever present in `pit_daily_bars`.

## Spot checks against EDGAR (fetched 2026-09-29 from `www.sec.gov/Archives`)

| # | accession | DB row | EDGAR primary document | acceptance |
|---|---|---|---|---|
| 1 | 0001127602-20-010727 (WFC, CIK 72971; Charles Scharf, CEO & President) | 2020-03-13 P 173,000 @ 28.69 A, 176,916 after; filed 2020-03-16, usable 2020-03-17 | 2020-03-13 P 173000 @ 28.69 A, 176916 | 2020-03-16 13:13 ET |
| 2 | 0001179110-20-005097 (ABBV, CIK 1551152), a `4/A` superseding 0001179110-20-004036 (three P rows: 2,200 @ 68.18; 800 @ 67.50; 750 @ 69.00) | 2020-03-20 P 1,550 @ 69.00 A, 17,644 after; filed 2020-04-24, usable 2020-04-27 | 2020-03-20 P 1550 @ 69 A, 17644 (one row) | 2020-04-24 17:07 ET |
| 3 | 0001104659-23-074791 (LCID, CIK 1811210; 10% owner, joint filing, 2 reporting owners) | 2023-06-22 P 265,693,703 @ 6.83 A, 1,366,658,905 after; filed 2023-06-26, usable 2023-06-27 | 2023-06-22 P 265693703 @ 6.83 A, 1366658905 | 2023-06-26 17:09 ET |

All three agree field for field; checks 2 and 3 were accepted after the close, where the
next-session rule is exact.

## Rows per quarter (filings / non-derivative transactions, as loaded)

| year | Q1 | Q2 | Q3 | Q4 |
|---|---:|---:|---:|---:|
| 2014 | 72,485 / 109,861 | 56,980 / 76,117 | 43,042 / 62,643 | 47,604 / 72,233 |
| 2015 | 73,143 / 108,960 | 57,345 / 75,633 | 42,553 / 61,134 | 43,357 / 65,272 |
| 2016 | 68,776 / 98,625 | 53,490 / 67,992 | 40,073 / 58,453 | 41,983 / 62,517 |
| 2017 | 70,217 / 106,323 | 53,955 / 72,124 | 37,451 / 52,857 | 40,873 / 60,811 |
| 2018 | 68,559 / 105,102 | 53,913 / 72,030 | 38,571 / 57,676 | 41,008 / 71,126 |
| 2019 | 67,537 / 107,842 | 53,007 / 72,587 | 37,647 / 56,014 | 38,145 / 57,423 |
| 2020 | 69,408 / 105,852 | 51,459 / 70,429 | 39,096 / 60,619 | 43,607 / 72,973 |
| 2021 | 74,944 / 125,620 | 58,798 / 89,108 | 44,057 / 74,002 | 46,675 / 80,096 |
| 2022 | 71,395 / 110,327 | 55,261 / 69,774 | 37,955 / 53,743 | 38,307 / 58,429 |
| 2023 | 69,457 / 106,093 | 55,207 / 68,588 | 37,036 / 53,078 | 38,108 / 57,852 |
| 2024 | 67,671 / 111,404 | 52,802 / 69,412 | 36,075 / 55,881 | 38,547 / 63,647 |
| 2025 | 63,284 / 103,029 | 50,861 / 66,915 | 35,457 / 55,125 | 36,421 / 59,678 |
| 2026 | 69,259 / 103,733 | - | - | - |

Totals: 49 quarters, 2,522,861 filings, 3,756,762 non-derivative and 1,492,816 derivative
transactions; per-quarter 4/A and superseded counts: `fetch_insider_form345.py report`.
## Caveats

- Joint filings produce one row per reporting owner; sum shares per `accession, trans_sk` or weight by `1/n_owners`.
- Derivative transactions (table II) are stored in `form345_deriv_trans`, not returned by
  `load_insider_events`.
- 3,262 amendments stand alone as effective; a `4/A` that changes the numbers and names a
  wrong original date outside tiers 2-5 still double-counts, as does an owner's amendment of a
  filing older than 45 days (LSCC `0000855658-22-000265`, OKE `0001741956-22-000014`).
- `www.sec.gov/Archives` refuses a parenthesised User-Agent (403 "Undeclared Automated Tool");
  `MMR Research joelpob@gmail.com` works.
