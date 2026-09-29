# SEC Form 3/4/5 insider-transaction data (component C03)

Built 2026-09-29 by `scripts/fetch_insider_form345.py build` from the SEC's
quarterly Insider Transactions Data Sets, 2014q1 to 2026q1. One DuckDB file per research
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
- **Amendments replace originals**, matched in six tiers, each tried only when the previous
  found nothing: (1) DATE: same issuer, base form and a shared reporting owner, `FILING_DATE` equal
  to the amendment's `DATE_OF_ORIG_SUB` (chains are walked to their root); (2) CONTENT: shared
  owner, identical non-derivative transaction list within the prior 45 days; (3) PERIOD: shared
  owner, a UNIQUE original with the same period of report within 45 days; (4) OWNER-FREE: the
  amendment corrects the reporting owner's identity (CYAN `0001010549-14-000096`): same named date and period, IDENTICAL transaction
  list including shares owned after, UNIQUE; (5) OWNER-FREE, CORE CONTENT: an owner correction
  that also corrects the holdings (BATL `0001209191-21-056275`, shares after 9,551 -> 11,601): same named date and period, identical
  date/code/shares/price/A-D, UNIQUE, same submitter prefix on both accessions, and the amending
  owner has no earlier filing of their own for that period - footnotes confirm the six full-history matches (BATL, CLRO, RTEC, SFST, FMCB, WCUI); ABCB (two self-filers with identical grants) is refused; (6) OWNER + PERIOD,
  NO WINDOW: tier 3 beyond 45 days - a UNIQUE same-owner original with the same period, any age
  (LSCC `0000855658-22-000265`, 157 days; OKE `0001741956-22-000014`, 308 days; 14 sampled pairs confirmed by footnotes). Within a tier the top-ranked candidates (same period
  +2, original +1) are linked; a group's effective member is its latest-filed (then higher
  accession). Every named case is pinned on the real database plus sixteen fixture patterns.
  Full history: 54,268 amendments (46,060 `4/A`); 51,676 linked (tier 1: 49,729; 2: 377; 3: 844; 4: 50; 5: 6; 6: 670), 627 ambiguous (all superseded), 23 tier-3, 3 tier-4, 13 tier-5 and 9 tier-6 candidates refused, 2,592 unlinked (original before 2014 or no match) and effective; 52,225 filings superseded. Residual duplicate check (same-issuer effective Form 4 pairs within 10 days with identical transaction lists): 77 same-owner pairs involve an amendment (348 before tiers 2-6); 1,387 owner-free pairs are different directors' identical grants (sample of ten). Supersession is recomputed inside the dev
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
`scripts/pit_panel.py`): 56,756 member-months; 29,247 (51.5%) have an effective Form 4
non-derivative filing usable that month, 2,738 (4.8%) an open-market purchase (P).
The unmatched residue, 13,401 member-months (23.6%) over 1,134 tickers, is almost entirely
instruments without Section 16 insiders: 12,966 member-months over 1,111 tickers have
no 10-K/10-Q fundamentals either (ETFs like XLE/TLT/GLD, ADRs, trusts). Among
operating companies (ticker in the `fundamentals` table) coverage is 27,127 of 38,001
member-months (71.4%); the residue, 435 member-months (1.1%) over 23 tickers, is mostly
grantor trusts and foreign private issuers that file no Form 4 (GLD, SLV, IAU, SHOP,
SHPG, CBOE, BF.B, GBTC ...).

### Open-market purchases (code P, Form 4, effective filings) per filing year

| year | P transactions | P filings | issuers (CIK) | matched PIT tickers |
|---|---:|---:|---:|---:|
| 2014 | 32,314 | 17,510 | 3,065 | 1,303 |
| 2015 | 39,444 | 20,430 | 3,228 | 1,537 |
| 2016 | 33,991 | 17,552 | 2,957 | 1,473 |
| 2017 | 27,517 | 15,022 | 2,683 | 1,402 |
| 2018 | 44,096 | 18,942 | 2,872 | 1,611 |
| 2019 | 38,605 | 17,246 | 2,739 | 1,531 |
| 2020 | 30,574 | 18,409 | 3,047 | 1,797 |
| 2021 | 25,549 | 14,417 | 2,784 | 1,581 |
| 2022 | 33,058 | 16,928 | 2,934 | 1,748 |
| 2023 | 27,978 | 15,304 | 2,638 | 1,474 |
| 2024 | 21,861 | 12,104 | 2,419 | 1,302 |
| 2025 | 21,636 | 12,225 | 2,512 | 1,372 |
| 2026 | 4,851 | 2,969 | 1,110 | 555 |

`matched PIT tickers` = P-purchase tickers ever present in `pit_daily_bars`.

## Spot checks against EDGAR (fetched 2026-09-29 from `www.sec.gov/Archives`)

| # | accession | DB row | EDGAR primary document | acceptance |
|---|---|---|---|---|
| 1 | 0001127602-20-010727 (WFC, CIK 72971; Charles Scharf, CEO & President) | 2020-03-13 P 173,000 @ 28.69 A, 176,916 after; filed 2020-03-16, usable 2020-03-17 | 2020-03-13 P 173000 @ 28.69 A, 176916 | 2020-03-16 13:13 ET |
| 2 | 0001179110-20-005097 (ABBV, CIK 1551152), a `4/A` superseding 0001179110-20-004036 (three P rows: 2,200 @ 68.18; 800 @ 67.50; 750 @ 69.00) | 2020-03-20 P 1,550 @ 69.00 A, 17,644 after; filed 2020-04-24, usable 2020-04-27 | 2020-03-20 P 1550 @ 69 A, 17644 (one row) | 2020-04-24 17:07 ET |
| 3 | 0001104659-23-074791 (LCID, CIK 1811210; 10% owner, joint filing, 2 reporting owners) | 2023-06-22 P 265,693,703 @ 6.83 A, 1,366,658,905 after; filed 2023-06-26, usable 2023-06-27 | 2023-06-22 P 265693703 @ 6.83 A, 1366658905 | 2023-06-26 17:09 ET |

All three agree field for field; checks 2 and 3 were accepted after the close.

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
- **Unmatched amendments (D14)**: 2,592 amendments no tier could pair with an original (2,556
  effective) are kept and counted, not special-cased; BIF `0001247997-20-000001` (a trust re-filing an
  affiliate's self-filed purchase under its own CIK) has the ABCB look-alike shape. They carry 553 of 381,474 effective Form 4 P transactions (0.14%) and $2.71 B of $1.48 T
  purchase value (0.18%); within PIT tickers 105 of 152,676 (0.07%) and $155 M of $278 B (0.06%).
  Value sums exclude 430 rows above $10 B or $100,000/share (mis-keyed prices).
  `link_tier` (NULL = unmatched) and `amends_accession` are persisted per amendment.
- `www.sec.gov/Archives` 403s a parenthesised User-Agent; `MMR Research joelpob@gmail.com` works.
