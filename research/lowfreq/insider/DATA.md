# SEC Form 3/4/5 insider-transaction data (component C03)

Built 2026-09-29 by `scripts/fetch_insider_form345.py build` from the SEC's
quarterly Insider Transactions Data Sets, 2014q1 to 2026q1 (the latest
published; 2026q2 answered 404 on 2026-09-29). Two DuckDB files, one per
research directory, next to each history DB (D03):

| file | filings | non-derivative transactions | max filing date |
|---|---:|---:|---|
| `~/.local/share/mmr/research_full/insider.duckdb` | 2,522,861 | 3,756,762 | 2026-03-31 |
| `~/.local/share/mmr/research_dev/insider.duckdb` (FILING_DATE <= 2024-06-30) | 2,192,957 | derived | 2024-06-28 |

`fetch_insider_form345.py check` and `tests/test_insider_data.py::test_real_dev_database_has_no_row_after_cutoff`
prove the dev copy has no row filed after 2024-06-30 in any of its four tables.
Loader: `trader.data.insider.load_insider_events(start, end, codes=None)` (read-only,
Form 4 by default, superseded filings excluded, one row per transaction x reporting owner
with `n_owners` for joint filings). Consumers should treat `usable_from` as the first
session whose CLOSE may act on the event (D10); the loader asserts `usable_from > filing_date`
on every row it returns.

## What was applied (D10)

- **usable_from** = first XNYS session strictly after `FILING_DATE`. Filings are frequently
  accepted after 16:00 ET (two of the three spot checks below: 17:07 and 17:09) and the data
  set has no time, so the next session is the earliest honest one.
- **Amendments replace originals.** A `4/A` is linked to the filing of the same issuer and
  base form whose `FILING_DATE` equals the amendment's `DATE_OF_ORIG_SUB` and that shares a
  reporting owner (ties broken by period of report); the latest-filed member of the group is
  effective and the rest carry `superseded_by`. Full history: 54,268 amendments, 49,584
  linked, 595 ambiguous (several candidates all superseded), 4,684 unlinked (original outside
  2014q1+, or no owner match) and kept as effective on their own; 50,102 filings superseded.
  Supersession is recomputed inside the dev copy on its own rows, so an amendment filed after
  the cutoff cannot erase an original that was the best information before it (pinned by
  `test_dev_copy_recomputes_supersession_on_its_own_rows`).
- **Re-runs are idempotent**: each quarter is keyed by its zip SHA-256; unchanged quarters are
  skipped, changed/forced ones are deleted and re-inserted in one transaction, and every derived
  column is rebuilt from scratch (`test_rerun_is_idempotent`, `test_changed_zip_replaces_only_its_quarter`).

## Ticker / CIK matching

The stable identity is the issuer CIK (`cik`). The as-filed `ISSUERTRADINGSYMBOL` is
normalised to the PIT universe's Polygon spelling: upper-case, exchange prefixes stripped
(`NYSE: KRC` -> `KRC`), OTC suffixes stripped, list -> first entry, class separators -> dot
(`BRK-B` -> `BRK.B`), and `NONE`/`N/A`/prose -> null. Null symbols inherit the same CIK's most
recent earlier symbol (else earliest later): 2,487,375 filings carry the filed symbol, 12,687
were filled from the issuer, 22,799 (0.9%) have none. Matching to the PIT panel is by
normalised ticker at the event's `usable_from`; a ticker reused by a different company over
time is therefore matched to whoever held it when the filing was made, which is what a
point-in-time join needs. No current-ticker table (e.g. `company_tickers.json`) is used, so
delisted names keep their historical symbols.

**Coverage, PIT top-500 member-months 2016-09 .. 2024-06** (membership rule of
`scripts/pit_panel.py`): 56,756 member-months; 29,249 (51.5%) have at least one effective
Form 4 non-derivative filing usable that month, 2,738 (4.8%) an open-market purchase (P).
The unmatched residue is 13,401 member-months (23.6%) over 1,134 tickers, and it is almost
entirely instruments with no Section 16 insiders: 12,966 member-months over 1,111 tickers have
no 10-K/10-Q fundamentals either (ETFs such as XLE/XLK/TLT/GLD/IWM, ADRs, trusts). Among
operating companies (ticker present in the `fundamentals` table) coverage is 27,129 of 38,001
member-months (71.4%) and the residue is 435 member-months (1.1%) over 23 tickers, itself
mostly grantor trusts and foreign private issuers that file no Form 4 (GLD, SLV, IAU, SHOP,
SHPG, CBOE, BF.B, GBTC, IBIT ...). Coverage in a given month is bounded by insider activity,
not by matching: most companies have no reportable transaction in most months.

### Open-market purchases (code P, Form 4, effective filings) per filing year

| year | P transactions | P filings | issuers (CIK) | matched PIT tickers |
|---|---:|---:|---:|---:|
| 2014 | 32,337 | 17,529 | 3,065 | 1,303 |
| 2015 | 39,504 | 20,462 | 3,229 | 1,537 |
| 2016 | 34,012 | 17,569 | 2,957 | 1,473 |
| 2017 | 27,542 | 15,043 | 2,684 | 1,402 |
| 2018 | 44,107 | 18,953 | 2,872 | 1,611 |
| 2019 | 38,617 | 17,255 | 2,739 | 1,531 |
| 2020 | 30,607 | 18,433 | 3,047 | 1,797 |
| 2021 | 25,572 | 14,430 | 2,785 | 1,582 |
| 2022 | 33,072 | 16,939 | 2,934 | 1,748 |
| 2023 | 28,001 | 15,317 | 2,638 | 1,474 |
| 2024 | 21,882 | 12,114 | 2,420 | 1,303 |
| 2025 | 21,657 | 12,236 | 2,513 | 1,373 |
| 2026 | 4,852 | 2,970 | 1,110 | 555 |

`matched PIT tickers` = distinct tickers with a P purchase that year that ever appear in
`pit_daily_bars` (the panel starts 2016-08; earlier years are matched against its ticker set).

## Spot checks against EDGAR (fetched 2026-09-29 from `www.sec.gov/Archives`)

| # | accession | DB row | EDGAR primary document | acceptance |
|---|---|---|---|---|
| 1 | 0001127602-20-010727 (WFC, CIK 72971; Charles Scharf, CEO & President) | 2020-03-13 P 173,000 @ 28.69 A, 176,916 after; filed 2020-03-16, usable 2020-03-17 | 2020-03-13 P 173000 @ 28.69 A, 176916 | 2020-03-16 13:13 ET |
| 2 | 0001179110-20-005097 (ABBV, CIK 1551152) — a `4/A` superseding 0001179110-20-004036 (filed 2020-03-20: P 2,200 @ 68.18; 800 @ 67.50; 750 @ 69.00) | 2020-03-20 P 1,550 @ 69.00 A, 17,644 after; filed 2020-04-24, usable 2020-04-27 (Mon) | one transaction: 2020-03-20 P 1550 @ 69 A, 17644 | 2020-04-24 17:07 ET |
| 3 | 0001104659-23-074791 (LCID, CIK 1811210; 10% owner, joint filing, 2 reporting owners) | 2023-06-22 P 265,693,703 @ 6.83 A, 1,366,658,905 after; filed 2023-06-26, usable 2023-06-27 | 2023-06-22 P 265693703 @ 6.83 A, 1366658905 | 2023-06-26 17:09 ET |

All three agree field for field. Check 2 shows why D10 replaces rather than appends: the original's
three purchases would double-count with the amended single line. Checks 2 and 3 were accepted
after the close, so the next-session rule is not conservative there, it is exact.

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

Totals: 49 quarters, 2,522,861 filings, 3,756,762 non-derivative and 1,492,816 derivative transactions;
54,268 are 3/A, 4/A or 5/A amendments (4/A: 46,060); per-quarter 4/A and superseded counts are in `fetch_insider_form345.py report`.
## Caveats

- Joint filings produce one row per reporting owner for the same transaction; sum shares
  per `accession, trans_sk` or weight by `1/n_owners`.
- Derivative transactions (option exercises, table II) are loaded into `form345_deriv_trans`
  but are not part of `load_insider_events`; code `M` in the non-derivative table is the
  share leg of an exercise.
- 595 amendments matched several originals and superseded all of them; 4,684 stand alone.
- The SEC Archives return HTTP 403 "Undeclared Automated Tool" unless the User-Agent is of
  the form `<name> <email>`; the data-set downloads accept either form.
