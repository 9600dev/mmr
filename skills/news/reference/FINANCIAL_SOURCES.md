# Financial News Sources — What Works, What Doesn't

Pragmatic field notes on which finance publications survive the scraper cleanly
and which ones fight back. Updated as we find out more. **Always verify
`fetcher_source` on each scrape** — notes below describe the *typical* outcome,
not a guarantee.

## Tier 1 — usually `httpx` clean, fast

These sources serve plain HTML with minimal bot detection. Direct `httpx` fetch
lands in ~1s.

| Source | Notes |
|---|---|
| **Reuters** (`reuters.com`) | Clean extraction. Good ticker coverage. |
| **Stockhead** (`stockhead.com.au`) | ASX-focused. Publishes daily mining/energy digests (*Monsters of Rock*, *High Voltage*, *Ground Breakers*). Primary source for AU small/mid-cap news. |
| **The Motley Fool AU** (`fool.com.au`) | Retail-oriented ASX commentary. Clean HTML. Use with skepticism — editorial, not reporting. |
| **Market Index** (`marketindex.com.au`) | Fast, clean, decent analyst commentary. Carl Capolingua is a useful voice. |
| **Smallcaps** (`smallcaps.com.au`) | ASX explorer/developer coverage. Clean HTML. |
| **Investing News Network** (`investingnews.com`) | Commodity/sector digests with quarterly reviews. Useful for uranium/gold/lithium macro. |
| **CNBC** (`cnbc.com`) | Works fine; watch for stub/video-only pages. |
| **The Guardian Business** (`theguardian.com/business`) | Clean. |

## Tier 2 — usually Playwright, sometimes archive

Direct `httpx` is soft-blocked or returns stub content; Playwright with stealth
plugin usually pushes through. Expect 5–15s per fetch.

| Source | Notes |
|---|---|
| **MarketWatch** (`marketwatch.com`) | Mild bot-check, clears with Playwright. |
| **Barron's** (`barrons.com`) | Paywall-heavy but often partial content is visible to Playwright. Archive fallback for full text. |
| **Seeking Alpha** (`seekingalpha.com`) | Meter paywall. Scraper does pull the ticker block (populates `article.metadata.tickers`). Archive.ph usually has recent articles. |
| **The Australian Financial Review** (`afr.com`) | Paywall; `company-announcements.afr.com` is usually free. |
| **Bloomberg.com** (`bloomberg.com`) | Aggressive bot detection + hard paywall. Playwright-with-stealth sometimes works; otherwise archive.ph. |
| **Financial Times** (`ft.com`) | Hard paywall. Almost always routes through archive.ph fallback. Content often stale by hours-to-days. |

## Tier 3 — archive.ph is the usual path

Hard paywalls that the direct fetchers rarely beat. The archive tier does most
of the work. **Always mention staleness** to the user — archive snapshots can
be days, weeks, or months old.

| Source | Notes |
|---|---|
| **WSJ** (`wsj.com`) | Hard paywall. Archive usually has the latest. |
| **NYT Business / DealBook** (`nytimes.com`) | Metered paywall. Archive usually works. |
| **The Information** (`theinformation.com`) | Hard paywall. Archive hit-rate is mediocre. |
| **The Australian** (`theaustralian.com.au`) | Hard paywall. Archive hit-rate is good for business section. |

## Tier 4 — known problematic

| Source | Issue |
|---|---|
| **mining.com.au** | URLs 404 across all fetcher tiers in testing (Apr 2026). Find the same story on Stockhead or Smallcaps instead. |
| **Twitter / X** (`twitter.com`, `x.com`) | Don't scrape. Content is dynamic, rate-limited, and low-signal for trading. Filter with `exclude_regex`. |
| **YouTube** (`youtube.com`) | Scraper is tuned for prose — video transcripts aren't extracted. Filter with `exclude_regex`. |
| **LinkedIn** (`linkedin.com`) | Aggressive bot-wall, not worth the fight. |
| **Reddit** (`reddit.com`, `old.reddit.com`) | Works but low signal for trading. Consider filtering. |

## Recommended `exclude_regex` for financial search

Keep the results list clean by dropping sources that never give usable content:

```python
FINANCE_EXCLUDE = [
    r"youtube\.com",
    r"twitter\.com",
    r"x\.com",
    r"linkedin\.com",
    r"facebook\.com",
    r"reddit\.com",   # optional — some signal but mostly noise
]

res = await NewsHelpers.search(
    "ASX uranium miners",
    engine="google_news",
    max_results=15,
    exclude_regex=FINANCE_EXCLUDE,
)
```

## Regional notes

### Australia (ASX-focused work)

Primary outlets: **Stockhead**, **Market Index**, **Smallcaps**, **AFR**
(paywalled), **The Australian Business** (paywalled), **The Motley Fool AU**.

Pass `exchange_hint="ASX"` to `ticker_news()` so the query becomes
`"{TICKER} stock ASX news"` — avoids misfiring on US tickers that collide
with AU codes (e.g. AGE = Alligator Energy on ASX, no US overlap currently).

For direct ASX filings (annual reports, quarterlies, substantial holder
notices) the scraper **cannot parse PDFs** — go via `company-announcements.afr.com`
for HTML summaries, or use the S&P Global Market Intelligence data through
sites like `stockanalysis.com` / `simplywall.st` which the scraper handles cleanly.

### US

Primary outlets: **Reuters**, **CNBC**, **MarketWatch**, **Barron's** (tier 2),
**WSJ** (archive), **Seeking Alpha** (meter), **Bloomberg.com** (tier 2/archive).

### UK / Europe

Primary: **FT** (archive), **The Guardian Business**, **Reuters**.

### Asia

Primary: **Reuters Asia**, **Nikkei Asia** (metered, archive-friendly), **SCMP**
(partial paywall).

## Freshness cheat-sheet

`google_news` engine is the only mode that returns `published_at` directly in
search results. When you need time-sensitive news (event trading, earnings
reactions), always prefer:

```python
NewsHelpers.search(query, engine="google_news", max_results=10)
```

For scrape results, `article.metadata.published_at` is populated from JSON-LD
/ OG meta / Dublin Core / `<time>` tags — it's best-effort and may be `null`.
If `published_at` is missing and `fetcher_source` starts with `archive:`, treat
the article as "unknown age" and hunt for a primary source before trading on it.


## Quick-reference: "latest news" query templates

Copy-paste-ready query strings that work well with `engine="google_news"`.
All of these benefit from including the current `MONTH YEAR` to push recency.

### Single ticker
- `"{TICKER} stock news"` — generic, US default
- `"{TICKER} ASX news this week"` — Australia
- `"{TICKER} earnings {MONTH} {YEAR}"` — reporting season
- `"{TICKER} analyst upgrade downgrade"` — rating changes
- `"{TICKER} guidance"` — guidance updates
- `"{COMPANY NAME} {TICKER} news"` — when the ticker is ambiguous (e.g. "AGE Alligator Energy ASX news")

### Sector — ASX
- `"ASX gold miners news this week"`
- `"ASX uranium miners news this week"`
- `"ASX lithium miners news this week"`
- `"ASX copper miners news this week"`
- `"ASX iron ore news"`
- `"ASX tech stocks news today"`
- `"ASX bank stocks news today"`

### Sector — US
- `"semiconductor stocks news today"`
- `"AI stocks news this week"`
- `"oil stocks news {MONTH} {YEAR}"`
- `"biotech stocks news today"`

### Macro / global
- `"Fed rate decision today"` / `"FOMC meeting {MONTH} {YEAR}"`
- `"RBA rate decision"` (Australia) / `"ECB rate decision"` (Europe) / `"BoJ rate decision"` (Japan)
- `"US CPI report {MONTH} {YEAR}"` / `"US jobs report today"`
- `"China GDP {QUARTER} {YEAR}"`
- `"crude oil price news today"` / `"gold price news today"` / `"bitcoin news today"`
- `"Middle East oil {MONTH} {YEAR}"` — geopolitical risk premium
- `"IMF WEO {MONTH} {YEAR}"` — IMF economic outlook

### Event-driven
- `"{COMPANY} CEO resigns"` / `"{COMPANY} CFO"`
- `"{COMPANY} acquisition"` / `"{COMPANY} merger"`
- `"{COMPANY} trading halt"`
- `"{COMPANY} FDA approval"` (biotech)
- `"{COMPANY} production update"` (miners)
- `"{COMPANY} quarterly activities report"` (ASX explorers/developers)

### ASX-specific signals
- `"ASX trading halts today"` — always high-signal for the portfolio
- `"ASX 200 movers today"`
- `"Stockhead Monsters of Rock"` (weekly mining digest column)
- `"Stockhead High Voltage"` (weekly energy-transition digest)
- `"Stockhead Ground Breakers"` (exploration-focused digest)

## Composing queries programmatically

```python
import datetime as dt

now = dt.datetime.now()
month_year = now.strftime("%B %Y")       # "April 2026"
this_week = "this week"

# Portfolio scan
queries = [
    f"{t} stock ASX news {this_week}" for t in ["BHP", "RIO", "FMG", "NEM", "NST"]
]

# Sector-wide sweep
sectors = ["gold", "uranium", "lithium", "copper", "iron ore"]
queries = [f"ASX {s} miners news {this_week}" for s in sectors]

# Macro daily
macro = [
    "Fed rate decision today",
    "US CPI report today",
    f"RBA rate decision {month_year}",
    "crude oil price news today",
]
```
