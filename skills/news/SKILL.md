---
name: news
description: Fetch, search, and summarize financial news articles via the local news-service at http://127.0.0.1:8089. Handles Cloudflare, paywalls (FT/WSJ/Bloomberg/NYT via archive.ph fallback), and HTML→Markdown extraction. Purpose-built for mmr — includes ticker_news() and portfolio_news() helpers that compose search+scrape for an mmr portfolio or idea scan.
metadata:
  author: mmr
  version: "0.1"
---

# News Skill (mmr)

**LLMVM**: Use `<helpers>...</helpers>` blocks to call `NewsHelpers` methods. All methods are `async`. Returns are dicts in the mmr convention — check `result.get("ok")` and report `fetcher_source` before trusting content.

This skill wraps a local Dockerized scraper that lives in `~/dev/news`. The service provides browser-style fetching (Playwright + stealth), an `httpx → playwright → archive.ph` fallback chain, aggressive HTML→Markdown extraction via `rustdown`, and (optionally) LLM extraction over the scraped body.

## When to invoke this skill

Trigger when the user:

- Asks you to **fetch / scrape / read / summarize** a specific article URL
- Asks for **news on a ticker** or portfolio position (use `ticker_news()` / `portfolio_news()`)
- Wants a **narrative digest** on a market catalyst (use `search_and_scrape()`)
- Hits a **paywall, 403, or Cloudflare challenge** on a finance source
- Wants **Reuters / Bloomberg / WSJ / FT / CNBC / Seeking Alpha / Stockhead / Motley Fool AU / Market Index** article content

Do **not** invoke for:

- Raw price/quote lookups — use **mmr-skill** (`MMRHelpers.snapshot`, `MMRHelpers.history_massive`)
- Fundamentals — use **mmr-skill** (`MMRHelpers.balance_sheet`, etc.)
- Arbitrary non-article URLs (API endpoints, images, GitHub pages)

## Service pre-flight: always check health first

```python
h = await NewsHelpers.health()
# {"status": "ok", "version": "0.1.0", "llm_enabled": false, "ok": true}
```

If this returns `ok: False` with `"connection refused"`, the service isn't running. Start it:

```bash
cd ~/dev/news && ./docker.sh -g
```

`-g` builds (if needed), starts, and tails logs. Wait for `news service ready on http://...` in the log, then re-check health.

## `NewsHelpers` — Method Summary

| Method | Purpose |
|--------|---------|
| `NewsHelpers.health()` | Ping the service — run before any scrape batch |
| `NewsHelpers.search(query, engine="auto", max_results=10, exclude_regex=None)` | Search only. Engines: `auto`, `ddg`, `google`, `google_news`. Use `google_news` for freshness + source attribution |
| `NewsHelpers.scrape(url, extract=None, follow_links=0, allow_archive_fallback=True, include_html=False)` | Scrape one URL. Optional LLM extraction (requires `llm_enabled: true`), optional N-hop follow |
| `NewsHelpers.markdown(url)` | Convenience: returns just the article body markdown string |
| `NewsHelpers.search_and_scrape(query, max_results=5, concurrency=3, engine="google_news")` | Composite: search + parallel scrape of top N results. Preserves search metadata (source, published_at) per article |
| `NewsHelpers.ticker_news(ticker, max_results=5, extract=None, exchange_hint=None)` | **mmr-native**: google_news search `"{TICKER} stock [exchange_hint] news"` + parallel scrape |
| `NewsHelpers.portfolio_news(tickers, max_results_per_ticker=3, exchange_hint=None)` | **mmr-native**: fan out `ticker_news()` across a list of tickers |
| `NewsHelpers.get_images(url, max_images=5, min_bytes=5000, max_bytes=1_500_000)` | Extract inline images from an article as llmvm `ImageContent` (LLM-visible). Filters logos/icons/tracking pixels. `og:image` / `twitter:image` are ranked first so the hero photo wins over sidebar promos |

## Core patterns

### Pattern 1: Read a specific article

```python
res = await NewsHelpers.scrape("https://www.reuters.com/markets/...")
if res.get("ok") and res.get("status") == "ok":
    art = res["article"]
    print(art["title"])
    print(f"fetched via: {art['fetcher_source']}")
    print(f"published: {art.get('metadata', {}).get('published_at')}")
    print(art["markdown"])
```

**Always inspect `fetcher_source`** — if it starts with `archive:` the content is a snapshot and may be days/weeks/months old. That matters for trading decisions. Mention it to the user.

### Pattern 2: News on a specific ticker

```python
res = await NewsHelpers.ticker_news("BHP", max_results=5, exchange_hint="ASX")
for a in res["articles"]:
    if a.get("ok"):
        art = a["article"]
        meta = a.get("search", {})
        print(f"[{meta.get('source')}] {art['title']}")
        print(f"  {meta.get('published_at')} · via {art['fetcher_source']}")
```

`exchange_hint` disambiguates foreign tickers. Use `"ASX"`, `"LSE"`, `"HKEX"`, `"TSE"` where relevant. Without it, the query is just `"{TICKER} stock news"` which biases US-centric.

### Pattern 3: News digest for an entire portfolio

```python
portfolio = await MMRHelpers.portfolio()
tickers = [p["symbol"] for p in portfolio["data"][:10]]
news = await NewsHelpers.portfolio_news(tickers, max_results_per_ticker=3)

for sym, articles in news["by_ticker"].items():
    print(f"\n=== {sym} ===")
    for a in articles:
        if a.get("ok"):
            print(f"  - {a['article']['title']}")
```

### Pattern 4: Narrative digest on a macro catalyst

```python
bundle = await NewsHelpers.search_and_scrape(
    "Fed rate decision 2026 dot plot",
    max_results=5,
    engine="google_news",
)
# Hand the lot to llm_call for synthesis
corpus = "\n\n---\n\n".join(
    a["article"]["markdown"] for a in bundle["articles"] if a.get("ok")
)
digest = await llm_call([corpus], "Summarize the narrative and quote key voices.")
emit(digest)
```

### Pattern 5: Targeted LLM extraction (requires `llm_enabled: true` on the service)

```python
res = await NewsHelpers.scrape(
    "https://www.reuters.com/...",
    extract="Name the CEO quoted and summarize their statement in one sentence.",
)
if res.get("extraction"):
    print(res["extraction"]["text"])
# If the service has llm_enabled=false this returns HTTP 422 with
# error.code == "llm_disabled" — see reference/operations.md to enable.
```

### Pattern 6: Follow-link synthesis (mode 3)

```python
res = await NewsHelpers.scrape(
    "https://www.reuters.com/markets/weekly-wrap/",
    extract="Which central banks moved rates this week and by how much?",
    follow_links=3,
)
# The LLM picks up to 3 linked URLs from the seed, scrapes them in parallel,
# and synthesises the extraction across the combined corpus.
print(res.get("extraction", {}).get("text"))
print("followed:", [p["url"] for p in res.get("follow_pages", []) if p.get("ok")])
```

### Pattern 7: Extract images from an article (LLM-visible)

The news-service strips images during HTML→Markdown conversion. Use
`get_images()` when the visual content is part of the story — a
chart, a price graph, a map, a product photo, a CEO headshot.

```python
res = await NewsHelpers.get_images(
    "https://stockhead.com.au/resources/...",
    max_images=3,       # cap — LLM visual budget is not infinite
    min_bytes=5000,     # filters out logos, icons, tracking pixels
)
if res["ok"]:
    for img in res["images"]:
        emit(img)                # LLM can now see the image
    for m in res["image_meta"]:
        print(f"  {m['alt']} ({m['bytes']} bytes, {m['content_type']})")
```

Under the hood this:
1. Calls `scrape(url, include_html=True)` to get the cleaned HTML.
2. Parses `<img>` tags (including lazy-load variants `data-src`, `data-lazy-src`).
3. Resolves relative URLs, filters ad/analytics hosts, SVGs, GIFs.
4. Downloads surviving candidates in parallel via `httpx`.
5. Drops anything under `min_bytes` (most logos/icons/favicons are <5KB).
6. Wraps remaining bytes as `llmvm_lite.llm_types.ImageContent` objects.

Returns:

```python
{
  "ok": True,
  "article_title": "…",
  "article_url": "…",              # final URL after redirects
  "fetcher_source": "httpx",        # or "playwright", "archive:*"
  "count": 3,
  "images": [ImageContent, ...],    # pass to emit() or llm_call()
  "image_meta": [{src, alt, bytes, content_type}, ...],
  "skipped": [{src, reason}, ...],  # why each candidate was dropped
}
```

**Use sparingly.** Each image adds meaningful tokens to the LLM context
(a 1200×675 JPEG is typically 1,500–3,000 visual tokens). 1–3 images per
article is usually the sweet spot for market commentary; set `max_images=1`
if you only need the hero image.

**Works via the scrape fallback chain.** Since `get_images()` piggybacks
on `scrape(include_html=True)`, it benefits from the same
`httpx → playwright → archive` tier — so it'll still pull images out of
Cloudflare-protected or paywalled articles that come through the
archive tier.

**Passing to `llm_call()` for visual analysis:**

```python
res = await NewsHelpers.get_images(chart_article_url, max_images=1)
if res["images"]:
    analysis = await llm_call(
        res["images"] + [res.get("markdown", "")],
        "Read the chart in the image and tell me the key price levels.",
    )
    emit(analysis)
```

## Finding the latest financial news

The server doesn't support date filters directly — fresh news is a function of
**(a) using `google_news`** (only engine that returns `published_at`), **(b) phrasing queries
with temporal anchors** (`"today"`, `"this week"`, the current month/year), and
**(c) sorting/filtering on `published_at` client-side** after fetching.

### Recipe A: latest news for a single ticker

`ticker_news()` already uses `google_news` under the hood. For max recency,
add a date anchor to the query by passing it through `search_and_scrape()`:

```python
import datetime as dt

this_month = dt.datetime.now().strftime("%B %Y")  # e.g. "April 2026"
bundle = await NewsHelpers.search_and_scrape(
    f"AGE Alligator Energy ASX news {this_month}",
    max_results=8,
    engine="google_news",
)
# Sort by published_at (most recent first); None values sink to the bottom.
articles = sorted(
    [a for a in bundle["articles"] if a.get("ok")],
    key=lambda a: a.get("search", {}).get("published_at") or "",
    reverse=True,
)
for a in articles[:5]:
    s = a["search"]
    print(f"{s.get('published_at')} · [{s.get('source')}] {a['article']['title']}")
```

### Recipe B: latest news on an entire sector

For "what's moving in ASX uranium miners this week" style scans:

```python
bundle = await NewsHelpers.search_and_scrape(
    "ASX uranium miners news this week",
    max_results=15,
    engine="google_news",
    exclude_regex=[r"youtube\.com", r"twitter\.com", r"x\.com", r"reddit\.com"],
)
# Group by source or sort by date
for a in bundle["articles"]:
    if a.get("ok"):
        s = a["search"]
        print(f"{s.get('published_at')} · [{s.get('source')}] {a['article']['title']}")
        print(f"  {a['article']['final_url']}")
```

Good sector-level query templates that tend to surface *new* stories:

- `"ASX {sector} miners news this week"` — e.g. gold, uranium, lithium, copper
- `"{sector} stocks today {MONTH} {YEAR}"` — e.g. "semiconductor stocks today April 2026"
- `"{sector} earnings {MONTH} {YEAR}"` — around reporting season
- `"{country} central bank decision {MONTH} {YEAR}"` — for macro catalysts
- `"{company} CEO resigns"` / `"{company} guidance cut"` — high-signal event queries

### Recipe C: daily pre-market digest (mmr portfolio)

The canonical "what happened overnight that I need to know about" workflow —
run this off `cron` or inside `mmr-loop-skill`:

```python
# 1. Health check — fail loudly if service is down
h = await NewsHelpers.health()
assert h.get("ok"), f"news-service down: {h.get('error')}"

# 2. Pull portfolio tickers
pf = await MMRHelpers.portfolio()
syms = [p["symbol"] for p in pf["data"]]

# 3. Fan out news for each ticker
news = await NewsHelpers.portfolio_news(
    syms, max_results_per_ticker=3, exchange_hint="ASX",  # or drop hint for US
)

# 4. Build a compact corpus of (source · date · title · body) per ticker
from collections import defaultdict
corpus = []
for sym, articles in news["by_ticker"].items():
    for a in articles:
        if a.get("ok"):
            meta = a.get("search", {})
            art = a["article"]
            corpus.append(
                f"### {sym} · {meta.get('published_at','?')} · {meta.get('source','?')}\n"
                f"**{art['title']}**\n\n{art['markdown'][:2000]}"
            )
            break  # top article per ticker is usually enough

# 5. Summarise for the user
digest = await llm_call(
    ["\n\n---\n\n".join(corpus)],
    "Produce a pre-market briefing: for each ticker, 1-2 sentences on the top news "
    "item and whether it's materially bullish, bearish, or neutral for the next "
    "trading session. Flag anything that could move the stock >3%.",
)
emit(digest)
```

### Recipe D: latest news with event-driven filter (extract mode)

When you want the scraper + LLM to do the "is this material?" filtering for you:

```python
res = await NewsHelpers.ticker_news(
    "BHP", max_results=8, exchange_hint="ASX",
    extract=(
        "Is this article reporting a NEW event from the last 48 hours "
        "(earnings, guidance change, M&A, management change, major production update)? "
        "If yes, summarise in one sentence and classify direction (bullish/bearish/neutral). "
        "If no (e.g. it's an analyst think-piece or evergreen explainer), reply 'SKIP'."
    ),
)
for a in res["articles"]:
    if not a.get("ok"):
        continue
    ex = a.get("extraction", {}).get("text", "")
    if ex.strip().startswith("SKIP"):
        continue
    meta = a.get("search", {})
    print(f"[{meta.get('source')}] {a['article']['title']}")
    print(f"  {ex}")
```

Requires `llm_enabled: true` on the news-service.

### Recipe E: macro catalyst watch (no ticker)

For macro events (rate decisions, CPI, jobs reports, geopolitical shocks):

```python
bundle = await NewsHelpers.search_and_scrape(
    "Fed rate decision today",        # or "CPI report", "jobs report", "RBA decision"
    max_results=10,
    engine="google_news",
)
# The freshest 3 usually suffice
fresh = sorted(
    [a for a in bundle["articles"] if a.get("ok")],
    key=lambda a: a.get("search", {}).get("published_at") or "",
    reverse=True,
)[:3]

corpus = "\n\n---\n\n".join(a["article"]["markdown"] for a in fresh)
briefing = await llm_call(
    [corpus],
    "Summarise the decision and market reaction in 5 bullets. Quote any explicit numbers."
)
emit(briefing)
```

### Gotchas when chasing freshness

- **`published_at` may be `null`.** Some sites don't expose it in their HTML metadata. Don't filter those out — they might still be recent. Surface them separately or treat them as "unknown age".
- **`archive:*` sources are always stale.** If `fetcher_source` starts with `archive:`, the article might be days/weeks/months old regardless of what `published_at` says. Always mention this to the user.
- **`rate_limited: true`** on an empty `google_news` result means SerpAPI hit a quota — retry with backoff, or fall back to `engine="ddg"` (no date metadata but still functional).
- **Query phrasing matters more than you\'d expect.** `"ASX gold miners"` returns evergreen explainers; `"ASX gold miners news this week"` returns actual news. Always include a temporal anchor.
- **Don\'t trust a single source for breaking news.** Scrape 3–5 in parallel via `search_and_scrape` and cross-reference before acting.

## mmr integration guidance

### Pairing with mmr-skill

- **Ideas → News**: run an `MMRHelpers.ideas()` scan, then `portfolio_news()` on the top 10 hits to vet catalysts before acting.
- **Portfolio → News**: daily open-of-market digest — `MMRHelpers.portfolio()` → `portfolio_news()` → `llm_call` to summarize.
- **Positions at risk**: `MMRHelpers.orders()` + `ticker_news(symbol, extract="Any catalyst that could move the stock 5%+ today?")` on each pending order.

### Pairing with mmr-loop-skill

The news skill is safe to use inside a `loop()` iteration — it's fully async, stateless, and won't collide with DuckDB locks (no CLI subprocess). Good candidates:

- Per-tick sentiment gate ("don't enter if there's fresh negative news in last 2h")
- End-of-day wrap-up (summarize all news on held positions)

### Respecting mmr principles

- **Precision over convenience**: the `attempts` array and `fetcher_source` field give you a full provenance trace on every scrape. Use them — don't paper over a failed primary fetch by silently relying on an archive snapshot.
- **Fail loudly**: when `ok=False`, surface the `error` field to the user with the `request_id` (the service echoes one in the `X-Request-Id` header and `NewsHelpers` attaches it to the response). Don't swallow the failure.
- **No Yahoo Finance**: this skill never touches Yahoo. All content comes from first-party news sources via the scraper.

## Failure modes

| `status` | Meaning | What to do |
|---|---|---|
| `ok` | Article fetched and extracted | Use `article.markdown`; report `fetcher_source` |
| `blocked` | All fetchers (including archive) blocked | Try a different source; don't retry blindly |
| `not_found` | Genuine 404 | Double-check URL; don't retry |
| `timeout` | Fetcher exceeded timeout | Retry once; then report the URL as slow/hung |
| `paywalled` | Direct fetch paywalled AND archive had nothing | Suggest alternate source |
| `extraction_error` | Fetched HTML but rustdown failed to parse | Report URL — something's off with page structure |
| `error` | Server/network failure (not bot-wall) | Transient — retry with backoff |
| `bad_request` | Invalid input (HTTP 422) | Fix input per `error.field` |

Top-level failures (connection refused, timeout) appear as `{"ok": false, "error": "...", "hint": "..."}` — the service itself is down, nothing about the scrape.

## Gotchas

- **Stateless service** — no caching. Each call is a fresh fetch.
- **Playwright adds latency** — clean `httpx` is ~1s, Cloudflare-challenged Playwright can be 10–30s, archive tier can stack another 20–60s per provider. The helper defaults to a **120s** timeout so the full fallback chain can finish before the client drops — shorter deadlines were clipping the archive tier mid-submission and returning an empty `attempts` trace.
- **LLM extraction is opt-in** — `extract=` requires `llm_enabled: true` on the service (`~/.config/news/news.yaml`). Returns HTTP 422 otherwise.
- **LLM extraction failures don't discard the article.** If the extraction prompt fails (auth, rate-limit, provider 5xx), the response still has `article.markdown` populated — the failure is surfaced on `extraction.error`. Check that field before treating the response as a total failure.
- **`follow_links>0` requires `extract=`.** The LLM uses the extract prompt to decide which links are relevant; without one there's nothing to route on. Server rejects the combination with HTTP 422.
- **`follow_links` is server-capped at 5.** Deeper chains are a scan-multiple-seeds problem, not a deeper-follow problem — the return on extra hops drops fast and the cost climbs.
- **`allow_archive_fallback=False`** for live-only content. Default is `True` because paywall bypass is usually what you want.
- **Per-article isolation** — `ticker_news()` / `portfolio_news()` / `search_and_scrape()` never fail the whole batch when one URL fails. Each article comes back with its own `ok` / `status` / `attempts`, so a blocked Bloomberg URL doesn't poison the rest of the bundle.
- **`include_html=True` is heavy** — raw HTML is 10–30× the size of markdown. Only set when you need to run your own extractor.
- **Max 50 results per search** — server rejects `max_results > 50` with 422. `search_and_scrape` already clamps effective scrapes to `max_results`.

## Configuration

Environment variable `NEWS_SERVICE_URL` overrides the default endpoint (defaults to `http://127.0.0.1:8089`). Useful when running the service on a non-default port during dev.

## Deeper reference

- [reference/api.md](reference/api.md) — full request/response schemas, status enum
- [reference/fallback-chain.md](reference/fallback-chain.md) — httpx → playwright → archive tier in detail
- [reference/operations.md](reference/operations.md) — `docker.sh` commands, logs, config, common failures
- [reference/FINANCIAL_SOURCES.md](reference/FINANCIAL_SOURCES.md) — known-good / known-bad finance news sources
