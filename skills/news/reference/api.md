# Scraper Service — HTTP API reference

Base URL: `http://127.0.0.1:8089` (override via `SCRAPER_HTTP_ADDRESS` / `SCRAPER_HTTP_PORT`).

**Endpoints:** `/v1/health` (liveness), `/v1/search` (web + academic search),
`/v1/scrape` (three modes: heuristic, LLM extraction, LLM extraction +
link-follow), `/v1/pdf` (PDF at an https URL → Markdown via Mathpix),
`/v1/convert` (uploaded HTML/PDF file → Markdown), `/v1/scrapingbee/*`
(proxy/scraper pass-through), `/v1/scrapes` (archive search).

All responses are JSON. The service never returns 5xx for domain-level failures
(blocked, paywalled, not found) — those are encoded in the response body's
`ok` + `status` fields. 5xx means the service itself is in trouble.

---

## `GET /v1/health`

Liveness check. Cheap — does not touch any fetcher.

**Response 200:**

```json
{
  "status": "ok",
  "version": "0.1.0",
  "llm_enabled": false,
  "scrapingbee_enabled": false,
  "store_enabled": true
}
```

- `status`: `"ok"` when the service is accepting requests and no enabled
  optional subsystem (LLM, Mathpix) has been verified as broken;
  `"degraded"` when one has (see `degraded_reasons`).
- `version`: matches `scraper.__version__` in the Python package.
- `llm_enabled`: whether the service has LLM-assisted features turned on. If
  `false`, calls to any LLM-backed endpoint will error.
- `store_enabled`: whether the persistent scrape store is on (default `true`).
  When `true`, every scrape is archived to a searchable SQLite DB and the
  `/v1/scrapes` endpoints are usable; when `false` those routes return a
  structured `422 store_disabled`. See the `/v1/scrapes` section below.
- `scrapingbee_enabled`: `true` when `scrapingbee_enabled: true` in
  `scraper.yaml` **and** a non-empty `SCRAPINGBEE_API_KEY` was present at boot.
  When `true`, the scrape pipeline gains a paid ScrapingBee fallback tier and
  the `/v1/scrapingbee/*` routes are usable. When `false` those routes still
  exist but return a structured `422 scrapingbee_disabled` (see below) — check
  this field before calling them.

---

## `POST /v1/search`

Run a search query against one of seven engines. Falls back gracefully
(empty list, not 5xx) when the chosen engine errors or rate-limits.

**Request body:**

```json
{
  "query": "IMF growth outlook 2026",
  "max_results": 10,
  "exclude_regex": ["youtube\\.com", "twitter\\.com"],
  "engine": "auto"
}
```

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `query` | string | yes | — | Non-empty |
| `max_results` | int | no | `10` | 1–50 inclusive. Out-of-range → 422 rejection (not clamped). |
| `exclude_regex` | string[] | no | `[]` | Regex patterns matched against each result URL. Max 25 patterns, 200 chars each; a pattern that nests a quantifier inside a quantified group (`(a+)+`) is rejected — it can hang the matcher. Out-of-bounds → 422. |
| `engine` | string | no | `"auto"` | One of `auto` / `ddg` / `google` / `google_news` / `google_scholar` / `arxiv` / `research` (case-insensitive). Anything else → **422**. `auto` → Google via SerpAPI when `SERPAPI_API_KEY` is set, else DDG. `research` → arXiv + Google Scholar fan-out (academic sources; arXiv-only without SerpAPI). |

**Response 200:**

```json
{
  "query": "IMF growth outlook 2026",
  "count": 3,
  "engine": "google",
  "engines_tried": ["google"],
  "engine_failures": {},
  "rate_limited": false,
  "results": [
    {
      "url": "https://www.imf.org/en/Publications/WEO",
      "title": "World Economic Outlook",
      "snippet": "The latest IMF projections...",
      "engine": "Google",
      "source": null,
      "published_at": null
    }
  ]
}
```

- Top-level `engine`: the engine that actually ran (`auto` resolves to
  `google` or `ddg` at request time). An unknown engine never gets this
  far — it's a 422 at request validation, and an engine whose credential
  the service lacks is a 422 `search_engine_disabled` (see below).
- `engine_failures`: per-engine failure tag for this request, e.g.
  `{"google": "http_429"}` (`http_401` / `http_5xx` / `network` /
  `bad_json` / `bad_xml` / `unavailable` / `exception`). Always reported,
  including when another engine saved the request.
- Per-result `engine`: the provider that produced this row —
  `DuckDuckGo`, `Brave` (DDG's fallback), `Google`, `GoogleNews`,
  `GoogleScholar`, or `ArXiv`.
- `source`: Google News → publisher name (`"Reuters"`); Google Scholar →
  the publication summary line (authors — venue, year — host); arXiv →
  author list. `null` for `ddg` / `google`.
- `published_at`: ISO 8601 timestamp when the upstream provides one
  (Google News publish date, arXiv first-version submission date).

### Research search (`engine=research`)

The academic meta-engine: queries **arXiv** (official Atom API, keyless)
and **Google Scholar** (SerpAPI) concurrently, interleaves the two result
lists so neither crowds the other out, and dedupes by URL — every arXiv
link form (`abs/1234.5678v3`, `pdf/1234.5678v2`, `pdf/1234.5678v2.pdf`)
counts as the same paper and is returned as the canonical
`https://arxiv.org/abs/1234.5678`, which `/v1/scrape` reads through the
free HTML path (`export.arxiv.org` links collapse onto it too). Tracking
parameters (`?utm_*`, `?gclid`, `?fbclid`) don't create a second slot
either — but `?ref=` / `?source=` do, since both are real identifiers on
some hosts. If one leg fails the other's results are still returned, with
`rate_limited: true` marking the answer as partial.
Without `SERPAPI_API_KEY` it degrades to arXiv-only and logs a warning.
arXiv snippets are the paper abstract — richer than a SERP snippet — and
the returned URLs feed straight into `/v1/scrape` (a `pdf` URL you pass in
yourself still auto-routes through Mathpix to Markdown).

```bash
curl -fsS -X POST http://127.0.0.1:8089/v1/search \
  -H 'content-type: application/json' \
  -d '{"query": "state space models long context", "engine": "research", "max_results": 10}' | jq .
```

The `arxiv` engine also accepts arXiv's own query syntax when you need
precision — e.g. `{"query": "cat:cs.LG AND ti:mamba", "engine": "arxiv"}`.
Passthrough is triggered by a **field prefix** (`ti:` `au:` `abs:` `co:`
`jr:` `cat:` `rn:` `id:` `all:`); with one present the whole query goes
through untouched, operators (`AND` / `OR` / `ANDNOT`) and quoted phrases
included. A bare `AND`/`OR` with no field prefix is treated as the English
it usually is: `"AND gate design"` and `"OR tools scheduling"` are ordinary
queries, and passing them through verbatim made arXiv answer **HTTP 400**
(measured). Free-text queries are ANDed server-side (arXiv's default OR
semantics are useless for relevance — an unfielded `"OR-Tools scheduling"`
matched 158310 papers) over their **content** terms: stop words are dropped
first, because arXiv's index doesn't contain them and ANDing one in returns
zero results — `"diffusion models for image generation"` used to match
nothing at all thanks to the word "for". Ask questions in plain English if
you like; quote a phrase to force it through verbatim.

An empty `results` list can mean: no matches, upstream rate-limited us,
or a SerpAPI call returned an error (401 / 429 / 5xx). Two fields separate
those. `engine_failures` says **what went wrong upstream, per engine**;
`rate_limited: true` says **this answer is incomplete because an engine
failed and nothing covered for it**:

- Set when a `research` fan-out lost a leg. The legs are additive —
  Scholar's papers aren't in arXiv's list — so the missing content is
  missing however much the surviving leg returned.
- Set when a chain hit a failure and no engine went on to deliver
  results — including a single explicit engine that 429'd.
- Set when an `engine=auto` chain tried both preferred engines
  (`engines_tried: ["google", "ddg"]`) and both returned a clean empty
  response — two independent empties are more likely a soft block.
- **Not** set when `auto`'s fallback answered in full: DDG answers the same
  question Google would have, so the answer is whole. That case reports
  `engine_failures: {"google": "http_429"}` with `rate_limited: false` —
  degraded but complete, nothing to retry.
- Engines that answer cleanly with no matches report empty
  `engine_failures` and `rate_limited: false` — a real "nothing found".

An engine the service has no credential for is **not** an upstream problem
and is no longer answered with a 200: `engine=google` / `google_news` /
`google_scholar` without `SERPAPI_API_KEY` returns **422** with
`error.code = "search_engine_disabled"` and `field: "body.engine"`, the
same shape as `llm_disabled` / `mathpix_disabled` / `store_disabled` /
`scrapingbee_disabled`. `auto`, `ddg`, `arxiv` and `research` never 422 for
a missing key — they degrade to the keyless engines by design.

---

## `POST /v1/scrape`

Fetch one URL through the fallback chain and return it as Markdown. Three
modes, selected by request fields:

- **Mode 1** (default): heuristic only. Returns markdown + metadata.
- **Mode 2** (`extract` set): mode 1 + LLM extraction over the markdown.
- **Mode 3** (`extract` + `follow_links > 0`): mode 2 + LLM picks up to N
  relevant links from the article body, fetches each in parallel, and
  the extraction runs over the combined corpus.

**Request body:**

```json
{
  "url": "https://example.com/article",
  "allow_archive_fallback": true,
  "allow_paid_fallback": true,
  "extract": "What is the CEO's stated reason for the layoffs?",
  "follow_links": 2
}
```

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `url` | string (HttpUrl) | yes | — | Validated by pydantic; malformed → 422 |
| `allow_archive_fallback` | bool | no | `true` | Disable to skip archive.ph/wayback tier |
| `allow_paid_fallback` | bool | no | `true` | Set `false` to drop the paid ScrapingBee tier from the chain for this request (chain becomes httpx → playwright → archive). Use it for anything that sweeps blocked hosts — batch probes, debugging, test harnesses — where every blocked URL would otherwise bill 25-100 credits on its way to failing. The attempt is absent rather than recorded as `skipped`. |
| `extract` | string \| null | no | `null` | Natural-language extraction prompt. Requires `llm_enabled=true` |
| `follow_links` | int | no | `0` | 0–5 (server cap). Only meaningful with `extract`; LLM picks the URLs |
| `include_html` | bool | no | `false` | Return raw upstream HTML + post-preclean HTML on the response. Each capped at 2 MB (truncated with a marker). Off by default because HTML payloads are 10–30× the markdown size |
| `use_cache` | bool | no | `false` | Replay a previously stored OK result for this URL instead of re-fetching, when the store is on and `store_cache_ttl_seconds > 0` and a hit younger than the TTL exists. Skips the network entirely. Off by default so content stays fresh. On a cache hit, `article.fetcher_source` gets a `+cache` suffix (e.g. `httpx+cache`). Scoped to `/v1/scrape` — only replays a prior `/v1/scrape` result. A plain (mode-1) request replays the newest OK record whatever mode produced it, with that record's `extraction`/`follow_pages` stripped and `cost` null; an `extract` request needs a record with the same mode and prompt (a mode-2 miss still reuses a stored fetch as its seed and runs only the LLM). The response's `cache` field says `hit` or `miss: <reason>`, so a re-fetch is never silent |
| `images` | str | no | `"urls"` | Image handling for `article.markdown`: `"embed"` inlines every image as a base64 `data:` URI (self-contained; inflates the payload ~4/3); `"hosted"` downloads every image onto the service and rewrites refs to `GET /v1/images/<sha256>.<ext>` (compact + durable — survives CDN link rot, LLM-friendly); `"urls"` keeps original refs (Mathpix-CDN extraction artifacts from auto-routed `.pdf` URLs are still re-homed to `/v1/images` — the service never emits `cdn.mathpix.com`). Defaults: `"urls"` here, `"embed"` on the conversion endpoints `/v1/pdf` + `/v1/convert`. Download caps (config) apply to embed and hosted alike: 1 MB/image, 16 images, 10 MB total, 10s per download — over-cap or failed images keep their previous reference. The scrape store always archives non-base64 markdown (hosted/urls run before recording, embed after), and `use_cache` hits get the image pass applied fresh |
| `embed_images` | — | — | — | **Removed.** Sending it is a 422 with `embed_images was removed; use images=…` — never silently ignored |

**Response 200 (shared envelope for success and domain failure):**

```json
{
  "url": "https://example.com/article",
  "ok": true | false,
  "status": "ok" | "blocked" | "paywalled" | "not_found" | "not_an_article" | "timeout" | "error" | "extraction_error",
  "article": ArticleModel | null,
  "attempts": [AttemptInfo, ...],
  "error": string | null
}
```

### `ArticleModel`

Populated when `ok` is `true`. Null otherwise.

```json
{
  "url": "https://example.com/article",
  "final_url": "https://example.com/article",
  "title": "Article title or null",
  "markdown": "# Article title\n\nBody text...",
  "fetched_at": "2026-04-18T12:00:00+00:00",
  "fetcher_source": "httpx",
  "fetch_notes": "",
  "extractor_stats": {
    "nodes_processed": 1234,
    "ads_removed": 5,
    "total_time_ms": 42
  },
  "metadata": {
    "published_at": "2026-04-18T10:30:00+00:00",
    "modified_at": "2026-04-18T14:00:00+00:00",
    "tickers": ["TLT", "SPY", "QQQ"]
  }
}
```

- `final_url`: after redirects (may differ from request URL).
- `title`: parsed from `<title>` tag; may be null for oddly-structured pages.
- `markdown`: article body converted by rustdown with aggressive ad removal.
- `fetcher_source`: one of `httpx`, `playwright`, `scrapingbee`,
  `archive:archive.ph`, `archive:archive.today`, `archive:web.archive.org`.
  **Inspect this to know whether the content is live or a snapshot.**
  `scrapingbee` means the free direct fetchers were exhausted and the paid
  ScrapingBee proxy tier got the page (only present when
  `scrapingbee_enabled` is on).
- `fetch_notes`: human-readable note from the successful fetcher (e.g.
  `"cloudflare challenge cleared"` or `"snapshot via archive.ph"`).
- `cost` (top-level on the response) + `attempts[].cost`: ScrapingBee credits
  this request spent — the fallback tier bills even when a later free tier
  won the page, and mode-3 follow fetches are included in the total. `null`
  when nothing was billed. Recorded to the scrape store, so
  `/v1/health → scrapingbee_usage` covers pipeline-tier spend.
- `extractor_stats`: `rustdown` internals — useful for debugging when extraction
  seems off. With `images: "embed"`, image-download counters are merged in:
  `images_found` / `images_embedded` / `images_failed` (network or HTTP
  errors) / `images_skipped` (over a size/count cap, non-image content-type,
  or blocked host) / `images_bytes` (raw bytes embedded, pre-base64). With
  `images: "hosted"` (or Mathpix-CDN refs in `"urls"` mode),
  `images_mirrored` / `images_mirror_failed` land here instead.
- `html_raw` / `html_cleaned`: Null unless the request set `include_html: true`.
  - `html_raw` is the upstream HTML, untouched — useful for debugging
    extraction or running your own readability/extraction pipeline.
  - `html_cleaned` is what rustdown saw — post-preclean (domain-specific nav
    and paywall elements stripped), pre-markdown-conversion. Diff this
    against `html_raw` to see what our preclean rules removed.
  - Each is capped at 2 MB; a larger page is truncated with
    `<!-- scraper-service: truncated N bytes -->` appended.
- `metadata`: Structured heuristics. Keys:
  - `published_at` / `modified_at`: ISO 8601 strings when extractable from
    JSON-LD (`NewsArticle` / `Article` with `datePublished` / `dateModified`),
    Open Graph `article:published_time`, Dublin Core, schema.org microdata,
    or `<time datetime=...>` fallback. `null` when nothing usable was found.
  - `tickers`: Stock ticker symbols lifted from source-specific ticker blocks
    (currently Seeking Alpha). Empty list on sources where no pattern matches.
  - New keys may land here (authors, section, summary) without breaking
    clients — always read defensively.

### Extraction + follow-pages (mode 2/3 responses)

When the request used `extract`, the response has two additional top-level fields:

```json
{
  "extraction": {
    "text": "The CEO is Jane Smith.",
    "model": "claude-opus-4-8",
    "input_chars": 4812,
    "truncated": false,
    "elapsed_ms": 1432
  },
  "follow_pages": [
    {
      "url": "https://...",
      "final_url": "https://...",
      "title": "Related article title",
      "fetcher_source": "httpx",
      "markdown_length": 12840,
      "ok": true
    }
  ]
}
```

- `extraction`: null unless `extract` was set. `text` is whatever the LLM
  produced; callers who want structure can prompt for JSON in `extract`.
- `follow_pages`: empty unless `follow_links > 0`. One entry per URL the
  LLM selected. `markdown_length: 0` + `ok: false` means the follow-up
  fetch failed (blocked, not found, etc.); the extraction still runs over
  whatever follow-ups did succeed.

### `AttemptInfo`

One entry per fetcher that was tried. Order matches the fallback chain.

```json
{
  "fetcher": "httpx",
  "status": "blocked",
  "http_status": 403,
  "notes": ""
}
```

- `fetcher`: name of the attempting fetcher (`httpx`, `playwright`,
  `scrapingbee`, `archive`). The fallback chain runs in that order; a
  `scrapingbee` attempt only appears when the paid tier is enabled and the
  two free fetchers ahead of it didn't produce an article.
- `status`: outcome of this attempt — same enum as the top-level `status`.
- `http_status`: HTTP status if applicable (playwright may return `null`).
- `notes`: free-text hint, e.g. `"cloudflare challenge did not clear within timeout"`.

### Top-level `status` enum

| Value | Meaning |
|---|---|
| `ok` | Article fetched and extracted successfully |
| `blocked` | 401 with bot-wall signals, 403, 429, or unresolved Cloudflare interstitial |
| `paywalled` | Body matched paywall markers OR rendered title was "Subscribe/Register to read" |
| `not_found` | 404, or rendered page had a "Page not found" title / `prerender-status-code=404` meta |
| `not_an_article` | Fetched fine but the body is a section/listing index (no `<article>` element + link-dense DOM), an image, a binary asset, or otherwise not article-shaped. The URL works; the caller asked for the wrong thing. |
| `timeout` | Fetch exceeded its configured timeout |
| `error` | 5xx, genuine 401 without bot-wall signals, or oversized response |
| `extraction_error` | Fetched fine but rustdown couldn't parse the HTML |
| `bad_request` | HTTP 422 — request body failed validation OR semantic validation (e.g. `extract` set with LLM disabled) |

### Headers

Every response carries `X-Request-Id` — an 8-char hex correlation id that
shows up in the service logs alongside every per-attempt line. Clients can
also *supply* `X-Request-Id` on the request and it'll be echoed back,
letting a caller pick its own trace ids.

### `error` field

Free-text description of the failure. Null when `ok` is `true`.

---

## `POST /v1/pdf` (PDF at a URL → Markdown)

Convert a PDF at a **publicly reachable `https://` URL** to Markdown via
Mathpix. Mathpix downloads the PDF server-side — the scraper service never
proxies the bytes — so the URL must be resolvable from Mathpix's
infrastructure. Two hard consequences:

- **`http://` URLs fail.** Mathpix returns
  `Protocol "http:" not supported. Expected "https:"`.
- **localhost / LAN / private URLs fail.** Mathpix can't reach them (and the
  request validator rejects private hosts anyway). For local files use
  `POST /v1/convert` below.

Request body:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `url` | HttpUrl | required | URL of the PDF. Must be public + `https`. |
| `use_cache` | bool | `true` | Serve the on-disk cached conversion (keyed by URL hash) if present. Set `false` to force re-conversion when the PDF at the URL changed. Mathpix bills per page, so the cache is a real cost saving. |
| `images` | str | `"embed"` | Image handling for the converted markdown: `"embed"` (default — a converted document should carry all of its content) inlines the figures Mathpix extracted as base64 data URIs; `"hosted"` / `"urls"` mirror the Mathpix-CDN figures onto the service, served from `GET /v1/images/<sha256>.<ext>`, so the markdown never depends on Mathpix's CDN (the two modes behave the same for Mathpix output). The removed `embed_images` bool is rejected with a 422. |

Response is the standard `ScrapeResponse` envelope. `fetcher_source` is
`mathpix` (fresh conversion) or `mathpix:cache` (disk cache hit);
`article.metadata` carries `pdf_pages`, `pdf_id`, `pdf_output_format`,
`pdf_cached`. With `output_format: mmd` (the default), `\title{...}` /
`\author{...}` LaTeX macros are lifted into `article.title` /
`article.metadata.pdf_author`.

Requires `mathpix_enabled: true` + `MATHPIX_API_KEY` in the service env —
otherwise `422` with `error.code = "mathpix_disabled"`. Check
`GET /v1/health` → `mathpix_enabled` first.

`/v1/scrape` auto-routes here for `.pdf` URLs (and known extensionless PDF
hosts like `arxiv.org/pdf/...`), so callers usually don't need to pick the
endpoint themselves — `/v1/pdf` exists for non-`.pdf` URLs that serve PDFs.

```bash
curl -fsS -X POST http://127.0.0.1:8089/v1/pdf \
  -H 'content-type: application/json' \
  -d '{"url": "https://arxiv.org/pdf/2401.12345"}' \
  | jq -r '.article.markdown'
```

---

## `POST /v1/convert` (file upload → Markdown)

Convert an **uploaded** HTML or PDF file to Markdown. This is the endpoint
for local files, private documents, or anything Mathpix couldn't download
itself — the bytes are streamed to the service as a multipart form, and for
PDFs the service uploads them to Mathpix directly (no URL involved).

Multipart form fields:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `file` | file | required | The HTML or PDF payload. Max 20 MB (`413 upload_too_large` beyond). |
| `use_cache` | bool | `false` | PDF only: reuse + update the on-disk Mathpix cache, keyed by content sha256. Off by default — uploads are treated as one-off content. Opt in when re-uploading the same bytes to avoid Mathpix re-billing. |
| `include_html` | bool | `false` | HTML only: populate `article.html_raw` / `article.html_cleaned`. |
| `images` | str | `"embed"` | Image handling for the converted markdown: `"embed"` inlines referenced images as base64 data URIs; `"hosted"` mirrors every image onto the service (`GET /v1/images/`); `"urls"` keeps refs, except Mathpix-CDN figures (PDF uploads) which are still re-homed. Absolute URLs only for HTML uploads — there is no page URL to resolve relative refs against. Unknown modes → `422 invalid_images_mode`; the removed `embed_images` form field → `422 embed_images_removed`. |

File kind is detected by precedence: magic bytes (`%PDF-`) → Content-Type →
filename extension → content sniff. A PDF sent as
`application/octet-stream` is still classified correctly. Unclassifiable
uploads get `422 unsupported_upload`; zero-byte uploads get
`422 empty_upload`.

Response is the standard `ScrapeResponse` envelope with
`article.url = "upload:<filename>"`. `fetcher_source` is `upload` (HTML,
converted by the same rustdown extractor `/v1/scrape` uses) or
`mathpix` / `mathpix:cache` (PDF). PDF uploads with Mathpix off return
`422 mathpix_disabled`; HTML uploads work regardless of Mathpix.

```bash
# Local PDF → Markdown
curl -fsS -X POST http://127.0.0.1:8089/v1/convert \
  -F 'file=@/path/to/paper.pdf' \
  | jq -r '.article.markdown'

# Local HTML, keeping the cleaned HTML too
curl -fsS -X POST http://127.0.0.1:8089/v1/convert \
  -F 'file=@page.html' -F 'include_html=true' \
  | jq '{title: .article.title, md_len: (.article.markdown|length)}'
```

---

## `GET /v1/images/{name}` (self-hosted mirrored images)

Serves images the service copied onto its own disk: everything referenced by
an `images: "hosted"` request, plus Mathpix-CDN extraction artifacts from
`"urls"`-mode requests (the service never emits `cdn.mathpix.com` refs).
Files are content-addressed (`<sha256-of-bytes>.<png|jpg|gif|webp|svg|avif>`) and
stored in `<data_directory>/images` (host-mounted, survives container
rebuilds), so responses are immutable and served with
`Cache-Control: public, max-age=31536000, immutable`. Unknown or malformed
names return `404`. Retention rides the scrape store's window: a file's
mtime refreshes every time a scrape references it, and files unreferenced
for `store_retention_days` are swept (at boot + daily).

```bash
curl -fsSO http://127.0.0.1:8089/v1/images/<sha256>.png
```

---

## `/v1/agent` (agentic scraping jobs)

For tasks the deterministic pipeline can't finish alone: deep multi-page
crawls, and heavy-JS sites that need real interaction (consent walls,
infinite scroll, form-gated content). An llmvm-driven agent plans and
executes the task — its fetch tools are the service's own hardened chain
(`scrape_url`/`scrape_urls`/`web_search`/`research_search` helpers — the
last queries arXiv + Google Scholar for paper-hunting tasks) plus
interactive browser automation — and returns a Markdown deliverable.

Requires `agent_enabled: true` AND a working LLM subsystem (`/v1/health` →
`agent_enabled` is the usability signal); otherwise every route returns
`422 agent_disabled`. Jobs are **async**: submit returns a job id
immediately; poll for the result. Expect minutes, not seconds, and real
LLM token spend per job — use `/v1/scrape` for anything it can handle.

| Route | What it does |
|---|---|
| `POST /v1/agent` | Submit `{task, max_iterations?, max_seconds?, allowed_domains?, images?}` → job record (status `queued`). Budgets are clamped to the server's `agent_max_*` config (config is both default and ceiling). `allowed_domains` restricts fetches to those hosts + subdomains. `images`: `"urls"` (default — deliverable keeps refs, Mathpix-CDN artifacts re-homed to `/v1/images`) or `"hosted"` (every referenced image mirrored to `/v1/images`); `"embed"` is rejected (agent results are store-archived, and the store never holds base64). PDF URLs the agent fetches route through Mathpix exactly like `/v1/scrape`. |
| `GET /v1/agent/{id}` | Full job record. `status`: `queued → running → completed \| failed \| timeout \| cancelled`. `result` holds the Markdown deliverable; on `timeout` it may carry salvaged partial work (`partial: true`). `stats.scrapingbee_credits` reports the job's total paid-proxy spend when any of its fetches hit the ScrapingBee tier. |
| `GET /v1/agent` | Newest-first job summaries (no result bodies). |
| `POST /v1/agent/{id}/cancel` | Cancel a queued/running job. |
| `GET /v1/agent/{id}/events` | Parsed llmvm event stream (`logs/events.jsonl`): loop/iteration boundaries, LLM calls, code execution. `?limit=N` (tail, default 100), `?event=pre_llm_call` filters by type. Works **while the job is running** and for jobs from before a restart (file-backed, not registry-backed). |
| `GET /v1/agent/{id}/trace` | List every file in the job's scratch dir (llmvm session logs + agent workdir artifacts). |
| `GET /v1/agent/{id}/trace/{path}` | Raw trace file as text (e.g. `logs/transcript.jsonl`, `logs/conversation.prompt`, `logs/last_context_window.prompt`). Large files are tailed at `?max_bytes=` (default 2MB) with `X-Trace-Total-Bytes` / `X-Trace-Truncated` headers. |

Debugging a trace: start with `/events` (what did each iteration do, where
did time/tokens go), then `logs/transcript.jsonl` for the full
message-by-message record and `logs/last_context_window.prompt` for exactly
what the model saw last.

The registry is in-memory — a container restart drops it — but every
finished job is archived to the scrape store (`endpoint='agent'`,
`url='agent:<id>'`, result as `markdown`), so history survives via
`GET /v1/scrapes`. Per-job transcripts land under
`<data_directory>/agent/<id>/` for post-mortems.

```bash
job=$(curl -fsS -X POST http://127.0.0.1:8089/v1/agent \
  -H 'content-type: application/json' \
  -d '{"task": "Find the three most recent posts on example.com/blog and summarise each in two sentences.", "allowed_domains": ["example.com"]}' \
  | jq -r '.id')
# poll
curl -fsS http://127.0.0.1:8089/v1/agent/$job | jq '{status, result}'
```

---

## `POST /v1/scrapingbee/*` (ScrapingBee proxy/scraper API)

These routes are a thin pass-through to [ScrapingBee](https://www.scrapingbee.com).
They are **always mounted**, but only *work* when `scrapingbee_enabled: true`
in `scraper.yaml` and a non-empty `SCRAPINGBEE_API_KEY` is in the service
environment. When the feature is off, every endpoint returns a structured
`422 scrapingbee_disabled` (not a 404), so a caller can tell "feature off"
from "wrong path". The `api_key` is injected server-side; callers never send
it. Separate from `/v1/scrape`: these return ScrapingBee's *raw* output (HTML
or the provider's parsed JSON), not the scraper-service Markdown envelope.

Check `GET /v1/health` → `scrapingbee_enabled` before calling.

**Credits.** ScrapingBee bills per successful call: `1` (no JS) / `5`
(`render_js`) / `10` (`premium_proxy`) / `25` (premium + render_js) / `75`
(`stealth_proxy`); the Google API is `10` light / `15` full. Billing happens
only on HTTP `200`/`404`/`410` — failures like `401`/`429`/`500` are free. The
`cost` field (from the `Spb-cost` response header) reports the credits actually
charged.

### General scraper — `POST /v1/scrapingbee/scrape`

Fetch an arbitrary URL through ScrapingBee's proxy network, with the knobs
that get past Cloudflare / bot-walls.

**Request body:**

```json
{
  "url": "https://www.example.com/article",
  "render_js": true,
  "premium_proxy": true,
  "stealth_proxy": false,
  "country_code": "gb",
  "wait": 3000,
  "wait_for": ".article-body",
  "session_id": 42,
  "ai_query": "extract the headline",
  "custom_headers": {"Referer": "https://scraper.ycombinator.com"},
  "params": {"any_other_scrapingbee_flag": "value"}
}
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `url` | string (HttpUrl) | — | Required. Target to scrape. |
| `render_js` | bool \| null | provider default (`true`) | Render JS in a headless browser. `false` = plain fetch (1 credit vs 5). |
| `premium_proxy` | bool \| null | null | Premium/residential proxies (10 credits; 25 with `render_js`). |
| `stealth_proxy` | bool \| null | null | Cloudflare / bot-wall bypass pool (75 credits). |
| `country_code` | string \| null | null | ISO 3166-1 alpha-2 proxy country (geotargeting); needs premium, **auto-enabled** when set. See note below. |
| `wait` | int \| null | null | JS settle time ms (0–35000); only with `render_js`. |
| `wait_for` | string \| null | null | CSS/XPath selector to wait for before returning. |
| `block_resources` | bool \| null | null | Block images/CSS to speed up the render. |
| `session_id` | int \| null | null | Sticky IP — reuse the same proxy across calls. |
| `return_page_source` | bool \| null | null | Return the pre-JS HTML (before the browser executes scripts). |
| `transparent_status_code` | bool \| null | null | Return the target's real status code instead of ScrapingBee's. |
| `json_response` | bool \| null | null | Return ScrapingBee's structured JSON envelope → lands in `data`. |
| `extract_rules` | string \| null | null | Stringified JSON of CSS/XPath rules → returns parsed JSON in `data`. |
| `ai_query` | string \| null | null | Natural-language extraction → returns JSON in `data`. |
| `ai_extract_rules` | string \| null | null | Stringified JSON schema for AI extraction → returns JSON in `data`. |
| `ai_selector` | string \| null | null | CSS selector scoping the AI extraction. |
| `js_scenario` | string \| null | null | Stringified JSON of scripted browser steps (click, scroll, …). |
| `custom_google` | bool \| null | null | Allow scraping Google properties. |
| `custom_headers` | object \| null | null | Headers forwarded to the target — the service sets `forward_headers=true` and adds the `Spb-` prefix. |
| `method` | string | `"GET"` | `"GET"` or `"POST"`. `"POST"` forwards a body to the target (POST passthrough). |
| `data` | object \| string \| null | null | POST body: object → form-encoded, string → raw. Requires `method:"POST"`. |
| `json_body` | any \| null | null | POST body forwarded as JSON. Requires `method:"POST"`. |
| `params` | object | `{}` | Escape hatch for any flag not modelled above (`api_key` is ignored if passed). |
| `use_cache` | bool \| null | `false` | Replay a previously stored OK result for this URL (younger than `store_cache_ttl_seconds`) instead of spending proxy credits, when the store is on. Off by default so content stays fresh. Scoped per endpoint — a `/v1/scrapingbee/scrape` hit only replays a prior `/v1/scrapingbee/scrape` result. Also accepted by `/v1/scrapingbee/scrape_markdown` (replays a prior `scrape_markdown` result). |

**Geotargeting (`country_code`):** routes the request through a proxy in the
given country. Takes an [ISO 3166-1 alpha-2](https://en.wikipedia.org/wiki/ISO_3166-1)
code, e.g. `au` (Australia), `nz`, `gb`, `us`, `de`. It needs the premium
proxy pool, which is **auto-enabled** whenever you set `country_code` — so you
don't have to pass `premium_proxy` yourself. Example — fetch an Australian site
from an AU proxy:

```json
{ "url": "https://www.punters.com.au", "country_code": "au", "stealth_proxy": true }
```

The Google SERP API also accepts `country_code` as a first-class field —
e.g. `{ "search": "afl", "country_code": "au" }` for AU-localized Google
results.

**POST passthrough (`method: "POST"`):** ScrapingBee POSTs a body to the target
URL (form submit, JSON API, login). Supply the body via `data` (object →
form-urlencoded, string → raw) or `json_body` (JSON); `json_body` requires
`method:"POST"`. The matching `Content-Type` is forwarded automatically, so
`data` arrives as a form and `json_body` as JSON. All GET knobs (`render_js`,
`premium_proxy`, `country_code`, …) still apply. Sending `data`/`json_body`
with the default GET returns 422.

```json
{ "url": "https://httpbin.org/anything", "method": "POST", "data": { "user": "sonny" }, "premium_proxy": true }
```

**Response 200:**

```json
{
  "ok": true,
  "status_code": 200,
  "endpoint": "scrape",
  "content_type": "text/html; charset=utf-8",
  "body": "<html>...</html>",
  "data": null,
  "cost": 25,
  "error": null
}
```

- `status_code`: the upstream ScrapingBee HTTP status (`0` = network error /
  timeout from us). `ok` is true only on a ScrapingBee `200`.
- `body`: raw target body (HTML/text scrapes). `data`: parsed JSON instead,
  for the `extract_rules` / `ai_query` / `ai_extract_rules` / `json_response`
  modes. Exactly one is populated.
- `cost`: credits charged for the call, from the `Spb-cost` header.
- `error`: compact reason on failure, e.g.
  `"scrapingbee http 401 (api key rejected / no credits)"`. A non-200 here is
  **not** a service error — HTTP is still 200 with `ok: false`.

### Scrape → Markdown (`POST /v1/scrapingbee/scrape_markdown`)

Same as `/scrape`, but the fetched HTML is converted to Markdown by the
service's **own rustdown extractor** (the same one `/v1/scrape` uses), so you
get clean prose + title + metadata instead of raw HTML. Takes all the `/scrape`
flags (`render_js`, `premium_proxy`, `stealth_proxy`, `country_code`, `wait`,
`custom_headers`, POST passthrough, …); the extraction flags
(`extract_rules` / `ai_query` / `json_response`) are ignored here (this endpoint
always fetches HTML and converts it itself).

```json
{ "url": "https://www.ft.com/content/…", "stealth_proxy": true, "premium_proxy": true }
```

**Response 200** — `markdown` + `metadata`, not `body`:

```json
{ "ok": true, "status_code": 200, "url": "https://www.ft.com/content/…",
  "title": "Article title", "markdown": "# Article title\n\n…",
  "metadata": { "published_at": "2026-01-10T…", "modified_at": null, "tickers": ["BHP"] },
  "error": null }
```

A ScrapingBee-side failure → `ok:false` with `error` (HTTP still 200). A
response with no HTML body to convert → `ok:false` with an explanatory error.
Use this for hard article URLs where you want the clean Markdown; use `/scrape`
when you want the raw page.

### Google SERP API — `POST /v1/scrapingbee/google`

ScrapingBee's Google API (upstream `https://app.scrapingbee.com/api/v1/store/google`,
10 credits light / 15 full). Returns ScrapingBee's parsed SERP JSON under `data`.
News, maps, shopping, and images are reached via the `search_type` field — there
are **no separate vertical endpoints**.

```json
{ "search": "tesla stock", "search_type": "news", "country_code": "us", "page": 1 }
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `search` | string | — | Required. Search terms (a ticker for finance — but use `/google_finance` for quotes). |
| `search_type` | string | `"classic"` | One of `classic`, `news`, `maps`, `images`, `lens`, `shopping`, `ai_mode`, `ads`. Invalid → 422. |
| `country_code` | string \| null | null | Two-letter Google country code. |
| `language` | string \| null | null | UI language code. |
| `page` | int \| null | null | Result page (≥1). |
| `device` | string \| null | null | `"desktop"` or `"mobile"`. |
| `nfpr` | bool \| null | null | No auto-correction of the query. |
| `add_html` | bool \| null | null | Include the raw HTML alongside the parsed JSON. |
| `params` | object | `{}` | Escape hatch for any extra Google API flag. |

The `search_type` modes are how news / maps / shopping / images are reached.
Response top-level keys vary by type: `organic_results`, `news_results`,
`map_results`, `local_results`, `shopping_results`, `images`,
`top_ads`/`bottom_ads`, `related_queries`, `meta_data`.

**Response 200:**

```json
{ "ok": true, "status_code": 200, "endpoint": "google", "data": { ... }, "cost": 15, "error": null }
```

### Google Finance helper — `POST /v1/scrapingbee/google_finance`

ScrapingBee has **no finance API**, so this helper scrapes
`https://www.google.com/finance/quote/{ticker}` through the general HTML API
(`render_js=true`, `premium_proxy=true`, `custom_google=true`) and parses
Google's own figures with **deterministic CSS `extract_rules`** — not an LLM.
The price, change, and change_percent all come from the one header element
(the sign is taken from Google's up/down arrow, so they can't be transposed),
and `previous_close` is computed as `price − change`, so every field is
internally consistent (`price − change == previous_close`) for both US and
non-US (e.g. ASX) tickers. If Google rotates its class names and no price/change
is found, it falls back to AI extraction (the `data` then carries `"extraction":
"ai_fallback"`).

```json
{ "ticker": "GOOGL:NASDAQ" }
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `ticker` | string | — | Required. `"SYMBOL:EXCHANGE"`, e.g. `"GOOGL:NASDAQ"`, `"BHP:ASX"`. |
| `params` | object | `{}` | Escape hatch passed through to the HTML API. |

**Response 200** — `data` is the quote: top-line `price`, `change`,
`change_percent`, `previous_close`, `currency`, plus `stats` (Google's labelled
table: `Open`, `High`, `Low`, `Mkt. cap`, `P/E ratio`, `52-wk high/low`, `EPS`,
`Beta`, `Dividend`, …):

```json
{ "ok": true, "status_code": 200, "endpoint": "google_finance",
  "data": { "ticker": "BHP:ASX", "price": "A$58.99", "change": "+0.47",
            "change_percent": "+0.80%", "previous_close": "A$58.52",
            "currency": "AUD", "stats": { "Mkt. cap": "299.75B", "P/E ratio": "20.25", ... } },
  "cost": 15, "error": null }
```

### Google Search → Markdown (`POST /v1/scrapingbee/google_markdown`)

LLM shortcut: a Google search rendered as compact **Markdown** instead of JSON,
so a model can read it without parsing. Includes only organic results, images,
and local results — everything else (ads, knowledge graph, related questions)
is dropped.

```json
{ "query": "best coffee surry hills", "country": "au", "results": 5 }
```

| Field | Type | Default | Notes |
|---|---|---|---|
| `query` | string | — | Required. |
| `country` | string \| null | null | Two-letter Google country code, e.g. `au`. |
| `results` | int | `10` | How many organic results to include (1–100; trimmed client-side). |
| `language` | string \| null | null | UI language code. |
| `params` | object | `{}` | Extra raw Google params (`page`, …). |

**Response 200** — note the `markdown` field (not `data`):

```json
{ "ok": true, "status_code": 200, "query": "best coffee surry hills", "markdown": "# Google results: ...", "error": null }
```

The Markdown has up to three sections, each omitted when empty:

- **Organic results** — numbered `[title](url)` + description + displayed URL.
- **Images** — `[title](source-link)`. Google's organic items carry no inline
  image (images live in the separate `images` block), so these are links; when
  an image URL is present it renders as a real `![title](thumbnail)` image.
- **Local results** — `**[name](maps/website link)** — 4.8★ (269 reviews) — address`.
  The name links to the place's website when present, else a Google Maps link.
  A static map image (`local_map.image`) renders once as `![map](...)` when the
  response includes one.

`jq -r '.markdown'` gives you the document directly.

### Migration note — removed verticals are now DIY

Several previously dedicated vertical endpoints have no ScrapingBee equivalent:

- **News / maps / shopping / images** are now `search_type` modes of
  `/v1/scrapingbee/google` (not separate routes).
- **Finance** is the `/v1/scrapingbee/google_finance` helper above.
- **Scholar, trends, hotels, and LinkedIn** have no dedicated API at all —
  build them yourself with `POST /v1/scrapingbee/scrape` plus `extract_rules`,
  `ai_query`, or `ai_extract_rules` to pull structured data off the target page.

### When the feature is off

Whenever ScrapingBee isn't usable — no `SCRAPINGBEE_API_KEY`, or
`scrapingbee_enabled: false` — every endpoint returns **HTTP 422** with this
body (the routes are always mounted, so you never get a bare 404):

```json
{ "ok": false, "status_code": 0, "endpoint": "", "error": {"code": "scrapingbee_disabled", "detail": "Set SCRAPINGBEE_API_KEY ..."} }
```

---

## `GET /v1/scrapes` (persistent scrape store + search)

Every scrape that flows through `/v1/scrape`, `/v1/pdf`, and the direct
`/v1/scrapingbee/scrape` & `/v1/scrapingbee/scrape_markdown` endpoints is
archived to a searchable SQLite database (stdlib `sqlite3` + FTS5 — no new
dependency). The DB lives at `~/.local/share/scraper/data/scrapes.db` in the
host-mounted data dir, so it survives container restarts and is openable on
the host with any sqlite client. The store doubles as an opt-in result cache
(see `use_cache` on the scrape requests above). Recording is best-effort and
never breaks or slows a scrape.

These routes are **always mounted**. When the store is off (`store_enabled:
false`) they return **HTTP 422**:

```json
{ "error": { "code": "store_disabled", "detail": "..." } }
```

Check `GET /v1/health` → `store_enabled` before calling.

### Search / list — `GET /v1/scrapes`

Returns recorded scrapes newest-first, or by FTS relevance when `q` is set.
**Summaries only** — no markdown or response body (use the by-id route for
those).

| Query param | Type | Default | Notes |
|---|---|---|---|
| `q` | string | — | Full-text query over url + title + markdown (e.g. `q=tesla earnings`). Plain text just works: tokens FTS5 would misread (`AI-powered`, `Fed's`, `U.S.`, `C++`) are quoted for you; explicit FTS5 syntax — `"a phrase"`, `AND`/`OR`/`NOT`/`NEAR`, `title:term`, `term*` — passes through untouched. A **malformed** FTS5 expression (`AND` alone, an unbalanced quote, `a OR OR b`) returns `422 bad_query` (see below) — never an empty result set, so a typo can't masquerade as an empty archive. |
| `url` | string | — | Substring match on the scraped URL. |
| `status` | string | — | Exact status filter (`ok`, `blocked`, `paywalled`, `not_found`, …). |
| `fetcher_source` | string | — | Exact source filter (`httpx`, `playwright`, `scrapingbee`, `archive`, `mathpix`, …). |
| `since` | int | — | Epoch-second lower bound on record time (`created_at`). |
| `until` | int | — | Epoch-second upper bound on record time (`created_at`). |
| `limit` | int | `50` | 1–500. |
| `offset` | int | `0` | Pagination offset. |

**Response 200:**

```json
{
  "count": 2,
  "limit": 50,
  "offset": 0,
  "results": [
    {
      "id": 1843,
      "created_at": 1782950400,
      "endpoint": "scrape",
      "url": "https://www.reuters.com/markets/...",
      "final_url": "https://www.reuters.com/markets/...",
      "status": "ok",
      "ok": true,
      "fetcher_source": "httpx",
      "title": "Article headline",
      "cost": null,
      "mode": 1,
      "fetched_at": "2026-06-27T12:00:00+00:00"
    }
  ]
}
```

- `count`: the size of **this page** (`len(results)`, so never more than
  `limit`) — *not* the total number of matches. There is no total; page forward
  with `offset` until a short page comes back.
- `endpoint`: one of `scrape | pdf | scrapingbee_scrape |
  scrapingbee_scrape_markdown`.
- Each `results` entry is a summary; `cost` is the ScrapingBee credit charge
  when the record came from a ScrapingBee endpoint, else `null`.

A malformed `q` returns **HTTP 422** (bare `AND` / `OR` / `NEAR` are FTS5
operators; phrases need quoting):

```json
{ "error": { "code": "bad_query",
             "detail": "q='AND' is not a valid FTS5 query: fts5: syntax error near \"AND\"..." } }
```

### One full record — `GET /v1/scrapes/{id}`

Returns one record including the replayable response envelope: the summary
fields **plus** `markdown` and `response` (the full original response JSON —
e.g. the `ScrapeResponse` or `ScrapingBeeScrapeResponse`).

```json
{
  "id": 1843,
  "created_at": 1782950400,
  "endpoint": "scrape",
  "url": "https://www.reuters.com/markets/...",
  "final_url": "https://www.reuters.com/markets/...",
  "status": "ok",
  "ok": true,
  "fetcher_source": "httpx",
  "title": "Article headline",
  "cost": null,
  "mode": 1,
  "fetched_at": "2026-06-27T12:00:00+00:00",
  "markdown": "# Article headline\n\nBody text...",
  "response": { "ok": true, "status": "ok", "article": { ... }, "attempts": [ ... ] }
}
```

An unknown id returns **HTTP 404**:

```json
{ "error": { "code": "not_found", "detail": "..." } }
```

### Stats — `GET /v1/scrapes/stats`

```json
{
  "total": 1843,
  "by_status": { "ok": 1500, "blocked": 200, "paywalled": 100, "not_found": 43 },
  "by_source": { "httpx": 1200, "playwright": 400, "scrapingbee": 150, "archive": 93 },
  "oldest": 1780358400,
  "newest": 1782950400,
  "db_bytes": 48234496,
  "db_path": "/home/scraper/.local/share/scraper/data/scrapes.db"
}
```

- `oldest` / `newest`: epoch seconds, or `null` when the store is empty.
- `db_bytes`: **physical** on-disk size of the SQLite file. The
  `store_max_db_bytes` cap is enforced against *logical* bytes in use (free
  pages excluded), so `db_bytes` can read above the cap between a prune and its
  VACUUM, and below it while recent pages are still in the `-wal` file. Two
  different bases — not a violated limit.

### Examples

```bash
# Full-text search over archived scrapes, newest matches first
curl -fsS 'http://127.0.0.1:8089/v1/scrapes?q=tesla+earnings&limit=20' | jq .

# Filter by status + source, time-bounded
curl -fsS 'http://127.0.0.1:8089/v1/scrapes?status=ok&fetcher_source=httpx&since=1780358400' | jq .

# Fetch one full record (markdown + replayable response) by id
curl -fsS http://127.0.0.1:8089/v1/scrapes/1843 | jq '.response'

# Store stats
curl -fsS http://127.0.0.1:8089/v1/scrapes/stats | jq .
```

---

## Error responses

All error responses use the same `{ok, status, article, attempts, error}`
envelope as success — no special shape for validation errors.

| HTTP | When | Envelope `status` | `error` |
|---|---|---|---|
| 422 | Invalid request body (e.g. `url` not a valid HttpUrl) | `bad_request` | `{code, detail, field, errors}` — `field` points at the offending path (e.g. `"body.url"`) |
| 422 | `extract` set with `llm_enabled: false` on the service | `bad_request` | `{code: "llm_disabled", detail, field: "body.extract"}` |
| 422 | `/v1/search` `engine` needs a credential the service lacks (`google` / `google_news` / `google_scholar` without `SERPAPI_API_KEY`) | `bad_request` | `{code: "search_engine_disabled", detail, field: "body.engine"}` |
| 503 | Pipeline not ready (startup in progress, or fatal init error) | — | `{"detail": "pipeline not ready"}` (FastAPI's default shape — not a domain error) |

Example 422 body:

```json
{
  "url": "",
  "ok": false,
  "status": "bad_request",
  "article": null,
  "attempts": [],
  "error": {
    "code": "url_parsing",
    "detail": "Input should be a valid URL, relative URL without a base",
    "field": "body.url",
    "errors": [ ... ]
  }
}
```

Connection refused from the service port means the container isn't running —
start with `./docker.sh -g` from `~/dev/scraper`.
