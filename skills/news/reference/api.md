# News Service — HTTP API reference

Base URL: `http://127.0.0.1:8089` (override via `NEWS_HTTP_ADDRESS` / `NEWS_HTTP_PORT`).

**Endpoints:** `/v1/health` (liveness), `/v1/search` (DDG query),
`/v1/scrape` (three modes: heuristic, LLM extraction, LLM extraction +
link-follow).

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
  "llm_enabled": false
}
```

- `status`: always `"ok"` when the service is accepting requests.
- `version`: matches `news.__version__` in the Python package.
- `llm_enabled`: whether the service has LLM-assisted features turned on. If
  `false`, calls to any LLM-backed endpoint will error.

---

## `POST /v1/search`

Run a search query against one of four engines. Falls back gracefully
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
| `exclude_regex` | string[] | no | `[]` | Regex patterns matched against each result URL |
| `engine` | string | no | `"auto"` | One of `auto` / `ddg` / `google` / `google_news`. `auto` → Google via SerpAPI when `SERPAPI_API_KEY` is set, else DDG. |

**Response 200:**

```json
{
  "query": "IMF growth outlook 2026",
  "count": 3,
  "engine": "google",
  "engines_tried": ["google"],
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
  `google` or `ddg` at request time; `none` if the request specified an
  unknown engine).
- Per-result `engine`: the provider that produced this row —
  `DuckDuckGo`, `Brave` (DDG's fallback), `Google`, or `GoogleNews`.
- `source`: publisher name, populated only for Google News results
  (e.g. `"Reuters"`, `"Bloomberg"`).
- `published_at`: ISO 8601 publish timestamp, populated only for Google
  News results when SerpAPI's `date` field is parsable.

An empty `results` list can mean: no matches, upstream rate-limited us,
or a SerpAPI call returned an error (401 / 429 / 5xx). The response's
`rate_limited: true` flag tells you specifically when every engine in
the chain returned empty — i.e. worth retrying with backoff rather than
accepting "no matches" as the answer:

- `engine=auto` fires both preferred engines → `engines_tried: ["google", "ddg"]`.
  Both empty → `rate_limited: true`.
- Explicit engines (`ddg` / `google` / `google_news`) don't cross-fall — a
  single empty response has `rate_limited: false` because we only tried
  one engine.

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
  "extract": "What is the CEO's stated reason for the layoffs?",
  "follow_links": 2
}
```

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `url` | string (HttpUrl) | yes | — | Validated by pydantic; malformed → 422 |
| `allow_archive_fallback` | bool | no | `true` | Disable to skip archive.ph/wayback tier |
| `extract` | string \| null | no | `null` | Natural-language extraction prompt. Requires `llm_enabled=true` |
| `follow_links` | int | no | `0` | 0–5 (server cap). **Requires `extract`** — sending `follow_links>0` without it returns HTTP 422 |
| `include_html` | bool | no | `false` | Return raw upstream HTML + post-preclean HTML on the response. Each capped at 2 MB (truncated with a marker). Off by default because HTML payloads are 10–30× the markdown size |

**Response 200 (shared envelope for success and domain failure):**

```json
{
  "url": "https://example.com/article",
  "ok": true | false,
  "status": "ok" | "blocked" | "paywalled" | "not_found" | "timeout" | "error" | "extraction_error",
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
- `fetcher_source`: one of `httpx`, `playwright`, `archive:archive.ph`,
  `archive:archive.today`, `archive:web.archive.org`. **Inspect this to know
  whether the content is live or a snapshot.**
- `fetch_notes`: human-readable note from the successful fetcher (e.g.
  `"cloudflare challenge cleared"` or `"snapshot via archive.ph"`).
- `extractor_stats`: `rustdown` internals — useful for debugging when extraction
  seems off.
- `html_raw` / `html_cleaned`: Null unless the request set `include_html: true`.
  - `html_raw` is the upstream HTML, untouched — useful for debugging
    extraction or running your own readability/extraction pipeline.
  - `html_cleaned` is what rustdown saw — post-preclean (domain-specific nav
    and paywall elements stripped), pre-markdown-conversion. Diff this
    against `html_raw` to see what our preclean rules removed.
  - Each is capped at 2 MB; a larger page is truncated with
    `<!-- news-service: truncated N bytes -->` appended.
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
    "model": "claude-opus-4-7",
    "input_chars": 4812,
    "elapsed_ms": 1432,
    "error": null
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
- `extraction.error`: populated (e.g. `"AuthenticationError: ..."`) when
  the LLM call itself failed. The rest of the response is still valid —
  `ok: true`, `article.markdown` present — so callers can fall back to
  the markdown body instead of treating the scrape as a total loss.
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

- `fetcher`: name of the attempting fetcher (`httpx`, `playwright`, `archive`).
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

## `POST /v1/pdf`

Convert a PDF at the given URL to Markdown via Mathpix.

```json
{
  "url": "https://www.sec.gov/Archives/edgar/.../aapl-10-k.pdf",
  "use_cache": true
}
```

| Field | Type | Required | Default | Notes |
|---|---|---|---|---|
| `url` | string | yes | — | URL of the PDF. Mathpix downloads it server-side. |
| `use_cache` | bool | no | `true` | Return cached markdown if a prior conversion exists for this URL. Set `false` to force re-conversion (PDF content changed). |

Returns the **same `ScrapeResponse` envelope** as `/v1/scrape`, so callers can share response parsing. Notable response fields on a successful PDF convert:

- `article.fetcher_source`: `"mathpix"` (fresh convert) or `"mathpix:cache"` (disk-cache hit on the service).
- `article.markdown`: the converted body. Format determined by `mathpix_output_format` on the service (`mmd` = Mathpix Markdown with `$math$` + tables; `md` = plain CommonMark).
- `article.metadata.pdf_pages` / `pdf_id` / `pdf_output_format` / `pdf_cached`: PDF-specific diagnostics.
- `article.title`: `null` (Mathpix doesn't return a separate title field; title is typically the first heading in `markdown`).
- `attempts`: one entry, `{fetcher: "mathpix"|"mathpix:cache", status: "ok", http_status: 200, notes: "pdf_id=... pages=... format=..."}`.

PDF URLs sent to `/v1/scrape` (URL path ending in `.pdf`) transparently route to this endpoint — `/v1/pdf` is primarily useful when you know it's a PDF ahead of time, or when `use_cache=false` is wanted.

### Error responses (PDF-specific)

| HTTP | When | Envelope `status` | `error` |
|---|---|---|---|
| 422 | `mathpix_enabled: false` or `MATHPIX_API_KEY` empty | `bad_request` | `{code: "mathpix_disabled", detail, field: "body.url"}` |
| 200 | Mathpix conversion failure (auth, quota, corrupted PDF, timeout) | `error` | `error` string carries the Mathpix reason; `attempts[0].notes` has the detail |

---

## Error responses

All error responses use the same `{ok, status, article, attempts, error}`
envelope as success — no special shape for validation errors.

| HTTP | When | Envelope `status` | `error` |
|---|---|---|---|
| 422 | Invalid request body (e.g. `url` not a valid HttpUrl) | `bad_request` | `{code, detail, field, errors}` — `field` points at the offending path (e.g. `"body.url"`) |
| 422 | `follow_links>0` sent without `extract` | `bad_request` | Pydantic `value_error` on `body` — reshape the request with both fields set |
| 422 | `extract` set with `llm_enabled: false` on the service | `bad_request` | `{code: "llm_disabled", detail, field: "body.extract"}` |
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
start with `./docker.sh -g` from `~/dev/news`.
