# Fetcher fallback chain — how the service actually gets content

A single `POST /v1/scrape` call runs up to three fetchers in sequence and stops
at the first one that returns usable HTML. Understanding this chain lets you
explain unexpected results (e.g. why a Reuters URL came back as an archive
snapshot even though the site is normally public).

## The chain

```
┌─────────┐      blocked/paywalled/       ┌────────────┐
│  httpx  │ ───── timeout ─────────────▶  │ playwright │
└─────────┘                               └────────────┘
     │                                         │
     │ ok / not_found / error                  │ blocked/paywalled/timeout
     ▼                                         ▼
   stop                                   ┌─────────┐
                                          │ archive │
                                          └─────────┘
                                                │
                                                ▼
                                              stop
```

A `not_found` or unrecoverable `error` at any stage short-circuits the chain —
we don't ask archive.ph for a snapshot of a legitimate 404.

## Stage 1: `httpx`

Plain async HTTP with browser-like headers (modern Chrome UA, `Sec-Ch-Ua-*`,
`Accept-Language`, etc.). Follows redirects, max 5. 30s default timeout.

**Decides a response is blocked/paywalled via body heuristics**, not just HTTP
status. So a 200 page with `"Just a moment..."` in the title → `BLOCKED`, and
a 200 page with `"subscribe to continue reading"` → `PAYWALLED`. That's how
we avoid returning a Cloudflare interstitial as if it were the article.

Cost: ~200ms–2s. Almost always the winner on uncomplicated news sites.

## Stage 2: `playwright`

Headless Chromium (new `--headless=new` mode when enabled) with:

- `tf-playwright-stealth` applied to every context (patches webdriver, chrome
  runtime, plugins, WebGL, permissions API fingerprints).
- A custom init script (`news/stealth/init_script.py`) that hardens the same
  signals belt-and-braces and spoofs WebGL vendor/renderer to Intel/Apple.
- Docker-safe launch args: `--no-sandbox`, `--disable-dev-shm-usage`,
  `--disable-blink-features=AutomationControlled`, plus site-per-process
  isolation disabled (helps on some CF-protected sites).
- A challenge-detection loop that waits up to `playwright_challenge_wait_timeout_ms`
  (default 30s) for a Cloudflare Turnstile interstitial to clear before giving up.

If the `playwright_proxy_server` config is set, Playwright routes through that
proxy — useful for residential IPs to match a target site's geo expectations.

Cost: 5–30s on a Cloudflare-protected site; ~2–5s on a cooperative site. Resource-heavy.

## Stage 3: `archive`

Queries archive mirrors in configured order (defaults: archive.ph →
archive.today → web.archive.org). Each provider is hit with its "newest
snapshot" URL form:

- `https://archive.ph/newest/<url>` — 302s to most recent snapshot or a tiny
  landing page if none.
- `https://archive.today/newest/<url>` — same.
- `https://web.archive.org/web/<url>` — Wayback's latest capture or 404.

A 200 response whose body is shorter than 2KB is treated as "no snapshot"
(archive.ph's landing page for uncaptured URLs) and the next provider is tried.

**We never submit new snapshots.** That would take minutes and break the
request/response shape. If no provider has a snapshot, we return `NOT_FOUND`.

Cost: 1–5s per provider, capped at `archive_submit_timeout_seconds` (default 60).

## Reading the `attempts` trace

Every `POST /v1/scrape` response includes `attempts` — a list of what each
fetcher did. Example from a successful Cloudflare-bypass:

```json
[
  {"fetcher": "httpx", "status": "blocked", "http_status": 403, "notes": ""},
  {"fetcher": "playwright", "status": "ok", "http_status": 200, "notes": "cloudflare challenge cleared"}
]
```

From a paywalled article recovered via archive.ph:

```json
[
  {"fetcher": "httpx", "status": "paywalled", "http_status": 200, "notes": ""},
  {"fetcher": "playwright", "status": "paywalled", "http_status": 200, "notes": ""},
  {"fetcher": "archive:archive.ph", "status": "ok", "http_status": 200, "notes": "snapshot via archive.ph"}
]
```

From a complete failure:

```json
[
  {"fetcher": "httpx", "status": "blocked", "http_status": 403, "notes": ""},
  {"fetcher": "playwright", "status": "blocked", "http_status": null, "notes": "cloudflare challenge did not clear within timeout"},
  {"fetcher": "archive", "status": "not_found", "http_status": null, "notes": "archive.ph: body too short (847b); archive.today: http 404; web.archive.org: http 404"}
]
```

When explaining results to a user, the `attempts` trace is the single best
signal — it shows exactly what was tried and why. If the article came from an
archive snapshot, tell the user; the content may be stale.

## What's deliberately NOT in the chain

- **Googlebot UA spoofing** — works on some sites but violates their ToS and
  is a short-term trick. If users need it, they can set `fetch_user_agent`
  in `news.yaml`.
- **Paid CAPTCHA solvers (2Captcha, etc.)** — out of scope for v1; paid
  external dependency, long latency, separate compliance concerns.
- **Residential-proxy rotation** — the service accepts a single static proxy
  via config. Rotating proxies would need a proxy pool config and pool-aware
  concurrency.
