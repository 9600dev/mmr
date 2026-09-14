# Fetcher fallback chain — how the service actually gets content

A single `POST /v1/scrape` call runs up to four fetchers in sequence and stops
at the first one that returns usable HTML. Understanding this chain lets you
explain unexpected results (e.g. why a Reuters URL came back as an archive
snapshot even though the site is normally public).

## The chain

```
┌─────────┐   blocked/        ┌────────────┐   blocked/        ┌─────────────┐
│  httpx  │ ── paywalled/ ──▶ │ playwright │ ── paywalled/ ──▶ │ scrapingbee │
└─────────┘    timeout        └────────────┘    timeout        └─────────────┘
     │                              │            (key required)       │
     │ ok / not_found / error       │ ok                              │ blocked/
     ▼                              ▼                                 │ paywalled/
   stop                           stop                                │ timeout
                                                                      ▼
                                                                ┌─────────┐
                                                                │ archive │
                                                                └─────────┘
                                                                      │
                                                                      ▼
                                                                    stop
```

The **scrapingbee** stage is present only when `SCRAPINGBEE_API_KEY` is set
**and** `scrapingbee_fallback_enabled: true` (the default). Without a key the
chain is the original three (`httpx → playwright → archive`).

A `not_found` or unrecoverable `error` at any stage short-circuits the chain —
we don't ask ScrapingBee or archive.ph for a snapshot of a legitimate 404.

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
- A custom init script (`scraper/stealth/init_script.py`) that hardens the same
  signals belt-and-braces and spoofs WebGL vendor/renderer to Intel/Apple.
- Docker-safe launch args: `--no-sandbox`, `--disable-dev-shm-usage`,
  `--disable-blink-features=AutomationControlled`, plus site-per-process
  isolation disabled (helps on some CF-protected sites).
- A challenge-detection loop that waits up to `playwright_challenge_wait_timeout_ms`
  (default 30s) for a Cloudflare Turnstile interstitial to clear before giving up.

If the `playwright_proxy_server` config is set, Playwright routes through that
proxy — useful for residential IPs to match a target site's geo expectations.

Cost: 5–30s on a Cloudflare-protected site; ~2–5s on a cooperative site. Resource-heavy.

## Stage 3: `scrapingbee` (paid, optional)

Only in the chain when `SCRAPINGBEE_API_KEY` is set and
`scrapingbee_fallback_enabled: true`. Routes the request through
[ScrapingBee](https://www.scrapingbee.com)'s proxy network, which spends
credits — so it sits *after* the two free fetchers and only runs on pages they
couldn't crack. It sits *before* archive because a live render of the real page
beats a stale/partial snapshot when it succeeds.

The request uses the configured fallback knobs (defaults strong, since cheaper
tiers already failed): `render_js` (JS render), `premium_proxy` (premium/
residential proxies), `stealth_proxy` (Cloudflare/bot-wall bypass), optional
`country_code`/`wait`. The returned body runs through the **same `classify`
layer** as every other fetcher, so a 200-OK Cloudflare interstitial that
ScrapingBee returned anyway is still tagged `BLOCKED`, not `OK`.

Failure mapping is archive-friendly: a ScrapingBee-side problem (bad key, out
of credits, upstream 5xx) is reported as `BLOCKED` (not `ERROR`), so the free
archive tier still gets its turn rather than the chain short-circuiting.
ScrapingBee bills only on HTTP 200/404/410, so a rejected key or rate-limit
costs nothing.

With `scrapingbee_fallback_escalate_stealth: true`, a premium attempt that
returns a real 200 body classifying as blocked triggers one 75-credit
stealth retry. The **host verdict memory** then remembers each host's
outcome so the probe isn't re-run per scrape:

- stealth returned CONTENT (`needs_stealth`) → later fetches of that host
  start directly at the stealth pool (skipping the doomed 25cr premium probe;
  notes show `stealth pool via escalation memory`). TTL
  `scrapingbee_fallback_escalation_ttl_seconds`, default **1h**;
- stealth got past the wall and found a PAYWALL (`stealth_paywalled`) →
  later fetches still run the 25cr premium attempt but never buy the 75cr
  rung (notes show `stealth escalation skipped`). Shares the
  `stealth_blocked` TTL, default **3d**. This is the middle setting: unlike
  `stealth_blocked` the tier is NOT skipped, because a mostly-gated site can
  still serve a free URL for 25 credits;
- stealth was **also** blocked (`stealth_blocked`; Kasada-class hosts like
  realestate.com.au) → the paid tier is skipped entirely (`attempts[]` shows
  `status: "skipped"`, zero spend) and the chain goes straight to archive.
  TTL `scrapingbee_fallback_stealth_blocked_ttl_seconds`, default **3d**;
- ScrapingBee answered "this domain is no longer supported" (http 500;
  `unsupported_domain`, currently reuters.com) → the paid tier is skipped
  AND the dedicated `/v1/scrapingbee/*` endpoints 422 without calling out.
  TTL `scrapingbee_unsupported_domain_ttl_seconds`, default **7d**.

The two TTLs differ because the verdicts' cost signs are opposite: a stale
don't-spend verdict costs only freshness (the free tiers and archive still
run), while a stale `needs_stealth` overpays 50 credits on *every* fetch of
that host — hence days for one and hours for the other.

Entries are keyed by hostname with `www.` stripped, **persist across
restarts** in the scrape store's `host_verdicts` table with a wall-clock
expiry (a store that's off or unopenable silently degrades the memory to
in-process only), and expiry is also the re-probe — nothing else
rediscovers a host that has dropped its wall. ScrapingBee-side errors
(credits/key/429) are never remembered.

A successful attempt sets `fetcher_source: "scrapingbee"`. Watch the logs for
the `reached scrapingbee paid fallback tier` line — it's the cost signal (see
`reference/operations.md`).

Cost: ~3–60s upstream depending on `render_js`/proxy options; credits per call.

## Stage 4: `archive`

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
  in `scraper.yaml`.
- **Paid CAPTCHA solvers (2Captcha, etc.)** — out of scope; for hard targets
  the ScrapingBee tier's `stealth_proxy` covers most Cloudflare/bot-wall cases.
- **Residential-proxy rotation in Playwright** — the Playwright stage accepts
  only a single static proxy via `playwright_proxy_server`. Rotating
  residential proxies is instead delegated to the ScrapingBee tier
  (`scrapingbee_fallback_premium` / `stealth`), which is why it's the
  paid escalation rather than something baked into Playwright.
- **Kasada-protected sites** (e.g. realestate.com.au — the `KPSDK` challenge)
  beat the whole chain and aren't supported: ScrapingBee can't solve Kasada,
  and there's no raw residential proxy to point Playwright at. See
  `reference/operations.md` → "Kasada-protected sites".
