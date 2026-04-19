"""
NewsHelpers — thin llmvm-style async wrappers around the local news-service HTTP API.

The service is expected to be running at http://127.0.0.1:8089 (see
``reference/operations.md``). Every method returns a dict so the caller can inspect
``fetcher_source``/``attempts`` in line with mmr's fail-loudly principle.

Design choices:
  - Blocking HTTP via ``httpx`` executed in a worker thread with ``asyncio.to_thread``
    so the event loop stays responsive (mirrors the mmr_helpers pattern).
  - Errors are returned in the payload (``{"error": "...", "ok": False}``) rather
    than raised, again matching MMRHelpers.
  - ``ticker_news`` and ``search_and_scrape`` are mmr-centric convenience methods
    that compose the base endpoints for the common trading-news use case.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any


# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------

# Override via NEWS_SERVICE_URL env var (e.g. when running the service on a
# different port during development).
_NEWS_URL = os.environ.get("NEWS_SERVICE_URL", "http://127.0.0.1:8089").rstrip("/")
# Default per-request timeout. 120s covers the worst-case fallback chain —
# httpx (2s) → playwright with CF-challenge wait (up to 45s) → archive.ph
# → archive.today → web.archive.org (each up to ~20s). 60s clipped the
# archive tier mid-submission and left callers with an empty attempts
# trace; 120s lets the chain finish and return a structured result.
_DEFAULT_TIMEOUT = 120.0


# ----------------------------------------------------------------------
# Internal: synchronous HTTP plumbing, executed in to_thread workers
# ----------------------------------------------------------------------

def _get_sync(path: str, timeout: float = _DEFAULT_TIMEOUT) -> dict[str, Any]:
    """Blocking GET against the news-service. Imports httpx lazily."""
    import httpx

    url = f"{_NEWS_URL}{path}"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(url)
        if resp.status_code >= 400:
            return {
                "ok": False,
                "error": f"HTTP {resp.status_code}",
                "status_code": resp.status_code,
                "body": resp.text[:500],
                "url": url,
            }
        return resp.json()
    except httpx.ConnectError:
        return {
            "ok": False,
            "error": "connection refused — news-service is not running",
            "hint": "cd ~/dev/news && ./docker.sh -g",
            "url": url,
        }
    except httpx.TimeoutException:
        return {"ok": False, "error": f"timed out after {timeout}s", "url": url}
    except Exception as e:  # noqa: BLE001 — surface everything as a payload
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "url": url}


def _post_sync(path: str, payload: dict[str, Any], timeout: float = _DEFAULT_TIMEOUT) -> dict[str, Any]:
    """Blocking POST against the news-service."""
    import httpx

    url = f"{_NEWS_URL}{path}"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(url, json=payload)
        try:
            data = resp.json()
        except Exception:
            return {
                "ok": False,
                "error": f"non-JSON response HTTP {resp.status_code}",
                "body": resp.text[:500],
                "url": url,
            }
        if resp.status_code >= 400 and "error" not in data:
            data.setdefault("ok", False)
            data["error"] = f"HTTP {resp.status_code}"
        if "X-Request-Id" in resp.headers:
            data.setdefault("request_id", resp.headers["X-Request-Id"])
        return data
    except httpx.ConnectError:
        return {
            "ok": False,
            "error": "connection refused — news-service is not running",
            "hint": "cd ~/dev/news && ./docker.sh -g",
            "url": url,
        }
    except httpx.TimeoutException:
        return {"ok": False, "error": f"timed out after {timeout}s", "url": url}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "url": url}


async def _get(path: str, timeout: float = _DEFAULT_TIMEOUT) -> dict[str, Any]:
    return await asyncio.to_thread(_get_sync, path, timeout)


async def _post(path: str, payload: dict[str, Any], timeout: float = _DEFAULT_TIMEOUT) -> dict[str, Any]:
    return await asyncio.to_thread(_post_sync, path, payload, timeout)


# ----------------------------------------------------------------------
# Image extraction helpers (module-level, used by NewsHelpers.get_images)
# ----------------------------------------------------------------------

# Domains that typically serve tracking pixels, ad beacons, or social widgets
# rather than article content. Images from these hosts are dropped.
_IMG_BLOCKLIST_HOSTS = (
    "doubleclick.net", "googlesyndication.com", "googletagmanager.com",
    "google-analytics.com", "scorecardresearch.com", "facebook.net",
    "facebook.com/tr", "connect.facebook.net", "hotjar.com", "segment.io",
    "mixpanel.com", "amazon-adsystem.com", "adservice.google.",
    "taboola.com", "outbrain.com", "criteo.com", "quantserve.com",
    "pinterest.com/ct", "linkedin.com/px", "bing.com/action",
    "gravatar.com",
)

_IMG_EXT_BLOCKLIST = (".svg", ".gif")
_DATA_URI_PREFIX = "data:"


def _extract_og_image_urls(html: str, base_url: str) -> list[str]:
    '''Pull og:image / twitter:image meta URLs, in order.

    These are the publisher's own "headline image" claim — strong signal
    for hero photo ranking. Used to pre-seed candidates so the top-of-list
    isn't a sidebar promo that happens to appear first in the DOM.
    '''
    import re as _re
    from urllib.parse import urljoin

    meta_re = _re.compile(
        r'<meta\b[^>]*(?:property|name)\s*=\s*["\'](?:og:image(?::url)?|twitter:image)["\']'
        r'[^>]*content\s*=\s*["\']([^"\']+)["\']',
        _re.IGNORECASE,
    )
    urls: list[str] = []
    seen: set[str] = set()
    for m in meta_re.finditer(html):
        raw = m.group(1).strip()
        if not raw:
            continue
        resolved = urljoin(base_url, raw)
        if resolved in seen:
            continue
        seen.add(resolved)
        urls.append(resolved)
    return urls


def _extract_img_candidates(html: str, base_url: str) -> list[dict[str, str]]:
    '''Parse <img> tags out of HTML. Returns list of {src, alt} dicts
    biased so og:image / twitter:image URLs rank first.

    Uses regex rather than bs4 to keep the helper dependency-free. Picks up
    src, data-src, and data-lazy-src (common lazy-load patterns). alt text
    is HTML-unescaped — otherwise things like `S&amp;P 500` leak through.
    '''
    import html as _html
    import re as _re
    from urllib.parse import urljoin, urlparse

    tag_re = _re.compile(r"<img\b([^>]*)>", _re.IGNORECASE | _re.DOTALL)
    src_re = _re.compile(
        r"(?:src|data-src|data-lazy-src|data-original)\s*=\s*[\"']([^\"']+)[\"']",
        _re.IGNORECASE,
    )
    alt_re = _re.compile(r"alt\s*=\s*[\"']([^\"']*)[\"']", _re.IGNORECASE)

    # Hero-bias: publisher-claimed hero images first, in meta-tag order.
    og_urls = _extract_og_image_urls(html, base_url)
    seen: set[str] = set()
    records: list[dict[str, str]] = []
    for og_url in og_urls:
        parsed = urlparse(og_url)
        host = parsed.netloc.lower()
        if any(blocked in host for blocked in _IMG_BLOCKLIST_HOSTS):
            continue
        if parsed.path.lower().endswith(_IMG_EXT_BLOCKLIST):
            continue
        seen.add(og_url)
        records.append({"src": og_url, "alt": "", "_source": "og:image"})

    for match in tag_re.finditer(html):
        attrs = match.group(1)
        src_match = src_re.search(attrs)
        if not src_match:
            continue
        src_raw = src_match.group(1).strip()
        if not src_raw or src_raw.startswith(_DATA_URI_PREFIX):
            continue

        resolved = urljoin(base_url, src_raw)
        if resolved in seen:
            continue
        seen.add(resolved)

        parsed = urlparse(resolved)
        host = parsed.netloc.lower()
        if any(blocked in host for blocked in _IMG_BLOCKLIST_HOSTS):
            continue
        path_lower = parsed.path.lower()
        if path_lower.endswith(_IMG_EXT_BLOCKLIST):
            continue

        alt_match = alt_re.search(attrs)
        alt_raw = alt_match.group(1).strip() if alt_match else ""
        alt = _html.unescape(alt_raw)

        records.append({"src": resolved, "alt": alt, "_source": "img"})

    return records


def _download_image_sync(
    record: dict[str, str],
    min_bytes: int,
    max_bytes: int,
    timeout: float,
) -> dict[str, Any]:
    '''Download a single image synchronously. Call via asyncio.to_thread.

    Returns {ok, bytes, src, alt, content_type, reason?}.

    Size filters are symmetric: below ``min_bytes`` → likely favicon/
    tracking pixel, above ``max_bytes`` → too large for an LLM visual
    budget (a single 1.8MB photo burns ~6k vision tokens).
    '''
    import httpx

    src = record["src"]
    try:
        with httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/127.0.0.0 Safari/537.36"
                ),
                "Accept": "image/*,*/*;q=0.8",
            },
        ) as client:
            resp = client.get(src)
    except Exception as e:
        return {"ok": False, "src": src, "reason": f"{type(e).__name__}: {e}"}

    if resp.status_code >= 400:
        return {"ok": False, "src": src, "reason": f"HTTP {resp.status_code}"}

    content_type = resp.headers.get("content-type", "").lower()
    if "image" not in content_type:
        return {"ok": False, "src": src, "reason": f"non-image content-type: {content_type}"}
    if "svg" in content_type:
        return {"ok": False, "src": src, "reason": "SVG skipped (typically logos/icons)"}

    data = resp.content
    if len(data) < min_bytes:
        return {
            "ok": False,
            "src": src,
            "reason": f"too small ({len(data)} < {min_bytes} bytes)",
        }
    if max_bytes and len(data) > max_bytes:
        return {
            "ok": False,
            "src": src,
            "reason": f"too large ({len(data)} > {max_bytes} bytes; caller's max_bytes cap)",
        }

    return {
        "ok": True,
        "src": src,
        "alt": record.get("alt", ""),
        "bytes": data,
        "content_type": content_type,
    }


# ----------------------------------------------------------------------
# NewsHelpers — the public API the LLM calls
# ----------------------------------------------------------------------

class NewsHelpers:
    """Async helpers for mmr's local news-service (http://127.0.0.1:8089).

    **Always** inspect ``article.fetcher_source`` on a scrape result before
    trusting the content — ``archive:*`` means the article is a snapshot and
    may be days/weeks/months old. This matters for trading decisions.
    """

    # ------------------------------------------------------------------
    # Health & diagnostics
    # ------------------------------------------------------------------

    @staticmethod
    async def health() -> dict[str, Any]:
        """Ping the news-service health endpoint.

        Returns a dict with ``status``, ``version``, ``llm_enabled`` on success,
        or ``ok=False`` + ``error`` if the service is unreachable. Call this
        before the first scrape in any batch job so you can fail fast (or
        degrade gracefully) when the container is down.

        Example:
            result = await NewsHelpers.health()
            # {"status": "ok", "version": "0.1.0", "llm_enabled": false}
        """
        result = await _get("/v1/health", timeout=5.0)
        if "status" in result and result.get("status") == "ok":
            result["ok"] = True
        return result

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    @staticmethod
    async def search(
        query: str,
        engine: str = "auto",
        max_results: int = 10,
        exclude_regex: list[str] | None = None,
    ) -> dict[str, Any]:
        """Run a search query through the news-service.

        Args:
            query: Search text.
            engine: One of ``auto`` (default), ``ddg``, ``google``, ``google_news``.
                ``google_news`` attaches ``source`` + ``published_at`` per result
                and is the best choice for freshness-sensitive market news.
                Requires ``SERPAPI_API_KEY`` on the service for google/google_news.
            max_results: 1–50. Server rejects >50 with HTTP 422.
            exclude_regex: Optional URL-regex patterns to filter (e.g. to drop
                ``youtube.com``, ``twitter.com``).

        Returns a dict with ``results``, ``engine``, ``count``, ``rate_limited``,
        ``engines_tried``. **Check ``rate_limited``** before concluding "no
        matches" — on ``auto`` it signals "both engines failed, retry with
        backoff" rather than "no hits".

        Example:
            res = await NewsHelpers.search("Fed rate decision", engine="google_news", max_results=5)
            for r in res["results"]:
                print(r["title"], r.get("published_at"), r["url"])
        """
        payload: dict[str, Any] = {
            "query": query,
            "engine": engine,
            "max_results": max_results,
        }
        if exclude_regex:
            payload["exclude_regex"] = exclude_regex
        return await _post("/v1/search", payload)

    # ------------------------------------------------------------------
    # Scrape
    # ------------------------------------------------------------------

    @staticmethod
    async def scrape(
        url: str,
        extract: str | None = None,
        follow_links: int = 0,
        allow_archive_fallback: bool = True,
        include_html: bool = False,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> dict[str, Any]:
        """Scrape a single article URL.

        Handles Cloudflare challenges, paywalls, and HTML→Markdown extraction.
        The response always contains ``article.markdown`` on success plus an
        ``attempts`` trace showing which fetchers ran.

        Args:
            url: Target article URL.
            extract: Optional LLM extraction prompt (mode 2). Requires
                ``llm_enabled: true`` on the service. Adds ``extraction`` to
                the response; does **not** replace the markdown.
            follow_links: 0–5. When set with ``extract``, the LLM picks that
                many related URLs from the page, fetches them in parallel,
                and synthesises the extraction across the combined corpus.
            allow_archive_fallback: Set to ``False`` for live-only content
                (skips the archive.ph → archive.today → web.archive.org tier).
            include_html: Return raw + cleaned HTML on the article. Response
                gets 10–30× larger — only set when you actually need it.
            timeout: Per-request timeout. Playwright + archive can take
                10–30s; leave the default unless you know you need more.

        Returns a dict with ``ok``, ``status`` (see reference/api.md for the
        status enum), ``article`` (on success), ``attempts``, optionally
        ``extraction`` and ``follow_pages``.

        **Always check ``article.fetcher_source``** — if it's ``archive:*``
        the article may be stale relative to today. Mention that to the user
        when presenting results.

        Example:
            res = await NewsHelpers.scrape("https://www.reuters.com/...")
            if res.get("ok") and res.get("status") == "ok":
                print(res["article"]["markdown"])
                print("fetched via:", res["article"]["fetcher_source"])
        """
        payload: dict[str, Any] = {"url": url}
        if extract is not None:
            payload["extract"] = extract
        if follow_links:
            payload["follow_links"] = int(follow_links)
        if not allow_archive_fallback:
            payload["allow_archive_fallback"] = False
        if include_html:
            payload["include_html"] = True
        return await _post("/v1/scrape", payload, timeout=timeout)

    @staticmethod
    async def markdown(url: str, allow_archive_fallback: bool = True) -> str:
        """Convenience wrapper: return just the article markdown body.

        On failure returns an error string prefixed with ``ERROR:`` that
        carries both the terminal status and the per-fetcher attempts
        trace (``[httpx:blocked, playwright:paywalled, archive:not_found]``).
        That's usually enough to tell paywall-with-no-snapshot apart from
        CF-blocked apart from genuine 404 without dropping to ``scrape()``.

        Example:
            md = await NewsHelpers.markdown("https://www.reuters.com/...")
            if not md.startswith("ERROR:"):
                summary = await llm_call([md], "summarize this article")
        """
        res = await NewsHelpers.scrape(url, allow_archive_fallback=allow_archive_fallback)
        if not res.get("ok"):
            status = res.get("status") or "unknown"
            attempts = res.get("attempts") or []
            trace = ", ".join(
                f"{a.get('fetcher', '?')}:{a.get('status', '?')}" for a in attempts
            )
            suffix = f" [{trace}]" if trace else ""
            return f"ERROR: status={status}{suffix} — {res.get('error') or ''}".rstrip(" —")
        article = res.get("article") or {}
        md = article.get("markdown") or ""
        if not md:
            return f"ERROR: no markdown extracted (status={res.get('status')})"
        return md

    # ------------------------------------------------------------------
    # Composite helpers (mmr-centric)
    # ------------------------------------------------------------------

    @staticmethod
    async def search_and_scrape(
        query: str,
        max_results: int = 5,
        concurrency: int = 3,
        engine: str = "google_news",
        exclude_regex: list[str] | None = None,
    ) -> dict[str, Any]:
        """Search + scrape the top N results in parallel.

        Typical use: get a narrative digest on a catalyst (e.g. "Fed rate
        decision June 2026") without having to orchestrate search→fetch
        yourself. Failed scrapes are included with ``ok=False`` in the
        ``articles`` list so you can see what got blocked.

        Args:
            query: Search text.
            max_results: How many search hits to scrape (1–20).
            concurrency: Parallel fetches. Default 3. Keep ≤5 to be polite
                to upstream sites and avoid Cloudflare heuristics.
            engine: Defaults to ``google_news`` for financial news recency.
            exclude_regex: URL patterns to drop from the search results.

        Returns a dict with ``query``, ``engine``, ``articles`` (list of
        scrape responses in original search-result order).

        Example:
            bundle = await NewsHelpers.search_and_scrape(
                "ASX lithium miners March 2026", max_results=5,
            )
            for a in bundle["articles"]:
                if a.get("ok"):
                    print(a["article"]["title"], "→", a["article"]["fetcher_source"])
        """
        search_res = await NewsHelpers.search(
            query, engine=engine, max_results=max_results, exclude_regex=exclude_regex
        )
        if not search_res.get("results"):
            return {
                "ok": True,
                "query": query,
                "engine": search_res.get("engine", engine),
                "articles": [],
                "rate_limited": search_res.get("rate_limited", False),
                "count": 0,
            }

        urls = [r["url"] for r in search_res["results"][:max_results]]
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _bounded(u: str) -> dict[str, Any]:
            async with sem:
                res = await NewsHelpers.scrape(u)
                # Attach original search-result metadata (source, published_at)
                for sr in search_res["results"]:
                    if sr["url"] == u:
                        res.setdefault("search", {
                            "title": sr.get("title"),
                            "snippet": sr.get("snippet"),
                            "source": sr.get("source"),
                            "published_at": sr.get("published_at"),
                        })
                        break
                return res

        articles = await asyncio.gather(*[_bounded(u) for u in urls])
        return {
            "ok": True,
            "query": query,
            "engine": search_res.get("engine", engine),
            "count": len(articles),
            "articles": list(articles),
        }

    @staticmethod
    async def ticker_news(
        ticker: str,
        max_results: int = 5,
        extract: str | None = None,
        concurrency: int = 3,
        exchange_hint: str | None = None,
    ) -> dict[str, Any]:
        """Fetch and scrape recent news for a ticker — the mmr portfolio/idea use case.

        Under the hood this composes a ``google_news`` search with a
        finance-biased query template, then scrapes the top N results in
        parallel. Pair with ``MMRHelpers.portfolio()`` to scan news across
        every open position, or with ``MMRHelpers.ideas()`` to vet scan hits.

        Args:
            ticker: Ticker symbol (e.g. "AAPL", "BHP", "AGE.AX").
            max_results: Number of articles to scrape (1–20).
            extract: Optional LLM extraction prompt applied per-article
                (e.g. "What's the catalyst? What's the direction?"). Each
                result then carries an ``extraction`` field.
            concurrency: Parallel scrape limit (default 3).
            exchange_hint: Optional market hint for disambiguating foreign
                tickers. Pass e.g. ``"ASX"``, ``"LSE"``, ``"TSE"``. The
                query becomes ``"{TICKER} stock {hint} news"``.

        Returns a dict: ``ticker``, ``query``, ``count``, ``articles`` (list).

        Example:
            res = await NewsHelpers.ticker_news("BHP", max_results=5, exchange_hint="ASX")
            for a in res["articles"]:
                if a.get("ok"):
                    print(a["article"]["title"])
        """
        q = f"{ticker} stock"
        if exchange_hint:
            q += f" {exchange_hint}"
        q += " news"

        bundle = await NewsHelpers.search_and_scrape(
            q, max_results=max_results, concurrency=concurrency, engine="google_news",
        )

        if extract and bundle.get("articles"):
            # Re-scrape with extract prompt only on successful fetches
            sem = asyncio.Semaphore(max(1, concurrency))

            async def _enrich(existing: dict[str, Any]) -> dict[str, Any]:
                if not existing.get("ok"):
                    return existing
                url = (existing.get("article") or {}).get("final_url") or None
                if not url:
                    return existing
                async with sem:
                    enriched = await NewsHelpers.scrape(url, extract=extract)
                # Preserve the search metadata we attached earlier
                if "search" in existing:
                    enriched["search"] = existing["search"]
                return enriched

            bundle["articles"] = await asyncio.gather(*[_enrich(a) for a in bundle["articles"]])

        return {
            "ok": True,
            "ticker": ticker,
            "query": q,
            "count": bundle.get("count", 0),
            "articles": bundle.get("articles", []),
        }

    @staticmethod
    async def portfolio_news(
        tickers: list[str],
        max_results_per_ticker: int = 3,
        concurrency: int = 2,
        exchange_hint: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fan out ``ticker_news`` across a list of tickers in parallel.

        The canonical mmr use case: given the output of
        ``MMRHelpers.portfolio()``, pull the latest news on every held
        position in a single call so downstream strategies or narrative
        digests can reason about it.

        Args:
            tickers: Symbols to fetch news for.
            max_results_per_ticker: Articles per ticker.
            concurrency: Parallel ticker-level fetches (default 2 — each
                ticker spawns its own sub-pool so effective parallelism is
                this × inner concurrency).
            exchange_hint: Applied to all tickers (use when the whole
                batch is from one market, e.g. all ASX).

        Returns ``{"by_ticker": {symbol: [articles,...]}, "errors": {...}}``.

        Example:
            pf = await MMRHelpers.portfolio()
            syms = [p["symbol"] for p in pf["data"][:10]]
            news = await NewsHelpers.portfolio_news(syms, max_results_per_ticker=3)
        """
        sem = asyncio.Semaphore(max(1, concurrency))

        async def _one(t: str) -> tuple[str, dict[str, Any]]:
            async with sem:
                return t, await NewsHelpers.ticker_news(
                    t, max_results=max_results_per_ticker, exchange_hint=exchange_hint,
                )

        out = await asyncio.gather(*[_one(t) for t in tickers])
        by_ticker: dict[str, list[dict[str, Any]]] = {}
        errors: dict[str, str] = {}
        for t, res in out:
            articles = res.get("articles", [])
            by_ticker[t] = articles
            if not articles:
                errors[t] = "no results"
        return {"ok": True, "by_ticker": by_ticker, "errors": errors}

    # ------------------------------------------------------------------
    # Image extraction
    # ------------------------------------------------------------------

    @staticmethod
    async def get_images(
        url: str,
        max_images: int = 5,
        min_bytes: int = 5000,
        max_bytes: int = 1_500_000,
        include_markdown: bool = False,
        allow_archive_fallback: bool = True,
        timeout: float = _DEFAULT_TIMEOUT,
    ) -> dict[str, Any]:
        '''Fetch an article and return its inline images as llmvm ImageContent.

        The news-service itself does not expose images — it strips them during
        HTML→Markdown conversion. This helper bolts on image extraction by:

        1. Calling /v1/scrape with include_html=True to get the **cleaned**
           HTML (post nav/sidebar/paywall stripping).
        2. Parsing <img> tags out of that HTML.
        3. Resolving relative URLs against article.final_url.
        4. Filtering out data: URIs, tracking pixels, common ad/analytics
           domains, SVGs, GIFs, and anything smaller than min_bytes.
        5. Downloading each surviving image with httpx and wrapping it in
           llmvm_lite.llm_types.ImageContent so the LLM can actually see it.

        Args:
            url: Article URL to fetch.
            max_images: Cap on returned images (default 5). Prefer a small
                number — the LLM's visual budget is not infinite, and most
                financial articles have 1–3 useful images (headline photo,
                a chart, maybe a secondary exhibit).
            min_bytes: Minimum file size to keep. Defaults to 5000 bytes,
                which filters out most logos, favicons, tracking pixels,
                and author avatars while retaining real article images.
            max_bytes: Maximum file size to keep. Defaults to 1.5 MB.
                Anything larger is skipped with reason ``too large`` — a
                single 1.8 MB photo burns ~6k vision tokens, and most
                editorial photos land well under 1 MB anyway. Set to 0
                to disable the cap.
            include_markdown: If True, also return the article markdown in
                the response (useful when handing text + images to the LLM
                together).
            allow_archive_fallback: Same semantics as scrape().
            timeout: Per-request timeout.

        Returns a dict with:
            ok (bool), article_title (str), article_url (final URL),
            fetcher_source (str), count (int), images (list[ImageContent]),
            image_meta (list[{src, alt, bytes, content_type}]),
            skipped (list[{src, reason}]), markdown (str, optional).

        Example:
            res = await NewsHelpers.get_images(
                "https://stockhead.com.au/resources/...",
                max_images=3,
            )
            if res["ok"]:
                for img in res["images"]:
                    emit(img)   # LLM can see the image
                for m in res["image_meta"]:
                    print(f"  {m['alt'][:60]} ({m['bytes']} bytes)")
        '''
        # 1. Scrape with include_html=True
        scrape_res = await NewsHelpers.scrape(
            url,
            allow_archive_fallback=allow_archive_fallback,
            include_html=True,
            timeout=timeout,
        )
        if not scrape_res.get("ok") or scrape_res.get("status") != "ok":
            return {
                "ok": False,
                "error": scrape_res.get("error") or f"scrape failed: status={scrape_res.get('status')}",
                "attempts": scrape_res.get("attempts"),
                "images": [],
                "image_meta": [],
                "skipped": [],
            }

        article = scrape_res.get("article") or {}
        final_url = article.get("final_url") or url
        html_cleaned = article.get("html_cleaned") or article.get("html_raw") or ""
        if not html_cleaned:
            return {
                "ok": False,
                "error": "scrape succeeded but no HTML returned (include_html may be disabled on the service)",
                "images": [],
                "image_meta": [],
                "skipped": [],
            }

        # 2. Parse <img> tags + filter
        img_records = _extract_img_candidates(html_cleaned, final_url)
        if not img_records:
            out: dict[str, Any] = {
                "ok": True,
                "article_title": article.get("title"),
                "article_url": final_url,
                "fetcher_source": article.get("fetcher_source"),
                "count": 0,
                "images": [],
                "image_meta": [],
                "skipped": [],
            }
            if include_markdown:
                out["markdown"] = article.get("markdown", "")
            return out

        # Buffer so size-filtering has room to reject bad candidates
        buffer_count = min(max_images * 3, len(img_records))
        candidates = img_records[:buffer_count]

        # 3. Download images in parallel
        sem = asyncio.Semaphore(4)

        async def _fetch(rec: dict[str, str]) -> dict[str, Any]:
            async with sem:
                return await asyncio.to_thread(
                    _download_image_sync, rec, min_bytes, max_bytes, 15.0,
                )

        results = await asyncio.gather(*[_fetch(r) for r in candidates])

        # 4. Build ImageContent objects for surviving images
        from llmvm_lite.llm_types import ImageContent  # imported lazily

        images: list[Any] = []
        image_meta: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for res in results:
            if res["ok"]:
                if len(images) >= max_images:
                    skipped.append({"src": res["src"], "reason": "max_images cap reached"})
                    continue
                try:
                    img = ImageContent(image=res["bytes"])
                    images.append(img)
                    image_meta.append({
                        "src": res["src"],
                        "alt": res.get("alt", ""),
                        "bytes": len(res["bytes"]),
                        "content_type": res.get("content_type", ""),
                    })
                except Exception as e:
                    skipped.append({"src": res["src"], "reason": f"ImageContent wrap failed: {e}"})
            else:
                skipped.append({"src": res["src"], "reason": res.get("reason", "download failed")})

        out = {
            "ok": True,
            "article_title": article.get("title"),
            "article_url": final_url,
            "fetcher_source": article.get("fetcher_source"),
            "count": len(images),
            "images": images,
            "image_meta": image_meta,
            "skipped": skipped,
        }
        if include_markdown:
            out["markdown"] = article.get("markdown", "")
        return out
