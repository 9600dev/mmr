# Operating the scraper service — docker.sh, logs, config

## Starting / stopping

All lifecycle goes through `~/dev/scraper/docker.sh`. Auto-detects docker vs podman.

```bash
cd ~/dev/scraper

./docker.sh -g     # go: build + up + tail logs (most common dev flow)
./docker.sh -b     # build image only (stages sibling ~/dev/llmvm_lite + ~/dev/rustdown)
./docker.sh -u     # start existing image
./docker.sh -d     # stop and remove containers
./docker.sh -l     # tail logs of running container
./docker.sh -e     # exec a bash shell inside the running container
./docker.sh -s     # rsync local code changes into the running container (no rebuild)
./docker.sh -a     # sync_all — same as -s but without .gitignore filter
./docker.sh -c     # clean: remove images + containers (the DB volume SURVIVES)
./docker.sh -f     # force clean: also prune build cache, and offer to wipe the DB
./docker.sh -B     # backup: snapshot scrapes.db out to ~/.local/share/scraper/backups/
./docker.sh -B nm  # same, named "nm" — exempt from rotation, kept forever
```

Extra flags for the DB volume (see "Scrape store" below):

```bash
./docker.sh -B --keep 5           # keep 5 auto snapshots instead of the default 3
./docker.sh -u --seed-db /path/scrapes.db   # seed an EMPTY volume from a specific file
```

The `-b` step is the expensive one — it Rust-compiles the rustdown wheel,
downloads the Playwright Chromium binary (~170MB), and installs llmvm_lite +
dependencies. Subsequent `-b` runs reuse:

- Cargo registry/git/target caches (BuildKit cache mounts) — saves ~5min of Rust compile.
- pip's `/home/scraper/.cache/pip` (BuildKit cache mount) — saves Python dep redownload.
- Docker's layer cache if `requirements.txt` and sibling sources are unchanged.

## Health check from outside the container

```bash
curl -fsS http://127.0.0.1:8089/v1/health
```

Expected: `{"status":"ok","version":"0.1.0","llm_enabled":false}`.

## Health check from inside the container

```bash
./docker.sh -e
# now inside:
curl -fsS http://localhost:8089/v1/health
```

## Logs

Container logs stream from uvicorn + the `scraper` Python logger.

```bash
./docker.sh -l         # tail -f from docker compose logs
```

A persistent file log also lands at `/home/scraper/.local/share/scraper/logs/scraper_<timestamp>.log`
inside the container — mirrored to the host at `~/.local/share/scraper/logs/` via bind mount.

## Configuration

Two tiers, in override order:

1. **`~/.config/scraper/scraper.yaml`** — bind-mounted from host; survives container restarts.
   Seeded from `configs/scraper.yaml` on first start.
2. **`SCRAPER_*` environment variables** — any flat-key in the config can be overridden
   (e.g. `SCRAPER_HTTP_PORT=9000`, `SCRAPER_LLM_ENABLED=true`, `SCRAPER_PLAYWRIGHT_HEADLESS=false`).
   - Booleans accept `1/true/t/yes/y/on/enabled` and `0/false/f/no/n/off/disabled`
     (case- and surrounding-whitespace-insensitive). Anything else is a **boot
     error** naming the variable — a typo can never quietly disable a subsystem.
   - List fields (`archive_providers`, `playwright_launch_args`,
     `playwright_launch_ignore_default_args`, `llm_excluded_skills`,
     `llm_api_key_env_fallbacks`, `agent_disable_skills`) take a comma-separated
     value, e.g. `SCRAPER_ARCHIVE_PROVIDERS=archive.ph,web.archive.org`. For items
     that contain commas (Chromium's `--disable-features=A,B`) pass a JSON array
     instead — a value starting with `[` is parsed as JSON. `mathpix_options`
     takes a JSON object.

Unrecognised keys in `scraper.yaml` are ignored (a newer config still loads on an
older build) but logged as a `WARNING` at boot naming the file and the keys — check
the startup log after editing, since a typo like `llm_enable` leaves the setting on
its default.

To change a setting permanently, edit `~/.config/scraper/scraper.yaml`. Restart with
`./docker.sh -d && ./docker.sh -u`.

### Common config changes

```yaml
# Make Playwright visible (for debugging) — only useful outside Docker
playwright_headless: false

# Route through a residential proxy
playwright_proxy_server: "http://user:pass@proxy.example.com:8000"

# Turn on LLM features (requires ANTHROPIC_API_KEY in env)
llm_enabled: true
llm_model_name: claude-opus-4-8

# Change the archive order or drop a provider
archive_providers:
  - archive.ph
  - web.archive.org
```

## LLM credentials

LLM-assisted features are opt-in. `docker-compose.yml` forwards the
following env vars from the host shell (or a `.env` file at the repo root)
into the container:

| Env var | Notes |
|---|---|
| `ANTHROPIC_API_KEY` | Falls back to `ANT_API_KEY` when unset — matches the short-name alias in the local `~/.zshrc` |
| `OPENAI_API_KEY` | Falls back to `OAI_API_KEY` |
| `GEMINI_API_KEY` | Google Gemini |
| `DEEPSEEK_API_KEY` | DeepSeek |
| `LLAMA_API_KEY` | Meta Llama Cloud |
| `SERPAPI_API_KEY` | SerpAPI — unlocks Google / Google News / Google Scholar search (`/v1/search` engines `google`, `google_news`, `google_scholar`). When set, `engine=auto` prefers Google over DDG and `engine=research` fans out to arXiv + Scholar (arXiv-only otherwise — the boot log and `./docker.sh` both warn when the key is missing). |
| `SCRAPINGBEE_API_KEY` | ScrapingBee proxy/scraper API. When set, adds a paid fallback tier to `/v1/scrape` and enables the `/v1/scrapingbee/*` endpoints. See the dedicated section below. |

Each is optional. llmvm picks which one to read based on the model name
you set in `scraper.yaml` (`llm_model_name` — `claude-*` uses Anthropic, `gpt-*`
uses OpenAI, etc.). The service itself defaults to Claude.

To enable LLM features:

1. Confirm the relevant key is exported in your shell (e.g. `echo $ANT_API_KEY`).
2. Set `llm_enabled: true` in `~/.config/scraper/scraper.yaml`.
3. Optionally set `llm_model_name: gpt-5` or similar to switch providers.
4. `./docker.sh -d && ./docker.sh -u`.

`/v1/health`'s `llm_enabled` field flips to `true` when LLM features are on.
A failed call inside the container means the key wasn't forwarded —
`./docker.sh -e` then `env | grep -E '_API_KEY|_TOKEN'` confirms what
actually made it in.

## ScrapingBee (proxy/scraper API)

[ScrapingBee](https://www.scrapingbee.com) is a **paid** proxy/scraper API with
two roles, both gated on a non-empty `SCRAPINGBEE_API_KEY` in the container
environment (forwarded by `docker-compose.yml`):

1. **Fallback tier in `/v1/scrape`.** The pipeline order becomes
   `httpx → playwright → scrapingbee → archive`. ScrapingBee only runs after
   the two free fetchers fail to produce an article, and before the free
   archive tier — so it spends credits only on genuinely hard pages, and a
   live render still beats a stale snapshot when it succeeds. Opt out with
   `scrapingbee_fallback_enabled: false` (keeps the routes, drops the
   per-scrape spend).
2. **Dedicated endpoints** under `/v1/scrapingbee/*` (general HTML scrape +
   the Google SERP API + a Google Finance helper) — see `reference/api.md`.
   These are always mounted, but return a structured `422 scrapingbee_disabled`
   (not a 404) when the feature is off, so callers get a clear reason.

Both downgrade to off at boot when the key is empty (a `WARNING` is logged),
exactly like the LLM/Mathpix subsystems. `/v1/health`'s `scrapingbee_enabled`
reports the effective state — check it before calling the routes.

### Config (`scraper.yaml`)

```yaml
scrapingbee_enabled: true            # master switch (key still required)
scrapingbee_api_key_env: SCRAPINGBEE_API_KEY   # env var the key is read from
scrapingbee_base_url: https://app.scrapingbee.com/api/v1
scrapingbee_timeout_seconds: 70
scrapingbee_fallback_enabled: true   # wire into /v1/scrape as a fallback tier
# Fallback request shaping — strong by default since cheaper tiers already
# failed. Tune down to cut credit cost (render_js=5cr, premium=10cr,
# premium+render_js=25cr, stealth=75cr).
scrapingbee_fallback_render_js: true
scrapingbee_fallback_premium: true
scrapingbee_fallback_stealth: false
scrapingbee_fallback_escalate_stealth: true    # premium→stealth auto-retry
                                     # (code default false; bundled config
                                     # enables it — escalation memory caps
                                     # the repeat-spend risk)
# Per-host memory of escalation outcomes (needs escalate_stealth), persisted
# in the scrape store so it survives restarts. Hosts that needed stealth
# start there directly; hosts that defeated BOTH pools skip the paid tier
# entirely until the TTL expires. Three TTLs because the cost signs differ:
# starting-at-stealth SPENDS more when stale (hours), skipping the tier spends
# nothing when stale (days). 0 disables each.
scrapingbee_fallback_escalation_ttl_seconds: 3600        # "needs stealth", 1h
scrapingbee_fallback_stealth_blocked_ttl_seconds: 259200 # both pools walled, 3d
scrapingbee_unsupported_domain_ttl_seconds: 604800       # vendor dropped it, 7d
scrapingbee_fallback_country: ""     # ISO alpha-2, '' = provider default
scrapingbee_fallback_wait_ms: 0      # extra JS settle, 0..35000
```

Any of these is also overridable via `SCRAPER_SCRAPINGBEE_*` env (e.g.
`SCRAPER_SCRAPINGBEE_FALLBACK_STEALTH=true`).

### What to watch for in the logs (`./docker.sh -l`)

`docker.sh -b`/`-u` run `check_scrapingbee_key` up front — an informational
NOTE if `scrapingbee_enabled=true` but the key is missing. Then at runtime:

**At boot** — confirms the tier is live:

```
scraper.server  scrapingbee: ENABLED — paid fallback tier wired into the pipeline (httpx → playwright → scrapingbee → archive) and /v1/scrapingbee/* routes mounted
```

(or, when the key is empty, the `WARNING`:)

```
scraper.server  scraper.yaml sets scrapingbee_enabled=true but env var 'SCRAPINGBEE_API_KEY' is empty — ScrapingBee fallback + /v1/scrapingbee routes disabled.
```

**When the paid fallback engages on a `/v1/scrape`** — three lines tell the
whole story, in order:

```
scraper.scrapingbee.fetcher        scrape https://… reached scrapingbee paid fallback tier (render_js=True premium=True stealth=False)
scraper.scrapingbee.client         scrapingbee call endpoint=scrape http=200 ok=true cost=25
scraper.pipeline.scrape_pipeline   fetch scrapingbee=https://… status=ok http=200 elapsed_ms=… notes='scrapingbee'
```

The first is the **cost signal** (paid tier reached + the knobs used); it
only appears *after* the httpx/playwright attempt lines above it, so you can
see why it escalated. The second carries the credits charged (`cost=`). The
third is the pipeline's classified outcome.

**Direct `/v1/scrapingbee/*` calls** log one line each:

```
scraper.scrapingbee.client  scrapingbee call endpoint=scrape http=200 ok=true cost=25
```

**Failures** (bad key, out of credits, upstream error) log a `WARNING` with
the reason — the HTTP response is still 200 with `ok: false`:

```
scraper.scrapingbee.client  scrapingbee call endpoint=... ok=false: scrapingbee http 401 (api key rejected / no credits): ...
```

None of these lines include the `api_key` (we log the endpoint name, not the
URL), and httpx's own request logger is pinned to `WARNING`, so the
key-bearing request URL never reaches the log stream.

## Scrape store

Every scrape that flows through `/v1/scrape`, `/v1/pdf`, and the direct
`/v1/scrapingbee/scrape` & `/v1/scrapingbee/scrape_markdown` endpoints is
archived to a persistent, searchable SQLite database (stdlib `sqlite3` + FTS5
— no new dependency). It doubles as an opt-in result cache. The store is gated
on `store_enabled` (default on); `/v1/health` reports the effective
`store_enabled`, and the search API lives at `/v1/scrapes` (see
`reference/api.md`).

### Where the DB lives (named volume, not the bind mount)

The DB lives **inside the Docker named volume `scraper_scraper_db_data`**, at
`/home/scraper/.local/share/scraper/data/db/scrapes.db` in the container. It is
NOT on the host bind mount, and `~/.local/share/scraper/data/scrapes.db` is a
**frozen pre-migration copy** — querying that file returns the archive as it
stood at migration time, not now. (A `README-scrapes.db-is-legacy.txt` sits
beside it saying so; it is kept deliberately as a safety copy and can be deleted
once you trust the volume and the snapshots.)

Why: macOS Docker Desktop serves bind mounts over VirtioFS, whose fsync/mmap
semantics are unreliable for a write-heavy single-file SQLite DB. The volume
lives on the Docker VM's native ext4. `~/dev/mmr` and `~/dev/horserank` made the
same move for the same reason. Confirm it any time with:

```bash
docker exec -u scraper scraper-scraper-1 df -T \
  /home/scraper/.local/share/scraper/data/db \
  /home/scraper/.local/share/scraper/data
# data/db -> ext4 (/dev/vda1)      <- the volume
# data    -> fakeowner             <- VirtioFS bind mount
```

Everything *else* under `data/` is still bind-mounted and directly readable from
the host: `images/`, `agent/<id>/` traces, `mathpix_cache/`, plus `logs/` and
`backups/`. Only the write-heavy DB moved.

To read the live DB:

```bash
# live, in-container
./docker.sh -e
sqlite3 /home/scraper/.local/share/scraper/data/db/scrapes.db \
  'SELECT id,status,url FROM scrapes ORDER BY created_at DESC LIMIT 10;'

# or one-shot from the host
docker exec -u scraper scraper-scraper-1 sqlite3 \
  /home/scraper/.local/share/scraper/data/db/scrapes.db 'SELECT COUNT(*) FROM scrapes;'

# or take a host-readable snapshot and query that (see Backups)
./docker.sh -B
sqlite3 ~/.local/share/scraper/backups/backup.db 'SELECT COUNT(*) FROM scrapes;'
```

### Backups and rotation (`-B`)

```bash
./docker.sh -B                 # timestamped snapshot + rotation
./docker.sh -B before-purge    # named snapshot, never rotated away
./docker.sh -B --keep 5        # keep 5 auto snapshots this run
```

Snapshots land in `~/.local/share/scraper/backups/` (host-visible, on the bind
mount) as `scrapes-<ts>.db`, plus a `backup.db` symlink pointing at the newest —
that pointer is what a future reseed looks for.

- **Consistent, no downtime.** With the container running, `-B` runs SQLite
  `VACUUM INTO` *inside* the container against the live DB. That takes a read
  transaction and writes a transactionally consistent copy while the service
  keeps scraping — a plain `cp` of a live SQLite file does not, and would also
  miss anything still in the `-wal`. Container down → the same `VACUUM INTO`
  runs in a one-shot sidecar off the service image.
- **Not a compaction.** The snapshot comes out within ±1% of the live file
  (~2.14 GB) because the store's size cap plus its incremental-vacuum reclaim
  already keep the freelist tiny — there is no slack to squeeze. Budget a full
  copy per snapshot.
- **Verified, not assumed.** Each snapshot is checked non-empty and run through
  `PRAGMA integrity_check` (host `sqlite3` if present, else in a container)
  before the seed pointer is moved.
- **Rotation: `--keep N`, default 3** (or `SCRAPER_BACKUP_KEEP`). At ~2.14 GB
  per snapshot with no dedup, 3 is ~6.5 GB; 30 (what mmr uses for its smaller
  DuckDBs) would be ~65 GB. Only auto-timestamped snapshots rotate — a
  `-B <name>` snapshot is kept indefinitely. `-B` runs before every other
  action, so `./docker.sh -B -f` snapshots before it destroys anything.

### Seeding / disaster recovery

`./docker.sh -u` creates the volume if missing (compose refuses to create an
`external: true` volume) and, **only when the volume is empty**, seeds it. A
populated volume is never overwritten. Sources, highest priority first:

1. `--seed-db PATH` or `$SEED_DB` — explicit wins.
2. Otherwise the **newest by mtime** of: `~/.local/share/scraper/data/scrapes.db`
   (the legacy pre-migration DB), `backups/backup.db`, and the newest
   `backups/scrapes-<ts>.db`.

Recency, not a fixed order, is what keeps the legacy file safe to leave on disk:
on migration day there are no snapshots so it wins, and afterwards its mtime
freezes while snapshots get fresher — so a later reseed picks the recent
snapshot instead of rolling the archive back to migration day. The seed copies
the `-wal` when non-empty, `chown -R`s the mount directory (WAL creates its
sidecars beside the DB and needs write access on the parent, or opens fail
"readonly database"), and verifies the copied byte count matches the source.

Empty volume + no seed source is a normal outcome on a fresh machine — the
service just starts a new archive, and says so.

### Destroying it

`./docker.sh -c` cannot take the volume: it is `external: true`, which makes
compose refuse to delete it even though `-c` runs `down --volumes`. `-c` prints
whether the volume survived rather than assuming. `./docker.sh -f` is the only
path that removes it, and asks you to type `DELETE`
(`SCRAPER_FORCE_CLEAN_KEEP_DB=1` skips the prompt and keeps the DB).

Recording is best-effort and never breaks or slows a scrape — it's a
sub-millisecond insert run off the event loop. **Every** scrape status is
archived (not just OK ones), but only OK results are ever served from the
cache.

### Config (`scraper.yaml`)

```yaml
store_enabled: true                  # master switch
store_db_path: ~/.local/share/scraper/data/scrapes.db
store_cache_ttl_seconds: 86400       # freshness window for opt-in cache replay; 0 disables replay
store_max_content_bytes: 2000000     # per-field cap on stored markdown/body/html (0 = no cap)
```

Any of these is also overridable via `SCRAPER_STORE_*` env (e.g.
`SCRAPER_STORE_ENABLED=false`, `SCRAPER_STORE_CACHE_TTL_SECONDS=3600`).

**In Docker, `store_db_path` is overridden and the YAML value above does not
apply.** `docker-compose.yml` sets
`SCRAPER_STORE_DB_PATH=/home/scraper/.local/share/scraper/data/db/scrapes.db`
to point the service at the named volume. The YAML default is left pointing at
the bind-mount path on purpose, so a local non-Docker run still works — the
volume only exists inside the container. Check the effective value with:

```bash
docker exec -u scraper scraper-scraper-1 printenv SCRAPER_STORE_DB_PATH
docker logs scraper-scraper-1 2>&1 | grep 'store: ENABLED'
```

`store_cache_ttl_seconds` is the freshness window for the opt-in cache: a
request with `use_cache: true` replays a stored OK result for that URL only
when it's younger than the TTL (and `> 0`). `store_max_content_bytes` caps the
stored markdown/body/html per field so one huge page can't bloat the DB.

### Boot log line

When enabled, the service logs at boot:

```
store: ENABLED — scrapes archived + searchable at /home/scraper/.local/share/scraper/data/db/scrapes.db
```

The `/data/db/` in that path is how you confirm from the logs alone that the
service is on the named volume rather than the bind mount.

### Disk retention

Three things accumulate, each swept by its own daily background task (and once
at boot). Only the first is in the named volume; the other two are on the host
bind mount:

| Path | Knob | Default |
|---|---|---|
| `data/db/scrapes.db` (named volume) | `store_retention_days` + `store_max_db_bytes` | 90 days / 2 GB |
| `images/` (mirrored images) | rides `store_retention_days` | 90 days |
| `mathpix_cache/` (converted PDF markdown) | `mathpix_cache_retention_days` | 90 days |

Retention for the two file caches is by **last use** — an entry's mtime is
refreshed every time it's referenced/served, so the window means "not wanted
for N days". `0` disables a sweep. The Mathpix window is deliberately generous:
a dropped entry costs a re-billed conversion, not just a re-fetch. Its cache key
covers the URL (or upload bytes) *and* the conversion settings, so changing
`mathpix_output_format` / `mathpix_options` re-converts instead of serving
markdown produced under the old settings — the orphaned entries age out.

A fourth thing accumulates but is **not** on a background sweep:
`~/.local/share/scraper/backups/`. Snapshots are pruned only when you run `-B`,
to `--keep N` (default 3, ~6.5 GB). If you stop taking backups, the existing
ones stay forever — deliberately, since nothing should silently delete the last
copy of the archive.

## Common failures and fixes

### `external volume "scraper_scraper_db_data" not found`

Compose refuses to create an `external: true` volume. `./docker.sh -u` creates
it for you; this error means compose was invoked directly. Fix:

```bash
docker volume create scraper_scraper_db_data   # then ./docker.sh -u
```

### `attempt to write a readonly database` after a manual volume restore

The DB file is writable but its **parent directory** is not. SQLite in WAL mode
creates `-wal`/`-shm` sidecars *next to* the DB, so it needs write permission on
the directory too. `./docker.sh -u`'s seed path handles this; a hand-rolled copy
usually doesn't:

```bash
docker run --rm -v scraper_scraper_db_data:/v alpine \
  sh -c 'chown -R 1000:1000 /v && ls -l /v'
```

### The archive looks like it lost months of data

Check you are not reading the frozen legacy copy at
`~/.local/share/scraper/data/scrapes.db`. The live DB is in the volume — see
"Where the DB lives" above. `docker logs scraper-scraper-1 | grep 'store: ENABLED'`
prints the path actually in use.

If the volume genuinely came up empty (lost/reset), `./docker.sh -u` seeds it
from the newest of the legacy DB and `backups/`; check the `-u` output, which
names the source it chose and verifies the copied byte count.

### `Connection refused` on port 8089

Service isn't running. `./docker.sh -l` will show if the container crashed.
If no container, `./docker.sh -g` to start fresh.

### Build fails at `maturin build --release`

Likely one of:

- **rustc too old** — the rust-builder stage pins a floating `rust:bookworm`
  tag. If crates bump their MSRV past what's shipped, this stage fails. Fix
  is to update `FROM rust:bookworm` (or pin a specific newer version).
- **pyo3 version mismatch** — `~/dev/rustdown/rustdown_py/Cargo.toml` pins a
  pyo3 version that must match the source code's pyo3 API. If you see
  `no method named 'detach' found for struct pyo3::Python`, bump pyo3 in
  rustdown_py's Cargo.toml and `rm ~/dev/rustdown/Cargo.lock` to regenerate.
- **rustdown workspace member missing** — the Dockerfile must COPY all three
  workspace members (`rustdown_core`, `rustdown_cli`, `rustdown_py`) even
  though maturin only builds `rustdown_py`; otherwise `cargo metadata` fails.

### `COPY ./ ...` fails with "cannot allocate memory"

Podman on macOS occasionally fails xattr reads on large staged files (e.g.
rustdown's test HTML fixtures). `docker.sh` excludes `rustdown/tests/`,
`rustdown/docs/`, and `llmvm_lite/tests/` during staging — if you see this
error on a file not already excluded, add it to `stage_sources()` in
`docker.sh`.

### Playwright timeout / Cloudflare never clears

Tune `playwright_challenge_wait_timeout_ms` upward in `scraper.yaml` (default 30000).
If a specific site is hostile, you'll need a residential proxy —
set `playwright_proxy_server` (and optional username/password).

### Kasada-protected sites (e.g. realestate.com.au) — `blocked` everywhere

Kasada (the `window.KPSDK` / `x-kpsdk-*` challenge) beats the whole chain:
httpx/Playwright `429` at navigation, and the paid proxy tier only returns the
`~800-byte` challenge JS. **ScrapingBee can't solve Kasada** (and its proxy
pools can't lend our browser a clean IP either). There's no fix wired into
this service — treat these sites as unsupported.

### Scraper returns `extraction_error`

The fetched HTML was too mangled for rustdown to parse. Confirm the fetch
actually got the article (check `fetcher_source` in the attempts trace) and
not a login wall or error page. If the real article is there but rustdown
can't convert it, file a bug against `~/dev/rustdown` with the offending
HTML.

## Running the test suite

```bash
cd ~/dev/scraper
python3 -m venv .venv   # first time only
.venv/bin/pip install -e .[test]
.venv/bin/python -m pytest tests/
```

The rustdown extractor test skips unless `rustdown_py` is importable. To run
it locally, build the wheel in `~/dev/rustdown` and `pip install` it into
`.venv` — or run the tests inside the container (they're at
`/home/scraper/scraper/tests/`).
