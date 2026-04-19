# Operating the news service — docker.sh, logs, config

## Starting / stopping

All lifecycle goes through `~/dev/news/docker.sh`. Auto-detects docker vs podman.

```bash
cd ~/dev/news

./docker.sh -g     # go: build + up + tail logs (most common dev flow)
./docker.sh -b     # build image only (stages sibling ~/dev/llmvm_lite + ~/dev/rustdown)
./docker.sh -u     # start existing image
./docker.sh -d     # stop and remove containers
./docker.sh -l     # tail logs of running container
./docker.sh -e     # exec a bash shell inside the running container
./docker.sh -s     # rsync local code changes into the running container (no rebuild)
./docker.sh -a     # sync_all — same as -s but without .gitignore filter
./docker.sh -c     # clean: remove images + volumes (keeps build cache)
./docker.sh -f     # force clean: also prune build cache
```

The `-b` step is the expensive one — it Rust-compiles the rustdown wheel,
downloads the Playwright Chromium binary (~170MB), and installs llmvm_lite +
dependencies. Subsequent `-b` runs reuse:

- Cargo registry/git/target caches (BuildKit cache mounts) — saves ~5min of Rust compile.
- pip's `/home/news/.cache/pip` (BuildKit cache mount) — saves Python dep redownload.
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

Container logs stream from uvicorn + the `news` Python logger.

```bash
./docker.sh -l         # tail -f from docker compose logs
```

A persistent file log also lands at `/home/news/.local/share/news/logs/news_<timestamp>.log`
inside the container — mirrored to the host at `~/.local/share/news/logs/` via bind mount.

## Configuration

Two tiers, in override order:

1. **`~/.config/news/news.yaml`** — bind-mounted from host; survives container restarts.
   Seeded from `configs/news.yaml` on first start.
2. **`NEWS_*` environment variables** — any flat-key in the config can be overridden
   (e.g. `NEWS_HTTP_PORT=9000`, `NEWS_LLM_ENABLED=true`, `NEWS_PLAYWRIGHT_HEADLESS=false`).

To change a setting permanently, edit `~/.config/news/news.yaml`. Restart with
`./docker.sh -d && ./docker.sh -u`.

### Common config changes

```yaml
# Make Playwright visible (for debugging) — only useful outside Docker
playwright_headless: false

# Route through a residential proxy
playwright_proxy_server: "http://user:pass@proxy.example.com:8000"

# Turn on LLM features (requires ANTHROPIC_API_KEY in env)
llm_enabled: true
llm_model_name: claude-opus-4-7

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
| `SERPAPI_API_KEY` | SerpAPI — unlocks Google / Google News search (`/v1/search?engine=google` or `engine=google_news`). When set, `engine=auto` prefers Google over DDG. |

Each is optional. llmvm picks which one to read based on the model name
you set in `news.yaml` (`llm_model_name` — `claude-*` uses Anthropic, `gpt-*`
uses OpenAI, etc.). The service itself defaults to Claude.

To enable LLM features:

1. Confirm the relevant key is exported in your shell (e.g. `echo $ANT_API_KEY`).
2. Set `llm_enabled: true` in `~/.config/news/news.yaml`.
3. Optionally set `llm_model_name: gpt-5` or similar to switch providers.
4. `./docker.sh -d && ./docker.sh -u`.

`/v1/health`'s `llm_enabled` field flips to `true` when LLM features are on.
A failed call inside the container means the key wasn't forwarded —
`./docker.sh -e` then `env | grep -E '_API_KEY|_TOKEN'` confirms what
actually made it in.

## Common failures and fixes

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

### `useradd: user 'news' already exists`

Debian ships a legacy NNTP `news` system user (uid 9). The Dockerfile deletes
it before creating our `news` user. If you see this error, the `userdel`
step got skipped — verify the Dockerfile still has the `userdel -f news`
line before `useradd`.

### `COPY ./ ...` fails with "cannot allocate memory"

Podman on macOS occasionally fails xattr reads on large staged files (e.g.
rustdown's test HTML fixtures). `docker.sh` excludes `rustdown/tests/`,
`rustdown/docs/`, and `llmvm_lite/tests/` during staging — if you see this
error on a file not already excluded, add it to `stage_sources()` in
`docker.sh`.

### Playwright timeout / Cloudflare never clears

Tune `playwright_challenge_wait_timeout_ms` upward in `news.yaml` (default 30000).
If a specific site is hostile, you'll need a residential proxy —
set `playwright_proxy_server` (and optional username/password).

### Scraper returns `extraction_error`

The fetched HTML was too mangled for rustdown to parse. Confirm the fetch
actually got the article (check `fetcher_source` in the attempts trace) and
not a login wall or error page. If the real article is there but rustdown
can't convert it, file a bug against `~/dev/rustdown` with the offending
HTML.

## Running the test suite

```bash
cd ~/dev/news
python3 -m venv .venv   # first time only
.venv/bin/pip install -e .[test]
.venv/bin/python -m pytest tests/
```

The rustdown extractor test skips unless `rustdown_py` is importable. To run
it locally, build the wheel in `~/dev/rustdown` and `pip install` it into
`.venv` — or run the tests inside the container (they're at
`/home/news/news/tests/`).
