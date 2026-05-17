# MMR LLM-friendliness pass — DONE

Driven from review of llmvm_scratch_l_5ezu7i exploration logs.

## Phase 1 — Rich-out-of-data-layer ✅
- `trader/sdk.py:status()` now returns raw structured values:
  - `account_values` is a dict of `{value, currency}` per balance
  - `pnl` is a dict `{daily, unrealized, realized, total}` of floats
  - `cushion`, `leverage`, `margin_used` are typed properly
  - **No more Rich markup** like `[red]-$nan[/red]` leaking through
- `trader/mmr_cli.py` cmd=='status' formats raw values for human display

## Phase 2 — Helpers return dicts ✅
Switched from `_run_cli_json_str` (str) to `_run_cli_json` (dict):
- `strategies()`, `proposals()`
- `data_summary()`, `sweeps_list()`, `sweeps_show()`
- `backtests_list()`, `backtests_show()`, `backtests_archive/unarchive()`
- `backtests_confidence()`
No more defensive `isinstance(raw, str)` / `json.loads(raw)` needed.

## Phase 3 — New helpers ✅
- `proposal_show(id)` — wraps `proposals show <id>` for full FAILED diagnostics
- `strategy_provenance(name)` — links deployed strategy back to source backtest
  run + sweep, diffs deployed-vs-source params (catches param drift in 1 call)
- `data_freshness(stale_days, min_history_days)` — auto-detects stale,
  short_history, unresolved_conid, range_gap anomalies in DuckDB

## Phase 4 — Schema ✅
- `data_summary` records keep empty `conId` strings (so caller can see them);
  `data_freshness` is the structured equivalent for "find anomalies"
- All list helpers now return `{data: [...], title: ...}` consistently

## Phase 5 — Unit disambiguation ✅
`backtests_confidence` and `backtests_show` now expose:
- `mean_trade_pnl_ci_lo_dollars` / `_hi_dollars` (was `return_ci_lo/hi`)
- `sharpe_ratio_ci_lo_annualized` / `_hi_annualized` (was `sharpe_ci_lo/hi`)
The bare `return_ci_*` / `sharpe_ci_*` aliases are kept for back-compat
but documented as deprecated.

## Phase 6 — Filter args ✅
- `backtests list --symbol XLK` (case-insensitive ticker filter)
- Output JSON enriched with `conids`, `symbols`, `params` fields
- Symbol filter pulls full pool when set, then filters and limits — no
  empty-result-from-top-N-doesn't-include-XLK surprises
- Symbol resolution uses pre-built `conid → symbol` map (one universe scan)
  to avoid N×M DuckDB lookups that were timing out the CLI

## Files changed
- `trader/sdk.py` — status() refactor
- `trader/mmr_cli.py` — JSON enrichment, --symbol arg, CI rename
- `trader/simulation/backtest_stats.py` — (no changes; aliasing in CLI layer)
- `skills/mmr-skill/scripts/mmr_helpers.py` — return-type changes, new helpers
- `skills/mmr-skill/SKILL.md` — new helpers doc, json.loads guidance updated

## Verified end-to-end
- `data_freshness(stale_days=7)` flagged: 70 stale, 8 unresolved_conid (XLB/XLC/XLRE/XLU), 2 range_gap (GLD)
- `strategy_provenance("orb_xlk")` → run 474, sweep 4, params_match: True
- `backtests_list(symbol="XLK", sort_by="sharpe", limit=3)` → 3 records incl. run 474
- `status` --json → fully typed dict; status human → unchanged Rich table
