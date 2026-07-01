"""MMR read-only web dashboard.

Renders per-currency cash, positions + P&L, strategy state, and trade
proposals. Everything is read-only except proposal approve / reject.

Runs *inside* the mmr container: it needs the proposals DuckDB (in the
``mmr_db_data`` named volume) plus the trader_service ZMQ RPC. Launched by
pycron as ``python3 -m web.app``; listens on ``0.0.0.0:7424`` (mapped to the
host as ``127.0.0.1:7424``).

Design notes:
- The MMR SDK's RPC client uses a *synchronous* ZMQ socket guarded by its own
  internal ``threading.Lock`` (``clientserver.RPCClient``), so a single shared
  connection is safe across FastAPI's threadpool.
- Route handlers are plain ``def`` (not ``async def``) so FastAPI runs them in
  its threadpool. The SDK internally calls ``asyncio.run()``, which would raise
  if invoked from inside an already-running event loop — the threadpool avoids
  that.
- Each dashboard section fetches independently and degrades to an error banner
  if trader_service is unreachable, so a service blip never blanks the page.
  Proposals are local DuckDB and keep working even when trader_service is down.
"""
from __future__ import annotations

import logging
import os
import threading
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote

import pandas as pd
from fastapi import FastAPI, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

logger = logging.getLogger('web')

app = FastAPI(title='MMR Dashboard')
_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / 'templates'))

# States that count as "live / enabled" (mirrors strategy_runtime semantics).
_ENABLED_STATES = {'RUNNING', 'WAITING_HISTORICAL_DATA', 'INSTALLED'}

# ---------------------------------------------------------------------------
# Shared SDK connection
# ---------------------------------------------------------------------------
_mmr_lock = threading.Lock()
_mmr: Optional[Any] = None


def _get_mmr():
    global _mmr
    if _mmr is None:
        from trader.sdk import MMR
        _mmr = MMR().connect()
    return _mmr


def _reset_mmr():
    global _mmr
    try:
        if _mmr is not None and hasattr(_mmr, 'close'):
            _mmr.close()
    except Exception:
        pass
    _mmr = None


def _call(fn: Callable[[Any], Any], *, retry: bool = True):
    """Run an SDK op under the shared lock, reconnecting once on drop."""
    with _mmr_lock:
        try:
            return fn(_get_mmr())
        except (ConnectionError, TimeoutError):
            if not retry:
                raise
            _reset_mmr()
            return fn(_get_mmr())


# ---------------------------------------------------------------------------
# Fetchers — each converts SDK output to plain JSON-friendly structures
# ---------------------------------------------------------------------------
def _records(df) -> list[dict]:
    """DataFrame -> list of clean dicts (NaN -> None, numpy scalars -> native)."""
    if df is None or not hasattr(df, 'to_dict') or getattr(df, 'empty', True):
        return []
    out = []
    for rec in df.to_dict('records'):
        clean: dict = {}
        for k, v in rec.items():
            if isinstance(v, float) and pd.isna(v):
                clean[k] = None
            elif hasattr(v, 'item') and not isinstance(v, (list, tuple)):
                try:
                    clean[k] = v.item()
                except Exception:
                    clean[k] = v
            else:
                clean[k] = v
        out.append(clean)
    return out


def fetch_cash() -> Optional[dict]:
    return _call(lambda m: m.account_cash())


def fetch_snapshot() -> Optional[dict]:
    return _call(lambda m: m.portfolio_snapshot())


def fetch_positions() -> list[dict]:
    return _records(_call(lambda m: m.portfolio()))


def fetch_strategies() -> list[dict]:
    rows = _records(_call(lambda m: m.strategies()))
    for r in rows:
        state = str(r.get('state') or '').upper()
        r['enabled'] = state in _ENABLED_STATES
        if isinstance(r.get('conids'), (list, tuple)):
            r['conids'] = ', '.join(str(c) for c in r['conids'])
    return rows


def fetch_proposals() -> list[dict]:
    # No status filter -> every proposal, with a 'status' column.
    return _records(_call(lambda m: m.proposals(limit=100)))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get('/healthz')
def healthz():
    return {'ok': True}


@app.get('/')
def dashboard(request: Request, flash: str = ''):
    sections: dict[str, Any] = {}
    errors: dict[str, str] = {}
    fetchers: dict[str, Callable[[], Any]] = {
        'cash': fetch_cash,
        'snapshot': fetch_snapshot,
        'positions': fetch_positions,
        'strategies': fetch_strategies,
        'proposals': fetch_proposals,
    }
    for key, fn in fetchers.items():
        try:
            sections[key] = fn()
        except Exception as exc:  # noqa: BLE001 - surface, don't crash the page
            logger.warning('dashboard section %s failed: %s', key, exc)
            sections[key] = None
            errors[key] = f'{type(exc).__name__}: {exc}'

    strategies = sections.get('strategies') or []
    return _TEMPLATES.TemplateResponse(request, 'dashboard.html', {
        'cash': sections.get('cash'),
        'snapshot': sections.get('snapshot'),
        'positions': sections.get('positions') or [],
        'strategies': strategies,
        'enabled_count': sum(1 for s in strategies if s.get('enabled')),
        'proposals': sections.get('proposals') or [],
        'errors': errors,
        'flash': flash,
    })


@app.post('/proposals/{pid}/approve')
def approve(pid: int):
    """Approve a proposal — this PLACES A LIVE ORDER via trader_service."""
    try:
        result = _call(lambda m: m.approve(pid), retry=False)
        ok = getattr(result, 'success', None)
        if ok is False:
            msg = f'#{pid} approve failed: {getattr(result, "error", "unknown error")}'
        else:
            msg = f'#{pid} approved & submitted'
    except Exception as exc:  # noqa: BLE001
        logger.warning('approve #%s failed: %s', pid, exc)
        msg = f'#{pid} approve error: {type(exc).__name__}: {exc}'
    return RedirectResponse(url=f'/?flash={quote(msg)}', status_code=303)


@app.post('/proposals/{pid}/reject')
def reject(pid: int, reason: str = Form('')):
    try:
        ok = _call(lambda m: m.reject(pid, reason), retry=False)
        msg = f'#{pid} rejected' if ok else f'#{pid} not rejected (not pending?)'
    except Exception as exc:  # noqa: BLE001
        logger.warning('reject #%s failed: %s', pid, exc)
        msg = f'#{pid} reject error: {type(exc).__name__}: {exc}'
    return RedirectResponse(url=f'/?flash={quote(msg)}', status_code=303)


def main():
    import uvicorn
    port = int(os.environ.get('WEB_PORT', '7424'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )
    logger.info('MMR dashboard starting on 0.0.0.0:%d', port)
    uvicorn.run(app, host='0.0.0.0', port=port, log_level='info')


if __name__ == '__main__':
    main()
