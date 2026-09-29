"""Fixtures for the lowfreq harness tests: a temporary research DuckDB pair,
a temporary LOWFREQ_RESEARCH_CONFIG naming ONLY them, and a temporary family
root - so no test can touch the live or dev databases (D03)."""
from __future__ import annotations

import os
import tempfile

os.environ.setdefault('MMR_LOG_DIR', os.path.join(tempfile.gettempdir(), 'mmr-pytest-logs'))

import datetime as dt          # noqa: E402
import sys                     # noqa: E402
from pathlib import Path       # noqa: E402

_REPO = str(Path(__file__).resolve().parents[2])
if _REPO not in sys.path:      # `tests.research.*` must import as a package from the repo root
    sys.path.insert(0, _REPO)

import duckdb                  # noqa: E402
import numpy as np             # noqa: E402
import pandas as pd            # noqa: E402
import pytest                  # noqa: E402
import yaml                    # noqa: E402

TICKERS = ('SPY', 'IEF', 'GLD', 'BIL')


def make_etf_rows(n_days: int = 700, seed: int = 0, drift: dict | None = None, start='2015-01-01'):
    """Synthetic etf_daily_bars rows: lognormal closes, open = prior close, a
    small dividend stream so tr_close != close, GLD listed 200 days late."""
    rng = np.random.default_rng(seed)
    days = pd.bdate_range(start, periods=n_days)
    rows = []
    for k, t in enumerate(TICKERS):
        mu = (drift or {}).get(t, 0.0) / 252.0
        rets = rng.normal(mu, 0.01 if t != 'BIL' else 0.0002, n_days)
        close = 100.0 * (1 + k) * np.cumprod(1 + rets)
        first = 200 if t == 'GLD' else 0
        tr = 1.0
        for i in range(first, n_days):
            div = 0.5 if (i % 63 == 62 and t != 'GLD') else 0.0
            if i > first:
                tr *= (close[i] + div) / close[i - 1]
            opn = close[i - 1] if i > 0 else close[i]
            tr_close = 100.0 * tr
            tr_open = tr_close * opn / close[i]
            rows.append((days[i].date(), t, float(opn), float(max(opn, close[i]) * 1.001),
                         float(min(opn, close[i]) * 0.999), float(close[i]), 1e6, div,
                         float(tr_close), float(tr_open), float(close[i])))
    return rows


@pytest.fixture
def research_env(tmp_path, monkeypatch):
    """Temp history + main DuckDB, config YAML, family root. Yields the paths."""
    hist = tmp_path / 'hist.duckdb'
    main = tmp_path / 'main.duckdb'
    con = duckdb.connect(str(hist))
    con.execute("""CREATE TABLE etf_daily_bars(day DATE, ticker VARCHAR, open DOUBLE, high DOUBLE,
                   low DOUBLE, close DOUBLE, volume DOUBLE, dividend DOUBLE, tr_close DOUBLE,
                   tr_open DOUBLE, td_adj_close DOUBLE, PRIMARY KEY(day, ticker))""")
    con.executemany('INSERT INTO etf_daily_bars VALUES (?,?,?,?,?,?,?,?,?,?,?)', make_etf_rows())
    con.execute("""CREATE TABLE pit_daily_bars(day DATE, ticker VARCHAR, open DOUBLE, high DOUBLE,
                   low DOUBLE, close DOUBLE, volume DOUBLE, dollar_volume DOUBLE, dv_rank INTEGER,
                   PRIMARY KEY(day, ticker))""")
    con.close()
    con = duckdb.connect(str(main))
    con.execute("""CREATE TABLE fundamentals(conid VARCHAR, ticker VARCHAR, period_end DATE,
                   accepted_at TIMESTAMPTZ, form_type VARCHAR, accession_no VARCHAR, revenue DOUBLE,
                   net_income DOUBLE, gross_profit DOUBLE, operating_income DOUBLE, total_assets DOUBLE,
                   total_liabilities DOUBLE, stockholders_equity DOUBLE, cash_from_operations DOUBLE,
                   shares_diluted DOUBLE, fetched_at TIMESTAMPTZ)""")
    con.close()
    cfg = tmp_path / 'trader_research.yaml'
    cfg.write_text(yaml.safe_dump({'duckdb_path': str(main), 'history_duckdb_path': str(hist),
                                   'strategies_directory': 'strategies'}))
    root = tmp_path / 'lowfreq'
    root.mkdir()
    monkeypatch.setenv('LOWFREQ_RESEARCH_CONFIG', str(cfg))
    monkeypatch.setenv('LOWFREQ_RESEARCH_ROOT', str(root))
    return {'hist': hist, 'main': main, 'config': cfg, 'root': root, 'tmp': tmp_path}


def write_family(root: Path, name: str, candidates: list, **over) -> Path:
    fam = {'family': name, 'universe': 'etf', 'dev_period': {'start': '2015-06-01', 'end': '2017-08-01'},
           'walk_forward': {'train_days': 365, 'test_days': 182}, 'warmup_days': 40}
    fam.update(over)
    fam['candidates'] = candidates
    d = root / name
    d.mkdir(parents=True, exist_ok=True)
    (d / 'candidates.yaml').write_text(yaml.safe_dump(fam, sort_keys=False))
    return d


def entry(cid, role, cls, module='tests/research/synthetic_strategies.py', params=None, benchmarks=(), **kw):
    e = {'id': cid, 'role': role, 'hypothesis': f'{cid} hypothesis', 'rationale': f'{cid} rationale',
         'strategy': {'module': module, 'class': cls}, 'params': params or {},
         'benchmarks': list(benchmarks), 'registered_at': '2026-09-28'}
    e.update(kw)
    return e
