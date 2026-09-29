#!/usr/bin/env python3
"""Preserve one immutable results artifact per ledger row (C03).

    uv run python research/lowfreq/equity/archive_trials.py

``evaluate`` overwrites ``results/<id>.json`` on every run, so a pass-1 row's
metrics would stop matching any file once pass 2 runs. For every ``ok`` ledger
row whose canonical results file still carries that row's timestamp, this
copies it to ``results/trials/<id>__<timestamp>.json`` (never overwritten).
Idempotent; prints the rows that have no artifact (their file was already
replaced before this ran).
"""
from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from trader.research.lowfreq import ledger   # noqa: E402


def artifact_name(row: dict) -> str:
    ts = str(row['timestamp']).replace('-', '').replace(':', '').replace('+0000', '').replace('+00:00', '')
    return f"{row['id']}__{ts}.json"


def main() -> int:
    rows = ledger.read(HERE / 'trials.jsonl')
    out = HERE / 'results' / 'trials'
    out.mkdir(parents=True, exist_ok=True)
    missing = []
    for r in rows:
        if r.get('status') != 'ok':
            continue
        dst = out / artifact_name(r)
        if dst.exists():
            continue
        src = Path(r['results_path']) if r.get('results_path') else None
        if src is None or not src.exists():
            missing.append(r); continue
        data = json.loads(src.read_text())
        if data.get('ledger_row', {}).get('timestamp') != r['timestamp']:
            missing.append(r); continue
        shutil.copyfile(src, dst)
        print('archived', dst.name)
    for r in missing:
        print('NO ARTIFACT for ledger row', r['timestamp'], r['id'], '(canonical file already replaced)')
    print(f'{len(list(out.glob("*.json")))} artifacts for {sum(1 for r in rows if r.get("status") == "ok")} ok rows')
    return 1 if missing else 0


if __name__ == '__main__':
    sys.exit(main())
