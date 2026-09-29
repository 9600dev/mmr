#!/usr/bin/env python3
"""Provenance check for the equity family (C03, PREREG.md addenda R3/R4).

    uv run python research/lowfreq/equity/provenance.py [--record --note "..."]

``--record`` APPENDS a record to PROVENANCE.json (sha256 of PREREG.md and
candidates.yaml, UTC timestamp, ledger rows and last ledger timestamp at that
instant). Without it, the script verifies: (1) the LATEST record's hashes
match the current files (older records are the history of the file); (2)
every record's ledger state is still a prefix of the append-only ledger (the
row at its recorded count carries its recorded timestamp); (3) for every
ledger row with an entry_sha256, the CURRENT registry entry hashes to it;
(4) for every row with a strategy_sha256, the CURRENT module source hashes to
it. Exit 1 on any mismatch - the committed files are then not the ones that
ran, or a record was taken against a ledger that has since been rewritten.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from trader.research.lowfreq import engine, ledger                       # noqa: E402
from trader.research.lowfreq.registry import entry_sha256, load_family   # noqa: E402

FILES = ('PREREG.md', 'candidates.yaml')


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_records(prov: Path) -> List[Dict[str, Any]]:
    if not prov.exists():
        return []
    data = json.loads(prov.read_text())
    if isinstance(data, dict) and 'records' in data:
        return list(data['records'])
    return [data]                                     # the original single-record format (R3)


def main(argv) -> int:
    files = {k: sha(HERE / k) for k in FILES}
    prov = HERE / 'PROVENANCE.json'
    rows = ledger.read(HERE / 'trials.jsonl')
    records = read_records(prov)
    if '--record' in argv:
        note = argv[argv.index('--note') + 1] if '--note' in argv else ''
        rec = {'recorded_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'sha256': files,
               'ledger_rows_at_record': len(rows), 'last_ledger_timestamp_at_record': rows[-1]['timestamp'] if rows else None,
               'note': note}
        records.append(rec)
        prov.write_text(json.dumps({'records': records}, indent=1))
        print('recorded', rec)
        return 0
    bad = 0
    for i, rec in enumerate(records):
        n = int(rec['ledger_rows_at_record'])
        last = rec.get('last_ledger_timestamp_at_record')
        prefix_ok = n <= len(rows) and (n == 0 or rows[n - 1]['timestamp'] == last)
        bad += not prefix_ok
        print(f"{'ok ' if prefix_ok else 'BAD'} record {i + 1}/{len(records)} at {rec['recorded_at_utc'][:19]} ledger rows {n} "
              f"(last row {str(last)[:19]}) prefix-of-ledger={'yes' if prefix_ok else 'NO'} :: {rec.get('note', '')[:70]}")
        for k, v in rec['sha256'].items():
            cur = files[k] == v
            if i == len(records) - 1:
                bad += not cur
            print(f"    {'ok ' if cur else ('BAD' if i == len(records) - 1 else 'old')} {k} {v[:12]}"
                  f"{'' if cur else ' (differs from the current file' + (')' if i < len(records) - 1 else ' - LATEST record must match)')}")
    fam = load_family('equity', HERE.parent)
    src_sha = {}
    for cid, e in fam.entries.items():
        cls = engine.load_strategy_class(e['strategy']['module'], e['strategy']['class'])
        src_sha[cid] = engine.strategy_source_sha256(cls)
    for r in rows:
        cid = r['id']
        e = fam.entries.get(cid)
        if e is None:
            print('BAD', r['timestamp'][:19], cid, 'not in registry'); bad += 1; continue
        e_ok = r.get('entry_sha256') == entry_sha256(e)
        s_ok = r.get('strategy_sha256') in (None, src_sha[cid])
        bad += (not e_ok) + (not s_ok)
        print(f"{'ok ' if e_ok and s_ok else 'BAD'} {r['timestamp'][:19]} {cid:<24} entry={'match' if e_ok else 'MISMATCH'} "
              f"strategy={'match' if s_ok else 'MISMATCH'} git={str(r.get('git_sha'))[:7]}")
    print('MISMATCHES:', bad)
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
