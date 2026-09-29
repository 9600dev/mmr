#!/usr/bin/env python3
"""Provenance check for the equity family (C03, PREREG.md addendum R3).

    uv run python research/lowfreq/equity/provenance.py [--record]

``--record`` writes PROVENANCE.json (sha256 of PREREG.md and candidates.yaml,
UTC timestamp) - run once, BEFORE evaluations. Without it, the script
verifies: (1) PROVENANCE.json hashes still match the files; (2) for every
ledger row with an entry_sha256, the CURRENT registry entry hashes to it;
(3) for every row with a strategy_sha256, the CURRENT module source hashes
to it. Exit 1 on any mismatch - the committed files are then not the ones
that ran.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
from trader.research.lowfreq import engine, ledger                       # noqa: E402
from trader.research.lowfreq.registry import entry_sha256, load_family   # noqa: E402


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main(argv) -> int:
    files = {'PREREG.md': sha(HERE / 'PREREG.md'), 'candidates.yaml': sha(HERE / 'candidates.yaml')}
    prov = HERE / 'PROVENANCE.json'
    if '--record' in argv:
        rows = ledger.read(HERE / 'trials.jsonl')
        rec = {'recorded_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'sha256': files,
               'ledger_rows_at_record': len(rows), 'last_ledger_timestamp_at_record': rows[-1]['timestamp'] if rows else None,
               'note': 'sha256 of the pre-registration files at the instant before the R3 evaluations (PREREG addendum R3)'}
        prov.write_text(json.dumps(rec, indent=1))
        print('recorded', rec)
        return 0
    bad = 0
    if prov.exists():
        rec = json.loads(prov.read_text())
        for k, v in rec['sha256'].items():
            ok = files[k] == v
            bad += not ok
            print(f"{'ok ' if ok else 'BAD'} PROVENANCE.json {k} {v[:12]}  (recorded {rec['recorded_at_utc'][:19]}, ledger rows then {rec['ledger_rows_at_record']})")
    fam = load_family('equity', HERE.parent)
    rows = ledger.read(HERE / 'trials.jsonl')
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
        print(f"{'ok ' if e_ok and s_ok else 'BAD'} {r['timestamp'][:19]} {cid:<24} entry={'match' if e_ok else 'MISMATCH'} strategy={'match' if s_ok else 'MISMATCH'} git={str(r.get('git_sha'))[:7]}")
    print('MISMATCHES:', bad)
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
