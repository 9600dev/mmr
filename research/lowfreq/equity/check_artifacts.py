#!/usr/bin/env python3
"""Final-artifact check for the equity family (C03, PREREG.md addendum R3).

    uv run python research/lowfreq/equity/check_artifacts.py

Asserts: every registered candidate has a canonical results JSON and all of
them carry the same DSR trial count N (= distinct candidate hashes in the
ledger) and the same trial-Sharpe key set; every ok ledger row has an
artifact in results/trials/; REPORT.md names the champion make_report's rule
selects, states its JSON verdict, and is under 3000 words; the candidate-role
row count is <= 40. Exit 1 on any failure.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
sys.path.insert(0, str(HERE))
from trader.research.lowfreq import ledger                     # noqa: E402
import make_report                                            # noqa: E402
from archive_trials import artifact_name                       # noqa: E402


def main() -> int:
    d = make_report.load()
    rows = d['rows']
    fails = []
    hashes = {r['entry_sha256'] for r in rows if r.get('role') == 'candidate' and r.get('entry_sha256')}
    n_rows = sum(1 for r in rows if r.get('role') == 'candidate')
    if n_rows > 40:
        fails.append(f'{n_rows} candidate-role ledger rows > 40')
    ns, keysets = set(), set()
    for cid in d['ids']:
        r = d['results'].get(cid)
        if r is None:
            fails.append(f'{cid}: no canonical results JSON'); continue
        ns.add(r['dsr']['n_trials'])
        keysets.add(frozenset(r['dsr']['trial_sharpes']))
    if len(ns) != 1 or (ns and next(iter(ns)) != len(hashes)):
        fails.append(f'final N differs across candidates or != distinct hashes: N={sorted(ns)} hashes={len(hashes)}')
    if len(keysets) != 1:
        fails.append(f'trial-Sharpe key sets differ across final JSONs ({len(keysets)} distinct)')
    for r in rows:
        if r.get('status') == 'ok' and not (HERE / 'results' / 'trials' / artifact_name(r)).exists():
            fails.append(f"no artifact for ledger row {r['timestamp']} {r['id']}")
    champ, _ = make_report.choose_champion(d['ids'], d['results'])
    rep = HERE / 'REPORT.md'
    if not rep.exists():
        fails.append('REPORT.md missing')
    else:
        text = rep.read_text()
        words = len(text.split())
        if words >= 3000:
            fails.append(f'REPORT.md has {words} words (limit 3000)')
        if champ is None:
            if 'No eligible champion' not in text:
                fails.append('rule selects no champion but REPORT.md does not say so')
        else:
            v = d['results'][champ]['verdict']
            m = re.search(r'Champion by the pre-registered rule: `([a-z0-9_]+)`.*?harness verdict `([A-Z]+)`', text)
            if not m or m.group(1) != champ or m.group(2) != v:
                fails.append(f'REPORT.md champion/verdict ({m.groups() if m else None}) != rule ({champ}, {v})')
            stress = HERE / 'results' / f'{champ}__d07_stress.json'
            if not stress.exists():
                fails.append(f'no D07 stress artifact for champion {champ}')
            elif not json.loads(stress.read_text()).get('events_reconciled_with_backtester_diagnostics'):
                fails.append('stress artifact not reconciled with backtester diagnostics')
    for x in fails:
        print('FAIL', x)
    print(f'checked: {len(d["ids"])} candidates, {len(rows)} ledger rows ({n_rows} candidate-role), N={sorted(ns)}, champion={champ}; '
          f'{"ALL OK" if not fails else str(len(fails)) + " failure(s)"}')
    return 1 if fails else 0


if __name__ == '__main__':
    sys.exit(main())
