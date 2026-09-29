#!/usr/bin/env python3
"""Final-artifact check for the equity family (C03, PREREG.md addenda R3/R4).

    uv run python research/lowfreq/equity/check_artifacts.py

Asserts: every registered candidate has a canonical results JSON and all of
them carry the same DSR trial count N (= distinct candidate hashes in the
ledger) and the same trial-Sharpe key set; every ok ledger row has an
artifact in results/trials/ whose embedded ledger row and metrics EQUAL the
trials.jsonl row; REPORT.md names the champion make_report's 92653bf rule
selects, states its JSON verdict, and is under 3000 words; the champion has
a reconciled D07 stress artifact whose source verdict equals the JSON's; the
candidate-role row count is <= 40. Exit 1 on any failure.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[2]))
sys.path.insert(0, str(HERE))
import make_report                                            # noqa: E402
from archive_trials import artifact_name                       # noqa: E402


def _canon(o):
    return json.loads(json.dumps(o, sort_keys=True, default=str))


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
        if r.get('verdict') != r['ledger_row'].get('verdict'):
            fails.append(f'{cid}: results verdict != embedded ledger row verdict')
    if len(ns) != 1 or (ns and next(iter(ns)) != len(hashes)):
        fails.append(f'final N differs across candidates or != distinct hashes: N={sorted(ns)} hashes={len(hashes)}')
    if len(keysets) != 1:
        fails.append(f'trial-Sharpe key sets differ across final JSONs ({len(keysets)} distinct)')
    for r in rows:
        if r.get('status') != 'ok':
            continue
        art = HERE / 'results' / 'trials' / artifact_name(r)
        if not art.exists():
            fails.append(f"no artifact for ledger row {r['timestamp']} {r['id']}"); continue
        a = json.loads(art.read_text())
        emb = _canon(a.get('ledger_row'))
        if emb != _canon(r):
            diff = [k for k in set(emb) | set(r) if emb.get(k) != _canon(r).get(k)]
            fails.append(f"artifact {art.name}: embedded ledger row differs from trials.jsonl on {sorted(diff)}")
        strip = lambda m: {k: v for k, v in _canon(m or {}).items() if v is not None}   # noqa: E731 - schema None-fill
        if strip(a.get('ledger_metrics')) != strip(r['metrics']):
            fails.append(f"artifact {art.name}: ledger_metrics differ from the trials.jsonl row's metrics")
        if a.get('verdict') != r.get('verdict') or a.get('entry_sha256') != r.get('entry_sha256'):
            fails.append(f"artifact {art.name}: verdict/entry hash differ from its ledger row")
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
            else:
                s = json.loads(stress.read_text())
                if not s.get('events_reconciled_with_backtester_diagnostics'):
                    fails.append('stress artifact not reconciled with backtester diagnostics')
                if s.get('source_verdict') != v:
                    fails.append(f"stress artifact source verdict {s.get('source_verdict')} != final verdict {v} (re-run the stress)")
                rc = s['reproduction_check']
                if abs(rc['oos_sharpe_recomputed_unstressed'] - rc['oos_sharpe_from_results_json']) > 1e-6:
                    fails.append('stress reproduction check: unstressed OOS Sharpe differs from the results JSON')
    for x in fails:
        print('FAIL', x)
    print(f'checked: {len(d["ids"])} candidates, {len(rows)} ledger rows ({n_rows} candidate-role), N={sorted(ns)}, champion={champ}; '
          f'{"ALL OK" if not fails else str(len(fails)) + " failure(s)"}')
    return 1 if fails else 0


if __name__ == '__main__':
    sys.exit(main())
