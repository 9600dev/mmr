"""CLI contract (D02): exit codes, --quiet, --json, fixed-cell argument validation."""
from __future__ import annotations

import json

from trader.research.lowfreq.__main__ import main
from research.conftest import entry, write_family

BENCH = entry('bench_ew', 'benchmark', 'EtfEqualWeight', module='trader.research.lowfreq.reference_strategies')


def test_cli_quiet_json_and_exit_codes(research_env, capsys):
    write_family(research_env['root'], 'fam', [BENCH, entry('spy', 'candidate', 'HoldOne', benchmarks=['bench_ew'])])
    assert main(['evaluate', 'fam/spy', '--quiet']) == 0
    out = capsys.readouterr().out.strip().splitlines()[-1]
    assert out.startswith('fam/spy: ') and out.split(': ')[1] in ('PASS', 'PROMISING', 'FAIL')
    assert main(['evaluate', 'fam/spy', '--json']) == 0
    raw = capsys.readouterr().out
    data = json.loads(raw[raw.index('{'):])
    assert data['id'] == 'spy' and 'verdict' in data
    assert main(['evaluate', 'fam/spy']) == 0
    assert capsys.readouterr().out.rstrip().splitlines()[-1].startswith('VERDICT')
    assert main(['evaluate', 'fam/unregistered', '--quiet']) == 1
    assert main(['evaluate', 'fam/spy', '--start', '2016-01-01']) == 2
    assert main(['evaluate', 'fam/spy', '--fixed-params', '{bad json']) == 2
    assert main(['evaluate', 'fam/spy', '--fixed-params', '{}', '--start', '2016-06-01', '--end', '2017-01-01', '--quiet']) == 0
    lines = (research_env['root'] / 'fam' / 'trials.jsonl').read_text().splitlines()
    assert len(lines) == 5          # 3 ok + 1 refused + 1 fixed; argument errors never reach the ledger
