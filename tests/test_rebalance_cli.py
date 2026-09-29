"""CLI: every refusal exits non-zero and creates nothing; plan/propose/status render and --json parses."""
from __future__ import annotations

import io
import json

import pytest

from rebalance_fakes import AS_OF, FakeGateway, executed, make_doc, write_targets
from trader.rebalance.__main__ import main
from trader.rebalance.plan import PositionLimits

PRICES = {'AAPL': 200.0, 'MSFT': 400.0, 'TSLA': 250.0}
TODAY = ['--today', '2026-09-29']


def _run(argv, gw, limits=PositionLimits()):
    out = io.StringIO()
    code = main(argv, gateway_factory=lambda: gw, limits=limits, out=out)
    return code, out.getvalue()


def test_plan_prints_the_full_order_list_and_creates_nothing(tmp_path, capsys):
    path = write_targets(tmp_path)
    gw = FakeGateway(prices=PRICES, positions={'TSLA': 10})
    code, out = _run(['plan', str(path), *TODAY], gw)
    assert code == 0
    assert 'BUY' in out and 'AAPL' in out and '25' in out and 'MSFT' in out
    assert 'expected cash after execution $2,200.00' in out
    assert 'TSLA' not in out                                    # outside position untouched, not even mentioned
    assert gw.propose_calls == [] and gw.closed


def test_plan_json_is_machine_readable(tmp_path):
    path = write_targets(tmp_path)
    gw = FakeGateway(prices=PRICES)
    code, out = _run(['plan', str(path), *TODAY, '--json'], gw)
    doc = json.loads(out)
    assert code == 0 and doc['sleeve'] == 's2p' and doc['group'] == 'sleeve_s2p'
    assert [(o['action'], o['symbol'], o['quantity']) for o in doc['orders']] == [('BUY', 'AAPL', 25), ('BUY', 'MSFT', 7)]
    assert doc['cash_after'] == pytest.approx(2_200.0) and doc['source'] == f'rebalance:s2p:{AS_OF}'


def test_plan_flags_orders_a_position_limit_would_refuse(tmp_path):
    path = write_targets(tmp_path)
    code, out = _run(['plan', str(path), *TODAY], FakeGateway(prices=PRICES), limits=PositionLimits(max_position_usd=3_000))
    assert code == 0 and 'POSITION LIMIT would refuse' in out and 'AAPL' in out


@pytest.mark.parametrize('doc_over, gw_kwargs, expect_code, needle', [
    ({'as_of': '2026-09-18', 'prices_as_of': '2026-09-18'}, {}, 1, 'STALE_TARGETS'),
    ({'weights': {'AAPL': 0.6, 'MSFT': 0.5}}, {}, 2, 'sum'),
    ({}, {'prices': {'AAPL': 200.0}}, 1, 'UNPRICEABLE'),
    ({'weights': {'MSFT': 1.0}}, {'proposals': [executed(1, 'AAPL', 'BUY', 50)], 'positions': {'AAPL': 50}}, 1, 'GROSS_EXCEEDS_CAPITAL'),
    ({}, {'proposals': [executed(9, 'AAPL', 'BUY', 25, source=f'rebalance:s2p:{AS_OF}', status='PENDING')]}, 1, 'DUPLICATE_BATCH'),
    ({}, {'fail_positions': True}, 1, 'IB_UNREADABLE'),
])
def test_every_refusal_exits_nonzero_and_creates_nothing(tmp_path, capsys, doc_over, gw_kwargs, expect_code, needle):
    path = write_targets(tmp_path, make_doc(**doc_over))
    kwargs = {'prices': PRICES, **gw_kwargs}
    gw = FakeGateway(**kwargs)
    code, out = _run(['propose', str(path), *TODAY], gw)
    err = capsys.readouterr().err
    assert code == expect_code and code != 0
    assert needle in err
    assert gw.propose_calls == [] and not gw.groups


def test_propose_creates_pending_proposals_and_prints_ids(tmp_path):
    path = write_targets(tmp_path)
    gw = FakeGateway(prices=PRICES)
    code, out = _run(['propose', str(path), *TODAY], gw)
    assert code == 0
    assert 'PROPOSED 2 PENDING proposal(s) complete' in out and '#101' in out and '#102' in out
    assert 'mmr approve <id>' in out                         # approval is the operator's act, by hand
    assert [(c['symbol'], c['quantity']) for c in gw.propose_calls] == [('AAPL', 25.0), ('MSFT', 7.0)]
    assert all(p.status == 'PENDING' for p in gw.proposals)


def test_propose_json_carries_ids(tmp_path):
    path = write_targets(tmp_path)
    code, out = _run(['propose', str(path), *TODAY, '--json'], FakeGateway(prices=PRICES))
    doc = json.loads(out)
    assert code == 0 and doc['proposal_ids'] == [101, 102] and doc['complete'] is True


def test_sells_only_flag_splits_a_transition(tmp_path):
    path = write_targets(tmp_path, make_doc(weights={'MSFT': 1.0}))
    gw = FakeGateway(prices=PRICES, proposals=[executed(1, 'AAPL', 'BUY', 50)], positions={'AAPL': 50})
    code, out = _run(['plan', str(path), *TODAY, '--sells-only'], gw)
    assert code == 0 and 'SELL' in out and 'sells-only run' in out


def test_missing_targets_file_is_a_usage_error(tmp_path, capsys):
    gw = FakeGateway(prices=PRICES)
    code, _ = _run(['plan', str(tmp_path / 'nope.json'), *TODAY], gw)
    assert code == 2 and 'not found' in capsys.readouterr().err and gw.calls == []


def test_status_shows_holdings_value_open_proposals_and_drift(tmp_path):
    write_targets(tmp_path, sleeve_dir=True)
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 20),
                                executed(5, 'MSFT', 'BUY', 7, source=f'rebalance:s2p:{AS_OF}', status='PENDING')],
                     positions={'AAPL': 20, 'TSLA': 3}, prices=PRICES)
    code, out = _run(['status', 's2p', '--targets-dir', str(tmp_path)], gw)
    assert code == 0
    assert 'market value $4,000.00' in out
    assert 'AAPL' in out and '-5' in out                         # holds 20, target 25 -> drift -5
    assert '#5 BUY 7 MSFT [PENDING]' in out
    assert 'TSLA' not in out
    code, out = _run(['status', 's2p', '--targets-dir', str(tmp_path), '--json'], gw)
    doc = json.loads(out)
    assert code == 0 and doc['market_value'] == 4_000.0 and doc['targets_as_of'] == AS_OF
    drift = {h['symbol']: h['drift_shares'] for h in doc['holdings']}
    assert drift == {'AAPL': -5, 'MSFT': -7}
    assert doc['gross_drift_value'] == pytest.approx(5 * 200 + 7 * 400)
    assert gw.propose_calls == []


def test_status_without_targets_still_reports_holdings(tmp_path):
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 20)], positions={'AAPL': 20}, prices=PRICES)
    code, out = _run(['status', 's2p', '--targets-dir', str(tmp_path)], gw)
    assert code == 0 and 'latest targets: none found' in out and 'AAPL' in out


def test_status_refuses_a_targets_file_for_another_sleeve(tmp_path, capsys):
    path = write_targets(tmp_path, make_doc(sleeve='other'))
    code, _ = _run(['status', 's2p', '--targets-file', str(path)], FakeGateway(prices=PRICES))
    assert code == 1 and 'WRONG_SLEEVE' in capsys.readouterr().err


def test_module_is_runnable_as_python_dash_m(tmp_path):
    import subprocess, sys
    proc = subprocess.run([sys.executable, '-m', 'trader.rebalance', '--help'], capture_output=True, text=True)
    assert proc.returncode == 0 and 'plan' in proc.stdout and 'propose' in proc.stdout and 'status' in proc.stdout
