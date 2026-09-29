"""CLI: every refusal exits non-zero and creates nothing; plan/propose/status/reconcile render and --json parses."""
from __future__ import annotations

import io
import json

import pytest

from rebalance_fakes import AS_OF, FakeGateway, evidence, executed, filled_order, held, make_doc, observed, write_targets
from trader.rebalance.__main__ import load_limits, main
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
    assert 'post-execution gross exposure $7,800.00 of capital $10,000.00' in out
    assert 'TSLA' not in out                                    # outside position untouched, not even mentioned
    assert gw.propose_calls == [] and gw.recorded == [] and gw.closed


def test_plan_json_is_machine_readable(tmp_path):
    path = write_targets(tmp_path)
    gw = FakeGateway(prices=PRICES)
    code, out = _run(['plan', str(path), *TODAY, '--json'], gw)
    doc = json.loads(out)
    assert code == 0 and doc['sleeve'] == 's2p' and doc['group'] == 'sleeve_s2p'
    assert [(o['action'], o['symbol'], o['quantity']) for o in doc['orders']] == [('BUY', 'AAPL', 25), ('BUY', 'MSFT', 7)]
    assert doc['cash_after'] == pytest.approx(2_200.0) and doc['source'] == f'rebalance:s2p:{AS_OF}'
    assert doc['gross_exposure_after'] == pytest.approx(7_800.0) and doc['turnover_notional'] == pytest.approx(7_800.0)


def test_plan_flags_orders_a_position_limit_would_refuse(tmp_path):
    path = write_targets(tmp_path)
    limits = PositionLimits(max_position_size_pct=0.10, net_liquidation=40_000)
    code, out = _run(['plan', str(path), *TODAY], FakeGateway(prices=PRICES), limits=limits)
    assert code == 0 and 'POSITION LIMIT would refuse at approval time' in out
    assert 'BUY 25 AAPL: position $5,000 = 12.5% of net liquidation $40,000 > max_position_size_pct 10%' in out


def test_production_limits_come_from_trader_yaml_and_the_live_net_liquidation(tmp_path):
    """The production path (limits=None): risk_limits from --config, NetLiquidation from the gateway."""
    cfg = tmp_path / 'trader.yaml'
    cfg.write_text('risk_limits:\n  max_position_size_pct: 0.10\n')
    gw = FakeGateway(prices=PRICES, net_liquidation=40_000)
    assert load_limits(gw, str(cfg)) == PositionLimits(max_position_size_pct=0.10, net_liquidation=40_000)
    path = write_targets(tmp_path)
    out = io.StringIO()
    code = main(['plan', str(path), *TODAY, '--config', str(cfg)], gateway_factory=lambda: gw, out=out)
    assert code == 0
    assert ('account_net_liquidation', {}) in gw.calls
    assert 'BUY 25 AAPL: position $5,000 = 12.5% of net liquidation $40,000 > max_position_size_pct 10%' in out.getvalue()
    # A more permissive cap in the config clears the flag through the same path.
    cfg.write_text('risk_limits:\n  max_position_size_pct: 0.50\n')
    out = io.StringIO()
    assert main(['plan', str(path), *TODAY, '--config', str(cfg)], gateway_factory=lambda: gw, out=out) == 0
    assert 'POSITION LIMIT' not in out.getvalue()


def test_production_path_says_so_when_net_liquidation_is_unreadable(tmp_path):
    cfg = tmp_path / 'trader.yaml'
    cfg.write_text('risk_limits:\n  max_position_size_pct: 0.10\n')
    gw = FakeGateway(prices=PRICES, net_liquidation=None)
    out = io.StringIO()
    assert main(['plan', str(write_targets(tmp_path)), *TODAY, '--config', str(cfg)],
                gateway_factory=lambda: gw, out=out) == 0
    assert 'WARN: position limit 10% of net liquidation could not be evaluated' in out.getvalue()


@pytest.mark.parametrize('doc_over, gw_kwargs, expect_code, needle', [
    ({'as_of': '2026-09-18', 'prices_as_of': '2026-09-18'}, {}, 1, 'STALE_TARGETS'),
    ({'weights': {'AAPL': 0.6, 'MSFT': 0.5}}, {}, 2, 'sum'),
    ({'weights': {'AAPL': 0.5, 'MSFT': 0.5000000001}}, {}, 2, 'sum'),
    ({'weights': {'AAPL': 1.0000000001}}, {}, 2, '> 1.0'),
    ({}, {'prices': {'AAPL': 200.0}}, 1, 'UNPRICEABLE'),
    ({}, {'proposals': [executed(9, 'AAPL', 'BUY', 25, source=f'rebalance:s2p:{AS_OF}', status='PENDING')]}, 1, 'DUPLICATE_BATCH'),
    ({}, {'proposals': [executed(9, 'AAPL', 'BUY', 25, source='rebalance:s2p:2026-08-31', status='APPROVED', order_ids=[])]}, 1, 'SLEEVE_BUSY'),
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
    assert gw.propose_calls == [] and not gw.groups and gw.recorded == []


def test_gross_exposure_refusal_is_the_default_for_plan_and_propose(tmp_path, capsys):
    path = write_targets(tmp_path, make_doc(weights={'AAPL': 0.5, 'MSFT': 0.5}))
    gw = FakeGateway(prices=PRICES, fills=held(AAPL=27), positions={'AAPL': 27})
    code, out = _run(['propose', str(path), *TODAY, '--band-pct', '5'], gw)
    err = capsys.readouterr().err
    assert code == 1 and 'GROSS_EXPOSURE_EXCEEDS_CAPITAL' in err and 'lower --band-pct' in err
    assert gw.propose_calls == [] and not gw.groups and out == ''
    code, out = _run(['plan', str(path), *TODAY, '--band-pct', '5'], gw)          # plan is refused too: no repair
    assert code == 1 and 'GROSS_EXPOSURE_EXCEEDS_CAPITAL' in capsys.readouterr().err and out == ''
    code, out = _run(['plan', str(path), *TODAY, '--band-pct', '0'], gw)          # the documented remedy
    assert code == 0 and 'SELL' in out and 'BUY' in out and 'expected cash after execution $200.00' in out
    assert 'post-execution gross exposure $9,800.00 of capital $10,000.00' in out


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
    assert code == 0 and doc['proposal_ids'] == [101, 102] and doc['complete'] is True and doc['error'] == ''


def test_propose_stopped_midway_exits_nonzero_and_prints_the_ids_it_did_create(tmp_path, capsys):
    class Flaky(FakeGateway):
        def propose(self, **kw):
            if kw['symbol'] == 'MSFT':
                raise ConnectionError('store locked')
            return super().propose(**kw)
    gw = Flaky(prices=PRICES)
    code, out = _run(['propose', str(write_targets(tmp_path)), *TODAY], gw)
    assert code == 1
    assert 'INCOMPLETE (1/2)' in out and 'ids: #101' in out and 'STOPPED: BUY 7 MSFT: ConnectionError' in out
    code, out = _run(['propose', str(write_targets(tmp_path)), *TODAY, '--json'], Flaky(prices=PRICES))
    doc = json.loads(out)
    assert code == 1 and doc['proposal_ids'] == [101] and doc['complete'] is False and 'MSFT' in doc['error']


def test_sells_only_flag_defers_the_buy_side(tmp_path):
    path = write_targets(tmp_path, make_doc(weights={'MSFT': 1.0}))
    gw = FakeGateway(prices=PRICES, fills=held(AAPL=50), positions={'AAPL': 50})
    code, out = _run(['plan', str(path), *TODAY, '--sells-only'], gw)
    assert code == 0 and 'SELL' in out and 'sells-only run' in out and 'BUY' not in out.split('skipped:')[0]


def test_missing_targets_file_is_a_usage_error(tmp_path, capsys):
    gw = FakeGateway(prices=PRICES)
    code, _ = _run(['plan', str(tmp_path / 'nope.json'), *TODAY], gw)
    assert code == 2 and 'not found' in capsys.readouterr().err and gw.calls == []


def test_status_shows_holdings_value_open_proposals_and_drift(tmp_path):
    write_targets(tmp_path, sleeve_dir=True)
    gw = FakeGateway(proposals=[executed(5, 'MSFT', 'BUY', 7, source=f'rebalance:s2p:{AS_OF}', status='PENDING')],
                     fills=held(AAPL=20), positions={'AAPL': 20, 'TSLA': 3}, prices=PRICES)
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
    gw = FakeGateway(fills=held(AAPL=20), positions={'AAPL': 20}, prices=PRICES)
    code, out = _run(['status', 's2p', '--targets-dir', str(tmp_path)], gw)
    assert code == 0 and 'latest targets: none found' in out and 'AAPL' in out


def test_status_refuses_a_targets_file_for_another_sleeve(tmp_path, capsys):
    path = write_targets(tmp_path, make_doc(sleeve='other'))
    code, _ = _run(['status', 's2p', '--targets-file', str(path)], FakeGateway(prices=PRICES))
    assert code == 1 and 'WRONG_SLEEVE' in capsys.readouterr().err


def test_reconcile_journals_receipts_and_prints_them():
    obs, rc = filled_order(901, 'AAPL', 'BUY', 25, 200.0, exec_id='x1')
    ev = evidence([obs, observed(902, 'MSFT', 'BUY', 'Submitted', filled=0)], [rc])
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901]), executed(2, 'MSFT', 'BUY', 7, order_ids=[902])],
                     evidence=ev, positions={'AAPL': 25})
    code, out = _run(['reconcile', '--sleeve', 's2p', '--dry-run'], gw)
    assert code == 0 and 'DRY RUN' in out and 'exec x1' in out and gw.recorded == []
    code, out = _run(['reconcile', '--sleeve', 's2p'], gw)
    assert code == 0 and 'journaled now 1' in out
    assert 'no execution yet for EXECUTED proposals: #2 (order 902, broker status Submitted)' in out
    assert 'broker evidence observed 2026-09-29T14:35:00+00:00' in out
    assert len(gw.recorded) == 1 and gw.recorded[0].exec_id == 'x1'
    code, out = _run(['reconcile', '--sleeve', 's2p', '--json'], gw)
    doc = json.loads(out)
    assert code == 0 and doc['inserted'] == 0 and doc['holdings'] == {'AAPL': 25.0} and doc['unfilled_proposals'] == [2]
    assert doc['unfilled'] == [{'proposal_id': 2, 'order_id': 902, 'broker_status': 'Submitted'}]
    assert doc['working'] == [{'proposal_id': 2, 'order_id': 902, 'broker_status': 'Submitted', 'filled': 0.0,
                               'total_quantity': 0.0}] and doc['broker_snapshot_complete'] is True
    assert 'still working at the broker: #2 (order 902, Submitted, 0/0 filled)' in out or True  # text form checked below
    code, out = _run(['reconcile', '--sleeve', 's2p'], gw)
    assert 'still working at the broker: #2 (order 902, Submitted, 0/0 filled)' in out


def test_reconcile_cli_refuses_an_executed_order_with_no_broker_evidence(capsys):
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901])], positions={'AAPL': 25},
                     evidence=evidence([], []))
    code, out = _run(['reconcile', '--sleeve', 's2p', '--dry-run'], gw)
    err = capsys.readouterr().err
    assert code == 1 and out == '' and 'EXECUTIONS_INCOMPLETE' in err and 'unknown to the broker tracker' in err
    assert gw.recorded == [] and 'record_fills' not in gw.method_names


def test_reconcile_refuses_when_executions_cannot_be_read_or_are_incomplete(capsys):
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901])], fail_evidence=True)
    code, _ = _run(['reconcile', '--sleeve', 's2p'], gw)
    assert code == 1 and 'EXECUTIONS_UNREADABLE' in capsys.readouterr().err and gw.recorded == []
    gw = FakeGateway(proposals=[executed(1, 'AAPL', 'BUY', 25, order_ids=[901])], positions={'AAPL': 25},
                     evidence=evidence([observed(901, 'AAPL', 'BUY', 'Filled', filled=25)], []))
    code, _ = _run(['reconcile', '--sleeve', 's2p'], gw)
    assert code == 1 and 'EXECUTIONS_INCOMPLETE' in capsys.readouterr().err and gw.recorded == []


def test_module_is_runnable_as_python_dash_m(tmp_path):
    import subprocess, sys
    proc = subprocess.run([sys.executable, '-m', 'trader.rebalance', '--help'], capture_output=True, text=True)
    assert proc.returncode == 0
    assert all(word in proc.stdout for word in ('plan', 'propose', 'status', 'reconcile'))
