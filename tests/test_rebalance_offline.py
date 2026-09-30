"""``plan --offline``: an EMPTY sleeve priced at stored ``pit_daily_bars`` closes, nothing created;
``--sleeve`` resolution of the newest targets file. The end-to-end pin for the paper path:
``lowfreq_live targets`` on the research dev DB -> ``rebalance plan --offline`` within capital."""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path

import duckdb
import pytest

from rebalance_fakes import AS_OF, FakeGateway, make_doc, write_targets
from trader.rebalance.__main__ import main
from trader.rebalance.plan import PositionLimits

TODAY = ['--today', '2026-09-29']
PRICES = {'AAPL': 200.0, 'MSFT': 400.0, 'TSLA': 250.0}


def _bars_db(tmp_path: Path, closes: dict[str, float], day: str = AS_OF) -> Path:
    path = tmp_path / 'hist.duckdb'
    con = duckdb.connect(str(path))
    con.execute('CREATE TABLE pit_daily_bars (day DATE, ticker VARCHAR, open DOUBLE, high DOUBLE, low DOUBLE, '
                'close DOUBLE, volume DOUBLE, dollar_volume DOUBLE, dv_rank INTEGER)')
    for ticker, close in closes.items():
        con.execute('INSERT INTO pit_daily_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    [day, ticker, close, close, close, close, 1e6, 1e8, 1])
    con.close()
    return path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _no_sdk():
    pytest.fail('offline mode must never build the SDK gateway')


def _run(argv, limits=PositionLimits()):
    out = io.StringIO()
    code = main(argv, gateway_factory=_no_sdk, limits=limits, out=out)
    return code, out.getvalue()


def test_offline_plan_prices_an_empty_sleeve_from_stored_closes_and_creates_nothing(tmp_path, capsys):
    targets = write_targets(tmp_path)                       # AAPL 0.5, MSFT 0.3 of $10,000
    bars = _bars_db(tmp_path, PRICES)
    before = _sha(bars)
    cfg = tmp_path / 'trader.yaml'
    cfg.write_text('risk_limits:\n  max_position_size_pct: 0.10\n')
    code, out = _run(['plan', str(targets), '--offline', '--bars-db', str(bars), *TODAY, '--config', str(cfg)],
                     limits=None)                           # production limits path: NetLiquidation unreadable
    assert code == 0, capsys.readouterr().err
    assert out.startswith('OFFLINE DRY RUN: empty sleeve, prices = stored closes of 2026-09-28')
    assert 'BUY' in out and 'AAPL' in out and 'MSFT' in out and 'SELL' not in out
    assert 'expected cash after execution $2,200.00' in out
    assert 'post-execution gross exposure $7,800.00 of capital $10,000.00' in out
    assert 'NetLiquidation is unreadable' in out            # reported, never guessed
    assert _sha(bars) == before                             # the bars DB was only read


def test_offline_plan_json_reports_the_bars_db_and_stays_within_capital(tmp_path):
    targets = write_targets(tmp_path)
    bars = _bars_db(tmp_path, PRICES)
    code, out = _run(['plan', str(targets), '--offline', '--bars-db', str(bars), *TODAY, '--json'])
    doc = json.loads(out)
    assert code == 0
    assert doc['offline'] == {'bars_db': str(bars), 'prices_as_of': AS_OF, 'today': TODAY[1]}
    assert [(o['action'], o['symbol'], o['quantity']) for o in doc['orders']] == [('BUY', 'AAPL', 25), ('BUY', 'MSFT', 7)]
    assert doc['holdings_before'] == {} and doc['open_proposals'] == []
    assert 0 < doc['gross_exposure_after'] <= doc['capital_usd'] and doc['cash_after'] >= 0


def test_offline_plan_without_today_checks_staleness_as_of_the_decision_session(tmp_path, capsys):
    """A historical file is a valid offline replay: no --today means 'as of its own as_of'. Live
    (no --offline) the same file is stale and refused, and an explicit --today still wins offline."""
    old = make_doc(as_of='2024-06-03', prices_as_of='2024-06-03')
    targets = write_targets(tmp_path, old, name='2024-06-03.json')
    bars = _bars_db(tmp_path, PRICES, day='2024-06-03')
    code, out = _run(['plan', str(targets), '--offline', '--bars-db', str(bars), '--json'])
    assert code == 0 and json.loads(out)['offline']['today'] == '2024-06-03'
    code, _ = _run(['plan', str(targets), '--offline', '--bars-db', str(bars), '--today', '2026-09-29'])
    assert code == 1 and 'STALE_TARGETS' in capsys.readouterr().err
    live = io.StringIO()
    assert main(['plan', str(targets)], gateway_factory=lambda: FakeGateway(prices=PRICES), limits=PositionLimits(),
                out=live) == 1                                            # live: today is today, the file is stale
    assert 'STALE_TARGETS' in capsys.readouterr().err and live.getvalue() == ''


def test_offline_plan_refuses_a_ticker_without_a_stored_close(tmp_path, capsys):
    targets = write_targets(tmp_path)
    bars = _bars_db(tmp_path, {'AAPL': 200.0, 'MSFT': 0.0})   # a zero close is not a price
    code, _ = _run(['plan', str(targets), '--offline', '--bars-db', str(bars), *TODAY])
    assert code == 1 and 'UNPRICEABLE' in capsys.readouterr().err


def test_offline_plan_prices_from_the_prices_as_of_session_not_any_other_day(tmp_path, capsys):
    targets = write_targets(tmp_path)
    bars = _bars_db(tmp_path, PRICES, day='2026-09-25')       # closes exist, but not for prices_as_of
    code, _ = _run(['plan', str(targets), '--offline', '--bars-db', str(bars), *TODAY])
    assert code == 1 and 'UNPRICEABLE' in capsys.readouterr().err


def test_propose_offline_is_refused_before_anything_is_built(tmp_path, capsys):
    targets = write_targets(tmp_path)
    bars = _bars_db(tmp_path, PRICES)
    code, out = _run(['propose', str(targets), '--offline', '--bars-db', str(bars), *TODAY])
    assert code == 1 and out == ''
    assert 'refused (OFFLINE)' in capsys.readouterr().err


def test_offline_gateway_refuses_every_write_and_broker_read(tmp_path):
    from trader.rebalance.offline import OfflineEmptySleeve
    from trader.rebalance.plan import Refusal
    import datetime as dt
    gw = OfflineEmptySleeve(str(_bars_db(tmp_path, PRICES)), dt.date.fromisoformat(AS_OF))
    assert gw.price('AAPL') == 200.0 and gw.price('aapl') == 200.0 and gw.price('NOPE') is None
    assert gw.proposals_in_group('sleeve_s2p') == () and gw.stock_positions() == {} and gw.sleeve_holdings('s2p') == {}
    assert gw.account_net_liquidation() is None
    for call in (lambda: gw.execution_evidence([1]), lambda: gw.record_fills([]),
                 lambda: gw.ensure_group('sleeve_s2p', ''),
                 lambda: gw.propose(symbol='AAPL', action='BUY', quantity=1, execution=None, reasoning='',
                                    source='', group='', metadata={})):
        with pytest.raises(Refusal) as exc:
            call()
        assert exc.value.code == 'OFFLINE'


def test_offline_gateway_names_a_missing_bars_db(tmp_path, capsys):
    targets = write_targets(tmp_path)
    code, _ = _run(['plan', str(targets), '--offline', '--bars-db', str(tmp_path / 'absent.duckdb'), *TODAY])
    assert code == 1 and 'absent.duckdb' in capsys.readouterr().err


def test_sleeve_flag_resolves_the_newest_targets_file_and_ignores_the_benchmark_sidecar(tmp_path):
    older = make_doc(as_of='2026-09-01', prices_as_of='2026-09-01', weights={'TSLA': 0.5})
    write_targets(tmp_path, older, name='2026-09-01.json', sleeve_dir=True)
    newest = write_targets(tmp_path, sleeve_dir=True)          # 2026-09-28.json
    (tmp_path / 's2p' / f'{AS_OF}.benchmark.json').write_text('{"schema": "mmr.lowfreq_benchmark.v1"}')
    bars = _bars_db(tmp_path, PRICES)
    code, out = _run(['plan', '--sleeve', 's2p', '--targets-dir', str(tmp_path), '--offline', '--bars-db', str(bars),
                      *TODAY, '--json'])
    doc = json.loads(out)
    assert code == 0 and doc['as_of'] == AS_OF and doc['targets_file'] == str(newest)
    assert 'TSLA' not in doc['prices']
    # the same resolution feeds the live gateway path
    gw = FakeGateway(prices=PRICES)
    live = io.StringIO()
    assert main(['plan', '--sleeve', 's2p', '--targets-dir', str(tmp_path), *TODAY], gateway_factory=lambda: gw,
                limits=PositionLimits(), out=live) == 0
    assert f'targets: {newest}' in live.getvalue() and gw.propose_calls == []


def test_a_targets_path_or_a_sleeve_is_required_but_not_both(tmp_path, capsys):
    targets = write_targets(tmp_path)
    assert _run(['plan', str(targets), '--sleeve', 's2p', *TODAY])[0] == 2
    assert 'not both' in capsys.readouterr().err
    assert _run(['plan', *TODAY])[0] == 2
    assert 'not neither' in capsys.readouterr().err
    assert _run(['plan', '--sleeve', 'ghost', '--targets-dir', str(tmp_path), *TODAY])[0] == 2
    assert "no <as_of>.json targets file for sleeve 'ghost'" in capsys.readouterr().err


def _research_config_available() -> bool:
    try:
        from trader.research.lowfreq.config import research_db_paths
        return all(Path(p).is_file() for p in research_db_paths().values())
    except Exception:  # noqa: BLE001 - absent config or DBs: skip, the dev DB is a local artefact
        return False


@pytest.mark.skipif(not _research_config_available(), reason='research dev DB not available')
def test_end_to_end_targets_on_the_dev_db_then_offline_plan_stays_within_capital(tmp_path):
    """Acceptance pin: ``lowfreq_live targets equity/s2p_top_quintile --as-of 2024-06-03`` on the research
    dev DB, then ``rebalance plan --offline`` on the file it wrote (empty sleeve). Found on the first run:
    the writer emitted 69 x repr(1/69), which the reader's exact-decimal sum called 1.000000000000000014."""
    from trader.lowfreq_live.__main__ import main as live_main
    out_file = tmp_path / 's2p_2024-06-03.json'
    assert live_main(['targets', 'equity/s2p_top_quintile', '--sleeve', 's2p', '--capital', '100000',
                      '--as-of', '2024-06-03', '--out', str(out_file)]) == 0
    assert out_file.is_file() and (tmp_path / 's2p_2024-06-03.benchmark.json').is_file()
    code, out = _run(['plan', str(out_file), '--offline', '--today', '2024-06-04', '--json'])
    doc = json.loads(out)
    assert code == 0
    assert len(doc['orders']) >= 30 and {o['action'] for o in doc['orders']} == {'BUY'}      # D05: >= 30 names
    assert 0 < doc['gross_exposure_after'] <= 100_000.0 and doc['cash_after'] >= 0
    assert doc['offline']['prices_as_of'] == '2024-06-03'
