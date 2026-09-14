"""Long-only automation cannot establish custody by covering a manual short."""
import pandas as pd
import pytest

from review.test_review_strategy_contract import TS, make_work
from test_execution_recovery import recovery
from trader.objects import Action


@pytest.mark.parametrize('held', [-100, 0, 100])
def test_automated_entry_refuses_manual_short_and_accepts_flat_or_long_book(recovery, monkeypatch, held):
    executor, sdk, _ = recovery
    monkeypatch.setenv('MMR_PROTECTIVE_STOP_PCT', '0')
    sdk.broker[1111] = held

    def snapshot(**kwargs):
        return dict(complete=True, positions_complete=True,
                    positions=sdk.positions().to_dict('records'), orders=sdk.trades().to_dict('records'))

    monkeypatch.setattr(sdk, 'execution_snapshot', snapshot, raising=False)
    executor._process_signal(make_work(quantity=140))
    if held < 0:
        assert sdk.propose_calls == []
        assert executor.intents.all(kind='OPEN') == []
        assert executor.state.open_position('orb_test', 1111) is None
        assert sdk.broker[1111] == held
        row = executor.state.db.execute(
            "SELECT decision, reason FROM auto_exec_bar_log WHERE strategy=? AND conid=?",
            ['orb_test', 1111], fetch='one')
        assert row[0] == 'refused'
        assert 'short' in row[1]
    else:
        assert len(sdk.approve_calls) == 1
        assert sdk.broker[1111] == held + 140
        executor._process_signal(make_work(action=Action.SELL, bar_ts=TS + pd.Timedelta(minutes=1)))
        assert sdk.broker[1111] == held
        assert sdk.propose_calls[-1]['quantity'] == 140


def test_short_in_another_contract_does_not_block_long_only_entry(recovery):
    executor, sdk, _ = recovery
    sdk.broker[2222] = -100
    executor._process_signal(make_work(quantity=40))
    assert sdk.broker[2222] == -100
    assert sdk.broker[1111] == 40
    assert len(sdk.approve_calls) == 1


def test_observed_short_still_refuses_entry_during_incomplete_broader_snapshot(recovery, monkeypatch):
    executor, sdk, _ = recovery
    sdk.broker[1111] = -100
    monkeypatch.setattr(sdk, 'execution_snapshot', lambda **kwargs:
                        dict(complete=False, positions_complete=False,
                             positions=sdk.positions().to_dict('records'), orders=[]), raising=False)
    executor._process_signal(make_work(quantity=140))
    assert sdk.propose_calls == []
    assert sdk.broker[1111] == -100


def test_pyramid_add_cannot_cover_an_externally_created_short(recovery):
    executor, sdk, _ = recovery
    executor.cooldown_seconds = 0
    executor._process_signal(make_work(quantity=40, pyramid_max_adds=1))
    sdk.broker[1111] = -10  # an external trade changed custody after entry
    executor._process_signal(make_work(quantity=40, pyramid_max_adds=1,
                                      bar_ts=TS + pd.Timedelta(minutes=1)))
    assert len(sdk.approve_calls) == 1
    assert sdk.broker[1111] == -10
