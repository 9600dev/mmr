"""strategies() reports state NAMES (2026-11-05): str(IntEnum) gave '3', and the
dashboard (which compares names) marked every strategy disabled."""
from test_sdk import _make_mmr_with_mock, _make_mock_rpc
from trader.common.reactivex import SuccessFail
from trader.trading.strategy import StrategyState


class _Cfg:
    def __init__(self, name, state):
        self.name, self.state = name, state
        self.bar_size, self.conids, self.historical_days_prior = '1 min', [1], 5


def test_intenum_states_become_names():
    client = _make_mock_rpc()
    client.rpc.return_value.get_strategies.return_value = SuccessFail.success(
        obj=[_Cfg('a', StrategyState.RUNNING), _Cfg('b', 4), _Cfg('c', 'INSTALLED')])
    df = _make_mmr_with_mock(client).strategies()
    assert list(df['state']) == ['RUNNING', 'DISABLED', 'INSTALLED']
    assert list(df['state_code'])[:2] == [3, 4]
