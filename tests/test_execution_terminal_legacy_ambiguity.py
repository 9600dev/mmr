"""The supported historical DataFrame adapter retains identity uncertainty."""
import pandas as pd

from test_execution_recovery import recovery


def test_legacy_dataframe_identity_ambiguity_cannot_confirm_terminal_order(recovery, monkeypatch):
    executor, sdk, _ = recovery
    frame = pd.DataFrame([dict(
        orderId=711, conId=1111, action='SELL', orderType='STP', orderRef='orb_test',
        status='Filled', brokerStatus='Filled', filled=40.0, totalQuantity=40.0,
        remaining=0.0, fillQuantityKnown=True, identityAmbiguous=True,
    )])
    # This flag describes complete enumeration in the legacy adapter. Unlike
    # the native snapshot RPC it does not fold per-row identity ambiguity into
    # the global flag, so the consumer must retain the row-level refusal.
    frame.attrs['complete'] = True
    monkeypatch.setattr(sdk, 'trades', lambda: frame)
    assert executor._order_is_terminal(711) is False

    frame.loc[0, 'identityAmbiguous'] = False
    assert executor._order_is_terminal(711) is True
