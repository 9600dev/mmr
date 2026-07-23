"""Signature-drift guard between sdk.py RPC calls and the service API classes.

The RPC layer dispatches by method name with no schema check, and the sdk
tests mock the client boundary — so a kwarg added on the client but not the
server (or vice versa) only surfaces as a live TypeError. That is exactly
how protective-stop placement silently broke: ``sdk.place_protective_order``
passed ``order_ref`` but ``TraderServiceApi.place_standalone_order`` never
grew the parameter, and every disaster stop since the deploy failed.

These tests statically extract every ``self._rpc.rpc(...).<method>(...)``
call from sdk.py and bind its arguments against the real API signature.
"""

import ast
import inspect
import pathlib

from trader.messaging.data_service_api import DataServiceApi
from trader.messaging.trader_service_api import TraderServiceApi
from trader.trading.trading_runtime import Trader

SDK_PATH = pathlib.Path(__file__).resolve().parent.parent / 'trader' / 'sdk.py'

# Which API class serves each RPC client attribute in sdk.py. A new client
# attribute must be added here or the test fails loudly rather than skipping.
CLIENT_APIS = {
    '_rpc': TraderServiceApi,
    '_data_rpc': DataServiceApi,
}


def _collect_rpc_calls():
    """Yield (client_attr, method_name, positional_arg_count, kwarg_names,
    lineno) for every ``self.<client>.rpc(...).<method>(...)`` call in sdk.py."""
    tree = ast.parse(SDK_PATH.read_text())
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        chain = node.func.value
        if (isinstance(chain, ast.Call)
                and isinstance(chain.func, ast.Attribute)
                and chain.func.attr == 'rpc'):
            client = chain.func.value
            client_attr = client.attr if isinstance(client, ast.Attribute) else None
            kwargs = [kw.arg for kw in node.keywords if kw.arg is not None]
            calls.append((client_attr, node.func.attr, len(node.args), kwargs, node.lineno))
    return calls


def test_sdk_rpc_calls_bind_to_service_api_signatures():
    calls = _collect_rpc_calls()
    # If the sdk's call idiom changes, this extractor must be updated rather
    # than silently checking nothing.
    assert len(calls) >= 10, f'expected many rpc calls in sdk.py, found {len(calls)}'

    failures = []
    for client_attr, method, npos, kwargs, lineno in calls:
        api = CLIENT_APIS.get(client_attr)
        if api is None:
            failures.append(
                f'sdk.py:{lineno} calls {method}() via unknown client '
                f'{client_attr!r} — add it to CLIENT_APIS')
            continue
        func = getattr(api, method, None)
        if func is None:
            failures.append(f'sdk.py:{lineno} calls {method}() — not defined on {api.__name__}')
            continue
        sig = inspect.signature(func)
        params = [p for name, p in sig.parameters.items() if name != 'self']
        try:
            inspect.Signature(params).bind(
                *[object()] * npos, **{k: object() for k in kwargs})
        except TypeError as ex:
            failures.append(f'sdk.py:{lineno} {api.__name__}.{method}(): {ex}')
    assert not failures, 'sdk RPC calls drifted from service APIs:\n' + '\n'.join(failures)


def test_place_standalone_order_api_passes_through_to_runtime():
    api_params = set(inspect.signature(TraderServiceApi.place_standalone_order).parameters) - {'self'}
    runtime_params = set(inspect.signature(Trader.place_standalone_order).parameters) - {'self'}
    missing = api_params - runtime_params
    assert not missing, f'API params not accepted by Trader.place_standalone_order: {missing}'


def test_place_standalone_order_accepts_order_ref():
    # Regression: protective stops carry the strategy name as orderRef for
    # ledger attribution; the API wrapper must accept it.
    assert 'order_ref' in inspect.signature(TraderServiceApi.place_standalone_order).parameters
