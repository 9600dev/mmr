"""Static proof (D15): the rebalancer package cannot approve, place, cancel or close anything.

Walks the AST of every module under trader/rebalance/: no import, attribute,
name, string-keyed getattr or raw RPC chain may reach a trading action.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

PACKAGE = Path(__file__).resolve().parents[1] / 'trader' / 'rebalance'

FORBIDDEN_NAMES = {
    'approve', 'reject', 'buy', 'sell', 'cancel', 'cancel_all', 'close_position', 'close_all',
    'place_order', 'place_order_simple', 'place_expressive_order', 'place_standalone_order',
    'place_protective_order', 'emergency_close_position', 'buy_option', 'sell_option',
    'convert_to_market', 'resize_positions', 'execute_resize_plan', 'approve_all',
    'update_status', 'try_transition', 'delete', 'append_once',   # the proposal state machine / event log are not ours to drive
    'rpc', '_rpc', '_client', 'RPCClient', 'consume',        # no raw RPC chain: it could name any method
    'subprocess', 'os.system', 'system',
}
FORBIDDEN_MODULES = {'trader.trading.trading_runtime', 'trader.trading.executioner',
                     'trader.messaging.clientserver', 'subprocess', 'ib_async',
                     # local ORDER_FILLED events are tracker-derived fill deltas, not IB execution receipts (D13)
                     'trader.data.event_store'}


def _modules() -> list[Path]:
    files = sorted(PACKAGE.glob('*.py'))
    assert files, f'no modules found under {PACKAGE}'
    return files


@pytest.mark.parametrize('path', _modules(), ids=lambda p: p.name)
def test_module_never_names_a_trading_action(path: Path):
    tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_NAMES:
            offenders.append(f'{path.name}:{node.lineno} attribute .{node.attr}')
        elif isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            offenders.append(f'{path.name}:{node.lineno} name {node.id}')
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            mod = getattr(node, 'module', None) or ''
            names = {a.name for a in node.names}
            if mod in FORBIDDEN_MODULES or any(n in FORBIDDEN_MODULES for n in names):
                offenders.append(f'{path.name}:{node.lineno} import {mod or names}')
            if names & FORBIDDEN_NAMES:
                offenders.append(f'{path.name}:{node.lineno} imports {names & FORBIDDEN_NAMES}')
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'getattr':
            for arg in node.args[1:2]:
                if isinstance(arg, ast.Constant) and arg.value in FORBIDDEN_NAMES:
                    offenders.append(f'{path.name}:{node.lineno} getattr({arg.value!r})')
    assert not offenders, '\n'.join(offenders)


def test_package_has_no_dynamic_dispatch_into_the_sdk():
    """`getattr(mmr, <variable>)` would defeat the name scan; the package must not do it."""
    for path in _modules():
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == 'getattr':
                target = node.args[1] if len(node.args) > 1 else None
                assert isinstance(target, ast.Constant), f'{path.name}:{node.lineno}: getattr with a non-literal name'
                assert target.value in {'close'}, f'{path.name}:{node.lineno}: getattr({target.value!r})'


def test_only_the_gateway_module_imports_the_sdk():
    for path in _modules():
        tree = ast.parse(path.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                mod = getattr(node, 'module', None) or ''
                names = {a.name for a in node.names}
                touches_sdk = mod == 'trader.sdk' or 'trader.sdk' in names
                assert not touches_sdk or path.name == 'sdk_gateway.py', f'{path.name} imports trader.sdk'


def test_ports_cannot_express_a_trading_action():
    """The Protocols are the whole vocabulary; none of them is a trade."""
    import trader.rebalance.ports as ports
    methods = set()
    for proto in (ports.ProposalReader, ports.Broker, ports.Pricer, ports.Proposer, ports.Gateway):
        methods |= {m for m in vars(proto) if not m.startswith('_')}
    for proto in (ports.FillsJournal,):
        methods |= {m for m in vars(proto) if not m.startswith('_')}
    # Reads, one PENDING-proposal write, one write to the journal the package owns (D13). No trade.
    assert methods == {'proposals_in_group', 'stock_positions', 'account_net_liquidation', 'execution_evidence',
                       'price', 'sleeve_holdings', 'record_fills', 'ensure_group', 'propose'}
