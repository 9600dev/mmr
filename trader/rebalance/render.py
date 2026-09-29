"""Plain-text and JSON rendering for the CLI (no Rich: output must survive a pipe)."""
from __future__ import annotations

import dataclasses
from typing import Any

from trader.rebalance.fills import ReconcileReport
from trader.rebalance.plan import Plan
from trader.rebalance.propose import ProposeResult
from trader.rebalance.status import SleeveStatus
from trader.rebalance.targets import Targets


def _money(v: float | None) -> str:
    return 'n/a' if v is None else f'${v:,.2f}'


def plan_to_dict(plan: Plan, targets: Targets) -> dict[str, Any]:
    return {
        'sleeve': plan.sleeve, 'as_of': plan.as_of.isoformat(), 'group': targets.group,
        'source': targets.source, 'targets_file': targets.path,
        'strategy': dataclasses.asdict(targets.strategy),
        'capital_usd': plan.capital_usd, 'weight_sum': targets.weight_sum,
        'holdings_before': dict(plan.holdings), 'holdings_after': plan.holdings_after(),
        'prices': dict(plan.prices),
        'orders': [{
            'symbol': o.symbol, 'action': o.action, 'quantity': o.quantity, 'price': o.price,
            'notional': o.notional, 'held_before': o.held_before, 'target_shares': o.target_shares,
            'target_weight': o.target_weight, 'limit_flag': o.limit_flag,
        } for o in plan.orders],
        'skipped': [dataclasses.asdict(s) for s in plan.skipped],
        'cash_before': plan.cash_before, 'cash_after': plan.cash_after,
        'turnover_notional': plan.turnover_notional, 'gross_exposure_after': plan.gross_exposure_after,
        'limit_flags': [o.symbol for o in plan.limit_flags],
        'warnings': list(plan.warnings),
        'open_proposals': [{'id': r.id, 'symbol': r.symbol, 'action': r.action,
                            'quantity': r.quantity, 'status': r.status, 'source': r.source}
                           for r in plan.open_proposals],
    }


def render_plan(plan: Plan, targets: Targets) -> str:
    lines = [
        f'REBALANCE PLAN  sleeve={plan.sleeve}  as_of={plan.as_of}  group={targets.group}',
        f'  targets: {targets.path or "<memory>"}',
        f'  strategy: {targets.strategy.family}/{targets.strategy.id} '
        f'sha256 {targets.strategy.source_sha256[:12]}…  weight sum {targets.weight_sum:.4f}',
        f'  capital {_money(plan.capital_usd)}  holdings value {_money(plan.capital_usd - plan.cash_before)}'
        f'  cash before {_money(plan.cash_before)}',
    ]
    for w in plan.warnings:
        lines.append(f'  WARN: {w}')
    for r in plan.open_proposals:
        lines.append(f'  WARN in flight: proposal #{r.id} {r.action} {r.quantity:g} {r.symbol} '
                     f'[{r.status}] {r.source}')
    lines.append('')
    if not plan.orders:
        lines.append('  no orders (holdings are within the no-trade band of the targets)')
    else:
        lines.append(f'  {"#":>2} {"ACTION":<6} {"QTY":>7} {"SYMBOL":<8} {"PRICE":>10} {"NOTIONAL":>12} '
                     f'{"HELD":>6} {"TARGET":>7} {"WEIGHT":>7}  FLAGS')
        for i, o in enumerate(plan.orders, 1):
            lines.append(f'  {i:>2} {o.action:<6} {o.quantity:>7d} {o.symbol:<8} {o.price:>10.2f} '
                         f'{o.notional:>12,.2f} {o.held_before:>6d} {o.target_shares:>7d} '
                         f'{o.target_weight:>7.4f}  {"LIMIT" if o.limit_flag else ""}')
    lines.append('')
    lines.append(f'  sells {len(plan.sells)} ({_money(sum(o.notional for o in plan.sells))})  '
                 f'buys {len(plan.buys)} ({_money(sum(o.notional for o in plan.buys))})  '
                 f'turnover {_money(plan.turnover_notional)}')
    lines.append(f'  expected cash after execution {_money(plan.cash_after)}  '
                 f'post-execution gross exposure {_money(plan.gross_exposure_after)} '
                 f'of capital {_money(plan.capital_usd)}')
    if plan.limit_flags:
        lines.append('  POSITION LIMIT would refuse at approval time:')
        for o in plan.limit_flags:
            lines.append(f'    {o.action} {o.quantity} {o.symbol}: {o.limit_flag}')
    if plan.skipped:
        lines.append('  skipped:')
        for s in plan.skipped:
            lines.append(f'    {s.symbol}: delta {s.delta:+d} ({_money(s.notional)}) — {s.reason}')
    return '\n'.join(lines)


def render_propose(result: ProposeResult) -> str:
    ids = ', '.join(f'#{i}' for i in result.proposal_ids) or '(none)'
    state = 'complete' if result.complete else f'INCOMPLETE ({len(result.proposal_ids)}/{result.orders_total})'
    text = (f'PROPOSED {len(result.proposal_ids)} PENDING proposal(s) {state}\n'
            f'  source={result.source}  group={result.group}\n'
            f'  ids: {ids}\n')
    if result.error:
        text += (f'  STOPPED: {result.error}\n'
                 f'  the ids above are real PENDING proposals — review or reject them by hand before re-running\n')
    return text + '  review with `mmr proposals show <id>`; approve by hand with `mmr approve <id>`'


def status_to_dict(status: SleeveStatus) -> dict[str, Any]:
    return {
        'sleeve': status.sleeve, 'group': status.group,
        'targets_file': status.targets_path,
        'targets_as_of': status.targets.as_of.isoformat() if status.targets else None,
        'capital_usd': status.targets.capital_usd if status.targets else None,
        'market_value': status.market_value,
        'holdings': [{
            'symbol': h.symbol, 'shares': h.shares, 'price': h.price, 'value': h.value,
            'target_weight': h.target_weight, 'target_shares': h.target_shares,
            'drift_shares': h.drift_shares,
        } for h in status.holdings],
        'gross_drift_value': status.gross_drift_value,
        'open_proposals': [{'id': r.id, 'symbol': r.symbol, 'action': r.action,
                            'quantity': r.quantity, 'status': r.status, 'source': r.source}
                           for r in status.open_proposals],
        'warnings': list(status.warnings),
    }


def render_status(status: SleeveStatus) -> str:
    lines = [f'SLEEVE STATUS  sleeve={status.sleeve}  group={status.group}']
    if status.targets:
        lines.append(f'  latest targets: {status.targets_path} (as_of {status.targets.as_of}, '
                     f'capital {_money(status.targets.capital_usd)}, '
                     f'{len(status.targets.weights)} names, weight sum {status.targets.weight_sum:.4f})')
    else:
        lines.append('  latest targets: none found')
    lines.append(f'  market value {_money(status.market_value)}')
    for w in status.warnings:
        lines.append(f'  WARN: {w}')
    lines.append('')
    lines.append(f'  {"SYMBOL":<8} {"SHARES":>7} {"PRICE":>10} {"VALUE":>12} {"TGT W":>7} {"TGT SH":>7} {"DRIFT":>6}')
    for h in status.holdings:
        px = 'n/a' if h.price is None else f'{h.price:.2f}'
        val = 'n/a' if h.value is None else f'{h.value:,.2f}'
        tgt = 'n/a' if h.target_shares is None else str(h.target_shares)
        drift = 'n/a' if h.drift_shares is None else f'{h.drift_shares:+d}'
        lines.append(f'  {h.symbol:<8} {h.shares:>7d} {px:>10} {val:>12} {h.target_weight:>7.4f} {tgt:>7} {drift:>6}')
    lines.append('')
    lines.append(f'  gross drift vs targets {_money(status.gross_drift_value)}')
    if status.open_proposals:
        lines.append('  open proposals:')
        for r in status.open_proposals:
            lines.append(f'    #{r.id} {r.action} {r.quantity:g} {r.symbol} [{r.status}] {r.source}')
    else:
        lines.append('  open proposals: none')
    return '\n'.join(lines)


def reconcile_to_dict(report: ReconcileReport) -> dict[str, Any]:
    return {
        'sleeve': report.sleeve, 'dry_run': report.dry_run, 'inserted': report.inserted,
        'matched': [dataclasses.asdict(f) for f in report.matched],
        'matched_by_symbol': report.matched_by_symbol,
        'unfilled_proposals': list(report.unfilled_proposals),
        'unfilled': [dataclasses.asdict(u) for u in report.unfilled],
        'working': [dataclasses.asdict(w) for w in report.working],
        'broker_snapshot_complete': report.broker_snapshot_complete,
        'proposals_without_order_ids': list(report.proposals_without_order_ids),
        'evidence_observed_at': report.evidence_observed_at, 'replay_scope': report.replay_scope,
        'holdings': dict(report.holdings), 'ib_positions': dict(report.ib_positions),
        'warnings': list(report.warnings),
    }


def render_reconcile(report: ReconcileReport) -> str:
    mode = 'DRY RUN' if report.dry_run else 'RECONCILE'
    lines = [f'{mode}  sleeve={report.sleeve}  executions matched {len(report.matched)}  '
             f'journaled now {report.inserted}']
    for f in report.matched:
        px = 'n/a' if f.price is None else f'{f.price:.4f}'
        lines.append(f'  {f.trade_date} {f.ticker:<8} {f.quantity:+9.0f} @ {px:>10}  exec {f.exec_id}  '
                     f'order {f.order_id}  proposal #{f.proposal_id}')
    if report.unfilled:
        lines.append('  no execution yet for EXECUTED proposals: '
                     + ', '.join(f'#{u.proposal_id} (order {u.order_id}, broker status {u.broker_status})'
                                 for u in report.unfilled))
    if report.working:
        lines.append('  still working at the broker: '
                     + ', '.join(f'#{w.proposal_id} (order {w.order_id}, {w.broker_status}, '
                                 f'{w.filled:g}/{w.total_quantity:g} filled)' for w in report.working))
    if report.evidence_observed_at:
        lines.append(f'  broker evidence observed {report.evidence_observed_at}'
                     + (f'  replay scope {report.replay_scope}' if report.replay_scope else '')
                     + ('' if report.broker_snapshot_complete else '  (snapshot flagged INCOMPLETE by trader_service)'))
    lines.append('  journal holdings vs IB:')
    for sym in sorted(set(report.holdings) | set(report.ib_positions)):
        j = report.holdings.get(sym)
        ib = report.ib_positions.get(sym)
        if j is None:
            continue  # outside the sleeve
        lines.append(f'    {sym:<8} journal {j:>9g}  IB {"n/a" if ib is None else f"{ib:g}":>9}')
    for w in report.warnings:
        lines.append(f'  WARN: {w}')
    return '\n'.join(lines)
