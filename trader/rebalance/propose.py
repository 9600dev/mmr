"""``propose``: one PENDING proposal per planned order, nothing else (D09)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from trader.rebalance.plan import Plan, Refusal
from trader.rebalance.ports import ProposalReader, ProposalRecord, Proposer
from trader.rebalance.targets import Targets


@dataclass(frozen=True)
class ProposeResult:
    source: str
    group: str
    proposal_ids: Sequence[int]
    orders_total: int

    @property
    def complete(self) -> bool:
        return len(self.proposal_ids) == self.orders_total


def duplicate_batch(records: Sequence[ProposalRecord], source: str) -> list[ProposalRecord]:
    """PENDING/APPROVED proposals already stamped with this sleeve/as_of batch source."""
    return sorted((r for r in records if r.source == source and r.status in ('PENDING', 'APPROVED')),
                  key=lambda r: r.id)


def check_no_open_batch(targets: Targets, reader: ProposalReader) -> None:
    """D09: refuse a second batch for the same sleeve/as_of while one is in flight.

    Any other in-flight proposal in the sleeve is refused too: holdings would
    change under the new batch when it is approved. Approve or reject the
    earlier ones first.
    """
    records = list(reader.proposals_in_group(targets.group))
    dup = duplicate_batch(records, targets.source)
    if dup:
        ids = ', '.join(f'#{r.id}' for r in dup)
        raise Refusal('DUPLICATE_BATCH',
                      f'batch {targets.source} already has open proposals ({ids}); '
                      f'approve or reject them before proposing again')
    other = [r for r in records if r.status in ('PENDING', 'APPROVED')]
    if other:
        ids = ', '.join(f'#{r.id} ({r.source})' for r in sorted(other, key=lambda r: r.id))
        raise Refusal('SLEEVE_BUSY',
                      f'sleeve {targets.sleeve} has open proposals from another batch: {ids}; '
                      f'resolve them before proposing a new batch')


def reasoning_for(targets: Targets, order) -> str:
    return (f'rebalance sleeve {targets.sleeve} as_of {targets.as_of}: '
            f'{order.action} {order.quantity} {order.symbol} @~{order.price:.2f} '
            f'(held {order.held_before} -> target {order.target_shares}, weight {order.target_weight:.4f}); '
            f'targets file {targets.path or "<memory>"}; strategy {targets.strategy.family}/'
            f'{targets.strategy.id} ({targets.strategy.module}.{targets.strategy.class_name}) '
            f'source_sha256 {targets.strategy.source_sha256}')


def propose_plan(targets: Targets, plan: Plan, proposer: Proposer, reader: ProposalReader) -> ProposeResult:
    """Create the PENDING proposals for ``plan``. Refuses before creating anything.

    Orders go out in plan order (SELLs first). If a create call fails midway the
    ids already created are returned with ``complete == False``; they are real
    PENDING rows for the operator to review or reject — the rebalancer never
    rejects, approves or cancels (D09).
    """
    from trader.trading.proposal import ExecutionSpec

    if not plan.orders:
        raise Refusal('NOTHING_TO_DO', 'the plan has no orders; nothing proposed')
    check_no_open_batch(targets, reader)
    proposer.ensure_group(targets.group, f'rebalancer sleeve {targets.sleeve} (D09 holdings ledger)')
    spec = ExecutionSpec(order_type='MARKET', tif='DAY', outside_rth=False)
    ids: list[int] = []
    for order in plan.orders:
        metadata = {
            'rebalance': {
                'sleeve': targets.sleeve, 'as_of': targets.as_of.isoformat(),
                'targets_file': targets.path, 'strategy': targets.strategy.__dict__.copy(),
                'target_weight': order.target_weight, 'target_shares': order.target_shares,
                'held_before': order.held_before, 'plan_price': order.price,
                'capital_usd': targets.capital_usd, 'limit_flag': order.limit_flag,
            },
        }
        metadata['rebalance']['strategy'] = dict(metadata['rebalance']['strategy'])
        metadata['rebalance']['strategy']['params'] = dict(targets.strategy.params)
        pid = proposer.propose(
            symbol=order.symbol, action=order.action, quantity=float(order.quantity),
            execution=spec, reasoning=reasoning_for(targets, order), source=targets.source,
            group=targets.group, metadata=metadata,
        )
        ids.append(int(pid))
    return ProposeResult(source=targets.source, group=targets.group,
                         proposal_ids=tuple(ids), orders_total=len(plan.orders))
