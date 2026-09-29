"""CLI: ``python -m trader.rebalance {plan,propose,status,reconcile}``.

Exit codes: 0 success; 1 a D12 refusal (nothing created) or an incomplete
propose; 2 usage / targets validation error. ``propose`` writes only PENDING
proposals and ``reconcile`` writes only the D13 ``sleeve_fills`` journal.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from typing import Callable, Optional

from trader.rebalance.fills import reconcile_sleeve
from trader.rebalance.plan import DEFAULT_BAND_PCT, PositionLimits, Refusal, make_plan
from trader.rebalance.ports import Gateway
from trader.rebalance.propose import check_no_open_batch, propose_plan
from trader.rebalance.render import (
    plan_to_dict, reconcile_to_dict, render_plan, render_propose, render_reconcile,
    render_status, status_to_dict,
)
from trader.rebalance.status import sleeve_status
from trader.rebalance.targets import TargetsError, load_targets

GatewayFactory = Callable[[], Gateway]


def _parse_date(text: str) -> dt.date:
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f'not a YYYY-MM-DD date: {text!r}') from exc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog='python -m trader.rebalance',
                                description='Target-weights rebalancer: targets file -> order plan -> '
                                            'PENDING proposals. Never approves or trades (D12).')
    sub = p.add_subparsers(dest='cmd', required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument('targets', help='path to an mmr.targets.v1 JSON file (D08)')
        sp.add_argument('--band-pct', type=float, default=DEFAULT_BAND_PCT,
                        help=f'no-trade band per name, %% of sleeve capital (default {DEFAULT_BAND_PCT})')
        sp.add_argument('--today', type=_parse_date, default=None,
                        help='override the session date used for the staleness check (tests)')
        sp.add_argument('--sells-only', action='store_true',
                        help='plan only the SELL side (fund the BUYs in a later batch)')
        sp.add_argument('--strict', action='store_true',
                        help='never trim BUYs or restore banded SELLs to fit capital: refuse instead')
        sp.add_argument('--json', action='store_true', help='machine-readable output')
        sp.add_argument('--config', default='', help='trader.yaml path (default: ~/.config/mmr/trader.yaml)')

    common(sub.add_parser('plan', help='dry run: print the full order plan, create nothing'))
    common(sub.add_parser('propose', help='create one PENDING proposal per planned order'))
    st = sub.add_parser('status', help='holdings, market value, open proposals, drift vs latest targets')
    st.add_argument('sleeve')
    st.add_argument('--targets-dir', default=None, help='override ~/.local/share/mmr/data/targets')
    st.add_argument('--targets-file', default=None, help='compare against this file instead of the latest')
    st.add_argument('--json', action='store_true')
    st.add_argument('--config', default='')
    rc = sub.add_parser('reconcile', help='journal broker executions of the sleeve\'s EXECUTED proposals '
                                          'into sleeve_fills (D13)')
    rc.add_argument('--sleeve', required=True)
    rc.add_argument('--dry-run', action='store_true', help='match and print, write nothing')
    rc.add_argument('--json', action='store_true')
    rc.add_argument('--config', default='')
    return p


def load_limits(gateway: Gateway, config: str = '') -> PositionLimits:
    """The live gate's per-position cap: ``risk_limits.max_position_size_pct`` of NetLiquidation.

    The percentage comes from trader.yaml (``--config``, else ``TRADER_CONFIG``,
    else the default path; missing = the gate's default) and NetLiquidation from
    the account, read-only. An unreadable NetLiquidation leaves the cap
    unevaluable and the plan says so — it is never guessed.
    """
    from trader.trading.risk_gate import RiskLimits
    pct = float(RiskLimits.load(config or None).max_position_size_pct)
    try:
        net_liq = gateway.account_net_liquidation()
    except Exception:  # noqa: BLE001 - unreadable is reported on the plan, not guessed
        net_liq = None
    return PositionLimits(max_position_size_pct=pct, net_liquidation=net_liq)


def _default_gateway(config: str) -> Gateway:
    from trader.rebalance.sdk_gateway import SdkGateway
    return SdkGateway.connect(config_file=config)


def main(argv: Optional[list[str]] = None, *, gateway_factory: Optional[GatewayFactory] = None,
         limits: Optional[PositionLimits] = None, out=None) -> int:
    out = out or sys.stdout
    args = build_parser().parse_args(argv)
    gateway: Gateway | None = None
    try:
        factory = gateway_factory or (lambda: _default_gateway(args.config))
        if args.cmd in ('plan', 'propose'):
            targets = load_targets(args.targets)
            gateway = factory()
            plan = make_plan(targets, gateway, today=args.today, band_pct=args.band_pct,
                             limits=limits if limits is not None else load_limits(gateway, args.config),
                             sells_only=args.sells_only, strict=args.strict)
            if args.cmd == 'plan':
                print(json.dumps(plan_to_dict(plan, targets), indent=2, default=str) if args.json
                      else render_plan(plan, targets), file=out)
                return 0
            check_no_open_batch(targets, gateway)
            result = propose_plan(targets, plan, gateway, gateway)
            if args.json:
                payload = plan_to_dict(plan, targets)
                payload['proposal_ids'] = list(result.proposal_ids)
                payload['complete'] = result.complete
                payload['error'] = result.error
                print(json.dumps(payload, indent=2, default=str), file=out)
            else:
                print(render_plan(plan, targets), file=out)
                print(render_propose(result), file=out)
            return 0 if result.complete else 1
        if args.cmd == 'reconcile':
            gateway = factory()
            report = reconcile_sleeve(args.sleeve, gateway, dry_run=args.dry_run)
            print(json.dumps(reconcile_to_dict(report), indent=2, default=str) if args.json
                  else render_reconcile(report), file=out)
            return 0
        gateway = factory()
        status = sleeve_status(args.sleeve, gateway, targets_dir=args.targets_dir,
                               targets_file=args.targets_file)
        print(json.dumps(status_to_dict(status), indent=2, default=str) if args.json
              else render_status(status), file=out)
        return 0
    except TargetsError as exc:
        print(f'targets refused: {exc}', file=sys.stderr)
        return 2
    except Refusal as exc:
        print(f'refused ({exc.code}): {exc.message}', file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - surfaced, never swallowed
        print(f'rebalance failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    finally:
        closer = getattr(gateway, 'close', None)
        if callable(closer):
            try:
                closer()
            except Exception:  # noqa: BLE001
                pass


if __name__ == '__main__':
    sys.exit(main())
