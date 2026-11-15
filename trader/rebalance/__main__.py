"""CLI: ``python -m trader.rebalance {plan,propose,status,reconcile}``.

Exit codes: 0 success; 1 a D15 refusal (nothing created) or an incomplete
propose; 2 usage / targets validation error. ``propose`` writes only PENDING
proposals and ``reconcile`` (without ``--dry-run``) writes only the D13
``sleeve_fills`` journal; ``plan``, ``status`` and ``reconcile --dry-run``
write nothing at all.

Live ``plan`` and ``propose`` first check that the ``sleeve_fills`` journal is
not behind the broker (``check_journal_current``): holdings are read from it,
so an un-journaled execution is bought again. ``status`` reports the same
condition as a warning instead of refusing.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from typing import Callable, Optional

from trader.rebalance.fills import check_journal_current, reconcile_sleeve
from trader.rebalance.plan import DEFAULT_BAND_PCT, PositionLimits, Refusal, make_plan
from trader.rebalance.ports import Gateway
from trader.rebalance.propose import check_no_open_batch, propose_plan
from trader.rebalance.render import (
    plan_to_dict, reconcile_to_dict, render_plan, render_propose, render_reconcile,
    render_status, status_to_dict,
)
from trader.rebalance.status import sleeve_status
from trader.rebalance.targets import TargetsError, latest_targets_path, load_targets

GatewayFactory = Callable[[], Gateway]


def _parse_date(text: str) -> dt.date:
    try:
        return dt.date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f'not a YYYY-MM-DD date: {text!r}') from exc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog='python -m trader.rebalance',
                                description='Target-weights rebalancer: targets file -> order plan -> '
                                            'PENDING proposals. Never approves or trades (D15).')
    sub = p.add_subparsers(dest='cmd', required=True)

    def common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument('targets', nargs='?', default=None,
                        help='path to an mmr.targets.v1 JSON file (D08); or give --sleeve')
        sp.add_argument('--sleeve', default=None,
                        help='use the newest <as_of>.json under the sleeve\'s targets directory instead of a path')
        sp.add_argument('--targets-dir', default=None,
                        help='with --sleeve: override ~/.local/share/mmr/data/targets')
        sp.add_argument('--offline', action='store_true',
                        help='dry run without trader_service: an EMPTY sleeve priced at the stored pit_daily_bars '
                             'closes of prices_as_of (plan only; propose --offline is refused)')
        sp.add_argument('--bars-db', default=None,
                        help='with --offline: DuckDB holding pit_daily_bars (default: history_duckdb_path of '
                             'LOWFREQ_RESEARCH_CONFIG)')
        sp.add_argument('--band-pct', type=float, default=DEFAULT_BAND_PCT,
                        help=f'no-trade band per name, %% of sleeve capital (default {DEFAULT_BAND_PCT})')
        sp.add_argument('--today', type=_parse_date, default=None,
                        help='override the session date used for the staleness check (tests)')
        sp.add_argument('--sells-only', action='store_true',
                        help='plan only the SELL side (fund the BUYs in a later batch)')
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


def _resolve_targets_path(args: argparse.Namespace) -> str:
    """Exactly one of the positional path and ``--sleeve`` (the newest ``<as_of>.json``; sidecars ignored)."""
    if bool(args.targets) == bool(args.sleeve):
        raise TargetsError('give a targets file path or --sleeve NAME (not both, not neither)')
    if args.targets:
        return str(args.targets)
    found = latest_targets_path(args.sleeve, args.targets_dir)
    if found is None:
        raise TargetsError(f'no <as_of>.json targets file for sleeve {args.sleeve!r}'
                           + (f' under {args.targets_dir}' if args.targets_dir else ''))
    return str(found)


def _offline_gateway(bars_db: str, prices_as_of: dt.date) -> Gateway:
    """``--offline``: an empty sleeve priced at the stored closes of ``prices_as_of`` (plan only)."""
    from trader.rebalance.offline import OfflineEmptySleeve
    return OfflineEmptySleeve(bars_db, prices_as_of)


def _offline_bars_db(args: argparse.Namespace) -> str:
    if args.bars_db:
        return str(args.bars_db)
    from trader.rebalance.offline import default_bars_db
    return default_bars_db()


def main(argv: Optional[list[str]] = None, *, gateway_factory: Optional[GatewayFactory] = None,
         limits: Optional[PositionLimits] = None, out=None) -> int:
    out = out or sys.stdout
    args = build_parser().parse_args(argv)
    # Library DEBUG/INFO chatter (e.g. "trying RPCClient.connect()") must not
    # interleave with the plan / --json output on stdout. MMR_REBALANCE_DEBUG=1
    # restores it. The previous global level is restored on return.
    import logging as _logging
    import os as _os
    previous = _logging.root.manager.disable
    if _os.environ.get('MMR_REBALANCE_DEBUG') != '1':
        _logging.disable(_logging.INFO)
    try:
        return _run(args, gateway_factory=gateway_factory, limits=limits, out=out)
    finally:
        _logging.disable(previous)


def _run(args, *, gateway_factory: Optional[GatewayFactory], limits: Optional[PositionLimits], out) -> int:
    gateway: Gateway | None = None
    try:
        factory = gateway_factory or (lambda: _default_gateway(args.config))
        if args.cmd in ('plan', 'propose'):
            if args.offline and args.cmd == 'propose':
                raise Refusal('OFFLINE', 'the offline dry run creates nothing; run propose with trader_service')
            targets = load_targets(_resolve_targets_path(args))
            bars_db = _offline_bars_db(args) if args.offline else ''
            gateway = _offline_gateway(bars_db, targets.prices_as_of) if args.offline else factory()
            # Offline replays a (possibly historical) decision: without --today the staleness check
            # runs as of the file's own decision session. Live, today is today.
            today = args.today if args.today is not None or not args.offline else targets.as_of
            if not args.offline:
                # Holdings come from the journal: refuse to size anything on a
                # journal the broker says is behind (the offline run plans an
                # empty sleeve by definition and has no journal to check).
                check_journal_current(targets.sleeve, gateway)
            plan = make_plan(targets, gateway, today=today, band_pct=args.band_pct,
                             limits=limits if limits is not None else load_limits(gateway, args.config),
                             sells_only=args.sells_only)
            if args.cmd == 'plan':
                if args.json:
                    payload = plan_to_dict(plan, targets)
                    if args.offline:
                        payload['offline'] = {'bars_db': bars_db, 'prices_as_of': targets.prices_as_of.isoformat(),
                                              'today': today.isoformat()}
                    print(json.dumps(payload, indent=2, default=str), file=out)
                else:
                    if args.offline:
                        print(f'OFFLINE DRY RUN: empty sleeve, prices = stored closes of {targets.prices_as_of} '
                              f'from {bars_db}; staleness checked as of {today}; nothing is created', file=out)
                    print(render_plan(plan, targets), file=out)
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
        # status reports rather than refuses, but it must not present holdings
        # from a journal that is behind as if they were the sleeve.
        journal_problem = ''
        try:
            check_journal_current(args.sleeve, gateway)
        except Refusal as exc:
            journal_problem = f'{exc.code}: {exc.message}'
        status = sleeve_status(args.sleeve, gateway, targets_dir=args.targets_dir,
                               targets_file=args.targets_file)
        if args.json:
            payload = status_to_dict(status)
            payload['journal'] = {'current': not journal_problem, 'detail': journal_problem}
            print(json.dumps(payload, indent=2, default=str), file=out)
        else:
            if journal_problem:
                print(f'WARNING: the holdings below are NOT current — {journal_problem}\n', file=out)
            print(render_status(status), file=out)
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
