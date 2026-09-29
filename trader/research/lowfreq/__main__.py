"""CLI (D02): ``python -m trader.research.lowfreq evaluate <family>/<id> [--json] [--quiet]``
or ``... evaluate <family>/<id> --fixed-params '<json>' --start D --end D``."""
from __future__ import annotations

import argparse
import json
import sys


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog='python -m trader.research.lowfreq')
    sub = p.add_subparsers(dest='cmd', required=True)
    ev = sub.add_parser('evaluate', help='evaluate a registered candidate (D01 protocol)')
    ev.add_argument('target', help='<family>/<candidate_id>')
    ev.add_argument('--json', action='store_true', help='print the full results JSON instead of the summary')
    ev.add_argument('--quiet', action='store_true', help='print only the verdict line')
    ev.add_argument('--fixed-params', default=None, help='JSON of one parameter cell: fixed-cell mode (no selection)')
    ev.add_argument('--start', default=None, help='fixed-cell window start (YYYY-MM-DD, inclusive)')
    ev.add_argument('--end', default=None, help='fixed-cell window end (YYYY-MM-DD, inclusive)')
    args = p.parse_args(argv)

    import logging
    from trader.research.lowfreq.evaluate import evaluate, render_summary
    # The root rich handler writes INFO to stdout; the backtester's per-run
    # "panel: N instruments x M periods" line would otherwise precede the
    # verdict / JSON and break machine readers of --quiet / --json output.
    logging.getLogger('backtester').setLevel(logging.WARNING)
    fixed = None
    if args.fixed_params is not None:
        try:
            fixed = json.loads(args.fixed_params)
        except json.JSONDecodeError as exc:
            print(f'--fixed-params is not valid JSON: {exc}', file=sys.stderr)
            return 2
        if not isinstance(fixed, dict) or not args.start or not args.end:
            print('fixed-cell mode needs --fixed-params <json object> --start D --end D', file=sys.stderr)
            return 2
    elif args.start or args.end:
        print('--start/--end are only valid with --fixed-params', file=sys.stderr)
        return 2
    try:
        result = evaluate(args.target, fixed_params=fixed, start=args.start, end=args.end)
    except Exception as exc:                          # noqa: BLE001 - reported, ledgered upstream
        print(f'evaluate failed: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    elif args.quiet:
        print(f"{result['family']}/{result['id']}: {result['verdict']}")
    else:
        print(render_summary(result))
    return 0


if __name__ == '__main__':
    sys.exit(main())
