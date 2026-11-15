#!/usr/bin/env python3
"""Keep the paper sleeves' books current, and on a month's first session write the decision.

docs/LOWFREQ_LIVE.md lists what a sleeve needs every session after the close
(refresh the point-in-time bars, reconcile fills into the journal, track the
paper ledger) and once a month (refresh fundamentals, generate the targets
file). All of it was manual, and none of it had been run for six weeks when
this was written (2026-11-15): the bars stopped at the first rebalance, the
fills journal did not exist, the ledger was empty and November's decision
session passed without a targets file. This is those steps as one job that
pycron runs each weekday evening:

  every run, for the last COMPLETED NYSE session D
    1. scripts/build_pit_universe.py --incremental --end D          (once)
    2. scripts/fetch_splits.py                                      (once)
    3. scripts/pit_split_adjust.py --apply --accept-refusals        (once)
    4. python -m trader.rebalance reconcile --sleeve S              (per sleeve)
    5. python -m trader.lowfreq_live track --sleeve S

  additionally, when D is the first session of its month and S has no D.json
    6. scripts/fetch_fundamentals.py --incremental                  (once; needs SEC_API_KEY)
    7. python -m trader.lowfreq_live targets <family>/<id> --sleeve S --capital C --as-of D
       (candidate, capital and params are taken from the sleeve's newest targets file)
    8. python -m trader.rebalance plan --sleeve S   -> saved under ~/.local/share/mmr/reports/

Steps 2 and 3 keep prices and share counts on one split basis: bars are
adjusted as of the day they were fetched, so a split executed since leaves
the days before it on the old basis (a split-sized jump in the series, and a
market cap wrong by the ratio) until step 3 puts them on today's.

It never proposes, approves or trades: step 8 is the dry run, and turning it
into PENDING proposals (`rebalance propose`) and approving them stays the
operator's act (D15). A step that fails stops the later steps of THAT sleeve —
each one reads what the one before wrote — and the job exits non-zero, which
pycron reports. Two steps can finish with a complaint that is REPORTED (the
job still exits non-zero) without stopping what follows, because what follows
has its own gate and a month's decision is not retried the next night:
fundamentals when some names could not be read (`targets` refuses stale
fundamentals itself), and the split adjustment when it refused a series it
could not explain (everything it could, it did). A sleeve is any directory
under the targets directory holding at least one `<as_of>.json`.

    python scripts/sleeve_upkeep.py                  # what pycron runs
    python scripts/sleeve_upkeep.py --dry-run        # print the steps, run nothing
    python scripts/sleeve_upkeep.py --sleeve paper_s2p --session 2026-12-01
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Sequence

REPO = Path(__file__).resolve().parent.parent
REPORTS = Path('~/.local/share/mmr/reports').expanduser()


# Exit statuses that mean "done, with a complaint" rather than "did not run".
FUNDAMENTALS_SOME_NAMES_FAILED = 1      # scripts/fetch_fundamentals.py (2 = aborted: an outage)
ADJUST_SOMETHING_REFUSED = 1            # scripts/pit_split_adjust.py: a NEW refusal (2 = could not run)


@dataclass(frozen=True)
class Step:
    """One command of the upkeep. ``sleeve`` is '' for a step shared by all.

    ``continue_on``: non-zero exit statuses after which the LATER steps still
    run. The step is reported as failed and the job exits non-zero either way.
    """
    name: str
    argv: Sequence[str]
    sleeve: str = ''
    save_output_to: Optional[Path] = None
    continue_on: Sequence[int] = ()


@dataclass
class Outcome:
    step: Step
    returncode: Optional[int]          # None = not run (an earlier step of the sleeve failed)
    output: str = ''
    skipped_because: str = ''


@dataclass(frozen=True)
class SleeveSpec:
    """What the sleeve's next targets file is generated from: its newest one."""
    sleeve: str
    candidate: str
    capital: float
    params: dict = field(default_factory=dict)
    newest_as_of: Optional[dt.date] = None


def last_completed_session(now: dt.datetime, calendar) -> dt.date:
    """The most recent NYSE session whose regular close is at or before ``now``.

    ``calendar`` is an ``exchange_calendars`` calendar. An early close counts
    from its own (earlier) time, and a weekend or holiday answers with the
    session before it.
    """
    import pandas as pd
    moment = pd.Timestamp(now)
    moment = moment.tz_localize('UTC') if moment.tz is None else moment.tz_convert('UTC')
    day = moment.tz_convert(calendar.tz).normalize().tz_localize(None)
    session = calendar.date_to_session(day, direction='previous')
    if pd.Timestamp(calendar.session_close(session)) > moment:
        session = calendar.previous_session(session)
    return session.date()


def is_first_session_of_month(session: dt.date, calendar) -> bool:
    import pandas as pd
    previous = calendar.previous_session(pd.Timestamp(session))
    return (previous.year, previous.month) != (session.year, session.month)


def discover_sleeves(targets_dir: Path) -> List[str]:
    """Directories holding at least one ``<as_of>.json`` (sidecars do not count)."""
    found = []
    if not targets_dir.is_dir():
        return found
    for folder in sorted(path for path in targets_dir.iterdir() if path.is_dir()):
        if any(_as_of(path) is not None for path in folder.glob('*.json')):
            found.append(folder.name)
    return found


def _as_of(path: Path) -> Optional[dt.date]:
    try:
        return dt.date.fromisoformat(path.stem)
    except ValueError:
        return None                     # `<as_of>.benchmark.json` and anything else


def sleeve_spec(targets_dir: Path, sleeve: str) -> SleeveSpec:
    """Candidate, capital and params from the sleeve's newest targets file.

    Raises ``ValueError`` when there is none or it does not say: guessing a
    candidate or a capital would write a decision nobody made.
    """
    dated = sorted((when, path) for path in (targets_dir / sleeve).glob('*.json')
                   if (when := _as_of(path)) is not None)
    if not dated:
        raise ValueError(f'sleeve {sleeve!r} has no <as_of>.json under {targets_dir / sleeve}')
    newest_as_of, newest = dated[-1]
    document = json.loads(newest.read_text(encoding='utf-8'))
    strategy = document.get('strategy') or {}
    family, candidate_id = strategy.get('family'), strategy.get('id')
    capital = document.get('capital_usd')
    if not family or not candidate_id:
        raise ValueError(f'{newest} names no strategy family/id')
    if not isinstance(capital, (int, float)) or isinstance(capital, bool) or not capital > 0:
        raise ValueError(f'{newest} carries no positive capital_usd')
    return SleeveSpec(sleeve=sleeve, candidate=f'{family}/{candidate_id}', capital=float(capital),
                      params=dict(strategy.get('params') or {}), newest_as_of=newest_as_of)


def plan_steps(session: dt.date, sleeves: Sequence[str], targets_dir: Path, *, first_of_month: bool,
               python: str = sys.executable, reports_dir: Path = REPORTS, skip_bars: bool = False) -> List[Step]:
    """The commands for one run, in order. Pure: reads the targets directory, runs nothing."""
    day = session.isoformat()
    steps: List[Step] = []
    if not skip_bars:
        steps.append(Step('bars', (python, 'scripts/build_pit_universe.py', '--incremental', '--end', day)))
        steps.append(Step('splits', (python, 'scripts/fetch_splits.py')))
        # --accept-refusals: a series the tool cannot explain is recorded in
        # pit_split_refusals and left out by every reader of market caps, so
        # the basis moves on the same night instead of the next.
        steps.append(Step('adjust', (python, 'scripts/pit_split_adjust.py', '--apply', '--accept-refusals'),
                          continue_on=(ADJUST_SOMETHING_REFUSED,)))
    due = [sleeve for sleeve in sleeves
           if first_of_month and not (targets_dir / sleeve / f'{day}.json').exists()]
    for sleeve in sleeves:
        steps.append(Step('reconcile', (python, '-m', 'trader.rebalance', 'reconcile', '--sleeve', sleeve), sleeve))
        steps.append(Step('track', (python, '-m', 'trader.lowfreq_live', 'track', '--sleeve', sleeve), sleeve))
    if due:
        steps.append(Step('fundamentals', (python, 'scripts/fetch_fundamentals.py', '--incremental'),
                          continue_on=(FUNDAMENTALS_SOME_NAMES_FAILED,)))
    for sleeve in due:
        spec = sleeve_spec(targets_dir, sleeve)
        argv = [python, '-m', 'trader.lowfreq_live', 'targets', spec.candidate, '--sleeve', sleeve,
                '--capital', f'{spec.capital:g}', '--as-of', day]
        if spec.params:
            argv += ['--params', json.dumps(spec.params, sort_keys=True)]
        steps.append(Step('targets', tuple(argv), sleeve))
        steps.append(Step('plan', (python, '-m', 'trader.rebalance', 'plan', '--sleeve', sleeve), sleeve,
                          save_output_to=reports_dir / f'sleeve_{sleeve}_{day}_plan.txt'))
    return steps


Runner = Callable[[Step], 'tuple[int, str]']


def run_steps(steps: Sequence[Step], runner: Runner) -> List[Outcome]:
    """Run in order. A failed sleeve step stops that sleeve's later steps; a
    failed SHARED step (bars, splits, the adjustment, fundamentals) stops every
    step that reads it — which is everything after it — unless its exit status
    is one the step says to continue on (``Step.continue_on``)."""
    outcomes: List[Outcome] = []
    failed_sleeves: set = set()
    shared_failure = ''
    for step in steps:
        if shared_failure:
            outcomes.append(Outcome(step, None, skipped_because=f'{shared_failure} failed'))
            continue
        if step.sleeve and step.sleeve in failed_sleeves:
            outcomes.append(Outcome(step, None, skipped_because=f'an earlier step for {step.sleeve} failed'))
            continue
        code, output = runner(step)
        outcomes.append(Outcome(step, code, output))
        if code != 0 and code in step.continue_on:
            continue                        # reported as failed; what follows has its own gate
        if code != 0:
            if step.sleeve:
                failed_sleeves.add(step.sleeve)
            else:
                shared_failure = step.name
        elif step.save_output_to is not None:
            step.save_output_to.parent.mkdir(parents=True, exist_ok=True)
            step.save_output_to.write_text(output, encoding='utf-8')
    return outcomes


def _subprocess_runner(environment: dict) -> Runner:
    def run(step: Step) -> 'tuple[int, str]':
        done = subprocess.run(list(step.argv), cwd=str(REPO), env=environment, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        return done.returncode, done.stdout or ''
    return run


def _last_lines(text: str, count: int) -> str:
    return '\n'.join(line for line in text.strip().splitlines()[-count:])


def summarise(outcomes: Sequence[Outcome]) -> 'tuple[bool, List[str]]':
    """``(ok, lines)``. A skipped step is a failure of the run, not a pass."""
    lines, ok = [], True
    for outcome in outcomes:
        label = f'{outcome.step.name}' + (f' [{outcome.step.sleeve}]' if outcome.step.sleeve else '')
        if outcome.returncode is None:
            ok = False
            lines.append(f'SKIPPED {label}: {outcome.skipped_because}')
        elif outcome.returncode != 0:
            ok = False
            lines.append(f'FAILED  {label} (exit {outcome.returncode}):')
            lines.extend('    ' + line for line in _last_lines(outcome.output, 8).splitlines())
        else:
            tail = _last_lines(outcome.output, 1)
            lines.append(f'ok      {label}' + (f': {tail[:200]}' if tail else ''))
            if outcome.step.save_output_to is not None:
                lines.append(f'        plan saved to {outcome.step.save_output_to} — review it, then '
                             f'`python -m trader.rebalance propose --sleeve {outcome.step.sleeve}`')
    return ok, lines


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--sleeve', action='append', default=None,
                        help='only this sleeve (repeatable); default: every sleeve with a targets file')
    parser.add_argument('--session', default=None,
                        help='treat this YYYY-MM-DD as the last completed session (default: from the NYSE calendar)')
    parser.add_argument('--targets-dir', default=None, help='override ~/.local/share/mmr/data/targets')
    parser.add_argument('--skip-bars', action='store_true',
                        help='do not refresh the stored prices (bars, split history, split adjustment)')
    parser.add_argument('--dry-run', action='store_true', help='print the steps and run nothing')
    args = parser.parse_args(argv)

    import exchange_calendars

    from trader.rebalance.targets import default_targets_dir

    calendar = exchange_calendars.get_calendar('XNYS')
    session = (dt.date.fromisoformat(args.session) if args.session
               else last_completed_session(dt.datetime.now(dt.timezone.utc), calendar))
    targets_dir = Path(args.targets_dir).expanduser() if args.targets_dir else Path(default_targets_dir())
    sleeves = args.sleeve or discover_sleeves(targets_dir)
    if not sleeves:
        print(f'sleeve upkeep: no sleeve with a targets file under {targets_dir}; nothing to do')
        return 0
    first = is_first_session_of_month(session, calendar)
    try:
        steps = plan_steps(session, sleeves, targets_dir, first_of_month=first, skip_bars=args.skip_bars)
    except ValueError as ex:
        print(f'sleeve upkeep REFUSED: {ex}', file=sys.stderr)
        return 2
    print(f'sleeve upkeep: session {session}' + (' (first of its month)' if first else '')
          + f', sleeves {", ".join(sleeves)}')
    if args.dry_run:
        for step in steps:
            print('  ' + ' '.join(step.argv))
        return 0

    environment = dict(os.environ)
    environment.setdefault('LOWFREQ_RESEARCH_CONFIG',
                           os.environ.get('TRADER_CONFIG') or str(Path('~/.config/mmr/trader.yaml').expanduser()))
    ok, lines = summarise(run_steps(steps, _subprocess_runner(environment)))
    print('\n'.join(lines))
    print('sleeve upkeep ' + ('OK' if ok else 'FAILED'))
    return 0 if ok else 1


if __name__ == '__main__':
    sys.path.insert(0, str(REPO))
    sys.exit(main())
