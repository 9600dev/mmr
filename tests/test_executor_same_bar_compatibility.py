"""A saturated timer mailbox keeps a corrected legacy lifetime count."""
import datetime as dt

from test_executor_admission_observability import ENTRY, OWNER, _loop_controller
from trader.strategy.auto_executor import check_time_exit


def test_same_bar_legacy_count_correction_reaches_time_exit_policy():
    executor = _loop_controller()
    latest = ENTRY + dt.timedelta(minutes=3)
    # The four-argument compatibility API receives a full lifetime count.
    # A caller can revise that count after recovering older observations
    # without advancing the last completed timestamp. Keep the newest report.
    assert executor.submit_bar(*OWNER, latest, 2)
    assert executor.submit_bar(*OWNER, latest, 3)

    work = executor._bar_overflow[OWNER]
    assert work.bar_ts == latest
    count = executor._bar_count(work, durable=True)
    assert count == 3
    assert check_time_exit(work.bar_ts, count, None, 3) is not None
