"""Worker bar counts retain observed prefixes through a journal outage."""

import datetime as dt

import pytest

from trader.strategy import auto_executor as execution
from trader.strategy.auto_executor import AutoExecutor, BarWork
from trader.strategy.execution_intents import IntentStore, timestamp_text


OWNER = ("bar_progress_contract", 1111)
ENTRY = dt.datetime(2026, 7, 6, tzinfo=dt.timezone.utc)


def bar(number):
    return ENTRY + dt.timedelta(minutes=number)


@pytest.fixture
def counter(tmp_path):
    executor = AutoExecutor.__new__(AutoExecutor)
    path = str(tmp_path / "bar-progress.duckdb")
    executor.intents = IntentStore(path)
    try:
        yield executor, path
    finally:
        executor.intents.journal.close()


@pytest.mark.parametrize("optional_field", ["entry_only", "observations_only"])
def test_worker_keeps_legacy_full_count_with_partial_optional_metadata(counter, optional_field):
    executor, _ = counter
    metadata = ({"entry_bar_ts": ENTRY} if optional_field == "entry_only" else
                {"observed_bar_timestamps": (bar(1),)})
    work = BarWork(*OWNER, bar(1), 7, **metadata)
    assert executor._bar_count(work, durable=True) == 7


def test_worker_keeps_coalesced_prefix_when_disk_write_fails_and_frames_shrink(counter, monkeypatch):
    executor, path = counter
    store = executor.intents
    assert store.observe_bar_count(*OWNER, ENTRY, (bar(1),)) == 1
    coalesced = BarWork(*OWNER, bar(3), 3, entry_bar_ts=ENTRY,
                        observed_bar_timestamps=(bar(3),),
                        observed_progress=(timestamp_text(bar(3)), 3))
    failures = []
    monkeypatch.setattr(execution.logging, "exception", lambda *args, **kwargs: failures.append(True))

    def unavailable():
        raise OSError("bar-progress journal unavailable")

    with monkeypatch.context() as fault:
        fault.setattr(store.journal, "transaction", unavailable)
        assert executor._bar_count(coalesced, durable=True) == 3
    # The earlier frames have left retention. The next durable worker
    # observation must include the prefix remembered during the outage.
    next_work = BarWork(*OWNER, bar(4), 1, entry_bar_ts=ENTRY,
                       observed_bar_timestamps=(bar(4),))
    assert executor._bar_count(next_work, durable=True) == 4
    restarted = IntentStore(path)
    try:
        assert restarted.preview_bar_count(*OWNER, ENTRY, ()) == 4
    finally:
        restarted.journal.close()
    assert failures == [True]
