"""Observed holding bars survive retention, replay, and a local journal outage."""
import datetime as dt

import pytest

from trader.strategy.execution_intents import IntentStore


ENTRY = dt.datetime(2026, 7, 6, tzinfo=dt.timezone.utc)


def bar(day):
    return ENTRY + dt.timedelta(days=day)


def test_observed_bar_count_survives_shrinking_frame_and_restart(tmp_path):
    path = str(tmp_path / 'execution.duckdb')
    first = IntentStore(path)
    assert first.observe_bar_count('owned', 1111, ENTRY, (bar(1), bar(2), bar(2))) == 2
    restarted = IntentStore(path)
    assert restarted.preview_bar_count('owned', 1111, ENTRY, (bar(2), bar(3))) == 3
    assert restarted.observe_bar_count('owned', 1111, ENTRY, (bar(2), bar(3))) == 3
    assert restarted.observe_bar_count('owned', 1111, ENTRY, (bar(1), bar(2))) == 3
    assert restarted.observe_bar_count('owned', 1111, bar(3), (bar(2), bar(3), bar(4))) == 1


def test_bar_progress_seen_during_journal_outage_survives_recovery(tmp_path, monkeypatch):
    path = str(tmp_path / 'execution.duckdb')
    store = IntentStore(path)
    assert store.observe_bar_count('owned', 1111, ENTRY, (bar(1),)) == 1
    with monkeypatch.context() as patch:
        def unavailable():
            raise OSError('journal unavailable')
        patch.setattr(store.journal, 'transaction', unavailable)
        with pytest.raises(OSError):
            store.observe_bar_count('owned', 1111, ENTRY, (bar(2),))
        assert store.preview_bar_count('owned', 1111, ENTRY, (bar(2),), remember=True) == 2
    # The old observation has already left the runtime's retained frame.
    assert store.observe_bar_count('owned', 1111, ENTRY, (bar(3),)) == 3
    assert IntentStore(path).preview_bar_count('owned', 1111, ENTRY, ()) == 3


def test_bar_progress_uses_utc_instants_and_ignores_entry_and_older_bars(tmp_path):
    store = IntentStore(str(tmp_path / 'execution.duckdb'))
    local = dt.timezone(dt.timedelta(hours=-7))
    assert store.observe_bar_count('owned', 1111, ENTRY.astimezone(local),
                                   (bar(0), bar(1), bar(1).astimezone(local))) == 1
    assert store.preview_bar_count('owned', 1111, ENTRY, (bar(2),)) == 2
