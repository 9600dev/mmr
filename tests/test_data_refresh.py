"""A refresh job that ran cleanly and refreshed nothing is not a success.

On 2026-07-27 `data_refresh_us` fired 30 minutes after the US close. The vendor
had not published that day's DAILY bar yet, so every symbol came back empty,
every individual call succeeded, no symbol was marked failed, and the job
exited 0. pycron logged `ret: [0]`. The daily series fell a session behind and
stayed there for days, invisible, because nothing in the pipeline distinguished
"nothing to do" from "could not do anything".

The rule under test is the one the batch actually uses, imported rather than
restated: a test that re-implements the condition it is checking passes just as
happily when the production copy is wrong.
"""

from __future__ import annotations

import pytest

from trader.mmr_cli import _is_empty_window_response, _refresh_achieved_nothing


def _summary(completed=0, current=0, failed=0, no_data=0, rows=0):
    return {'completed': completed, 'skipped_up_to_date': current,
            'failed': failed, 'no_data': no_data, 'rows_written': rows}


class TestARefreshThatAchievedNothingIsNotSuccess:
    def test_every_symbol_empty_and_nothing_current(self):
        """The 2026-07-27 shape: 58 symbols, all empty, zero written."""
        assert _refresh_achieved_nothing(_summary(no_data=58)) is True

    def test_a_partially_empty_result_is_still_success(self):
        """One symbol with no session in the window is ordinary. Flagging it
        would make the check cry wolf, and a check that cries wolf gets turned
        off, which is worse than not having it."""
        assert _refresh_achieved_nothing(
            _summary(completed=55, no_data=3, rows=12_000)) is False

    def test_everything_already_current_is_a_successful_no_op(self):
        """Re-running a refresh with nothing to do must not look like failure,
        or every second run would report one."""
        assert _refresh_achieved_nothing(_summary(current=58)) is False

    def test_an_empty_universe_is_not_blamed_on_the_source(self):
        """No symbols and no empties is a configuration problem, reported
        elsewhere. Attributing it here would send someone to look at the
        vendor."""
        assert _refresh_achieved_nothing(_summary()) is False

    def test_missing_keys_do_not_crash_the_check(self):
        """Summaries come from a function that has grown fields over time; a
        freshness check must not be the thing that breaks on an old shape."""
        assert _refresh_achieved_nothing({}) is False
        assert _refresh_achieved_nothing({'no_data': 4}) is True

    def test_none_values_are_treated_as_zero(self):
        assert _refresh_achieved_nothing(
            {'completed': None, 'skipped_up_to_date': None, 'no_data': 7}) is True


class TestAnEmptyWindowIsNotAFailure:
    """TwelveData reports "I have nothing for that window" as an HTTP 400,
    which is indistinguishable from a real error unless the message is read.

    The incremental refresh always asks for the current edge, and today's daily
    bar does not exist until after the close, so a HEALTHY run hits this for
    most symbols. Counting it as a failure made a normal run report 46 of 58
    symbols failed, which is why the job's own success flag was useless and why
    adding an exit code to it would have made cron cry wolf every single day.

    The risk now runs the other way: a real failure quietly reclassified as
    "nothing to fetch" would hide exactly what this is meant to surface. So the
    match is narrow, and the cases that must STAY failures are pinned.
    """

    def test_the_vendors_no_data_response_is_not_a_failure(self):
        exc = Exception('{"code":400,"message":"No data is available on the '
                        'specified dates. Try setting different start/end dates.",'
                        '"status":"error","meta":{"symbol":"CAT"}}')
        assert _is_empty_window_response(exc) is True

    def test_case_does_not_matter(self):
        assert _is_empty_window_response(Exception('NO DATA AVAILABLE')) is True

    @pytest.mark.parametrize('message', [
        '401 Unauthorized: invalid api key',
        '429 Too Many Requests: rate limit exceeded',
        'HTTPSConnectionPool: Read timed out',
        'ConnectionResetError: [Errno 104] Connection reset by peer',
        '500 Internal Server Error',
        'JSONDecodeError: Expecting value: line 1 column 1',
        'symbol not found',
    ])
    def test_real_failures_stay_failures(self, message):
        """Auth, rate limits, timeouts, malformed responses and unknown symbols
        all mean the data did not arrive when it should have. Swallowing any of
        them as "empty window" would recreate the silence this whole change
        exists to remove."""
        assert _is_empty_window_response(Exception(message)) is False
