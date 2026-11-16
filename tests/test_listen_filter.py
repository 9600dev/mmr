"""`mmr listen SYMBOL` shows that symbol's ticks. Every stream is published on
one topic, and the subscriber used to pass all of them to its callback:
`listen F` printed GOOGL, XLK, T and the rest (found live 2026-11-16)."""
from types import SimpleNamespace

import pytest

from trader.sdk import tick_is_for


def _tick(con_id):
    return SimpleNamespace(contract=SimpleNamespace(conId=con_id, symbol='X'), bid=1.0, ask=1.1, last=1.05)


def test_the_instrument_asked_for_passes():
    assert tick_is_for(_tick(9599491), 9599491) is True


def test_another_instrument_does_not():
    assert tick_is_for(_tick(208813719), 9599491) is False


@pytest.mark.parametrize('carried', ['9599491', 9599491.0])
def test_the_conid_is_compared_as_a_number(carried):
    assert tick_is_for(_tick(carried), 9599491) is True


@pytest.mark.parametrize('tick', [SimpleNamespace(), SimpleNamespace(contract=None),
                                  SimpleNamespace(contract=SimpleNamespace(conId=0)),
                                  SimpleNamespace(contract=SimpleNamespace(conId=None)),
                                  SimpleNamespace(contract=SimpleNamespace(conId='n/a')), 'a raw message'])
def test_a_tick_that_does_not_say_what_it_is_for_is_passed_on(tick):
    assert tick_is_for(tick, 9599491) is True


@pytest.mark.parametrize('wanted', [0, None])
def test_a_subscriber_that_does_not_know_its_own_conid_sees_everything(wanted):
    assert tick_is_for(_tick(208813719), wanted) is True
