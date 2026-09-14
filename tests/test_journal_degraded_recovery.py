"""A transient journal failure must not disable new exposure for the process lifetime (review 2026-09-11).

``_journal_degraded`` is set at every journal failure site, including read-only
lookups, and was cleared nowhere: one ``database is locked`` (realistic, since
every read runs under BEGIN IMMEDIATE on a file shared by both services)
refused every open until the trader was restarted. It is now re-probed with a
real transaction before an open is refused and cleared on any successful
durable operation. Opens still fail closed at the reservation and audit writes.
"""
from types import SimpleNamespace

import pytest
from ib_async import Contract, Order

from trader.trading.trading_runtime import Trader


def _trader(journal):
    trader = object.__new__(Trader)
    trader.ib_account = 'DU_FAKE'
    trader.duckdb_path = None  # no restore marker to consult
    trader.order_tracker = None
    trader.server_order_journal = lambda: journal
    return trader


def _cash():
    # Forex short-circuits margin_checks right after the journal check.
    return Contract(secType='CASH', symbol='EUR', currency='USD', exchange='IDEALPRO', conId=12087792)


@pytest.mark.asyncio
async def test_degraded_flag_clears_when_the_journal_answers_again():
    probes = []
    trader = _trader(SimpleNamespace(reservations=lambda account: probes.append(account) or []))
    trader._journal_degraded = 'database is locked'

    result = await trader.margin_checks(_cash(), Order(action='BUY', totalQuantity=1))

    assert probes == ['DU_FAKE'], 'the flag is re-tested with a real transaction, not trusted forever'
    assert result.approved, result.reason
    assert trader._journal_degraded == ''


@pytest.mark.asyncio
async def test_a_journal_that_still_fails_keeps_refusing_opens():
    def broken(account):
        raise RuntimeError('database is locked')
    trader = _trader(SimpleNamespace(reservations=broken))
    trader._journal_degraded = 'database is locked'

    result = await trader.margin_checks(_cash(), Order(action='BUY', totalQuantity=1))

    assert not result.approved
    assert result.checks == {'execution_journal': 'unevaluable:durable-storage'}
    assert trader._journal_degraded == 'database is locked'


@pytest.mark.asyncio
async def test_healthy_journal_is_not_probed_on_every_open():
    probes = []
    trader = _trader(SimpleNamespace(reservations=lambda account: probes.append(account) or []))
    trader._journal_degraded = ''

    result = await trader.margin_checks(_cash(), Order(action='BUY', totalQuantity=1))

    assert result.approved and probes == []


def test_recovery_helper_clears_and_reports():
    trader = _trader(SimpleNamespace())
    trader._journal_degraded = 'disk I/O error'
    trader._journal_recovered()
    assert trader._journal_degraded == ''
    trader._journal_recovered()  # idempotent when already healthy
    assert trader._journal_degraded == ''
