"""A full publisher queue must not silence the whole ticker broadcast.

``MultithreadedTopicPubSub.put`` re-raises ``queue.Full``. The call site is
``publish_contract.on_next``, an observer on the SHARED ``contracts_subject``
(one subscription for every published contract, built by ``flat_map`` over
the IB pending-tickers event). With reactivex 4.1 an exception escaping
``on_next`` is turned into a terminal ``on_error`` on that chain, after which
no ticker for ANY contract is published again until restart. The old
``on_error`` then deleted one conId's filter and raised.
"""
import logging
import queue
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import reactivex as rx
import reactivex.operators as ops
from ib_async import Contract, Ticker
from reactivex.disposable import Disposable
from reactivex.subject import Subject

from trader.trading.trading_runtime import Trader


class _Publisher:
    def __init__(self, fail_times=0, error=queue.Full):
        self.fail_times = fail_times
        self.error = error
        self.published = []
        self.calls = 0

    def put(self, topic_item):
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise self.error()
        self.published.append(topic_item)


def _contract(conid):
    return Contract(conId=conid, symbol=f'S{conid}', secType='STK', exchange='SMART', currency='USD')


def _ticker(conid):
    return Ticker(contract=_contract(conid))


def _trader(publisher):
    """A real Trader wired to a real shared Rx chain shaped like IBAIORx's:
    a Subject of ticker SETS flat-mapped into single tickers. 'poison' errors
    the inner observable, which is how an exception surfaces on the chain."""
    t = object.__new__(Trader)
    source = Subject()

    def mapper(tickers):
        if tickers == 'poison':
            return rx.throw(RuntimeError('poisoned tick batch'))
        return rx.from_iterable(tickers)

    subscriptions = []

    def subscribe_contract_direct(contract, delayed=False, **kwargs):
        subscriptions.append(contract.conId)
        return rx.empty()

    t.client = SimpleNamespace(contracts_subject=source.pipe(ops.flat_map(mapper)),
                               subscribe_contract_direct=subscribe_contract_direct,
                               ib=SimpleNamespace(isConnected=lambda: True))
    t.zmq_pubsub_server = publisher
    t.zmq_pubsub_contracts = {}
    t.zmq_pubsub_contract_filters = {}
    t.zmq_pubsub_contract_subscription = Disposable()
    t.zmq_pubsub_published_contracts = {}
    t.zmq_pubsub_dropped_ticks = 0
    t._pubsub_drop_logged_at = 0.0
    t._pubsub_drops_at_last_log = 0
    t._publish_reestablish_times = []
    t._ib_upstream_connected = True
    t.book = None
    t.order_tracker = None
    return t, source, subscriptions


def test_queue_full_once_does_not_kill_the_shared_ticker_stream():
    publisher = _Publisher(fail_times=1)
    trader, source, _ = _trader(publisher)
    trader.publish_contract(_contract(1), delayed=False)
    trader.publish_contract(_contract(2), delayed=False)

    source.on_next({_ticker(1)})           # put raises queue.Full
    source.on_next({_ticker(2)})           # must still be published
    source.on_next({_ticker(1)})

    assert [item[1].contract.conId for item in publisher.published] == [2, 1]
    assert publisher.calls == 3
    assert trader.zmq_pubsub_dropped_ticks == 1
    assert set(trader.zmq_pubsub_contract_filters) == {1, 2}, 'filters were not touched by the drop'


def test_publisher_not_running_is_counted_not_raised():
    publisher = _Publisher(fail_times=2, error=RuntimeError)
    trader, source, _ = _trader(publisher)
    trader.publish_contract(_contract(1), delayed=False)

    source.on_next({_ticker(1), _ticker(1)})
    source.on_next({_ticker(1)})

    assert len(publisher.published) == 1
    assert trader.zmq_pubsub_dropped_ticks == 2


def test_drops_are_logged_at_a_bounded_rate_with_the_count(caplog):
    publisher = _Publisher(fail_times=50)
    trader, source, _ = _trader(publisher)
    trader.publish_contract(_contract(1), delayed=False)
    with caplog.at_level(logging.ERROR):
        for _ in range(50):
            source.on_next({_ticker(1)})
    lines = [r.getMessage() for r in caplog.records if 'ticker publish dropped' in r.getMessage()]
    assert len(lines) == 1, lines
    assert '1 tick(s)' in lines[0] and 'Full' in lines[0]
    assert trader.zmq_pubsub_dropped_ticks == 50


def test_stream_error_rebuilds_publishing_for_every_contract(caplog):
    publisher = _Publisher()
    trader, source, subscriptions = _trader(publisher)
    trader.publish_contract(_contract(1), delayed=False)
    trader.publish_contract(_contract(2), delayed=True)
    assert subscriptions == [1, 2]

    with caplog.at_level(logging.ERROR):
        source.on_next('poison')           # terminal on_error on the shared chain
    source.on_next({_ticker(1)})
    source.on_next({_ticker(2)})

    assert [item[1].contract.conId for item in publisher.published] == [1, 2]
    assert set(trader.zmq_pubsub_contract_filters) == {1, 2}
    assert subscriptions == [1, 2, 1, 2], 'both market-data requests were re-issued'
    assert any('rebuilding' in r.getMessage() for r in caplog.records)


def test_rebuild_is_bounded_when_the_source_keeps_erroring(caplog):
    publisher = _Publisher()
    trader, source, subscriptions = _trader(publisher)
    trader.publish_contract(_contract(1), delayed=False)
    with caplog.at_level(logging.ERROR):
        for _ in range(Trader._PUBLISH_REESTABLISH_LIMIT + 3):
            source.on_next('poison')
    # Rebuilt at most LIMIT times; afterwards the state is left cleared for
    # connected_event's _republish_ticker_subscriptions to restore.
    assert len(subscriptions) == 1 + Trader._PUBLISH_REESTABLISH_LIMIT
    assert trader.zmq_pubsub_contract_filters == {}
    assert any('not rebuilding until the next' in r.getMessage() for r in caplog.records)
    assert trader.zmq_pubsub_published_contracts.keys() == {1}, 'the request is remembered for the reconnect'


def test_pulse_reports_dropped_ticks(caplog):
    publisher = _Publisher(fail_times=3)
    trader, source, _ = _trader(publisher)
    trader.publish_contract(_contract(1), delayed=False)
    for _ in range(3):
        source.on_next({_ticker(1)})
    with caplog.at_level(logging.INFO):
        trader._log_pulse()
    pulse = [r.getMessage() for r in caplog.records if r.getMessage().startswith('pulse ')]
    assert pulse and 'dropped_ticks=3' in pulse[-1], pulse
