"""The approver key authorizes an attempt; it does not describe the order.

The durable intent fingerprint hashed ``bound.arguments`` including
``approver_key``, so (a) a retry of the same intent carrying a different or
newly supplied key was refused as "a different payload", and (b) the journal
held an unsalted SHA-256 of a secret.
"""
import hashlib
import hmac

import pytest

from review.test_review_order_contract import _coordinated_trader, _stock
from trader.trading.trading_runtime import _is_secret_argument


def _fingerprint(trader, intent_id):
    with trader.server_order_journal().journal.transaction() as conn:
        return conn.execute('SELECT fingerprint FROM server_order_intents WHERE intent_id=?',
                            (intent_id,)).fetchone()['fingerprint']


def test_secret_argument_names():
    assert _is_secret_argument('approver_key')
    assert _is_secret_argument('some_password') and _is_secret_argument('api_token') and _is_secret_argument('client_secret')
    for name in ('contract', 'action', 'quantity', 'execution_spec', 'algo_name', 'force_open', 'allow_open',
                 'client_intent_id', 'order_ref', 'keyboard'):
        assert not _is_secret_argument(name), name


@pytest.mark.asyncio
async def test_same_intent_with_a_different_approver_key_is_the_same_intent(tmp_path):
    trader = _coordinated_trader(tmp_path)
    try:
        first = await trader.place_expressive_order(_stock(), 'SELL', 20, {'order_type': 'MARKET'},
                                                    approver_key='first-key', client_intent_id='same-close')
        assert first.is_success(), first.error
        stored = _fingerprint(trader, 'same-close')

        retry = await trader.place_expressive_order(_stock(), 'SELL', 20, {'order_type': 'MARKET'},
                                                    approver_key='rotated-key', client_intent_id='same-close')

        assert not retry.is_success()
        assert 'different account or payload' not in retry.error, retry.error
        assert 'already claimed' in retry.error, 'refused as a duplicate of the SAME intent, not a new payload'
        assert len(trader.placed) == 1
        assert _fingerprint(trader, 'same-close') == stored
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_fingerprint_is_independent_of_the_key_and_does_not_hash_it(tmp_path):
    trader = _coordinated_trader(tmp_path)
    try:
        for intent, key in (('a', 'alpha-secret'), ('b', ''), ('c', 'gamma-secret')):
            result = await trader.place_expressive_order(_stock(), 'SELL', 5, {'order_type': 'MARKET'},
                                                         approver_key=key, client_intent_id=intent)
            assert result.is_success(), result.error
        prints = {intent: _fingerprint(trader, intent) for intent in 'abc'}
        assert len(set(prints.values())) == 1, prints
        for secret in ('alpha-secret', 'gamma-secret'):
            digest = hashlib.sha256(secret.encode()).hexdigest()
            assert not hmac.compare_digest(digest, prints['a'])
    finally:
        trader.order_tracker.close(timeout=1)


@pytest.mark.asyncio
async def test_a_genuinely_different_payload_is_still_refused(tmp_path):
    trader = _coordinated_trader(tmp_path)
    try:
        assert (await trader.place_expressive_order(_stock(), 'SELL', 20, {'order_type': 'MARKET'},
                                                    client_intent_id='fixed')).is_success()
        other = await trader.place_expressive_order(_stock(), 'SELL', 25, {'order_type': 'MARKET'},
                                                    client_intent_id='fixed')
        assert not other.is_success() and 'different account or payload' in other.error
        assert len(trader.placed) == 1
    finally:
        trader.order_tracker.close(timeout=1)
