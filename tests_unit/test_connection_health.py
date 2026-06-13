"""Health snapshot, connect/disconnect bookkeeping, active subscriptions, reconnect."""
from __future__ import annotations


def test_initial_health(make_connection):
    connection, client = make_connection()
    health = connection.health
    assert health.connected is True
    assert health.client_id == "test-client"
    assert health.subscriptions == ()
    assert health.connect_count == 0
    assert health.disconnect_count == 0
    assert health.last_connect_at is None
    assert health.last_disconnect_at is None


def test_connect_disconnect_bookkeeping(make_connection):
    connection, client = make_connection()

    connection._on_connect(client, None, {}, 0)
    after_connect = connection.health
    assert after_connect.connect_count == 1
    assert after_connect.last_connect_at is not None

    connection._on_disconnect(client, None, 0)
    after_disconnect = connection.health
    assert after_disconnect.disconnect_count == 1
    assert after_disconnect.last_disconnect_at is not None
    # A second connect (reconnect) keeps counting up.
    connection._on_connect(client, None, {}, 0)
    assert connection.health.connect_count == 2


def test_active_subscriptions_tracked(make_connection):
    connection, client = make_connection()

    def _cb(*_):
        pass

    connection.subscribe("a/b", _cb)
    connection.subscribe("c/+", _cb)
    assert set(connection.active_subscriptions) == {"a/b", "c/+"}
    assert set(connection.health.subscriptions) == {"a/b", "c/+"}

    connection.unsubscribe("a/b")
    assert connection.active_subscriptions == ("c/+",)


def test_seconds_since_inbound(make_connection):
    connection, client = make_connection()
    client.set_inbound_age(5.0)
    ssi = connection.health.seconds_since_inbound
    assert ssi is not None and ssi >= 4.0


def test_reconnect_delegates_to_client(make_connection):
    connection, client = make_connection()
    connection.reconnect()
    assert client.reconnect_calls == 1
