"""subscribe(wait_for_ack=True): success, deferred ack, timeout, rejection, send error."""
from __future__ import annotations

import time

import paho.mqtt.client as mqtt
import pytest

from jmqtt import SubscribeError, SubscribeRejected, SubscribeTimeout


def _cb(*_):
    pass


def test_wait_for_ack_success_immediate(make_connection):
    connection, client = make_connection()
    client.ack_mode = "sync"
    client.ack_codes = [1]
    result, mid = connection.subscribe("x/y", _cb, qos=1, wait_for_ack=True, ack_timeout=2.0)
    assert result == mqtt.MQTT_ERR_SUCCESS
    assert "x/y" in connection.active_subscriptions


def test_wait_for_ack_success_deferred(make_connection):
    connection, client = make_connection()
    client.ack_mode = "delayed"  # SUBACK arrives from a timer thread after subscribe returns
    client.ack_codes = [1]
    result, _mid = connection.subscribe("x/y", _cb, qos=1, wait_for_ack=True, ack_timeout=2.0)
    assert result == mqtt.MQTT_ERR_SUCCESS


def test_wait_for_ack_timeout(make_connection):
    connection, client = make_connection()
    client.ack_mode = "none"  # broker never confirms
    with pytest.raises(SubscribeTimeout):
        connection.subscribe("x/y", _cb, qos=1, wait_for_ack=True, ack_timeout=0.2)
    # Handler is kept on timeout: the subscription may still become active later.
    assert "x/y" in connection.active_subscriptions


def test_wait_for_ack_rejected(make_connection):
    connection, client = make_connection()
    client.ack_mode = "sync"
    client.ack_codes = [0x80]  # broker refused
    with pytest.raises(SubscribeRejected):
        connection.subscribe("x/y", _cb, qos=1, wait_for_ack=True, ack_timeout=2.0)
    # Rejected subscriptions drop the now-useless handler.
    assert "x/y" not in connection.active_subscriptions


def test_wait_for_ack_send_failure(make_connection):
    connection, client = make_connection()
    client.sub_result = mqtt.MQTT_ERR_NO_CONN
    with pytest.raises(SubscribeError):
        connection.subscribe("x/y", _cb, qos=1, wait_for_ack=True, ack_timeout=2.0)
    assert "x/y" not in connection.active_subscriptions


def test_fire_and_forget_default_unchanged(make_connection):
    connection, client = make_connection()
    client.ack_mode = "none"  # would time out if we waited
    result, mid = connection.subscribe("x/y", _cb, qos=1)  # no wait_for_ack
    assert result == mqtt.MQTT_ERR_SUCCESS
    assert isinstance(mid, int)


def test_fire_and_forget_does_not_accumulate(make_connection):
    # Regression guard: acks for fire-and-forget subscribes must not pile up in
    # the correlation maps (they are only kept while a caller is waiting).
    connection, client = make_connection()
    client.ack_mode = "sync"  # broker acks, but nobody is waiting on it
    for i in range(5):
        connection.subscribe(f"x/{i}", _cb, qos=1)
    time.sleep(0.1)  # let the async acks be delivered
    assert connection._sub_results == {}
    assert connection._sub_events == {}
