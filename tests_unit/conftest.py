"""Broker-free unit-test support.

These tests live in their own directory so they do not inherit the broker
integration fixtures in ``tests/conftest.py`` (which skip the whole session when
no broker env is configured). They drive the connection wrapper against a fake
paho client, so they need no network.
"""
from __future__ import annotations

import threading

import paho.mqtt.client as mqtt
import pytest

from jmqtt import MQTTConnectionV3


class FakeClient:
    """Minimal stand-in for ``paho.mqtt.client.Client``.

    Implements only what the jmqtt connection wrapper touches and records calls
    so tests can assert on them. SUBACK delivery is configurable to exercise the
    wait_for_ack success/timeout/reject paths.
    """

    def __init__(self, protocol=mqtt.MQTTv311):
        self.protocol = protocol
        self.on_connect = None
        self.on_disconnect = None
        self.on_message = None
        self.on_subscribe = None

        self._connected = True
        self._last_msg_in = mqtt.time_func()

        self.reconnect_calls = 0
        self.subscribe_calls = []
        self._next_mid = 1

        # SUBACK behaviour: "sync" (ack inside subscribe), "delayed" (ack from a
        # timer thread), or "none" (never ack). sub_result lets a test simulate a
        # send failure.
        self.ack_mode = "sync"
        self.ack_codes = [1]
        self.ack_delay = 0.05
        self.sub_result = mqtt.MQTT_ERR_SUCCESS

    # --- liveness ---------------------------------------------------------
    def is_connected(self):
        return self._connected

    def set_inbound_age(self, seconds):
        """Pretend the last inbound packet arrived `seconds` ago."""
        self._last_msg_in = mqtt.time_func() - seconds

    # --- paho surface used by the wrapper ---------------------------------
    def subscribe(self, topic, **kwargs):
        mid = self._next_mid
        self._next_mid += 1
        self.subscribe_calls.append((topic, kwargs, mid))
        # Real paho always delivers the SUBACK asynchronously on the network-loop
        # thread, never inside subscribe(); model that with a timer thread so the
        # waiter is registered before the ack is processed.
        if self.sub_result == mqtt.MQTT_ERR_SUCCESS and self.ack_mode in ("sync", "delayed"):
            delay = 0.0 if self.ack_mode == "sync" else self.ack_delay
            threading.Timer(delay, self._deliver_suback, args=(mid,)).start()
        return self.sub_result, mid

    def _deliver_suback(self, mid):
        if self.on_subscribe is not None:
            self.on_subscribe(self, None, mid, list(self.ack_codes))

    def reconnect(self):
        self.reconnect_calls += 1
        self._connected = True
        self._last_msg_in = mqtt.time_func()

    def unsubscribe(self, topics):
        pass

    def publish(self, *args, **kwargs):
        return None

    def connect(self, **kwargs):
        pass

    def disconnect(self):
        self._connected = False

    def loop_start(self):
        pass

    def loop_stop(self):
        pass


@pytest.fixture
def make_connection():
    """Factory returning a fresh (connection, fake_client) pair.

    Any watchdog started during the test is stopped on teardown so no guard
    threads leak between tests.
    """
    created = []

    def _make(keepalive=60):
        client = FakeClient()
        connection = MQTTConnectionV3()
        connection.inject_client(client, {"keepalive": keepalive}, None, client_id="test-client")
        created.append(connection)
        return connection, client

    yield _make

    for connection in created:
        watchdog = connection._watchdog
        if watchdog is not None:
            watchdog.stop()
