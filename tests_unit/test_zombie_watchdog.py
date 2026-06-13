"""ZombieWatchdog: triggers on stale-but-connected, stays quiet otherwise."""
from __future__ import annotations

import time


def _wait_for(predicate, timeout=2.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_watchdog_reconnects_on_zombie(make_connection):
    connection, client = make_connection(keepalive=1)  # threshold = 2s at idle_factor 2
    connection.enable_zombie_watchdog(idle_factor=2.0, check_interval=0.05)
    client._connected = True
    client.set_inbound_age(10.0)  # well past the 2s threshold

    connection._watchdog.start()
    assert _wait_for(lambda: client.reconnect_calls >= 1, timeout=2.0)
    connection._watchdog.stop()


def test_watchdog_quiet_when_traffic_fresh(make_connection):
    connection, client = make_connection(keepalive=1)
    connection.enable_zombie_watchdog(idle_factor=2.0, check_interval=0.05)
    client._connected = True
    client.set_inbound_age(0.0)  # fresh inbound traffic

    connection._watchdog.start()
    time.sleep(0.4)  # several check intervals
    connection._watchdog.stop()
    assert client.reconnect_calls == 0


def test_watchdog_quiet_when_disconnected(make_connection):
    # A genuine disconnect is auto-reconnect's job; the watchdog only targets
    # links that still claim to be connected.
    connection, client = make_connection(keepalive=1)
    connection.enable_zombie_watchdog(idle_factor=2.0, check_interval=0.05)
    client._connected = False
    client.set_inbound_age(10.0)

    connection._watchdog.start()
    time.sleep(0.4)
    connection._watchdog.stop()
    assert client.reconnect_calls == 0
