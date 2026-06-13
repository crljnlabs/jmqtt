from __future__ import annotations

import threading
from typing import Optional

from ..setup_logging import get_logger

logger = get_logger("ZombieWatchdog")


class ZombieWatchdog:
    """Optional background guard against half-dead ("zombie") MQTT connections.

    paho already detects a broken link via keep-alive and, when auto-reconnect is
    configured, reconnects on its own. This watchdog is a deliberate
    belt-and-suspenders for the case paho can miss: the client still believes it
    is connected (``is_connected()`` is True) yet no packet has arrived for far
    longer than the keep-alive interval - typically a silently dropped TCP
    connection. It then forces a reconnect through the existing connect path, so
    the normal on_connect callbacks (re-subscribe, availability, retained state)
    restore correct operation afterwards.

    It is disabled by default and only ever *repairs the transport*; it never
    touches application state. Detection relies on the network loop running; a
    fully wedged loop thread is out of its scope.
    """

    def __init__(self, connection, keepalive: int, idle_factor: float = 2.0,
                 check_interval: Optional[float] = None) -> None:
        """
        :param connection: The MQTT connection to guard (duck-typed: needs
            ``is_connected``, ``health`` and ``reconnect()``).
        :param keepalive: Connection keep-alive in seconds; the staleness
            threshold is derived from it.
        :param idle_factor: Multiplier on keep-alive after which a still-connected
            link with no inbound traffic is treated as dead. 2.0 means "two missed
            keep-alive windows".
        :param check_interval: Seconds between checks. Defaults to half the
            keep-alive (never below 5s).
        """
        self._connection = connection
        self._idle_threshold = max(1.0, float(keepalive) * float(idle_factor))
        self._check_interval = float(check_interval) if check_interval else max(5.0, float(keepalive) / 2.0)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background guard. Idempotent."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="jmqtt-zombie-watchdog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the guard to stop and join it (unless called from its own thread)."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=self._check_interval + 1.0)
        self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self._check_interval):
            try:
                self._check_once()
            except Exception as exc:
                # Never let the guard thread die on a transient error.
                logger.warning(f"zombie watchdog check failed: {exc}")

    def _check_once(self) -> None:
        # Only zombies look connected. A genuine disconnect is auto-reconnect's job.
        if not self._connection.is_connected:
            return
        idle = self._connection.health.seconds_since_inbound
        if idle is None or idle <= self._idle_threshold:
            return

        logger.warning(
            f"zombie connection detected: no inbound traffic for {idle:.0f}s "
            f"(threshold {self._idle_threshold:.0f}s) while still 'connected' -> forcing reconnect"
        )
        try:
            self._connection.reconnect()
        except Exception as exc:
            logger.warning(f"forced reconnect failed: {exc}")

        # Give the reconnect room to complete before measuring again so a slow
        # reconnect cannot trigger a second reconnect on the next tick.
        self._stop.wait(self._idle_threshold)
