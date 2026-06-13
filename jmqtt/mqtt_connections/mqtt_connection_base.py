from __future__ import annotations

import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from typing import Callable, Any, Dict, List, Optional

from jmqtt import MQTTMessage
from jmqtt.errors import SubscribeError, SubscribeRejected, SubscribeTimeout
from jmqtt.setup_logging import get_logger
from jmqtt.types import QualityOfService as QoS
from .connection_health import ConnectionHealth
from .zombie_watchdog import ZombieWatchdog

logger = get_logger("MqttConnectionBase")

# Per MQTT, a SUBACK return/reason code >= 0x80 means the broker refused the
# subscription (v3 failure code 0x80, v5 reason codes >= 128).
SUBACK_FAILURE_THRESHOLD = 0x80


def get_rc(rc):
    rc = getattr(rc, "value", rc)
    success = rc == 0
    return success, rc


def invoke_callbacks(callbacks, callback_name, *args, **kwargs):
    for callback in callbacks:
        try:
            callback(*args, **kwargs)
        except Exception as e:
            logger.warning(f"{callback_name} handler error: {e}")


class MqttConnectionBase:
    def __init__(self):
        self._client = None
        self._client_id: str | None = None
        self._connection_parameters = None
        self._availability_topic = None

        self._subscription_handlers: Dict[str, Callable[[Any, mqtt.Client, Any, MQTTMessage], None]] = {}
        self._on_connect_callbacks = []
        self._before_disconnect_callbacks = []
        self._on_disconnect_callbacks = []

        # Liveness bookkeeping owned by jmqtt (independent of paho internals).
        self._connect_count = 0
        self._disconnect_count = 0
        self._last_connect_at: Optional[float] = None
        self._last_disconnect_at: Optional[float] = None

        # SUBACK correlation for subscribe(wait_for_ack=True). Events let a caller
        # block until the matching SUBACK arrives; results carry the granted codes
        # and double as a stash for acks that arrive before the caller waits.
        self._sub_lock = threading.Lock()
        self._sub_events: Dict[int, threading.Event] = {}
        self._sub_results: Dict[int, List[int]] = {}

        self._watchdog: Optional[ZombieWatchdog] = None

    def inject_client(
            self,
            client: mqtt.Client,
            connection_parameters: dict,
            availability_topic: str | None,
            client_id: str | None = None,
    ) -> None:
        self._client: mqtt.Client = client
        self._client_id = client_id
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message_handler
        self._client.on_subscribe = self._on_subscribe
        self._connection_parameters = connection_parameters
        self._availability_topic = availability_topic

    @property
    def availability_topic(self) -> str | None:
        return self._availability_topic

    @property
    def client_id(self) -> str | None:
        return self._client_id

    @property
    def is_connected(self):
        return self._client.is_connected()

    @property
    def active_subscriptions(self) -> tuple[str, ...]:
        """Topic filters currently registered on this connection.

        These are re-applied on every (re)connect by the subscribing code, so the
        list reflects intent; after a reconnect, log it to confirm the broker was
        actually re-subscribed.

        :return: Tuple of registered topic filters.
        """
        return tuple(self._subscription_handlers.keys())

    @property
    def health(self) -> ConnectionHealth:
        """Point-in-time snapshot of connection liveness for diagnostics/watchdog.

        :return: A frozen :class:`ConnectionHealth`.
        """
        connected = False
        try:
            connected = bool(self._client.is_connected())
        except Exception:
            pass
        return ConnectionHealth(
            connected=connected,
            client_id=self._client_id,
            subscriptions=tuple(self._subscription_handlers.keys()),
            connect_count=self._connect_count,
            disconnect_count=self._disconnect_count,
            last_connect_at=self._last_connect_at,
            last_disconnect_at=self._last_disconnect_at,
            seconds_since_inbound=self._seconds_since_inbound(),
        )

    def _seconds_since_inbound(self) -> Optional[float]:
        """Seconds since the broker last sent any packet (best-effort).

        Reads paho's internal last-inbound timestamp, which is updated on every
        received packet including PINGRESP - the correct liveness signal. Returns
        ``None`` if nothing has been received yet or the value is unavailable.
        """
        last_in = getattr(self._client, "_last_msg_in", None)
        if not last_in:
            return None
        try:
            # paho stamps _last_msg_in with its own time_func; use the same clock.
            return max(0.0, mqtt.time_func() - last_in)
        except Exception:
            return None

    def enable_zombie_watchdog(self, idle_factor: float = 2.0, check_interval: Optional[float] = None) -> None:
        """Attach an optional background guard that forces a reconnect on a
        half-dead ("zombie") connection. The guard is started by :meth:`connect`
        and stopped by :meth:`close`. Off unless this is called (usually via the
        builder).

        :param idle_factor: Multiplier on keep-alive after which a still-connected
            link with no inbound traffic is considered dead.
        :param check_interval: Seconds between checks. Defaults to half keep-alive.
        """
        keepalive = (self._connection_parameters or {}).get("keepalive", 60)
        self._watchdog = ZombieWatchdog(self, keepalive, idle_factor=idle_factor, check_interval=check_interval)

    def reconnect(self) -> None:
        """Force a reconnect over the existing connect path.

        Re-opens the socket and replays CONNECT via paho, which fires on_connect
        again so every registered on_connect callback (re-subscribe, availability,
        retained state) runs exactly as on the first connect. Unlike
        ``disconnect()`` + ``connect()`` this sends no clean DISCONNECT, so the
        Last Will and configured auto-reconnect behaviour stay intact.

        Call from any thread other than the network-loop callback thread.

        :return: None
        """
        self._client.reconnect()

    def connect(self, blocking: bool = False, **connection_parameters):
        """
        Connect to Mqtt Broker.

        :param blocking: If True, run loop_forever(). If False, start a background network loop.
        :param connection_parameters: Kwargs to override connect parameters
        :return: MQTTConnectionV3 or MQTTConnectionV5 wrapper depending on protocol.
        """
        self._client.connect(**self._connection_parameters, **connection_parameters)

        # Start the guard before loop_forever() (which blocks) so it runs either way.
        if self._watchdog is not None:
            self._watchdog.start()

        if blocking:
            self._client.loop_forever()
        else:
            self._client.loop_start()

        return self

    def _version_filter(self, version3, version5):
        if self._client.protocol == mqtt.MQTTv5:
            return version5
        else:
            return version3

    def _on_connect_version_parameter_filter(self, client, userdata, flags, properties):
        return self._version_filter((self, client, userdata, flags), (self, client, userdata, flags, properties))

    def _on_disconnect_version_parameter_filter(self, client, userdata, rc, properties):
        return self._version_filter((client, userdata, rc), (client, userdata, rc, properties))

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        success, rc = get_rc(rc)

        if success:
            self._connect_count += 1
            self._last_connect_at = time.time()
            logger.info("MQTT connected")
            invoke_callbacks(self._on_connect_callbacks, "On Connect",
                             *self._on_connect_version_parameter_filter(client, userdata, flags, properties))
        else:
            logger.error(f"MQTT connect failed rc={rc}")

    def _on_disconnect(self, client, userdata, rc, properties=None):
        success, rc = get_rc(rc)

        self._disconnect_count += 1
        self._last_disconnect_at = time.time()

        if success:
            logger.info("MQTT disconnected")
        else:
            rs = getattr(properties, "ReasonString", None)
            if rs:
                logger.warning(f"MQTT disconnected unexpectedly rc={rc} reason={rs}")
            else:
                logger.warning(f"MQTT disconnected unexpectedly rc={rc}")

        invoke_callbacks(self._on_disconnect_callbacks, "On Disconnect",
                         *self._on_disconnect_version_parameter_filter(client, userdata, rc, properties))

    def _on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        """SUBACK handler for subscribe(wait_for_ack=True).

        Works across paho's v3/v5 callback shapes: the 4th argument is a list of
        granted QoS ints (v3) or reason codes (v5); both are normalised to ints.
        Results are only recorded when a caller is actually waiting on this mid;
        fire-and-forget subscribes are ignored so their acks never accumulate.
        """
        codes = [int(getattr(code, "value", code)) for code in (granted_qos or [])]
        with self._sub_lock:
            event = self._sub_events.get(mid)
            if event is None:
                return
            self._sub_results[mid] = codes
        event.set()

    def _on_message_handler(self, client, userdata, msg: mqtt.MQTTMessage):
        message = MQTTMessage(msg)
        on_message_callbacks = [handler for topic, handler in self._subscription_handlers.items()
                                if mqtt.topic_matches_sub(topic, message.topic)]
        invoke_callbacks(on_message_callbacks, f"On Message(Topic: {message.topic})", self, client, userdata, message)

    def _publish(self, topic: str, payload, qos: QoS = QoS.AtMostOnce, retain: bool = False,
                 properties: Optional[Properties] = None, wait_for_publish: bool = False) -> mqtt.MQTTMessageInfo:
        info = self._client.publish(topic, payload, qos, retain, properties)
        if wait_for_publish:
            info.wait_for_publish()
        return info

    def _subscribe(self, topic: str, on_message: Callable[[Any, mqtt.Client, Any, MQTTMessage], None],
                   wait_for_ack: bool = False, ack_timeout: float = 10.0, **kwargs) -> tuple[int, int]:
        self._subscription_handlers[topic] = on_message
        if not wait_for_ack:
            return self._client.subscribe(topic, **kwargs)

        # Hold the lock across subscribe + waiter registration so the SUBACK
        # callback (network-loop thread) cannot be processed before the waiter
        # exists. paho never delivers the SUBACK synchronously here, so this does
        # not deadlock.
        with self._sub_lock:
            result, mid = self._client.subscribe(topic, **kwargs)
            if result != mqtt.MQTT_ERR_SUCCESS:
                self._subscription_handlers.pop(topic, None)
                raise SubscribeError(f"subscribe to '{topic}' could not be sent (rc={result})")
            event = threading.Event()
            self._sub_events[mid] = event

        acked = event.wait(ack_timeout)
        with self._sub_lock:
            self._sub_events.pop(mid, None)
            codes = self._sub_results.pop(mid, None)

        if not acked or codes is None:
            # Keep the handler: the subscription may yet become active and we only
            # failed to observe the SUBACK in time. Dropping it would silently
            # discard messages that might still arrive.
            raise SubscribeTimeout(f"no SUBACK for '{topic}' within {ack_timeout}s")
        failed = [code for code in codes if code >= SUBACK_FAILURE_THRESHOLD]
        if failed:
            # The broker refused the subscription, so no messages will arrive;
            # drop the now-useless handler.
            self._subscription_handlers.pop(topic, None)
            raise SubscribeRejected(f"broker rejected subscription to '{topic}' (codes={codes})")
        return result, mid

    def unsubscribe(self, *topics: str):
        """
        Remove local handlers and send UNSUBSCRIBE.

        :param topics: One or more topic filters to unsubscribe from.
        :return: None
        """
        topics = list(topics)
        for topic in topics:
            self._subscription_handlers.pop(topic, None)
        self._client.unsubscribe(topics)

    def close(self):
        """
        Stop network loop and disconnect cleanly.

        :return: None
        """
        if self._watchdog is not None:
            self._watchdog.stop()
        invoke_callbacks(self._before_disconnect_callbacks, "Before Disconnect", self)
        self._client.loop_stop()
        self._client.disconnect()
