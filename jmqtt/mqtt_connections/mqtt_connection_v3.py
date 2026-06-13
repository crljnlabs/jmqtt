from __future__ import annotations
from typing import Callable, Any

from paho.mqtt.client import Client
from jmqtt import MQTTMessage

from jmqtt.types import QualityOfService as QoS
from .mqtt_connection_base import MqttConnectionBase


class MQTTConnectionV3(MqttConnectionBase):
    def add_on_connect(self, on_connect: Callable[[MQTTConnectionV3, Client, Any, dict[str, int]], None]):
        """
        Register a user on_connect callback.
        Callback is invoked after a successful connect with (connection, client, userdata, flags).

        :param on_connect: Callable to execute after successful broker connection.
        :return: MqttBuilder
        """
        self._on_connect_callbacks.append(on_connect)

    def add_before_disconnect(self, before_disconnect: Callable[[MQTTConnectionV3], None]):
        """
        Register a user before_disconnect callback.
        Callback is invoked before connection is closed with (connection).

        :param before_disconnect: Callable to execute before disconnect.
        :return: MqttBuilder
        """
        self._before_disconnect_callbacks.append(before_disconnect)

    def add_on_disconnect(self, on_disconnect: Callable[[Client, Any, int], None]):
        """
        Register an on_disconnect callback.
        Callback is invoked before connection is closed with (connection).

        :param on_disconnect: Callable to execute after disconnect.
        """
        self._on_disconnect_callbacks.append(on_disconnect)

    def publish(self, topic: str, payload=None, qos: QoS = QoS.AtMostOnce,
                retain: bool = False, wait_for_publish: bool = False):
        """
        Publish a message.

        :param topic: Exact topic to publish to.
        :param payload: Bytes, str, or None. If None the broker receives a zero-length payload.
        :param qos: Delivery guarantee. 0=AtMostOnce, 1=AtLeastOnce, 2=ExactlyOnce.
        :param retain: If True, broker stores the last message for this topic and serves it to future subscribers.
        :param wait_for_publish: If True, block until the client reports the publishing is complete.

        :return:
        """
        return super()._publish(topic, payload, qos, retain, None, wait_for_publish)

    def subscribe(self, topic: str, on_message: Callable[[MQTTConnectionV3, Client, Any, MQTTMessage], None],
                  qos: QoS = QoS.AtMostOnce, wait_for_ack: bool = False, ack_timeout: float = 10.0):
        """
        Subscribe to a topic filter.

        :param topic: Topic filter to subscribe to. Wildcards '+' and '#' are allowed.
        :param on_message: Callback invoked for each matching message. Signature: (connection, client, userdata, msg) -> None.
        :param qos: Requested Quality of Service for this subscription. 0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce.
        :param wait_for_ack: If True, block until the broker confirms the subscription (SUBACK)
            and raise on rejection or timeout instead of failing silently. Default keeps the
            fire-and-forget behaviour.
        :param ack_timeout: Seconds to wait for the SUBACK when wait_for_ack is True.

        :raises SubscribeError: The SUBSCRIBE could not be sent (e.g. not connected).
        :raises SubscribeTimeout: No SUBACK arrived within ack_timeout.
        :raises SubscribeRejected: The broker refused the subscription.
        :return: (result_code, mid) tuple, as returned by the underlying client.
        """
        return super()._subscribe(topic, on_message, wait_for_ack=wait_for_ack, ack_timeout=ack_timeout, qos=qos)
