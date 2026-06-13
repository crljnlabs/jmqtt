from __future__ import annotations
from typing import Callable, Any, Optional

from paho.mqtt.client import Client
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

from jmqtt import MQTTMessage
from jmqtt.types import QualityOfService as QoS, RetainHandling
from .mqtt_connection_base import MqttConnectionBase


class MQTTConnectionV5(MqttConnectionBase):
    def add_on_connect(self, on_connect: Callable[[MQTTConnectionV5, Client, Any, dict[str, int], Optional[Properties]], None]):
        """
        Register an on_connect callback.
        Callback is invoked after a successful connect with (connection, client, userdata, flags, properties).

        :param on_connect: Callable to execute after successful broker connection.
        """
        self._on_connect_callbacks.append(on_connect)

    def add_before_disconnect(self, before_disconnect: Callable[[MQTTConnectionV5], None]):
        """
        Register a before_disconnect callback.
        Callback is invoked before connection is closed with (connection).

        :param before_disconnect: Callable to execute before disconnect.
        """
        self._before_disconnect_callbacks.append(before_disconnect)

    def add_on_disconnect(self, on_disconnect: Callable[[Client, Any, int, Optional[Properties]], None]):
        """
        Register an on_disconnect callback.
        Callback is invoked before connection is closed with (connection).

        :param on_disconnect: Callable to execute after disconnect.
        """
        self._on_disconnect_callbacks.append(on_disconnect)

    def publish(self, topic: str, payload, qos: QoS = QoS.AtMostOnce, retain: bool = False,
                properties: Optional[Properties] = None, wait_for_publish: bool = False):
        """
        Publish a message.

        :param topic: Exact topic to publish to.
        :param payload: Bytes, str, or None. If None the broker receives a zero-length payload.
        :param qos: Delivery guarantee. 0=AtMostOnce, 1=AtLeastOnce, 2=ExactlyOnce.
        :param retain: If True, broker stores the last message for this topic and serves it to future subscribers.
        :param properties: MQTT v5 properties for the PUBLISH packet.
        :param wait_for_publish: If True, block until the client reports the publishing is complete.

        :return: paho.mqtt.client.MQTTMessageInfo
        """
        return super()._publish(topic, payload, qos, retain, properties, wait_for_publish)

    def subscribe(self, topic: str, on_message: Callable[[MQTTConnectionV5, Client, Any, MQTTMessage], None], qos: QoS = QoS.AtMostOnce,
                  no_local: bool = False, retain_as_published: bool = False, retain_handling: RetainHandling = RetainHandling.SendRetainedAlways,
                  wait_for_ack: bool = False, ack_timeout: float = 10.0):
        """
        Subscribe to a topic filter with MQTT v5 subscribe options.

        :param topic: Topic filter to subscribe to. Wildcards '+' and '#' are allowed.
        :param on_message: Callback invoked for each matching message. Signature: (connection, client, userdata, msg) -> None.
        :param qos: Requested Quality of Service for this subscription. 0 = AtMostOnce, 1 = AtLeastOnce, 2 = ExactlyOnce.
        :param no_local: If True, the client will not receive its own published messages that match 'topic'.
            Useful for pub/sub loops. Comparison is done against the same client/session identity.
        :param retain_as_published: If True, the broker delivers the retained flag exactly as published.
            If False, the broker may clear or modify the retained flag on delivery.
            This allows consumers to distinguish retained messages via msg.retain.
        :param retain_handling:
            Controls when the broker sends retained messages upon subscribing.
            0 = Send retained always: Retained messages are delivered on every subscribe action.
            1 = Send retained on new subscription only: Retained messages are sent only when the subscription is newly created.
                Useful after reconnect with an existing session to avoid repeated retained floods.
            2 = Do not send retained: No retained messages are delivered at the time of subscribing.
        :param wait_for_ack: If True, block until the broker confirms the subscription (SUBACK)
            and raise on rejection or timeout instead of failing silently. Default keeps the
            fire-and-forget behaviour.
        :param ack_timeout: Seconds to wait for the SUBACK when wait_for_ack is True.

        :raises SubscribeError: The SUBSCRIBE could not be sent (e.g. not connected).
        :raises SubscribeTimeout: No SUBACK arrived within ack_timeout.
        :raises SubscribeRejected: The broker refused the subscription.
        :return: (result_code, mid) tuple, as returned by the underlying client.
        """
        options = SubscribeOptions(qos, no_local, retain_as_published, retain_handling)
        return super()._subscribe(topic, on_message, wait_for_ack=wait_for_ack, ack_timeout=ack_timeout, options=options)

