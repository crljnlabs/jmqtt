from __future__ import annotations

from typing import TypeVar, Generic, Type

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from ..setup_logging import get_logger
from ..types import QualityOfService as QoS
from ..mqtt_config import MQTTConfig
from ..mqtt_connections import MQTTConnectionV3, MQTTConnectionV5
from .. import client_identity


C = TypeVar("C", MQTTConnectionV3, MQTTConnectionV5)
logger = get_logger("MqttBuilder")


class MqttBuilder(Generic[C]):
    def __init__(self, host: str, app_name: str, connector: Type[C]):
        """
        Initialize an MQTT builder with the minimal required identity information.

        `app_name` must be stable per tool/service and is part of the generated
        client ID. Format validation happens once during `build()`.

        Final client ID is always generated automatically:
        - `device_fingerprint + app_name + instance_id` if instance is set
        - `device_fingerprint + app_name` if no instance is set

        If you expect multiple instances of the same tool to connect to the same
        broker, set `instance_id(...)` explicitly right after constructor usage.

        :param host: Broker hostname or IPv4/IPv6 literal.
        :param app_name: Stable application name used for deterministic client IDs.
        :param connector: Connection wrapper type (v3 or v5).
        """
        serial_number, connections = client_identity.facts.collect_device_facts()
        self._config = MQTTConfig(host, app_name, serial_number, connections)
        self._config.protocol = mqtt.MQTTv311 if connector is MQTTConnectionV3 else mqtt.MQTTv5

        self._connection: Type[C] = connector()

    def instance_id(self, id: str) -> MqttBuilder[C]:
        """
        Set an explicit instance ID to separate parallel app instances.

        Use this when the same `app_name` can run more than once against the same
        broker (for example multiple deployments or repeated local runs).
        Different instance IDs produce different client IDs and avoid broker
        disconnects caused by duplicate client identifiers.

        Recommendation for library authors:
        expose this through config/env/CLI so end users can set the instance
        without changing code. If your wrapper already knows the instance at
        startup, you can apply this immediately after constructor creation.

        :param id: Instance identifier used for client-id composition.
                   Format validation happens once during `build()`.
        :return: MqttBuilder
        """
        self._config.instance_id = id
        return self

    def persistent_session(self, persistent_session: bool = True) -> MqttBuilder[C]:
        """
        Control session persistence.

        :param persistent_session: True for persistent session, False for clean start.
        :return: MqttBuilder
        """
        self._config.clean_session = not persistent_session
        return self

    def port(self, port: int) -> MqttBuilder[C]:
        """
        Set broker port. Default: 1883

        :param port: Usually 1883 (MQTT) or 8883 (MQTTS).
        :return: MqttBuilder
        """
        self._config.port = port
        return self

    def keep_alive(self, keep_alive: int) -> MqttBuilder[C]:
        """
        Set keepalive seconds.

        :param keep_alive: Interval in seconds for PINGREQ heartbeats.
        :return: MqttBuilder
        """
        self._config.keep_alive = keep_alive
        return self

    def login(self, username: str, password: str) -> MqttBuilder[C]:
        """
        Set username and password.

        :param username: Username string.
        :param password: Password string.
        :return: MqttBuilder
        """
        self._config.username = username
        self._config.password = password
        return self

    def availability(self, topic: str, payload_online: str = "online", payload_offline: str = "offline", qos: QoS = QoS.AtLeastOnce, retain: bool = True) -> MqttBuilder[C]:
        """
        Configure an availability topic.
        On successful connect, publish 'payload_online' to 'topic' with given qos/retain.
        Also sets the Last Will to 'payload_offline' for unclean disconnects.

        :param topic: Availability topic. Use a stable, retained topic so new subscribers see status immediately.
        :param payload_online: Payload published on connect.
        :param payload_offline: Payload set as Last Will and sent by broker on unclean disconnect.
        :param qos: Delivery level for availability messages. Usually 1.
        :param retain: Retain both online and will messages so late subscribers see the latest state.
        :return: MqttBuilder
        """
        args = topic, payload_online, qos, retain
        if isinstance(self._connection, MQTTConnectionV3):
            self._connection.add_on_connect(lambda connection, _1, _2, _3: connection.publish(*args))
        elif isinstance(self._connection, MQTTConnectionV5):
            self._connection.add_on_connect(lambda connection, _1, _2, _3, _4: connection.publish(*args))

        self._connection.add_before_disconnect(
            lambda connection: connection.publish(topic, payload_offline, qos, retain, wait_for_publish=True)
        )
        self.last_will(topic, payload_offline, qos, retain)
        return self

    def last_will(self, topic: str, payload: str = "offline",  qos: QoS = QoS.AtLeastOnce, retain: bool = True) -> MqttBuilder[C]:
        """
        Set MQTT Last Will and Testament.

        :param topic: Target topic for the will message.
        :param payload: Will payload sent by the broker on unclean disconnect.
        :param qos: Will QoS.
        :param retain: Whether the will is retained.
        :return: MqttBuilder
        """
        self._config.last_will = {
            "topic": topic,
            "payload": payload,
            "qos": qos,
            "retain": retain
        }
        return self

    # TLS Missing
    """    
    2. 
    client.tls_set(
        ca_certs="/path/ca.pem",
        certfile="/path/client.crt",
        keyfile="/path/client.key",
        # keyfile_password="***"  # optional if key encrypted
    )
    """

    def _tls(self, settings: dict, allow_insecure: bool = False) -> MqttBuilder[C]:
        self._config.tls = {
            "settings": settings,
            "allow_insecure": allow_insecure
        }
        return self

    def tls(self, allow_insecure: bool = False) -> MqttBuilder[C]:
        """
        Enable TLS with default settings.

        :param allow_insecure: If True, disable certificate hostname checks (insecure).
        :return: MqttBuilder
        """
        return self._tls({}, allow_insecure)

    def own_tls(self, ca_certs: str, allow_insecure: bool = False) -> MqttBuilder[C]:
        """
        Enable TLS with a custom CA bundle.

        :param ca_certs: Path to CA certificate bundle file.
        :param allow_insecure: If True, disable certificate hostname checks (insecure).
        :return: MqttBuilder
        """
        return self._tls({"ca_certs": ca_certs}, allow_insecure)

    def auto_reconnect(self, min_delay=1, max_delay=30) -> MqttBuilder[C]:
        """
        Enable exponential backoff reconnects.

        :param min_delay: Initial backoff in seconds before the first reconnect attempt.
        :param max_delay: Maximum backoff cap in seconds. The delay grows up to this value.
        :return: MqttBuilder
        """
        self._config.auto_reconnect = {
            "min_delay": min_delay,
            "max_delay": max_delay
        }
        return self

    def zombie_watchdog(self, enabled: bool = True, idle_factor: float = 2.0, check_interval: float | None = None) -> MqttBuilder[C]:
        """
        Enable an optional background guard against half-dead ("zombie") connections.

        A zombie connection still reports as connected but has received no traffic
        for far longer than the keep-alive interval - typically a silently dropped
        TCP link. paho already reconnects on a *detected* drop; this is a
        belt-and-suspenders for the drops its keep-alive check can miss. When it
        triggers it forces a reconnect over the normal connect path, so on_connect
        callbacks (re-subscribe, availability, retained state) restore operation.

        Disabled by default; it only repairs the transport, never application state.

        :param enabled: Turn the watchdog on (True) or off (False).
        :param idle_factor: Multiplier on keep-alive after which a still-connected
            link with no inbound traffic is treated as dead. 2.0 = two missed
            keep-alive windows.
        :param check_interval: Seconds between checks. Defaults to half keep-alive.
        :return: MqttBuilder
        """
        self._config.zombie_watchdog = {
            "enabled": enabled,
            "idle_factor": idle_factor,
            "check_interval": check_interval,
        }
        return self

    def build(self, **additional_client_params) -> C:
        """
        Create the client and apply configuration.

        The builder always generates the MQTT client ID from device facts plus
        app identity. It is intentionally not accepted via constructor or kwargs.

        :param additional_client_params: Extra kwargs forwarded to paho.Client(...), e.g. transport="websockets".
        :return: MQTTConnectionV3 or MQTTConnectionV5 wrapper depending on protocol.
        """
        self._config.client_id = client_identity.client_id.build_auto_client_id(
            self._config.app_name,
            self._config.instance_id,
            serial_number=self._config.serial_number,
            connections=self._config.connections,
        )

        if self._config.protocol != mqtt.MQTTv5:
            additional_client_params["clean_session"] = self._config.clean_session

        client = mqtt.Client(client_id=self._config.client_id, protocol=self._config.protocol, **additional_client_params)

        if self._config.has_auto_reconnect:
            client.reconnect_delay_set(**self._config.auto_reconnect)

        if self._config.has_tls:
            client.tls_set(**self._config.tls["settings"])
            client.tls_insecure_set(self._config.tls["allow_insecure"])

        if self._config.require_login:
            client.username_pw_set(self._config.username, self._config.password)

        availability_topic = None
        if self._config.has_last_will:
            client.will_set(**self._config.last_will)
            availability_topic = self._config.last_will["topic"]

        connection_parameters = {
            "host": self._config.host,
            "port": self._config.port,
            "keepalive": self._config.keep_alive,
        }

        if self._config.protocol == mqtt.MQTTv5:
            # MQTT version 5 clean session. Is persistent for 3600 sec
            connection_parameters["clean_start"] = self._config.clean_session
            props = Properties(PacketTypes.CONNECT)
            props.SessionExpiryInterval = 3600 if not self._config.clean_session else 0  # seconds
            connection_parameters["properties"] = props

        self._connection.inject_client(
            client,
            connection_parameters,
            availability_topic,
            client_id=self._config.client_id,
        )

        if self._config.has_zombie_watchdog:
            self._connection.enable_zombie_watchdog(
                idle_factor=self._config.zombie_watchdog["idle_factor"],
                check_interval=self._config.zombie_watchdog["check_interval"],
            )

        return self._connection

    def fast_build(self, **additional_client_params) -> C:
        """
        Create the client, apply configuration and connect.

        :param additional_client_params: Extra kwargs forwarded to paho.Client(...), e.g. transport="websockets".
        :return: MQTTConnectionV3 or MQTTConnectionV5 wrapper depending on protocol.
        """
        return self.build(**additional_client_params).connect()
