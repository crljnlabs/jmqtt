from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class ConnectionHealth:
    """Immutable snapshot of an MQTT connection's liveness state.

    Combines values jmqtt owns and tracks itself (connect/disconnect bookkeeping
    and the registered subscriptions) with a best-effort inbound-activity figure
    derived from the underlying paho client. Use it for diagnostics and for the
    optional zombie watchdog.

    Note on ``seconds_since_inbound``: it reflects the time since the broker last
    sent *any* packet (application message, PINGRESP, ...), which is the correct
    liveness signal. On a healthy but idle link PINGRESP alone keeps it low; only
    a truly dead link lets it grow without bound. It is ``None`` when no traffic
    has been observed yet or the value cannot be read from the client.
    """

    connected: bool
    client_id: Optional[str]
    subscriptions: Tuple[str, ...]
    connect_count: int
    disconnect_count: int
    last_connect_at: Optional[float]
    last_disconnect_at: Optional[float]
    seconds_since_inbound: Optional[float]
