"""jmqtt-specific exception hierarchy.

Kept intentionally small: only failures that callers can reasonably catch and
react to are modelled. Everything derives from :class:`JmqttError` so users can
catch the whole family with a single except clause.
"""
from __future__ import annotations


class JmqttError(Exception):
    """Base class for all jmqtt-specific errors."""


class SubscribeError(JmqttError):
    """Base class for subscribe failures surfaced by ``subscribe(wait_for_ack=True)``.

    Raised directly when the SUBSCRIBE packet could not even be handed to the
    network layer (e.g. the client is not connected).
    """


class SubscribeTimeout(SubscribeError):
    """No SUBACK was received from the broker within the acknowledgement timeout."""


class SubscribeRejected(SubscribeError):
    """The broker answered the SUBSCRIBE with a failure code (SUBACK >= 0x80)."""
