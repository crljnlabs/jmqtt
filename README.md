# jmqtt

Compact Python MQTT wrapper around **paho-mqtt**. Focus: clear builder pattern and an explicit split between MQTT v3.1.1 and MQTT v5.

## Installation

```bash
pip install jmqtt
```

Optional: for image helpers (`get_image_pil`) install `Pillow`:

```bash
python -m pip install Pillow
```

## Builders

This package exposes two concrete builders:

- `MQTTBuilderV3(host, app_name)` → builds a `MQTTConnectionV3` (MQTT v3.1.1)
- `MQTTBuilderV5(host, app_name)` → builds a `MQTTConnectionV5` (MQTT v5.0)

Both builders provide the same fluent configuration API.

The MQTT client ID is generated automatically from:
- `device_fingerprint + app_name`
- `device_fingerprint + app_name + instance_id` (if `.instance_id(...)` is set)

`build()` creates the connection wrapper and prepares the client.  
`fast_build()` equals `build().connect()`.

---

## Quickstart: minimal setup

Connect, subscribe, print messages. Identical for v3 and v5.

```python
from jmqtt import MQTTBuilderV3, QualityOfService as QoS  # for v5 swap to MQTTBuilderV5

conn = MQTTBuilderV3(host="localhost", app_name="demo-client").fast_build()

def on_msg(connection, client, userdata, msg):
    # msg is jmqtt.MQTTMessage
    print(f"[{msg.topic}] {msg.text!r} retain={msg.retain} qos={int(msg.qos)}")

conn.subscribe("test/topic", on_message=on_msg, qos=QoS.AtLeastOnce)
conn.publish("test/topic", "hello", qos=QoS.AtLeastOnce, retain=False)

conn.close()
```

---

## Defaults

- Port: `1883`, Keepalive: `60`
- Clean session: `True`
- For v5: `SessionExpiryInterval` = 0 by default (non‑persistent). If you call `.persistent_session(True)`, it is set to 3600 seconds.

---

## Build a connection

### 1) Minimal (constructor + connect)
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-1")
    .fast_build()  # build().connect()
)
```

### 2) With username/password
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-2")
    .login("user", "password")
    .fast_build()
)
```

### 3) Port + keepalive + persistent session + auto‑reconnect
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-3")
    .port(1884)
    .keep_alive(120)
    .persistent_session(True)
    .auto_reconnect(min_delay=1, max_delay=30)
    .fast_build()
)
```

### 4) Last Will (LWT)
```python
from jmqtt import MQTTBuilderV3, QualityOfService as QoS  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-4")
    .last_will("devices/dev42/availability", payload="offline", qos=QoS.AtLeastOnce, retain=True)
    .fast_build()
)
```

### 5) Availability topic
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-5")
    .availability("devices/dev42/availability", payload_online="online", payload_offline="offline")
    .fast_build()
)
```

> When `availability(...)` is enabled, the builder also sets the Last Will to `payload_offline`, publishes `payload_online` on connect, and publishes `payload_offline` once before disconnect (using the provided QoS/retain values).

### 6) TLS (defaults)
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-6")
    .tls()  # verify certificates using system defaults
    .fast_build()
)
```

### 7) TLS with custom CA
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = (
    MQTTBuilderV3("broker.example.org", "client-7")
    .own_tls("/etc/ssl/certs/ca-bundle.pem", allow_insecure=False)
    .fast_build()
)
```

### 8) Multiple instances of the same app on one broker
```python
from jmqtt import MQTTBuilderV3

conn = (
    MQTTBuilderV3("broker.example.org", "my-tool")
    .instance_id("worker-1")
    .fast_build()
)
```

> **TLS capabilities (current):**
>
> - Supported: server TLS with system CAs (`.tls()`), server TLS with custom CA bundle (`.own_tls(ca_certs=...)`), optional hostname skip via `allow_insecure=True`.
> - Not yet wired in the builder: client certificates (`certfile`/`keyfile` mTLS), custom ciphers/TLS versions, WebSockets-specific TLS options. These remain available through the raw `paho-mqtt` client.

---

## Use a connection

### Connect
```python
from jmqtt import MQTTBuilderV3  # for v5 swap to MQTTBuilderV5

conn = MQTTBuilderV3("broker.example.org", "client-1").build()
conn.connect()
# or
conn.connect(blocking=True)

# Effective MQTT client identity used by this connection
print(conn.client_id)
```

### Callbacks (V3)
```python
import logging
from jmqtt import MQTTBuilderV3
from jmqtt.mqtt_connections import MQTTConnectionV3

logger = logging.getLogger("Info")

conn = MQTTBuilderV3("broker.example.org", "client-2").fast_build()

def on_connect_v3(connection: MQTTConnectionV3, client, userdata, flags):
    connection.publish("say/hello", "hello :)")

def before_disconnect_v3(connection: MQTTConnectionV3):
    connection.publish("say/hello", "bye :(")

def on_disconnect_v3(client, userdata, rc):
    logger.info("Too late for publishing")

conn.add_on_connect(on_connect_v3)
conn.add_before_disconnect(before_disconnect_v3)
conn.add_on_disconnect(on_disconnect_v3)
```

### Callbacks (V5)
```python
import logging
from jmqtt import MQTTBuilderV5
from jmqtt.mqtt_connections import MQTTConnectionV5

logger = logging.getLogger("Info")

conn = MQTTBuilderV5("broker.example.org", "client-2").fast_build()

def on_connect_v5(connection: MQTTConnectionV5, client, userdata, flags, properties):
    connection.publish("say/hello", "hello :)")

def before_disconnect_v5(connection: MQTTConnectionV5):
    connection.publish("say/hello", "bye :(")

def on_disconnect_v5(client, userdata, rc, properties):
    logger.info("Too late for publishing")

conn.add_on_connect(on_connect_v5)
conn.add_before_disconnect(before_disconnect_v5)
conn.add_on_disconnect(on_disconnect_v5)
```

### Subscribe

Identical for v3 and v5.
```python
from jmqtt import QualityOfService as QoS

def on_msg(connection, client, userdata, msg):
    print(msg.topic, msg.text)

conn.subscribe("sensors/+/temp", on_message=on_msg, qos=QoS.AtLeastOnce)
```

By default `subscribe(...)` is fire-and-forget: it returns the `(result, mid)` tuple
from the underlying client and does not wait for the broker's SUBACK. Pass
`wait_for_ack=True` to block until the broker confirms the subscription and to fail
loudly instead of silently when it doesn't:
```python
from jmqtt import SubscribeRejected, SubscribeTimeout

try:
    conn.subscribe("sensors/+/temp", on_message=on_msg, qos=QoS.AtLeastOnce,
                   wait_for_ack=True, ack_timeout=5.0)
except SubscribeTimeout:
    ...  # no SUBACK within ack_timeout
except SubscribeRejected:
    ...  # broker refused the subscription (SUBACK failure code)
```

### Message object (`MQTTMessage`)

Callbacks receive a `jmqtt.MQTTMessage` instance.

Core attributes:

- `topic: str`
- `qos: Optional[QualityOfService]` (cast with `int(msg.qos)` to print numeric)
- `retain: bool`

Type flags:

- `is_text`, `is_json`, `is_image`, `is_audio`, `is_binary`

Accessors and conversions:

- Text: `msg.text` (auto‑decoded) or `msg.get_text("latin-1")` for a specific charset
- Bytes: `msg.payload_bytes`
- JSON: `msg.json_value`
- Numbers: `msg.boolean_value`, `msg.integer_value`, `msg.float_value`
- Images: `msg.image_bytes_and_media_type` → `(bytes, media_type)`, `msg.get_image_pil()` (requires Pillow)

Comparisons:

- `msg == "online"`, `msg == b"raw"`, `msg == {"k": "v"}` (JSON), `msg == True`

### Unsubscribe

Identical for v3 and v5.
```python
# Remove one or more filters
conn.unsubscribe("sensors/+/temp", "actuators/#")
```

### Close

Identical for v3 and v5.
```python
conn.close()          # loop_stop + disconnect
```

### Health & resilience

Every connection exposes a point-in-time liveness snapshot and a list of the
topic filters it currently has registered. Use them for diagnostics and to
confirm a reconnect actually re-subscribed:
```python
h = conn.health
print(h.connected, h.connect_count, h.disconnect_count)
print(h.seconds_since_inbound)   # time since the broker last sent any packet
print(conn.active_subscriptions) # () until something subscribes

# Force a reconnect over the normal connect path (re-fires on_connect, so all
# re-subscribe / availability callbacks run again). Unlike disconnect()+connect()
# this keeps the Last Will and auto-reconnect intact.
conn.reconnect()
```

`seconds_since_inbound` reflects the time since *any* packet arrived (application
message **or** PINGRESP), which is the correct liveness signal: on an idle but
healthy link PINGRESP alone keeps it low; only a truly dead link lets it grow.

#### Optional zombie watchdog

A "zombie" connection still reports as connected but stopped receiving anything
long ago - typically a silently dropped TCP link. paho already reconnects on a
*detected* drop; the watchdog is an opt-in safety net for the drops its
keep-alive check can miss. Enable it on the builder (off by default):
```python
conn = (
    MQTTBuilderV3("broker.example.org", "client")
    .keep_alive(60)
    .auto_reconnect(min_delay=1, max_delay=30)
    .zombie_watchdog(enabled=True, idle_factor=2.0)  # reconnect if idle > 2x keep-alive
    .fast_build()
)
```
When it fires it calls `reconnect()`; application state recovers through the
normal on_connect callbacks and retained messages. It only ever repairs the
transport - it never touches application state. Detection relies on the network
loop running; a fully wedged loop thread is out of scope.

---

## Protocol specifics

### MQTT v3.1.1

**Publish**
```python
from jmqtt import QualityOfService as QoS

# Simple
conn.publish("demo/topic", "payload")

# With QoS/retain
conn.publish("demo/topic", "payload", qos=QoS.AtLeastOnce, retain=True)

# Wait for publish completion
conn.publish("demo/topic", "payload", qos=QoS.AtLeastOnce, wait_for_publish=True)
```

**Subscribe**
```python
from jmqtt import QualityOfService as QoS

def on_msg_v3(connection, client, userdata, msg):
    print("v3:", msg.topic, msg.text)

conn.subscribe("demo/v3/#", on_message=on_msg_v3, qos=QoS.ExactlyOnce)
```

### MQTT v5

**Build a v5 connection**
```python
from jmqtt import MQTTBuilderV5
conn = MQTTBuilderV5("broker.example.org", "client-5").fast_build()
```

**Publish** with properties
```python
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from jmqtt import QualityOfService as QoS

props = Properties(PacketTypes.PUBLISH)
props.MessageExpiryInterval = 30  # seconds

# Simple
conn.publish("demo5/topic", "payload-v5")

# With QoS/retain/properties
conn.publish("demo5/topic", "payload-v5", qos=QoS.AtLeastOnce, retain=False, properties=props)

# Wait for completion
conn.publish("demo5/topic", "payload-v5", qos=QoS.AtLeastOnce, wait_for_publish=True, properties=props)
```

**Subscribe** with options
```python
from jmqtt import QualityOfService as QoS, RetainHandling

def on_msg_v5(connection, client, userdata, msg):
    print("v5:", msg.topic, msg.text, "retain:", msg.retain)

conn.subscribe(
    "demo5/#",
    on_message=on_msg_v5,
    qos=QoS.AtLeastOnce,
    no_local=True,
    retain_as_published=True,
    retain_handling=RetainHandling.SendRetainedOnNewSubscription,
)
```

---

## Logging

This package uses `logging` with a `NullHandler`. Enable it like this:

```python
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("jmqtt").setLevel(logging.DEBUG)
```

## Best practices

- Use a stable `app_name` and set `.instance_id(...)` when running the same app multiple times on one broker.
- Set LWT (`.last_will(...)`) with QoS ≥ 1 and `retain=True`.
- Enable auto‑reconnect for production.
- For v5, use `retain_handling` and `no_local` to reduce retained floods and pub/sub loops.

## License

MIT (see `LICENSE`).
