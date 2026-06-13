import paho.mqtt.client as mqtt


class MQTTConfig:
    def __init__(self, host, app_name, serial_number=None, connections=None):
        self.client_id = None
        self.app_name = app_name
        self.instance_id = None
        self.serial_number = serial_number
        self.connections = connections if connections is not None else []

        self.protocol = mqtt.MQTTv311
        self.clean_session = True

        self.host = host
        self.port = 1883
        self.keep_alive = 60

        self.username = None
        self.password = None

        self.auto_reconnect = None
        self.zombie_watchdog = None
        self.tls = None

        self.last_will = None
        self.on_connect_callbacks = []
        self.before_disconnect_callbacks = []
        self.on_disconnect_callbacks = []

    @property
    def require_login(self):
        return self.username is not None and self.password is not None

    @property
    def has_last_will(self):
        return self.last_will is not None

    @property
    def has_tls(self):
        return (self.tls is not None and
                all([key in self.tls for key in ("settings", "allow_insecure")]))

    @property
    def has_auto_reconnect(self):
        return (self.auto_reconnect is not None and
                all([key in self.auto_reconnect for key in ("min_delay", "max_delay")]))

    @property
    def has_zombie_watchdog(self):
        return self.zombie_watchdog is not None and self.zombie_watchdog.get("enabled", False)
