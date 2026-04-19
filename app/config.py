SERVICE_NAME = "fsu1a"
SERVICE_VERSION = "1.0.0"
GCP_PROJECT = "chiops"
GCP_REGION = "europe-west2"

# Betfair Exchange Streaming API
BETFAIR_STREAM_HOST = "stream-api.betfair.com"
BETFAIR_STREAM_PORT = 443

# Betfair Auth endpoints
BETFAIR_CERTLOGIN_URL = "https://identitysso-cert.betfair.com/api/certlogin"
BETFAIR_KEEPALIVE_URL = "https://identitysso.betfair.com/api/keepAlive"

# Secret Manager secret IDs (latest version always used)
SECRET_BETFAIR_USERNAME = "betfair-username"
SECRET_BETFAIR_PASSWORD = "betfair-password"
SECRET_BETFAIR_APP_KEY = "betfair-app-key"
SECRET_BETFAIR_CERT_PEM = "betfair-cert-pem"
SECRET_BETFAIR_KEY_PEM = "betfair-key-pem"

# Firestore — FSU1A owns all writes to this document
FIRESTORE_COLLECTION = "fsu-admin-settings"
FIRESTORE_DOCUMENT = "fsu1a"

# Streaming market data fields to subscribe to
MARKET_DATA_FIELDS = [
    "EX_BEST_OFFERS",   # batb / batl (best available level-based ladders)
    "EX_TRADED",        # trd (traded price-point ladder)
    "EX_TRADED_VOL",    # tv (total traded volume)
    "EX_LTP",           # ltp (last traded price)
    "EX_MARKET_DEF",    # marketDefinition
    "SP_PROJECTED",     # spn / spf (SP near/far)
]

# Default settings — Firestore document is bootstrapped from this on first run
DEFAULT_SETTINGS: dict = {
    "mode": "active",
    "heartbeat_ms": 5000,
    "reconnect_max_backoff_s": 300,
    "session_keepalive_hours": 20,
    "market_filter_event_type_ids": ["1", "2", "4717"],   # Horse Racing, Football, Tennis
    "market_filter_country_codes": ["GB", "IE"],
    "market_filter_market_types": ["WIN", "PLACE", "MATCH_ODDS", "NEXT_GOAL"],
    "max_markets": 200,
    "log_level": "INFO",
    "version": 0,
}

# Fields the admin UI may change via PUT /admin/settings
EDITABLE_FIELDS = [
    "mode",
    "heartbeat_ms",
    "reconnect_max_backoff_s",
    "session_keepalive_hours",
    "market_filter_event_type_ids",
    "market_filter_country_codes",
    "market_filter_market_types",
    "max_markets",
    "log_level",
]

# Validation rules consumed by admin router and GET /admin/settings form definition
VALIDATION_RULES: dict = {
    "mode": {
        "type": "enum",
        "values": ["active", "paused", "drain"],
        "label": "Operating mode",
    },
    "heartbeat_ms": {
        "type": "int",
        "min": 1000,
        "max": 30000,
        "label": "Heartbeat interval (ms)",
    },
    "reconnect_max_backoff_s": {
        "type": "int",
        "min": 10,
        "max": 3600,
        "label": "Max reconnect backoff (s)",
    },
    "session_keepalive_hours": {
        "type": "int",
        "min": 1,
        "max": 23,
        "label": "Session keepalive interval (hours)",
    },
    "market_filter_event_type_ids": {
        "type": "list_str",
        "label": "Event type IDs to subscribe",
    },
    "market_filter_country_codes": {
        "type": "list_str",
        "label": "Country codes to subscribe",
    },
    "market_filter_market_types": {
        "type": "list_str",
        "label": "Market types to subscribe",
    },
    "max_markets": {
        "type": "int",
        "min": 1,
        "max": 1000,
        "label": "Maximum markets in cache",
    },
    "log_level": {
        "type": "enum",
        "values": ["DEBUG", "INFO", "WARNING", "ERROR"],
        "label": "Log level",
    },
}
