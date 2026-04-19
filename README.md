# FSU1A — Betfair Exchange Live API Gateway

**Chimera Sports Trading Platform · Federated Service Unit 1A**

FSU1A maintains a persistent, real-time connection to the Betfair Exchange Streaming API (ESA) and serves fully-reconstructed market state from in-memory cache to all downstream Chimera consumers. It replaces on-demand REST polling with a single pub/sub stream per deployment, providing sub-second market data without Betfair API rate-limit pressure.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Repository Structure](#3-repository-structure)
4. [GCP Deployment Target](#4-gcp-deployment-target)
5. [Configuration](#5-configuration)
6. [Secrets](#6-secrets)
7. [Betfair Streaming Protocol](#7-betfair-streaming-protocol)
8. [Market State & Delta Reconstruction](#8-market-state--delta-reconstruction)
9. [API Reference — /api/*](#9-api-reference----api)
10. [Admin Reference — /admin/*](#10-admin-reference----admin)
11. [Observability Reference](#11-observability-reference)
12. [Data Models](#12-data-models)
13. [Error Handling & Resilience](#13-error-handling--resilience)
14. [IAM & Permissions](#14-iam--permissions)
15. [Deployment](#15-deployment)
16. [Local Development](#16-local-development)

---

## 1. Overview

| Property | Value |
|---|---|
| Service name | `fsu1a` |
| Platform | Chimera Sports Trading |
| Runtime | Python 3.12 / FastAPI / Uvicorn |
| Transport | Betfair Exchange Streaming API (TLS socket, not REST) |
| GCP project | `chiops` |
| GCP region | `europe-west2` (London — mandatory, Betfair UK/IE geo-restriction) |
| Cloud Run service | `fsu1a` |
| Service account | `fsu1a@chiops.iam.gserviceaccount.com` |
| Port | `8080` |

### What FSU1A does

1. Authenticates with Betfair using a self-signed SSL client certificate (non-interactive bot login).
2. Opens a persistent TLS socket to `stream-api.betfair.com:443` and subscribes to markets.
3. Receives a continuous stream of JSON delta messages and reconstructs full market state in-memory.
4. Serves that state synchronously to HTTP consumers — **zero on-demand Betfair REST calls at request time**.
5. Automatically reconnects with exponential backoff on any failure, supplying re-subscription tokens so Betfair sends only the delta since last disconnect rather than a full image.
6. Exposes operational endpoints for the Lay Engine, admin endpoints for Chimera Portal, and standard observability endpoints.

### What FSU1A does not do

- It does not place bets or interact with the Betfair Betting API.
- It does not replicate how the Lay Engine previously connected to Betfair. The Lay Engine's REST client is the problem being replaced, not a pattern to follow.
- It does not poll Betfair at request time. All `/api/*` responses are served from in-memory state.

---

## 2. Architecture

```
                        ┌─────────────────────────────────────────────────────┐
                        │                    FSU1A (Cloud Run)                 │
                        │                                                       │
  stream-api            │  TLS socket    ┌──────────────────┐                  │
  .betfair.com:443 ────►│───────────────►│  stream_client   │                  │
                        │                │  (asyncio loop)  │                  │
  identitysso-cert      │  cert login    │                  │                  │
  .betfair.com   ───────│───────────────►│  betfair_auth    │                  │
                        │                └────────┬─────────┘                  │
                        │                         │ delta messages              │
                        │                         ▼                             │
                        │                ┌──────────────────┐                  │
                        │                │  market_cache    │                  │
                        │                │  (in-memory)     │                  │
                        │                └────────┬─────────┘                  │
                        │                         │                             │
                        │          ┌──────────────┼──────────────┐             │
                        │          ▼              ▼              ▼             │
                        │     /api/*         /admin/*       /health etc.       │
                        └──────────┬──────────────┬──────────────┬─────────────┘
                                   │              │              │
                              Lay Engine    Chimera Portal   Cloud Run
                              + other FSUs               health checks
```

### Key design decisions

| Decision | Rationale |
|---|---|
| TLS socket (not WebSocket) | Betfair ESA is a raw TLS stream, not WebSocket |
| Client cert only for `certlogin` | The streaming socket authenticates via session token in the auth message, not mTLS |
| Single asyncio loop | All I/O non-blocking; no threads needed for the stream |
| `asyncio.Lock` on session | Prevents concurrent reconnects racing to acquire a new session token |
| `asyncio.Lock` on cache | Serialises all market state mutations within the event loop |
| New Firestore client per call | Cloud Run best practice for connection lifecycle management |
| Temp files for cert/key | `requests` library requires file paths; module-level refs prevent GC |
| `deque(maxlen=10000)` for logs | Bounded ring buffer; same limit as FSU1E |

---

## 3. Repository Structure

```
fsu1a/
├── Dockerfile                  # python:3.12-slim, port 8080
├── requirements.txt            # Pinned dependencies (mirrors FSU1E)
├── docs/
│   └── Betfair API Docs.pdf    # Official ESA specification (authoritative)
└── app/
    ├── __init__.py
    ├── config.py               # All constants, defaults, validation schema
    ├── secrets.py              # Secret Manager fetch + cert/key temp file management
    ├── firestore_client.py     # load_settings() / save_settings()
    ├── state.py                # AppState singleton — stream state, SSE, log buffer
    ├── betfair_auth.py         # Non-interactive cert login + keepalive
    ├── market_cache.py         # Full ESA delta reconstruction (RunnerState, MarketState)
    ├── stream_client.py        # asyncio TLS socket — full ESA protocol implementation
    ├── main.py                 # FastAPI app, lifespan, structured JSON logging
    └── routers/
        ├── __init__.py
        ├── api.py              # /api/* — operational endpoints for Lay Engine
        ├── admin.py            # /admin/* — CHI-ADR-010 admin endpoints for Portal
        └── observability.py    # /health /ready /info /status /metrics
```

### Module responsibilities

| Module | Responsibility |
|---|---|
| `config.py` | Single source of truth for all constants and default/editable settings schema |
| `secrets.py` | Fetches 5 secrets from Secret Manager once; caches in module scope; writes cert/key to temp files |
| `firestore_client.py` | Reads and writes the `fsu-admin-settings/fsu1a` Firestore document; auto-bootstraps on first run |
| `state.py` | `AppState` singleton holding all runtime state; never imports from other app modules |
| `betfair_auth.py` | Synchronous certlogin/keepalive via `requests`, dispatched with `run_in_executor`; guarded by `asyncio.Lock` |
| `market_cache.py` | Implements the complete ESA delta spec; the only place market state is mutated |
| `stream_client.py` | Manages the TLS socket lifecycle, ESA protocol handshake, reconnect loop, and background tasks |
| `main.py` | FastAPI lifespan (startup/shutdown), structured JSON log formatter, request counter middleware |
| `routers/api.py` | Serves market data from `market_cache`; zero I/O at request time |
| `routers/admin.py` | CHI-ADR-010 form-definition API; validates and persists settings changes |
| `routers/observability.py` | Health probes, Prometheus metrics, static info |

---

## 4. GCP Deployment Target

| Setting | Value |
|---|---|
| GCP project | `chiops` |
| Region | `europe-west2` (London) |
| Service | Cloud Run — `fsu1a` |
| Service account | `fsu1a@chiops.iam.gserviceaccount.com` |
| Container port | `8080` |
| Min instances | `1` (stream must be persistent; cold start loses the socket) |
| Concurrency | `80` (default; all endpoints are in-memory reads) |

> **Region is non-negotiable.** Betfair restricts ESA access to IP ranges in the UK/IE. Deploying outside `europe-west2` will result in TLS connection refusals from `stream-api.betfair.com`.

---

## 5. Configuration

FSU1A reads its configuration from a Firestore document at startup and applies it to `AppState`. The document is the single source of truth; FSU1A is the sole writer.

### Firestore path

```
Collection : fsu-admin-settings
Document   : fsu1a
```

### Settings reference

| Field | Type | Default | Min | Max | Description |
|---|---|---|---|---|---|
| `mode` | enum | `active` | — | — | Operating mode: `active`, `paused`, `drain` |
| `heartbeat_ms` | int | `5000` | `1000` | `30000` | ESA heartbeat interval in milliseconds |
| `reconnect_max_backoff_s` | int | `300` | `10` | `3600` | Maximum reconnect backoff in seconds |
| `session_keepalive_hours` | int | `20` | `1` | `23` | Hours between Betfair session keepalive calls |
| `market_filter_event_type_ids` | list[str] | `["1","2","4717"]` | — | — | Betfair event type IDs to subscribe (1=Horse Racing, 2=Football, 4717=Tennis) |
| `market_filter_country_codes` | list[str] | `["GB","IE"]` | — | — | Country codes to subscribe |
| `market_filter_market_types` | list[str] | `["WIN","PLACE","MATCH_ODDS","NEXT_GOAL"]` | — | — | Market types to subscribe |
| `max_markets` | int | `200` | `1` | `1000` | Maximum markets held in cache |
| `log_level` | enum | `INFO` | — | — | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `version` | int | `0` | — | — | Auto-incremented on every write (do not set manually) |

### Auto-bootstrap

If the Firestore document does not exist when FSU1A starts, it is created with all default values. This allows a fresh deployment to become operational without manual Firestore setup.

### Mode semantics

| Mode | Behaviour |
|---|---|
| `active` | Normal operation — stream connected, all endpoints serving |
| `paused` | Stream client continues running but `/health` reports `degraded` |
| `drain` | `/health` reports `draining`; use before scaling down to allow Lay Engine to stop sending requests |

---

## 6. Secrets

All credentials are stored in **Google Secret Manager** under the `chiops` project. FSU1A fetches all five secrets at startup and caches them in memory for the process lifetime.

| Secret ID | Content | Format |
|---|---|---|
| `betfair-username` | Betfair account username | Plain string |
| `betfair-password` | Betfair account password | Plain string |
| `betfair-app-key` | Betfair paid API application key | Plain string |
| `betfair-cert-pem` | Self-signed SSL client certificate | PEM string (includes `-----BEGIN CERTIFICATE-----`) |
| `betfair-key-pem` | Private key for the client certificate | PEM string (includes `-----BEGIN RSA PRIVATE KEY-----` or EC equivalent) |

FSU1A always fetches `versions/latest`. To rotate a credential, create a new secret version; the change takes effect on the next process restart.

### Cert/key temp files

The `requests` library requires file paths rather than in-memory strings for SSL client certificates. At startup, `secrets.py` writes the PEM values to `tempfile.NamedTemporaryFile(delete=False)` instances. The file objects are held as module-level variables so they are not garbage-collected during the process lifetime. The OS cleans them up when the process exits.

### Secret access pattern

```python
# secrets.py — module-level cache, fetched once
_cached: dict | None = None

def get_credentials() -> dict:
    global _cached
    if _cached is not None:
        return _cached
    # ... fetch from Secret Manager, write temp files ...
    _cached = { "username": ..., "app_key": ..., "cert_path": ..., ... }
    return _cached
```

Credentials are **never** hardcoded, logged, or included in API responses.

---

## 7. Betfair Streaming Protocol

This section documents how FSU1A implements the Betfair Exchange Streaming API specification.

### Transport

- Host: `stream-api.betfair.com`
- Port: `443`
- Protocol: Raw TLS (not WebSocket, not HTTP)
- Message framing: JSON objects terminated by `\r\n`
- Client cert: **Not used on the streaming socket** — standard server-auth TLS only
- Authentication on the socket: Done via the `op=authentication` JSON message after the TCP/TLS handshake

### Connection lifecycle

```
Client                                    Server (stream-api.betfair.com:443)
  │                                              │
  │──── TLS handshake ──────────────────────────►│
  │                                              │
  │◄─── {"op":"connection","connectionId":"..."} │  Server speaks first
  │                                              │
  │──── {"op":"authentication",                  │
  │      "id":1,                                 │
  │      "appKey":"...",                         │
  │      "session":"..."}                        │
  │                                              │
  │◄─── {"op":"status","id":1,                   │
  │      "statusCode":"SUCCESS",                 │
  │      "connectionClosed":false}               │
  │                                              │
  │──── {"op":"marketSubscription",              │
  │      "id":2,                                 │
  │      "marketFilter":{...},                   │
  │      "marketDataFilter":{"fields":[...]},    │  Optionally: "initialClk", "clk"
  │      "initialClk":"...", "clk":"..."}        │
  │                                              │
  │◄─── {"op":"status","id":2,                   │
  │      "statusCode":"SUCCESS",...}             │
  │                                              │
  │◄─── {"op":"mcm","ct":"SUB_IMAGE",...}        │  Full initial image
  │◄─── {"op":"mcm","ct":null,...}               │  Ongoing deltas
  │◄─── {"op":"mcm","ct":"HEARTBEAT"}            │  Keepalive (no market data)
  │                  ...                         │
```

### Session token (certlogin)

Before connecting to the stream, FSU1A acquires a session token via:

```
POST https://identitysso-cert.betfair.com/api/certlogin
Content-Type: application/x-www-form-urlencoded
X-Application: <app-key>
[TLS client certificate in request]

username=<username>&password=<password>
```

Response:
```json
{"loginStatus": "SUCCESS", "sessionToken": "abc123..."}
```

The `sessionToken` is then included in the `op=authentication` message on the streaming socket.

### Session keepalive

UK/IE Betfair sessions expire after 24 hours. FSU1A sends a keepalive every `session_keepalive_hours` (default 20):

```
POST https://identitysso.betfair.com/api/keepAlive
X-Application: <app-key>
X-Authentication: <session-token>
```

### Subscribed market data fields

| Field constant | Data provided |
|---|---|
| `EX_BEST_OFFERS` | `batb`, `batl` (best available level-based ladders) |
| `EX_TRADED` | `trd` (traded volume price-point ladder) |
| `EX_TRADED_VOL` | `tv` (total traded volume scalar) |
| `EX_LTP` | `ltp` (last traded price scalar) |
| `EX_MARKET_DEF` | `marketDefinition` (market and runner metadata) |
| `SP_PROJECTED` | `spn`, `spf` (SP near/far projected prices) |

### Heartbeat and dead connection detection

- The server sends `op=mcm` messages (including `ct=HEARTBEAT` messages when no market data has changed) at the interval negotiated by `heartbeat_ms`.
- If **no message of any kind** arrives within `2 × heartbeat_ms` seconds, the connection is considered dead.
- FSU1A closes the socket and initiates reconnect.

### Status 503

A `"status": 503` field inside an MCM message is a **latency indicator**, not an error. It means the server is experiencing high load and may be conflating updates. FSU1A:

1. Sets `app_state.stream_latency = True`
2. Logs a warning
3. **Does not disconnect**
4. Clears the flag when the next non-503 MCM arrives

### Segmentation

Large market images are split into segments. Each MCM message has a `segmentType` field:

| `segmentType` | Meaning |
|---|---|
| `SEG_START` | First segment of a multi-part message |
| `SEG` | Middle segment |
| `SEG_END` | Final segment |
| `null` (absent) | Non-segmented message |

FSU1A applies deltas from **every** segment immediately. The `clk` and `initialClk` re-subscription tokens are only updated on `SEG_END` or non-segmented messages.

### Re-subscription (clk tokens)

After any successful subscription, FSU1A stores `initialClk` and `clk` from MCM messages. On reconnect, these are included in the `op=marketSubscription` message:

```json
{
  "op": "marketSubscription",
  "id": 3,
  "marketFilter": {...},
  "marketDataFilter": {"fields": [...]},
  "initialClk": "abc...",
  "clk": "xyz..."
}
```

The server responds with `ct=RESUB_DELTA` (delta since last known position) instead of `ct=SUB_IMAGE` (full image), dramatically reducing reconnect latency and bandwidth.

If the tokens are stale or rejected, the server falls back to `ct=SUB_IMAGE` automatically — no special handling needed.

---

## 8. Market State & Delta Reconstruction

### Level-based ladders

Used for: `batb` (best available to back), `batl` (best available to lay), `bdatb`, `bdatl`.

Each update in the stream is a triple `[level, price, size]`:
- **Keyed by level** (integer). Level 0 = best price on that side.
- `size == 0` → remove that level from the dict.
- The ladder dict is `{level: [price, size], ...}`.

To read best 3 lay prices: `[ladder[lvl] for lvl in sorted(ladder)[:3]]`

### Price-point ladders

Used for: `atb` (full available to back), `atl` (full available to lay), `trd` (traded), `spb`, `spl`.

Each update is a pair `[price, size]`:
- **Keyed by price** (float).
- `size == 0` → remove that price from the dict.
- The ladder dict is `{price: size, ...}`.

### Scalar fields

`ltp`, `tv`, `spn`, `spf` — sent only when changed; absence means the previous value is still valid.

### Full image (`img=True`)

When a `MarketChange` has `"img": true`, FSU1A clears the entire `MarketState` for that market before applying the new data. This happens on initial subscription (`ct=SUB_IMAGE`) and after any period where Betfair could not guarantee delta integrity.

### Runner change sp sub-object

```json
"sp": {
  "spn": 4.5,
  "spf": 5.2,
  "bsp": 4.8,
  "spb": [[4.0, 200.0], [4.5, 0]],
  "spl": [[5.0, 100.0]]
}
```

- `spn`, `spf`, `bsp` — scalars, update when present
- `spb`, `spl` — price-point ladders, same delta rules as `atb`/`atl`

### Market definition sync

When `marketDefinition` is present in a `MarketChange`, runner status is synced from `marketDefinition.runners[].status` alongside the `RunnerChange` deltas.

### CLOSED market eviction

FSU1A runs a background task every 5 minutes that removes markets whose `marketDefinition.status == "CLOSED"` from the in-memory cache.

---

## 9. API Reference — `/api/*`

All endpoints serve from in-memory state. No Betfair API calls are made at request time. All responses are `application/json`.

---

### `GET /api/markets`

List all markets currently in the cache.

**Query parameters:**

| Parameter | Type | Description |
|---|---|---|
| `event_type_id` | string | Filter by Betfair event type ID (e.g. `"1"` for horse racing) |
| `status` | string | Filter by market status (`OPEN`, `SUSPENDED`, `CLOSED`) |
| `in_play` | boolean | Filter to in-play (`true`) or pre-race (`false`) markets |
| `market_type` | string | Filter by market type (`WIN`, `MATCH_ODDS`, etc.) |
| `country_code` | string | Filter by country code (`GB`, `IE`) |

**Response `200`:**

```json
{
  "markets": [
    {
      "marketId": "1.234567890",
      "status": "OPEN",
      "marketType": "WIN",
      "eventTypeId": "1",
      "eventId": "31234567",
      "countryCode": "GB",
      "venue": "Ascot",
      "name": "14:30 Ascot",
      "marketTime": "2026-04-19T13:30:00Z",
      "suspendTime": "2026-04-19T13:30:00Z",
      "totalMatched": 125432.50,
      "inPlay": false,
      "bspMarket": false,
      "runnerCount": 8,
      "lastUpdateAt": "2026-04-19T13:28:45.123456+00:00"
    }
  ],
  "count": 1
}
```

---

### `GET /api/markets/{market_id}`

Single market with runner prices (best 3 levels each side).

**Path parameter:** `market_id` — Betfair market ID (e.g. `1.234567890`)

**Response `200`:** Market summary fields (see above) plus:

```json
{
  "marketId": "1.234567890",
  "...": "...summary fields...",
  "runners": [
    {
      "selectionId": 12345678,
      "handicap": 0.0,
      "status": "ACTIVE",
      "lastPriceTraded": 4.5,
      "totalMatched": 15432.10,
      "spNear": null,
      "spFar": null,
      "spActual": null,
      "bestAvailableToBack": [[5.0, 200.50], [4.8, 150.00], [4.6, 100.00]],
      "bestAvailableToLay":  [[4.5, 50.00],  [4.6, 80.00],  [4.8, 120.00]]
    }
  ]
}
```

**Response `404`:** Market not in cache (not yet received from stream, or evicted after closing).

---

### `GET /api/markets/{market_id}/prices`

**Primary endpoint consumed by the Lay Engine.** Compact runner prices, best 3 levels each side.

**Response `200`:**

```json
{
  "marketId": "1.234567890",
  "status": "OPEN",
  "inPlay": false,
  "totalMatched": 125432.50,
  "runners": [
    {
      "selectionId": 12345678,
      "handicap": 0.0,
      "status": "ACTIVE",
      "lastPriceTraded": 4.5,
      "totalMatched": 15432.10,
      "spNear": null,
      "spFar": null,
      "spActual": null,
      "bestAvailableToBack": [[5.0, 200.50], [4.8, 150.00], [4.6, 100.00]],
      "bestAvailableToLay":  [[4.5, 50.00],  [4.6, 80.00],  [4.8, 120.00]]
    }
  ]
}
```

**Price array format:** `[price, size]` — price is the Betfair decimal odds, size is the matched/available amount in GBP.

**Ordering:**
- `bestAvailableToBack`: level 0 first = highest price (best for backer)
- `bestAvailableToLay`: level 0 first = lowest price (best for layer)

**Response `404`:** Market not in cache.

---

### `GET /api/markets/{market_id}/book`

Full order book — all price points for every runner, plus raw `marketDefinition`.

**Response `200`:**

```json
{
  "marketId": "1.234567890",
  "status": "OPEN",
  "inPlay": false,
  "totalMatched": 125432.50,
  "marketDefinition": { "...": "raw marketDefinition object from ESA" },
  "runners": [
    {
      "selectionId": 12345678,
      "handicap": 0.0,
      "status": "ACTIVE",
      "lastPriceTraded": 4.5,
      "totalMatched": 15432.10,
      "spNear": null,
      "spFar": null,
      "spActual": null,
      "bestAvailableToBack": [[5.0, 200.50], [4.8, 150.00], [4.6, 100.00]],
      "bestAvailableToLay":  [[4.5, 50.00],  [4.6, 80.00],  [4.8, 120.00]],
      "availableToBack": [[5.0, 200.50], [4.8, 150.00], ...],
      "availableToLay":  [[4.5, 50.00],  [4.6, 80.00],  ...],
      "traded":          [[3.5, 5000.00], [4.0, 8000.00], ...]
    }
  ]
}
```

`availableToBack` is sorted descending by price. `availableToLay` and `traded` are sorted ascending by price.

**Response `404`:** Market not in cache.

---

### `GET /api/stream/status`

Current streaming connection state. Useful for Lay Engine health checks before placing requests.

**Response `200`:**

```json
{
  "status": "connected",
  "connectionId": "001-220419-1234",
  "latencyIndicator": false,
  "lastMessageAt": "2026-04-19T13:28:45.123456+00:00",
  "reconnectCount": 0,
  "marketCount": 87
}
```

`status` values: `disconnected` | `connecting` | `connected` | `reconnecting`

---

## 10. Admin Reference — `/admin/*`

All admin endpoints follow **CHI-ADR-010**. They return structured form definitions rather than raw values so Chimera Portal can render edit UI without bespoke per-FSU knowledge.

---

### `GET /admin/health`

Portal header widget liveness check.

```json
{
  "service": "fsu1a",
  "version": "1.0.0",
  "health": "healthy",
  "mode": "active",
  "streamStatus": "connected"
}
```

`health` values: `healthy` | `degraded` | `draining` | `unhealthy`

---

### `GET /admin/status`

Full runtime snapshot.

```json
{
  "service": "fsu1a",
  "version": "1.0.0",
  "health": "healthy",
  "mode": "active",
  "stream": {
    "status": "connected",
    "connectionId": "001-220419-1234",
    "latencyIndicator": false,
    "lastMessageAt": "2026-04-19T13:28:45.123456+00:00",
    "reconnectCount": 0,
    "marketCount": 87
  },
  "session": {
    "acquiredAt": "2026-04-19T06:00:00.000000+00:00"
  },
  "requests": {
    "total": 14523,
    "errors": 0,
    "errorRate": 0.0
  }
}
```

---

### `GET /admin/settings`

Returns a **structured form definition** (CHI-ADR-010 format).

```json
{
  "service": "fsu1a",
  "version": 3,
  "fields": [
    {
      "name": "mode",
      "label": "Operating mode",
      "value": "active",
      "type": "enum",
      "options": ["active", "paused", "drain"]
    },
    {
      "name": "heartbeat_ms",
      "label": "Heartbeat interval (ms)",
      "value": 5000,
      "type": "int",
      "min": 1000,
      "max": 30000
    },
    {
      "name": "market_filter_event_type_ids",
      "label": "Event type IDs to subscribe",
      "value": ["1", "2", "4717"],
      "type": "list_str"
    }
  ]
}
```

---

### `PUT /admin/settings`

Apply a partial settings update. Returns an `applied`/`rejected` split.

**Request body:** Partial key/value map of settings to change.

```json
{
  "mode": "paused",
  "heartbeat_ms": 10000,
  "unknown_field": "ignored"
}
```

**Response `200`:**

```json
{
  "applied": {
    "mode": "paused",
    "heartbeat_ms": 10000
  },
  "rejected": {
    "unknown_field": "'unknown_field' is not an editable field"
  },
  "version": 4
}
```

Changes are persisted to Firestore immediately. `AppState` is updated in-memory. A `settings_updated` event is broadcast to all SSE subscribers.

---

### `GET /admin/config`

Static schema — Portal uses this to build advanced editor views or validate client-side.

```json
{
  "service": "fsu1a",
  "version": "1.0.0",
  "editableFields": ["mode", "heartbeat_ms", "..."],
  "validationRules": {
    "mode": {"type": "enum", "values": ["active", "paused", "drain"], "label": "..."},
    "heartbeat_ms": {"type": "int", "min": 1000, "max": 30000, "label": "..."}
  },
  "defaults": { "mode": "active", "heartbeat_ms": 5000, "..." : "..." }
}
```

---

### `GET /admin/logs`

Paginated structured log ring-buffer (last 10,000 entries).

**Query parameters:** `limit` (default `100`), `offset` (default `0`).

```json
{
  "logs": [
    {
      "timestamp": "2026-04-19T13:28:45.123456+00:00",
      "level": "INFO",
      "message": "Betfair session acquired"
    }
  ],
  "total": 47,
  "limit": 100,
  "offset": 0
}
```

---

### `GET /admin/stream`

**Server-Sent Events** stream of real-time admin events. Connect with `EventSource` in the Portal.

- Content-Type: `text/event-stream`
- Keepalive: SSE comment every 15 seconds if no event
- Each event `data` field is a JSON-encoded object

**Event types:**

| `type` | When sent | Additional fields |
|---|---|---|
| `stream_status` | Connection state changes | `status` |
| `mcm` | Market data received | `changeType`, `marketCount`, `latency` |
| `settings_updated` | Admin settings changed | `fields` (list of changed keys) |

**Example SSE frames:**
```
data: {"type": "stream_status", "status": "connected"}

data: {"type": "mcm", "changeType": "SUB_IMAGE", "marketCount": 87, "latency": false}

data: {"type": "settings_updated", "fields": ["mode"]}

: keepalive
```

---

## 11. Observability Reference

---

### `GET /health`

Liveness probe. Returns `200` if the service can handle requests; `503` if critically unhealthy.

```json
{"status": "healthy", "service": "fsu1a"}
```

| `status` | HTTP | Meaning |
|---|---|---|
| `healthy` | 200 | Stream connected, no latency flag |
| `degraded` | 200 | Connecting or reconnecting |
| `draining` | 200 | Mode set to `drain` |
| `unhealthy` | 503 | Disconnected and not attempting to reconnect |

---

### `GET /ready`

Readiness probe. Returns `503` while the stream is in initial `disconnected` state (before first connect attempt completes).

```json
{"ready": true}
```

---

### `GET /info`

Static service metadata. Useful for service registry / inventory tooling.

```json
{
  "service": "fsu1a",
  "version": "1.0.0",
  "project": "chiops",
  "region": "europe-west2",
  "startedAt": "2026-04-19T06:00:00.000000+00:00",
  "uptimeSeconds": 27045.3
}
```

---

### `GET /status`

Full runtime snapshot (same as `/admin/status`, available without admin auth for monitoring tools).

---

### `GET /metrics`

Prometheus text exposition format (`text/plain; version=0.0.4`).

```
# HELP fsu1a_stream_connected Stream connected (1=yes 0=no)
# TYPE fsu1a_stream_connected gauge
fsu1a_stream_connected 1

# HELP fsu1a_stream_latency Stream 503 latency indicator (1=yes)
# TYPE fsu1a_stream_latency gauge
fsu1a_stream_latency 0

# HELP fsu1a_market_count Markets in in-memory cache
# TYPE fsu1a_market_count gauge
fsu1a_market_count 87

# HELP fsu1a_http_requests_total Total HTTP requests received
# TYPE fsu1a_http_requests_total counter
fsu1a_http_requests_total 14523

# HELP fsu1a_http_errors_total Total HTTP 5xx responses
# TYPE fsu1a_http_errors_total counter
fsu1a_http_errors_total 0

# HELP fsu1a_stream_reconnects_total Total stream reconnect attempts
# TYPE fsu1a_stream_reconnects_total counter
fsu1a_stream_reconnects_total 0
```

---

## 12. Data Models

### RunnerState (internal)

| Field | Type | Source | Notes |
|---|---|---|---|
| `selection_id` | int | `rc.id` | Betfair runner selection ID |
| `handicap` | float | `rc.hc` | Asian handicap (0.0 for non-handicap) |
| `status` | str | `marketDefinition.runners[].status` | `ACTIVE`, `WINNER`, `LOSER`, `REMOVED`, `HIDDEN` |
| `batb` | dict[int→[float,float]] | `rc.batb` | Best available to back (level-based) |
| `batl` | dict[int→[float,float]] | `rc.batl` | Best available to lay (level-based) |
| `bdatb` | dict[int→[float,float]] | `rc.bdatb` | Best display ATB |
| `bdatl` | dict[int→[float,float]] | `rc.bdatl` | Best display ATL |
| `atb` | dict[float→float] | `rc.atb` | Full available to back |
| `atl` | dict[float→float] | `rc.atl` | Full available to lay |
| `trd` | dict[float→float] | `rc.trd` | Traded volume by price |
| `spb` | dict[float→float] | `rc.sp.spb` | SP back bets placed |
| `spl` | dict[float→float] | `rc.sp.spl` | SP lay bets placed |
| `ltp` | float\|None | `rc.ltp` | Last traded price |
| `tv` | float\|None | `rc.tv` | Total traded volume |
| `spn` | float\|None | `rc.sp.spn` / `rc.spn` | SP near projected |
| `spf` | float\|None | `rc.sp.spf` / `rc.spf` | SP far projected |
| `bsp` | float\|None | `rc.sp.bsp` | Calculated BSP |

### MarketState (internal)

| Field | Type | Source | Notes |
|---|---|---|---|
| `market_id` | str | `mc.id` | Betfair market ID |
| `market_definition` | dict | `mc.marketDefinition` | Full Betfair MarketDefinition object |
| `runners` | dict[int→RunnerState] | `mc.rc[]` | Keyed by selection_id |
| `status` | str\|None | `mc.status` | Override status (rare) |
| `total_volume` | float\|None | `mc.tv` | Market total matched |
| `last_update_at` | datetime | Internal | UTC timestamp of last delta applied |

### Market name construction

Betfair's streaming API does not provide a pre-formatted market name. FSU1A constructs one as:

```
"{HH:MM} {venue}"   e.g. "14:30 Ascot"
```

from `marketDefinition.marketTime` (ISO 8601) and `marketDefinition.venue`. Falls back to `"{venue} {marketType}"` or the raw `marketId` if fields are missing.

---

## 13. Error Handling & Resilience

### Stream reconnect

| Event | Response |
|---|---|
| TCP/TLS error | Close writer, begin backoff wait, reconnect |
| Heartbeat timeout (2× `heartbeat_ms`) | Same as above |
| `op=status` with `errorCode=INVALID_SESSION_INFORMATION` | Clear session token, force new certlogin, reconnect |
| `op=status` with `errorCode=MAX_CONNECTION_LIMIT_EXCEEDED` | Same as above |
| `status=503` in MCM | Set `stream_latency=True`, continue reading — do NOT disconnect |
| Server sends `RESUB_DELTA` rejection (stale clk) | Server falls back to `SUB_IMAGE` automatically — no special handling |
| Auth rejected at reconnect | `refresh_session()` forces a new cert login before attempting stream auth |

### Backoff schedule

```
Attempt 1 : wait 1s
Attempt 2 : wait 2s
Attempt 3 : wait 4s
Attempt 4 : wait 8s
...
Attempt N : wait min(2^(N-1), reconnect_max_backoff_s)
```

Default cap: `300s` (5 minutes). Configurable via `reconnect_max_backoff_s`.

### Settings load failure

If Firestore is unavailable at startup, FSU1A logs the error and proceeds with `DEFAULT_SETTINGS` hardcoded in `config.py`. The stream will still connect; settings can be loaded at next restart.

### Credentials load failure

If Secret Manager is unavailable at startup, FSU1A logs the error and continues. The stream loop will fail to authenticate and will retry with backoff, so the service self-heals when Secret Manager becomes available.

### Cache consistency

The `asyncio.Lock` in `MarketCache` serialises all writes. Since the stream client and HTTP handlers share the same event loop, reads from HTTP handlers never see a partial write — the lock ensures atomicity at the asyncio task level.

---

## 14. IAM & Permissions

The Cloud Run service account `fsu1a@chiops.iam.gserviceaccount.com` requires:

| Role | Resource | Purpose |
|---|---|---|
| `roles/secretmanager.secretAccessor` | `chiops` project | Read Betfair credentials from Secret Manager |
| `roles/datastore.user` | `chiops` project | Read/write `fsu-admin-settings/fsu1a` Firestore document |
| `roles/run.invoker` | (not needed externally if VPC-internal) | Allow Lay Engine to call `/api/*` |

### Granting access (gcloud commands)

```bash
# Secret Manager
gcloud projects add-iam-policy-binding chiops \
  --member="serviceAccount:fsu1a@chiops.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

# Firestore
gcloud projects add-iam-policy-binding chiops \
  --member="serviceAccount:fsu1a@chiops.iam.gserviceaccount.com" \
  --role="roles/datastore.user"
```

---

## 15. Deployment

### Build and push container image

```bash
gcloud builds submit \
  --tag europe-west2-docker.pkg.dev/chiops/images/fsu1a:latest \
  --project chiops
```

### Deploy to Cloud Run

```bash
gcloud run deploy fsu1a \
  --image europe-west2-docker.pkg.dev/chiops/images/fsu1a:latest \
  --region europe-west2 \
  --project chiops \
  --service-account fsu1a@chiops.iam.gserviceaccount.com \
  --port 8080 \
  --min-instances 1 \
  --max-instances 3 \
  --concurrency 80 \
  --memory 512Mi \
  --cpu 1 \
  --no-allow-unauthenticated
```

> `--min-instances 1` is **required**. The Betfair stream is a persistent connection — if the instance scales to zero, the socket closes and market data goes stale. At min=1 the stream is always live.

> `--no-allow-unauthenticated` restricts access to authenticated GCP identities (Lay Engine, Portal service accounts). If deploying behind an internal load balancer this can be omitted.

### Secret Manager — provision secrets

```bash
# Create each secret (run once)
for SECRET in betfair-username betfair-password betfair-app-key betfair-cert-pem betfair-key-pem; do
  gcloud secrets create "$SECRET" --project chiops --replication-policy automatic
done

# Add secret versions (supply actual values)
echo -n "your_username"    | gcloud secrets versions add betfair-username   --data-file=- --project chiops
echo -n "your_password"    | gcloud secrets versions add betfair-password   --data-file=- --project chiops
echo -n "your_app_key"     | gcloud secrets versions add betfair-app-key    --data-file=- --project chiops
cat betfair_cert.pem       | gcloud secrets versions add betfair-cert-pem   --data-file=- --project chiops
cat betfair_key.pem        | gcloud secrets versions add betfair-key-pem    --data-file=- --project chiops
```

---

## 16. Local Development

### Prerequisites

- Python 3.12
- A GCP project with Application Default Credentials: `gcloud auth application-default login`
- Betfair credentials provisioned in Secret Manager (or mock them locally)

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run locally

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

On startup, FSU1A will:
1. Attempt to load settings from Firestore (`chiops/fsu-admin-settings/fsu1a`)
2. Attempt to load credentials from Secret Manager
3. Open the Betfair stream

If you don't have real credentials, the stream connection will fail and retry. All HTTP endpoints that read from `market_cache` will return empty results (no 500 errors) — the cache simply has no markets yet.

### Endpoints during local development

| URL | Purpose |
|---|---|
| `http://localhost:8080/docs` | FastAPI auto-generated Swagger UI |
| `http://localhost:8080/status` | Runtime state snapshot |
| `http://localhost:8080/api/markets` | Market list (empty until stream connects) |
| `http://localhost:8080/admin/stream` | SSE event feed (open in `curl -N`) |

### Testing the SSE stream

```bash
curl -N http://localhost:8080/admin/stream
```

### Build and test the container locally

```bash
docker build -t fsu1a:local .
docker run -p 8080:8080 \
  -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
  fsu1a:local
```
