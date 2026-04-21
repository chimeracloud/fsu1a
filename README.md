# FSU1A — Betfair Exchange Live API Gateway

**Chimera Sports Trading Platform · Federated Service Unit 1A**

FSU1A maintains a persistent, real-time connection to the Betfair Exchange Streaming API (ESA) and serves fully-reconstructed market state from in-memory RAM to all downstream Chimera consumers. It replaces on-demand REST polling with a single pub/sub stream per deployment, providing sub-second market data without Betfair API rate-limit pressure.

---

## Live Deployment Status

| Property | Value |
|---|---|
| **Service URL** | `https://fsu1a-991649774709.europe-west2.run.app` |
| **Health** | `healthy` |
| **Stream** | `connected` |
| **Markets in cache** | `98` (live, as of first deploy 2026-04-21) |
| **Reconnects** | `0` |
| **Errors** | `0` |
| **Latency indicator** | `false` |
| **First deployed** | `2026-04-21` |

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [In-Memory Cache — What It Is](#4-in-memory-cache--what-it-is)
4. [Repository Structure](#4-repository-structure)
5. [GCP Deployment Target](#5-gcp-deployment-target)
6. [Runtime Specifications](#6-runtime-specifications)
7. [Configuration](#7-configuration)
8. [Secrets](#8-secrets)
9. [Betfair Streaming Protocol](#9-betfair-streaming-protocol)
10. [Market State & Delta Reconstruction](#10-market-state--delta-reconstruction)
11. [API Reference — /api/*](#11-api-reference----api)
12. [Admin Reference — /admin/*](#12-admin-reference----admin)
13. [Observability Reference](#13-observability-reference)
14. [Data Models](#14-data-models)
15. [Error Handling & Resilience](#15-error-handling--resilience)
16. [Known Issues & Fixes](#16-known-issues--fixes)
17. [IAM & Permissions](#17-iam--permissions)
18. [Deployment](#18-deployment)
19. [Local Development](#19-local-development)

---

## 1. Overview

| Property | Value |
|---|---|
| Service name | `fsu1a` |
| Platform | Chimera Sports Trading |
| Runtime | Python 3.12 / FastAPI / Uvicorn |
| Transport | Betfair Exchange Streaming API (raw TLS socket — not REST, not WebSocket) |
| GCP project | `chiops` |
| GCP region | `europe-west2` (London — mandatory, Betfair UK/IE geo-restriction) |
| Cloud Run service | `fsu1a` |
| Service account | `fsu1asa@chiops.iam.gserviceaccount.com` |
| Port | `8080` |

### What FSU1A does

1. Authenticates with Betfair using a self-signed SSL client certificate (non-interactive bot login).
2. Opens a persistent TLS socket to `stream-api.betfair.com:443` and subscribes to markets.
3. Receives a continuous stream of JSON delta messages and reconstructs full market state in container RAM.
4. Serves that state synchronously to HTTP consumers — **zero on-demand Betfair REST calls at request time**.
5. Automatically reconnects with exponential backoff on any failure, supplying re-subscription tokens so Betfair sends only the delta since last disconnect rather than a full image.
6. Exposes operational endpoints for the Lay Engine, admin endpoints for Chimera Portal, and standard observability endpoints.

### What FSU1A does not do

- It does not place bets or interact with the Betfair Betting API.
- It does not poll Betfair at request time. All `/api/*` responses are served from in-memory state.
- It does not persist market data to disk, Firestore, or any bucket — it is a **live feed**, not a recorder.
- It does not replicate how the Lay Engine previously connected to Betfair. The Lay Engine's REST client is the problem being replaced, not a pattern to follow.

---

## 2. Architecture

```
                        ┌─────────────────────────────────────────────────────┐
                        │              FSU1A (Cloud Run, europe-west2)         │
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
                        │                │  market_cache    │  ← Container RAM  │
                        │                │  (Python dict)   │    ~5–20MB live   │
                        │                └────────┬─────────┘                  │
                        │                         │                             │
                        │          ┌──────────────┼──────────────┐             │
                        │          ▼              ▼              ▼             │
                        │     /api/*         /admin/*       /health etc.       │
                        └──────────┬──────────────┬──────────────┬─────────────┘
                                   │              │              │
                              Lay Engine    Chimera Portal   Cloud Run
                              + other FSUs               health checks / Prometheus
```

### Key design decisions

| Decision | Rationale |
|---|---|
| TLS socket (not WebSocket) | Betfair ESA is a raw TLS stream, not WebSocket |
| Client cert only for `certlogin` | Streaming socket authenticates via session token in the JSON auth message, not mTLS |
| Single asyncio event loop | All I/O non-blocking; no threads needed for the stream |
| `asyncio.Lock` on session | Prevents concurrent reconnects racing to acquire a new session token |
| `asyncio.Lock` on cache | Serialises all market state mutations within the event loop |
| 10MB readline buffer | Betfair `SUB_IMAGE` responses exceed asyncio's 64KB default — raised to 10MB |
| New Firestore client per call | Cloud Run best practice for connection lifecycle management |
| Temp files for cert/key | `requests` library requires file paths; module-level refs prevent GC |
| `deque(maxlen=10000)` for logs | Bounded ring buffer matching FSU1E pattern |
| `min-instances=1` on Cloud Run | Stream is persistent — a cold start would lose the socket and all cached state |

---

## 3. In-Memory Cache — What It Is

FSU1A stores all live market data in **container RAM** — plain Python objects (`dict`, `dataclass`) inside the running Cloud Run process. There is no database, no bucket, no external store involved.

```
Betfair stream deltas
        │
        ▼
market_cache.py  ←── Python dict in RAM
  └── MarketState (per market)
        └── RunnerState (per runner)
              ├── batb / batl  (level-based price ladders)
              ├── atb / atl    (full price-point ladders)
              ├── trd          (traded volume ladder)
              └── ltp / tv / spn / spf  (scalars)
        │
        ▼
HTTP response (served directly from above dict — zero I/O)
```

### Characteristics

| Property | Value |
|---|---|
| **Location** | Container RAM (Cloud Run instance) |
| **Current size** | ~98 markets, estimated 5–20MB RAM |
| **Read latency** | Sub-millisecond (pure in-memory dict lookup) |
| **Persistence** | None — cache rebuilds from Betfair on container restart (seconds) |
| **Shared state** | Within one container instance only |
| **Update frequency** | Continuous — every Betfair delta immediately applied |

### As a data source

Because FSU1A is always running (min-instances=1) and the stream is always connected, the in-memory cache is effectively a **continuously updated live snapshot** of every subscribed market. This makes it a practical replacement for a dedicated data recorder for live pricing — no need to run a separate process just to keep Betfair data current.

What it does **not** provide:
- Historical tick data or replay
- Persistence across restarts (though rebuild is fast)
- Multi-instance shared state (each Cloud Run instance has its own cache)

---

## 4. Repository Structure

```
fsu1a/
├── Dockerfile                  # python:3.12-slim, port 8080
├── requirements.txt            # Pinned dependencies
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

## 5. GCP Deployment Target

| Setting | Value |
|---|---|
| GCP project | `chiops` |
| Region | `europe-west2` (London) |
| Service | Cloud Run — `fsu1a` |
| Service account | `fsu1asa@chiops.iam.gserviceaccount.com` |
| Service URL | `https://fsu1a-991649774709.europe-west2.run.app` |
| Container port | `8080` |
| Min instances | `1` |
| Max instances | `3` |
| Memory | `512Mi` |
| CPU | `1` |
| Concurrency | `80` |
| Authentication | Required (GCP identity token) |
| Firestore database | `(default)` in `europe-west2` |
| Artifact Registry | `europe-west2-docker.pkg.dev/chiops/images/fsu1a` |

> **Region is non-negotiable.** Betfair restricts ESA access to UK/IE IP ranges. Deploying outside `europe-west2` results in TLS connection refusals from `stream-api.betfair.com`.

> **Min instances = 1 is non-negotiable.** The Betfair stream is a persistent TCP connection. If the instance scales to zero the socket closes, the cache is lost, and all downstream consumers get stale or empty data. At min=1 the stream is always live.

---

## 6. Runtime Specifications

### Dependencies (`requirements.txt`)

| Package | Version | Purpose |
|---|---|---|
| `fastapi` | `0.115.0` | HTTP framework |
| `uvicorn[standard]` | `0.30.6` | ASGI server |
| `google-cloud-firestore` | `2.19.0` | Settings persistence |
| `google-cloud-secret-manager` | `2.21.1` | Credential storage |
| `requests` | `2.32.3` | Betfair cert login (sync HTTP) |
| `pydantic` | `2.9.2` | Data validation |
| `sse-starlette` | `2.1.3` | Server-Sent Events for admin stream |

### Background tasks (always running)

| Task | Purpose | Interval |
|---|---|---|
| `betfair_stream` | Main ESA socket loop — read, parse, apply deltas | Continuous |
| `betfair_keepalive` | POST keepalive to Betfair REST session endpoint | Every 20h (configurable) |
| `betfair_maintenance` | Evict CLOSED markets from RAM cache | Every 5 minutes |

### Startup sequence

```
1. Load settings from Firestore (auto-bootstrap if missing)
2. Fetch all 5 secrets from Secret Manager → cache in module RAM
3. Write cert/key PEM to temp files (for requests library)
4. Start background tasks (stream, keepalive, maintenance)
5. POST to identitysso-cert.betfair.com → acquire session token
6. Open TLS socket to stream-api.betfair.com:443
7. Send op=authentication → wait for SUCCESS
8. Send op=marketSubscription → wait for SUCCESS
9. Begin reading MCM deltas → populate market_cache
10. HTTP server ready to serve /api/* from RAM
```

---

## 7. Configuration

FSU1A reads its configuration from a Firestore document at startup. FSU1A is the **sole writer** to this document.

### Firestore path

```
Collection : fsu-admin-settings
Document   : fsu1a
```

### Settings reference

| Field | Type | Default | Min | Max | Description |
|---|---|---|---|---|---|
| `mode` | enum | `active` | — | — | Operating mode: `active`, `paused`, `drain` |
| `heartbeat_ms` | int | `5000` | `1000` | `30000` | ESA heartbeat interval (ms) |
| `reconnect_max_backoff_s` | int | `300` | `10` | `3600` | Max reconnect backoff (s) |
| `session_keepalive_hours` | int | `20` | `1` | `23` | Hours between Betfair session keepalive calls |
| `market_filter_event_type_ids` | list[str] | `["1","2","4717"]` | — | — | Event type IDs (1=Horse Racing, 2=Football, 4717=Tennis) |
| `market_filter_country_codes` | list[str] | `["GB","IE"]` | — | — | Country codes to subscribe |
| `market_filter_market_types` | list[str] | `["WIN","PLACE","MATCH_ODDS","NEXT_GOAL"]` | — | — | Market types to subscribe |
| `max_markets` | int | `200` | `1` | `1000` | Maximum markets held in cache |
| `log_level` | enum | `INFO` | — | — | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `version` | int | `0` | — | — | Auto-incremented on every write |

### Mode semantics

| Mode | Behaviour |
|---|---|
| `active` | Normal — stream connected, all endpoints serving |
| `paused` | Stream continues but `/health` reports `degraded` |
| `drain` | `/health` reports `draining` — use before scaling down |

### Auto-bootstrap

If the Firestore document does not exist at startup, FSU1A creates it with all default values. No manual Firestore setup required on first deploy.

---

## 8. Secrets

All credentials are stored in **Google Secret Manager** under the `chiops` project. FSU1A fetches all five at startup and caches them in RAM for the process lifetime.

| Secret ID (the name) | Content | Format |
|---|---|---|
| `betfair-username` | Betfair account username | Plain string |
| `betfair-password` | Betfair account password | Plain string |
| `betfair-app-key` | Betfair paid API application key | Plain string |
| `betfair-cert-pem` | Self-signed SSL client certificate | PEM string |
| `betfair-key-pem` | Private key for the client certificate | PEM string |

FSU1A always fetches `versions/latest`. To rotate a credential, add a new secret version — takes effect on next container restart.

### Credential flow

```
Secret Manager
  └── get_credentials() [once, cached in RAM]
        ├── username / password / app_key  → used in cert login POST
        └── cert_pem / key_pem             → written to temp files
                                              → used by requests as cert=(path, path)
```

Credentials are **never** hardcoded, logged, or included in API responses.

### Populating secrets (one-time setup)

```bash
echo -n "your_username" | gcloud secrets versions add betfair-username --data-file=- --project chiops
echo -n "your_password" | gcloud secrets versions add betfair-password --data-file=- --project chiops
echo -n "your_app_key"  | gcloud secrets versions add betfair-app-key  --data-file=- --project chiops
cat betfair_cert.pem    | gcloud secrets versions add betfair-cert-pem --data-file=- --project chiops
cat betfair_key.pem     | gcloud secrets versions add betfair-key-pem  --data-file=- --project chiops
```

---

## 9. Betfair Streaming Protocol

### Transport

| Property | Value |
|---|---|
| Host | `stream-api.betfair.com` |
| Port | `443` |
| Protocol | Raw TLS (not WebSocket, not HTTP) |
| Message framing | JSON objects terminated by `\r\n` |
| Read buffer | 10MB (raised from asyncio 64KB default — see §16) |
| Client cert | Not on the stream socket — standard server-auth TLS only |
| Authentication | Via `op=authentication` JSON message after TLS handshake |

### Connection lifecycle

```
Client                                    Server (stream-api.betfair.com:443)
  │──── TLS handshake ──────────────────►│
  │◄─── {"op":"connection","connectionId":"..."} ─────────── Server speaks first
  │──── {"op":"authentication","appKey":"...","session":"..."} ──────────────►│
  │◄─── {"op":"status","statusCode":"SUCCESS"} ──────────────────────────────│
  │──── {"op":"marketSubscription","marketFilter":{...},"initialClk":"..."} ►│
  │◄─── {"op":"status","statusCode":"SUCCESS"} ──────────────────────────────│
  │◄─── {"op":"mcm","ct":"SUB_IMAGE",...} ─── Full initial image ────────────│
  │◄─── {"op":"mcm","ct":null,...} ────────── Ongoing deltas ────────────────│
  │◄─── {"op":"mcm","ct":"HEARTBEAT"} ─────── Keepalive (no data) ───────────│
```

### Session token (certlogin)

```
POST https://identitysso-cert.betfair.com/api/certlogin
Content-Type: application/x-www-form-urlencoded
X-Application: <app-key>
[TLS client certificate attached]

username=...&password=...
→ {"loginStatus": "SUCCESS", "sessionToken": "abc123"}
```

### Session keepalive

UK/IE sessions expire after 24h. FSU1A keepalives every 20h (configurable):

```
POST https://identitysso.betfair.com/api/keepAlive
X-Application: <app-key>
X-Authentication: <session-token>
```

### Subscribed market data fields

| Field constant | Data delivered |
|---|---|
| `EX_BEST_OFFERS` | `batb`, `batl` — best 3 levels each side |
| `EX_TRADED` | `trd` — traded volume at each price |
| `EX_TRADED_VOL` | `tv` — total matched volume scalar |
| `EX_LTP` | `ltp` — last traded price scalar |
| `EX_MARKET_DEF` | `marketDefinition` — market and runner metadata |
| `SP_PROJECTED` | `spn`, `spf` — SP near/far projections |

### Heartbeat & dead connection detection

- Server sends messages at `heartbeat_ms` interval (default 5000ms)
- If no message in `2 × heartbeat_ms` → connection declared dead → reconnect

### Status 503

`"status": 503` inside an MCM = high latency indicator. FSU1A sets `stream_latency=True` and **continues reading** — does not disconnect.

### Segmentation

Large market images split into segments (`SEG_START` / `SEG` / `SEG_END`). Deltas applied from every segment immediately. `clk` tokens updated only on `SEG_END` or non-segmented messages.

### Re-subscription (clk tokens)

On reconnect, FSU1A supplies stored `initialClk` + `clk` → Betfair sends `RESUB_DELTA` (patch only) instead of full `SUB_IMAGE`, minimising reconnect latency.

---

## 10. Market State & Delta Reconstruction

### Level-based ladders

Used for: `batb`, `batl`, `bdatb`, `bdatl`

```
Each update: [level, price, size]
Stored as:   dict[int → [price, size]]
Level 0 = best. size==0 → remove that level.
```

### Price-point ladders

Used for: `atb`, `atl`, `trd`, `spb`, `spl`

```
Each update: [price, size]
Stored as:   dict[float → size]
size==0 → remove that price.
```

### Full image (`img=True`)

When a `MarketChange` has `"img": true`, FSU1A clears the entire `MarketState` for that market before applying new data. Happens on initial subscription and after any integrity gap.

### CLOSED market eviction

Background task runs every 5 minutes. Any market whose `marketDefinition.status == "CLOSED"` is removed from the RAM cache.

---

## 11. API Reference — `/api/*`

All endpoints serve from RAM. Zero Betfair API calls at request time.

---

### `GET /api/markets`

List all markets in cache. Optional query filters:

| Param | Type | Example |
|---|---|---|
| `event_type_id` | string | `"1"` (Horse Racing) |
| `status` | string | `"OPEN"` |
| `in_play` | boolean | `true` |
| `market_type` | string | `"WIN"` |
| `country_code` | string | `"GB"` |

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
      "marketTime": "2026-04-21T13:30:00Z",
      "totalMatched": 125432.50,
      "inPlay": false,
      "bspMarket": false,
      "runnerCount": 8,
      "lastUpdateAt": "2026-04-21T13:28:45.123456+00:00"
    }
  ],
  "count": 98
}
```

---

### `GET /api/markets/{market_id}`

Single market with runner prices (best 3 levels each side).

**404** if market not in cache.

---

### `GET /api/markets/{market_id}/prices` ⭐ Primary Lay Engine endpoint

Compact runner prices. Best 3 levels of `batl`/`batb` per runner.

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

**Price arrays:** `[price, size]` — decimal odds and GBP amount.
- `bestAvailableToBack`: level 0 first = highest price (best for backer)
- `bestAvailableToLay`: level 0 first = lowest price (best for layer)

---

### `GET /api/markets/{market_id}/book`

Full order book — all price points, traded ladder, raw `marketDefinition`.

---

### `GET /api/stream/status`

```json
{
  "status": "connected",
  "connectionId": "101-210426080137-1262744",
  "latencyIndicator": false,
  "lastMessageAt": "2026-04-21T08:05:29.454881+00:00",
  "reconnectCount": 0,
  "marketCount": 98
}
```

---

## 12. Admin Reference — `/admin/*`

All endpoints follow **CHI-ADR-010** — structured form definitions, not raw values.

| Endpoint | Method | Purpose |
|---|---|---|
| `/admin/health` | GET | Portal header widget liveness |
| `/admin/status` | GET | Full runtime snapshot |
| `/admin/settings` | GET | Structured form definition of editable settings |
| `/admin/settings` | PUT | Apply validated updates → applied/rejected split |
| `/admin/config` | GET | Static schema (validation rules, defaults) |
| `/admin/logs` | GET | Paginated log ring-buffer (last 10,000 entries) |
| `/admin/stream` | GET | SSE stream of real-time admin events (15s keepalive) |

### SSE event types (`/admin/stream`)

| `type` | Trigger | Extra fields |
|---|---|---|
| `stream_status` | Connection state change | `status` |
| `mcm` | Market data received | `changeType`, `marketCount`, `latency` |
| `settings_updated` | Admin settings changed | `fields` |

---

## 13. Observability Reference

| Endpoint | Method | Purpose | Health condition |
|---|---|---|---|
| `/health` | GET | Liveness probe | `200` if healthy/degraded/draining; `503` if disconnected |
| `/ready` | GET | Readiness probe | `503` while stream in initial `disconnected` state |
| `/info` | GET | Static metadata | Always `200` |
| `/status` | GET | Full runtime snapshot | Always `200` |
| `/metrics` | GET | Prometheus text format | Always `200` |

### Health values

| `health` | HTTP | Meaning |
|---|---|---|
| `healthy` | 200 | Stream connected, no latency flag |
| `degraded` | 200 | Connecting or reconnecting |
| `draining` | 200 | Mode = `drain` |
| `unhealthy` | 503 | Disconnected and not reconnecting |

### Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `fsu1a_stream_connected` | gauge | `1` if stream connected |
| `fsu1a_stream_latency` | gauge | `1` if status 503 latency indicator active |
| `fsu1a_market_count` | gauge | Markets in RAM cache |
| `fsu1a_http_requests_total` | counter | Total HTTP requests |
| `fsu1a_http_errors_total` | counter | Total HTTP 5xx responses |
| `fsu1a_stream_reconnects_total` | counter | Total stream reconnect attempts |

---

## 14. Data Models

### RunnerState (internal RAM)

| Field | Type | Source ESA field | Notes |
|---|---|---|---|
| `selection_id` | int | `rc.id` | Betfair runner ID |
| `handicap` | float | `rc.hc` | 0.0 for non-handicap markets |
| `status` | str | `marketDefinition.runners[].status` | `ACTIVE`, `WINNER`, `LOSER`, `REMOVED` |
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
| `spn` | float\|None | `rc.sp.spn` | SP near projected |
| `spf` | float\|None | `rc.sp.spf` | SP far projected |
| `bsp` | float\|None | `rc.sp.bsp` | Calculated BSP |

### Market name construction

Betfair streaming does not provide a formatted name. FSU1A constructs:
```
"{HH:MM} {venue}"   →  "14:30 Ascot"
```
from `marketDefinition.marketTime` + `marketDefinition.venue`. Falls back to `"{venue} {marketType}"` or raw `marketId`.

---

## 15. Error Handling & Resilience

### Stream reconnect table

| Event | Response |
|---|---|
| TCP/TLS error | Close socket, begin backoff, reconnect |
| Heartbeat timeout (2× `heartbeat_ms`) | Same |
| `errorCode=INVALID_SESSION_INFORMATION` | Clear session token, force new certlogin, reconnect |
| `errorCode=MAX_CONNECTION_LIMIT_EXCEEDED` | Same |
| `status=503` in MCM | Set `stream_latency=True`, **continue reading** |
| Stale clk tokens on resubscription | Server auto-falls back to `SUB_IMAGE` — no special handling |
| Auth rejected at reconnect | `refresh_session()` forces new cert login before next stream auth |

### Backoff schedule

```
Attempt 1 → wait 1s
Attempt 2 → wait 2s
Attempt 3 → wait 4s
Attempt N → wait min(2^(N-1), reconnect_max_backoff_s)
Default cap: 300s
```

### Startup failures

| Failure | Behaviour |
|---|---|
| Firestore unavailable | Log error, proceed with `DEFAULT_SETTINGS` — stream still starts |
| Secret Manager unavailable | Log error, continue — stream will retry with backoff until secrets load |
| Betfair cert login fails | Retry with backoff — service self-heals |

---

## 16. Known Issues & Fixes

### asyncio readline buffer — `ValueError: chunk exceed the limit` (fixed v1.0.1)

**Symptom:** Stream connects and authenticates but immediately disconnects with:
```
ValueError: Separator is not found, and chunk exceed the limit
```

**Root cause:** asyncio's `StreamReader` has a default read limit of 64KB. Betfair's initial `SUB_IMAGE` response (containing all runner and market definition data for every subscribed market) easily exceeds this.

**Fix:** `asyncio.open_connection()` called with `limit=10 * 1024 * 1024` (10MB):
```python
reader, writer = await asyncio.open_connection(
    BETFAIR_STREAM_HOST, BETFAIR_STREAM_PORT, ssl=ssl_ctx,
    limit=10 * 1024 * 1024,
)
```

**Deployed:** `2026-04-21` in commit `ae08f95`.

---

## 17. IAM & Permissions

### Service account: `fsu1asa@chiops.iam.gserviceaccount.com`

| Role | Display name | Purpose |
|---|---|---|
| `roles/secretmanager.secretAccessor` | Secret Manager Secret Accessor | Read Betfair credentials |
| `roles/datastore.user` | Datastore User | Read/write Firestore settings document |

### Granting access

```bash
gcloud projects add-iam-policy-binding chiops \
  --member="serviceAccount:fsu1asa@chiops.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding chiops \
  --member="serviceAccount:fsu1asa@chiops.iam.gserviceaccount.com" \
  --role="roles/datastore.user"
```

### Granting downstream callers access to FSU1A

```bash
# Example — Lay Engine service account
gcloud run services add-iam-policy-binding fsu1a \
  --region europe-west2 \
  --project chiops \
  --member="serviceAccount:lay-engine@chiops.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

---

## 18. Deployment

### Build and push container

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
  --service-account fsu1asa@chiops.iam.gserviceaccount.com \
  --port 8080 \
  --min-instances 1 \
  --max-instances 3 \
  --memory 512Mi \
  --cpu 1 \
  --no-allow-unauthenticated
```

### Verify after deploy

```bash
TOKEN=$(gcloud auth print-identity-token)
URL=https://fsu1a-991649774709.europe-west2.run.app

curl -s -H "Authorization: Bearer $TOKEN" $URL/health
curl -s -H "Authorization: Bearer $TOKEN" $URL/status | python3 -m json.tool
```

---

## 19. Local Development

### Prerequisites

- Python 3.12
- `gcloud auth application-default login` (ADC for Firestore + Secret Manager)
- Betfair credentials in Secret Manager (or mock locally)

### Install and run

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

### Useful local endpoints

| URL | Purpose |
|---|---|
| `http://localhost:8080/docs` | Swagger UI |
| `http://localhost:8080/status` | Runtime state |
| `http://localhost:8080/api/markets` | Market list (empty until stream connects) |
| `http://localhost:8080/metrics` | Prometheus metrics |
| `http://localhost:8080/admin/logs` | Structured log buffer |

### Test the SSE admin stream

```bash
curl -N http://localhost:8080/admin/stream
```

### Build and run as container

```bash
docker build -t fsu1a:local .
docker run -p 8080:8080 \
  -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
  fsu1a:local
```
