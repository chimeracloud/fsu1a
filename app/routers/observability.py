"""
Observability endpoints — consumed by Cloud Run health checks, Prometheus scraping,
and the Chimera Portal status dashboard.

  GET /health    — liveness probe  (200 = up, 503 = unhealthy)
  GET /ready     — readiness probe (200 = ready to serve, 503 = not ready)
  GET /info      — static service metadata
  GET /status    — full runtime snapshot
  GET /metrics   — Prometheus text format
"""

from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse, PlainTextResponse

from app.config import GCP_PROJECT, GCP_REGION, SERVICE_NAME, SERVICE_VERSION
from app.state import app_state

router = APIRouter()
UTC = timezone.utc


@router.get("/health")
async def health():
    h = app_state.health
    code = 200 if h in ("healthy", "degraded", "draining") else 503
    return JSONResponse({"status": h, "service": SERVICE_NAME}, status_code=code)


@router.get("/ready")
async def ready():
    if app_state.stream_status in ("connected", "reconnecting"):
        return JSONResponse({"ready": True})
    return JSONResponse({"ready": False, "reason": app_state.stream_status}, status_code=503)


@router.get("/info")
async def info():
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "project": GCP_PROJECT,
        "region": GCP_REGION,
        "startedAt": app_state.started_at.isoformat(),
        "uptimeSeconds": (datetime.now(UTC) - app_state.started_at).total_seconds(),
    }


@router.get("/status")
async def status():
    lm = app_state.last_message_at
    sa = app_state.session_acquired_at
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "health": app_state.health,
        "mode": app_state.mode,
        "stream": {
            "status": app_state.stream_status,
            "connectionId": app_state.connection_id,
            "latencyIndicator": app_state.stream_latency,
            "lastMessageAt": lm.isoformat() if lm else None,
            "reconnectCount": app_state.reconnect_count,
            "marketCount": app_state.market_count,
        },
        "session": {
            "acquiredAt": sa.isoformat() if sa else None,
        },
        "requests": {
            "total": app_state.request_count,
            "errors": app_state.error_count,
            "errorRate": round(app_state.error_rate, 6),
        },
        "startedAt": app_state.started_at.isoformat(),
        "uptimeSeconds": (datetime.now(UTC) - app_state.started_at).total_seconds(),
    }


@router.get("/metrics")
async def metrics():
    """Prometheus exposition format (text/plain; version=0.0.4)."""
    svc = SERVICE_NAME
    connected = 1 if app_state.stream_status == "connected" else 0
    lines = [
        f"# HELP {svc}_stream_connected Stream connected (1=yes 0=no)",
        f"# TYPE {svc}_stream_connected gauge",
        f"{svc}_stream_connected {connected}",
        "",
        f"# HELP {svc}_stream_latency Stream 503 latency indicator (1=yes)",
        f"# TYPE {svc}_stream_latency gauge",
        f"{svc}_stream_latency {1 if app_state.stream_latency else 0}",
        "",
        f"# HELP {svc}_market_count Markets in in-memory cache",
        f"# TYPE {svc}_market_count gauge",
        f"{svc}_market_count {app_state.market_count}",
        "",
        f"# HELP {svc}_http_requests_total Total HTTP requests received",
        f"# TYPE {svc}_http_requests_total counter",
        f"{svc}_http_requests_total {app_state.request_count}",
        "",
        f"# HELP {svc}_http_errors_total Total HTTP 5xx responses",
        f"# TYPE {svc}_http_errors_total counter",
        f"{svc}_http_errors_total {app_state.error_count}",
        "",
        f"# HELP {svc}_stream_reconnects_total Total stream reconnect attempts",
        f"# TYPE {svc}_stream_reconnects_total counter",
        f"{svc}_stream_reconnects_total {app_state.reconnect_count}",
        "",
    ]
    return PlainTextResponse(
        "\n".join(lines), media_type="text/plain; version=0.0.4; charset=utf-8"
    )
