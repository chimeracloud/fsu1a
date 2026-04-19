"""
Admin endpoints — consumed exclusively by Chimera Portal.
Follows CHI-ADR-010: all endpoints return structured form definitions,
not raw values, so the Portal can render edit UI without bespoke knowledge
of each FSU's schema.

  GET  /admin/health     Liveness + mode (Portal header widget)
  GET  /admin/status     Full runtime snapshot
  GET  /admin/settings   Structured form definition of editable settings
  PUT  /admin/settings   Apply validated updates; returns applied/rejected split
  GET  /admin/config     Static schema (field list, validation rules, defaults)
  GET  /admin/logs       Paginated structured log ring-buffer
  GET  /admin/stream     SSE stream of real-time admin events
"""

import asyncio
import json
import logging
from typing import Optional

from fastapi import APIRouter, Request
from sse_starlette.sse import EventSourceResponse

from app.config import (
    DEFAULT_SETTINGS,
    EDITABLE_FIELDS,
    SERVICE_NAME,
    SERVICE_VERSION,
    VALIDATION_RULES,
)
from app.firestore_client import save_settings
from app.state import app_state

router = APIRouter()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@router.get("/health")
async def admin_health():
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "health": app_state.health,
        "mode": app_state.mode,
        "streamStatus": app_state.stream_status,
    }


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@router.get("/status")
async def admin_status():
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
    }


# ---------------------------------------------------------------------------
# Settings — GET (structured form) and PUT (validated update)
# ---------------------------------------------------------------------------

@router.get("/settings")
async def get_settings():
    """
    Returns a structured form definition (per CHI-ADR-010) — not a raw key/value map.
    The Portal uses `fields` to render the edit UI.
    """
    settings = app_state.settings or {}
    form_fields = []

    for field_name in EDITABLE_FIELDS:
        current_value = settings.get(field_name, DEFAULT_SETTINGS.get(field_name))
        rule = VALIDATION_RULES.get(field_name, {})

        fd: dict = {
            "name": field_name,
            "label": rule.get("label", field_name),
            "value": current_value,
            "type": rule.get("type", "string"),
        }

        if rule.get("type") == "enum":
            fd["options"] = rule.get("values", [])
        elif rule.get("type") == "int":
            fd["min"] = rule.get("min")
            fd["max"] = rule.get("max")

        form_fields.append(fd)

    return {
        "service": SERVICE_NAME,
        "version": settings.get("version", 0),
        "fields": form_fields,
    }


@router.put("/settings")
async def update_settings(body: dict):
    """
    Apply a partial settings update.  Returns an applied/rejected split so the
    Portal can surface validation errors inline without a separate round-trip.
    """
    current = (app_state.settings or DEFAULT_SETTINGS).copy()
    applied: dict = {}
    rejected: dict = {}

    for key, value in body.items():
        if key not in EDITABLE_FIELDS:
            rejected[key] = f"'{key}' is not an editable field"
            continue

        err = _validate(key, value)
        if err:
            rejected[key] = err
            continue

        current[key] = value
        applied[key] = value

    if applied:
        saved = save_settings(current)
        app_state.settings = saved
        app_state.mode = saved.get("mode", "active")
        logger.info(f"Settings updated: {list(applied.keys())}")
        app_state.add_log("INFO", f"Settings updated: {list(applied.keys())}")
        await app_state.broadcast({"type": "settings_updated", "fields": list(applied.keys())})

    return {
        "applied": applied,
        "rejected": rejected,
        "version": current.get("version", 0),
    }


def _validate(key: str, value) -> Optional[str]:
    rule = VALIDATION_RULES.get(key)
    if not rule:
        return None
    vtype = rule.get("type")
    if vtype == "enum":
        if value not in rule.get("values", []):
            return f"Must be one of: {rule['values']}"
    elif vtype == "int":
        if not isinstance(value, int):
            return "Must be an integer"
        mn = rule.get("min")
        mx = rule.get("max")
        if mn is not None and value < mn:
            return f"Must be >= {mn}"
        if mx is not None and value > mx:
            return f"Must be <= {mx}"
    elif vtype == "list_str":
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            return "Must be a list of strings"
    return None


# ---------------------------------------------------------------------------
# Config (static schema)
# ---------------------------------------------------------------------------

@router.get("/config")
async def get_config():
    """Static schema dump — Portal uses this to build advanced editor views."""
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "editableFields": EDITABLE_FIELDS,
        "validationRules": VALIDATION_RULES,
        "defaults": DEFAULT_SETTINGS,
    }


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

@router.get("/logs")
async def get_logs(limit: int = 100, offset: int = 0):
    logs, total = app_state.get_logs(limit=limit, offset=offset)
    return {
        "logs": logs,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# SSE event stream
# ---------------------------------------------------------------------------

@router.get("/stream")
async def admin_stream(request: Request):
    """
    Server-Sent Events stream of real-time admin events.
    Sends a keepalive comment every 15s if no event arrives,
    per FSU1E pattern.
    """
    q = await app_state.subscribe()

    async def generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield {"data": json.dumps(event)}
                except asyncio.TimeoutError:
                    yield {"comment": "keepalive"}
        finally:
            await app_state.unsubscribe(q)

    return EventSourceResponse(generator())
