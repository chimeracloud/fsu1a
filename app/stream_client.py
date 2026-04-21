"""
Betfair Exchange Streaming API client.

Protocol (per official ESA spec):
  • Plain TLS socket to stream-api.betfair.com:443  (no client cert — cert is only for certlogin)
  • JSON messages, each terminated by CRLF (\\r\\n)
  • Server sends connection message immediately on connect
  • Client authenticates, then subscribes to markets
  • Server streams MarketChangeMessages (op=mcm) continuously

Connection lifecycle:
  1. Open TLS socket
  2. Receive op=connection  → store connectionId
  3. Send op=authentication (appKey + sessionToken)
  4. Receive op=status      → check statusCode == "SUCCESS"
  5. Send op=marketSubscription (with initialClk/clk if resuming)
  6. Receive op=status      → subscription acknowledged
  7. Read loop: MCM deltas → market_cache.apply_mcm()
  8. On error/timeout → reconnect with exponential backoff

Reconnect / resubscription:
  • Store initialClk and clk from mcm messages (only on non-segmented or SEG_END)
  • Supply them on resubscription → server sends RESUB_DELTA instead of full SUB_IMAGE
  • If the server rejects the clk tokens it will send a fresh SUB_IMAGE instead

Heartbeat health:
  • If no message received for 2× heartbeatMs → connection is dead → reconnect
  • status=503 inside an MCM → high-latency indicator → set stream_latency flag, do NOT disconnect

Segmentation (segmentType: SEG_START | SEG | SEG_END | null):
  • Apply mc[] deltas from every segment immediately
  • Only update stored clk on null (non-segmented) or SEG_END

Periodic maintenance:
  • Session keepalive every N hours (configurable via Firestore)
  • CLOSED market eviction every 5 minutes
"""

import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

from app.betfair_auth import get_session_token, keepalive, refresh_session
from app.config import (
    BETFAIR_STREAM_HOST,
    BETFAIR_STREAM_PORT,
    MARKET_DATA_FIELDS,
)
from app.market_cache import market_cache
from app.secrets import get_credentials
from app.state import app_state

logger = logging.getLogger(__name__)
UTC = timezone.utc

# Module-level task handles so lifespan can cancel them
_tasks: list[asyncio.Task] = []

# Re-subscription tokens (persist across reconnects)
_initial_clk: Optional[str] = None
_clk: Optional[str] = None

# Monotonically increasing message ID
_msg_id: int = 0


def _next_id() -> int:
    global _msg_id
    _msg_id += 1
    return _msg_id


# ---------------------------------------------------------------------------
# Public API — called from main.py lifespan
# ---------------------------------------------------------------------------

async def start_stream() -> None:
    """Spawn background tasks. Called once at application startup."""
    _tasks.append(asyncio.create_task(_stream_loop(), name="betfair_stream"))
    _tasks.append(asyncio.create_task(_keepalive_loop(), name="betfair_keepalive"))
    _tasks.append(asyncio.create_task(_maintenance_loop(), name="betfair_maintenance"))


async def stop_stream() -> None:
    """Cancel all background tasks. Called at application shutdown."""
    for task in _tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
    _tasks.clear()


# ---------------------------------------------------------------------------
# Background loops
# ---------------------------------------------------------------------------

async def _keepalive_loop() -> None:
    """Periodically keep the Betfair REST session alive."""
    while True:
        hours = app_state.settings.get("session_keepalive_hours", 20)
        await asyncio.sleep(hours * 3600)
        logger.info("Running scheduled session keepalive")
        await keepalive()


async def _maintenance_loop() -> None:
    """Periodic cleanup of CLOSED markets."""
    while True:
        await asyncio.sleep(300)   # every 5 minutes
        removed = await market_cache.remove_closed()
        if removed:
            logger.info(f"Evicted {removed} closed market(s) from cache")
            app_state.market_count = await market_cache.count()


async def _stream_loop() -> None:
    """
    Main streaming loop. Runs forever. Reconnects with exponential backoff
    (starting at 1s, doubling, capped at reconnect_max_backoff_s).
    """
    global _initial_clk, _clk

    backoff = 1
    first_attempt = True

    while True:
        if not first_attempt:
            app_state.reconnect_count += 1

        first_attempt = False
        app_state.stream_status = "connecting"
        await app_state.broadcast({"type": "stream_status", "status": "connecting"})

        try:
            await _run_connection()
            # Clean return means the connection closed gracefully (e.g. shutdown)
            break

        except asyncio.CancelledError:
            app_state.stream_status = "disconnected"
            return

        except Exception as exc:
            logger.warning(f"Stream connection failed: {exc!r}")
            app_state.add_log("WARNING", f"Stream error: {exc!r}")

        finally:
            if app_state.stream_status != "disconnected":
                app_state.stream_status = "reconnecting"
                await app_state.broadcast({"type": "stream_status", "status": "reconnecting"})

        max_backoff = app_state.settings.get("reconnect_max_backoff_s", 300)
        wait = min(backoff, max_backoff)
        logger.info(f"Reconnecting in {wait}s (attempt backoff: {backoff}s)")
        app_state.add_log("INFO", f"Reconnecting in {wait}s")
        try:
            await asyncio.sleep(wait)
        except asyncio.CancelledError:
            app_state.stream_status = "disconnected"
            return

        backoff = min(backoff * 2, max_backoff)


async def _run_connection() -> None:
    """
    Open one TLS connection, authenticate, subscribe, and read until the connection dies.
    Raises on any failure so _stream_loop can handle reconnect.
    """
    global _initial_clk, _clk

    ssl_ctx = ssl.create_default_context()
    # The streaming socket uses standard server-auth TLS — no client cert.
    # limit=10MB: asyncio default (64KB) is too small for Betfair SUB_IMAGE responses.
    reader, writer = await asyncio.open_connection(
        BETFAIR_STREAM_HOST, BETFAIR_STREAM_PORT, ssl=ssl_ctx,
        limit=10 * 1024 * 1024,
    )

    try:
        # --- Step 1: receive connection message from server ---
        conn_msg = await _recv(reader)
        if conn_msg.get("op") != "connection":
            raise RuntimeError(f"Expected op=connection, got: {conn_msg.get('op')}")
        app_state.connection_id = conn_msg.get("connectionId")
        logger.info(f"ESA connected: connectionId={app_state.connection_id}")
        app_state.add_log("INFO", f"ESA connected: {app_state.connection_id}")

        # --- Step 2: authenticate ---
        # If previous session was rejected, force a fresh cert login
        try:
            session = await get_session_token()
        except Exception:
            session = await refresh_session()

        creds = get_credentials()
        await _send(writer, {
            "op": "authentication",
            "id": _next_id(),
            "appKey": creds["app_key"],
            "session": session,
        })

        auth_resp = await _recv(reader)
        _check_status(auth_resp, "authentication")

        # --- Step 3: subscribe to markets ---
        settings = app_state.settings
        sub_msg: dict = {
            "op": "marketSubscription",
            "id": _next_id(),
            "marketFilter": _build_market_filter(settings),
            "marketDataFilter": {"fields": MARKET_DATA_FIELDS},
        }
        if _initial_clk:
            sub_msg["initialClk"] = _initial_clk
        if _clk:
            sub_msg["clk"] = _clk

        await _send(writer, sub_msg)

        sub_resp = await _recv(reader)
        _check_status(sub_resp, "marketSubscription")

        app_state.stream_status = "connected"
        await app_state.broadcast({"type": "stream_status", "status": "connected"})
        logger.info("Market subscription active")

        # --- Step 4: read loop ---
        heartbeat_ms: int = settings.get("heartbeat_ms", 5000)
        read_timeout_s: float = (heartbeat_ms * 2) / 1000.0

        async for msg in _message_stream(reader, read_timeout_s):
            app_state.last_message_at = datetime.now(UTC)
            op = msg.get("op")

            if op == "mcm":
                await _handle_mcm(msg)

            elif op == "status":
                _handle_status_msg(msg)

            elif op == "connection":
                # Server can send a new connection message on reconnect
                app_state.connection_id = msg.get("connectionId")

    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# MCM handling
# ---------------------------------------------------------------------------

async def _handle_mcm(msg: dict) -> None:
    global _initial_clk, _clk

    # status=503 is a high-latency indicator — do NOT disconnect
    stream_status = msg.get("status")
    if stream_status == 503:
        if not app_state.stream_latency:
            app_state.stream_latency = True
            logger.warning("Stream status 503: high latency — continuing")
            app_state.add_log("WARNING", "Stream 503 latency indicator")
    else:
        app_state.stream_latency = False

    # Apply deltas to in-memory cache
    if msg.get("mc"):
        await market_cache.apply_mcm(msg)
        app_state.market_count = await market_cache.count()

    # Update clk tokens — only on non-segmented messages or SEG_END
    seg = msg.get("segmentType")
    if seg is None or seg == "SEG_END":
        if "initialClk" in msg:
            _initial_clk = msg["initialClk"]
        if "clk" in msg:
            _clk = msg["clk"]

    # Broadcast lightweight event to SSE subscribers
    ct = msg.get("ct")   # SUB_IMAGE | RESUB_DELTA | HEARTBEAT | null
    await app_state.broadcast({
        "type": "mcm",
        "changeType": ct,
        "marketCount": app_state.market_count,
        "latency": app_state.stream_latency,
    })


def _handle_status_msg(msg: dict) -> None:
    error_code = msg.get("errorCode")
    if not error_code:
        return

    logger.error(f"Stream status error: {error_code} — {msg.get('errorMessage')}")
    app_state.add_log("ERROR", f"Stream status error: {error_code}")

    # Session errors require a fresh cert login before next reconnect
    if error_code in ("INVALID_SESSION_INFORMATION", "MAX_CONNECTION_LIMIT_EXCEEDED",
                      "NOT_AUTHORIZED"):
        app_state.session_token = None
        raise RuntimeError(f"Stream auth error: {error_code}")


# ---------------------------------------------------------------------------
# Low-level I/O helpers
# ---------------------------------------------------------------------------

async def _send(writer: asyncio.StreamWriter, msg: dict) -> None:
    data = json.dumps(msg) + "\r\n"
    writer.write(data.encode())
    await writer.drain()


async def _recv(reader: asyncio.StreamReader) -> dict:
    """Read exactly one CRLF-terminated JSON message."""
    line = await reader.readline()
    if not line:
        raise RuntimeError("Stream EOF on recv")
    return json.loads(line.decode().strip())


async def _message_stream(
    reader: asyncio.StreamReader, timeout: float
) -> AsyncIterator[dict]:
    """
    Yield parsed JSON messages from the stream.
    Raises RuntimeError if no message arrives within `timeout` seconds
    (2× heartbeat = dead connection).
    """
    while True:
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"No message in {timeout:.1f}s — heartbeat timeout, connection dead"
            )

        if not line:
            raise RuntimeError("Stream EOF")

        text = line.decode().strip()
        if not text:
            continue   # blank keep-alive line

        try:
            yield json.loads(text)
        except json.JSONDecodeError as exc:
            logger.warning(f"Malformed stream message: {exc} — raw={text[:200]!r}")
            continue


def _check_status(msg: dict, context: str) -> None:
    """Raise if a status response indicates failure."""
    code = msg.get("statusCode")
    if code != "SUCCESS":
        raise RuntimeError(
            f"{context} failed: statusCode={code} error={msg.get('errorCode')} "
            f"msg={msg.get('errorMessage')}"
        )


# ---------------------------------------------------------------------------
# Market filter builder
# ---------------------------------------------------------------------------

def _build_market_filter(settings: dict) -> dict:
    f: dict = {}
    event_type_ids = settings.get("market_filter_event_type_ids") or []
    if event_type_ids:
        f["eventTypeIds"] = event_type_ids
    country_codes = settings.get("market_filter_country_codes") or []
    if country_codes:
        f["countryCodes"] = country_codes
    market_types = settings.get("market_filter_market_types") or []
    if market_types:
        f["marketTypes"] = market_types
    return f
