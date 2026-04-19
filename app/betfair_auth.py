"""
Betfair authentication — non-interactive (bot) cert login and session keepalive.

Login flow (per Betfair ESA docs):
  POST https://identitysso-cert.betfair.com/api/certlogin
  with self-signed SSL client certificate + form-encoded username/password
  → {"loginStatus": "SUCCESS", "sessionToken": "..."}

Keepalive (UK/IE sessions expire after 24h, so refresh every ~20h):
  POST https://identitysso.betfair.com/api/keepAlive
  with X-Application and X-Authentication headers

All network calls are dispatched via run_in_executor so the event loop is not blocked.
An asyncio.Lock guards session state so concurrent stream reconnects don't race.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from app.config import BETFAIR_CERTLOGIN_URL, BETFAIR_KEEPALIVE_URL
from app.secrets import get_credentials
from app.state import app_state

logger = logging.getLogger(__name__)

UTC = timezone.utc
_lock = asyncio.Lock()


async def get_session_token() -> str:
    """
    Return a valid session token. Issues a new cert login if no token exists or
    the token age exceeds the configured keepalive interval.
    """
    async with _lock:
        settings = app_state.settings
        keepalive_hours: int = settings.get("session_keepalive_hours", 20)

        if app_state.session_token and app_state.session_acquired_at:
            age = datetime.now(UTC) - app_state.session_acquired_at
            if age < timedelta(hours=keepalive_hours):
                return app_state.session_token

        token = await _certlogin()
        app_state.session_token = token
        app_state.session_acquired_at = datetime.now(UTC)
        return token


async def refresh_session() -> str:
    """Force-expire the cached token and issue a fresh cert login."""
    async with _lock:
        app_state.session_token = None
        app_state.session_acquired_at = None

    return await get_session_token()


async def keepalive() -> bool:
    """
    Extend the current session lifetime. Returns True on success.
    If the keepalive fails the cached token is cleared so the next
    get_session_token() call will re-authenticate.
    """
    async with _lock:
        if not app_state.session_token:
            return False

        creds = get_credentials()
        loop = asyncio.get_event_loop()
        try:
            ok = await loop.run_in_executor(
                None, _do_keepalive, creds, app_state.session_token
            )
            if ok:
                app_state.session_acquired_at = datetime.now(UTC)
            else:
                app_state.session_token = None
            return ok
        except Exception as exc:
            logger.warning(f"Keepalive failed: {exc}")
            app_state.session_token = None
            app_state.add_log("WARNING", f"Betfair keepalive failed: {exc}")
            return False


# ---------------------------------------------------------------------------
# Private sync helpers (run in executor)
# ---------------------------------------------------------------------------

async def _certlogin() -> str:
    creds = get_credentials()
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _do_certlogin, creds)


def _do_certlogin(creds: dict) -> str:
    """
    Synchronous cert login. Runs in a thread pool executor.
    The client certificate is only used here — NOT on the streaming socket.
    """
    resp = requests.post(
        BETFAIR_CERTLOGIN_URL,
        data={
            "username": creds["username"],
            "password": creds["password"],
        },
        cert=(creds["cert_path"], creds["key_path"]),
        headers={
            "X-Application": creds["app_key"],
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    resp.raise_for_status()

    body = resp.json()
    status = body.get("loginStatus")
    if status != "SUCCESS":
        raise RuntimeError(f"Betfair certlogin failed: status={status}")

    token: Optional[str] = body.get("sessionToken")
    if not token:
        raise RuntimeError("Betfair certlogin returned no sessionToken")

    logger.info("Betfair session acquired via cert login")
    app_state.add_log("INFO", "Betfair session acquired")
    return token


def _do_keepalive(creds: dict, session_token: str) -> bool:
    resp = requests.post(
        BETFAIR_KEEPALIVE_URL,
        headers={
            "X-Application": creds["app_key"],
            "X-Authentication": session_token,
        },
        timeout=15,
    )
    resp.raise_for_status()

    body = resp.json()
    ok = body.get("status") == "SUCCESS"
    if ok:
        logger.info("Betfair session keepalive succeeded")
        app_state.add_log("INFO", "Betfair session keepalive succeeded")
    else:
        logger.warning(f"Betfair keepalive non-success: {body.get('status')}")
    return ok
