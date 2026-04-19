"""
FSU1A — Betfair Exchange Live API Gateway
Application entry point and lifespan management.
"""

import json
import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.config import SERVICE_NAME, SERVICE_VERSION
from app.firestore_client import load_settings
from app.secrets import get_credentials
from app.state import app_state
from app.stream_client import start_stream, stop_stream
from app.routers import admin, api, observability


# ---------------------------------------------------------------------------
# Structured JSON logging (GCP Cloud Logging compatible)
# ---------------------------------------------------------------------------

class _GCPJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        obj: dict = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "service": SERVICE_NAME,
        }
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)
        return json.dumps(obj)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_GCPJsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    logger = logging.getLogger(__name__)
    logger.info(f"Starting {SERVICE_NAME} v{SERVICE_VERSION}")

    # 1. Load settings from Firestore (auto-bootstraps if missing)
    try:
        settings = load_settings()
        app_state.settings = settings
        app_state.mode = settings.get("mode", "active")
        logger.info(f"Settings loaded — mode={app_state.mode} version={settings.get('version', 0)}")
    except Exception as exc:
        logger.error(f"Failed to load Firestore settings: {exc!r} — using defaults")
        from app.config import DEFAULT_SETTINGS
        app_state.settings = DEFAULT_SETTINGS.copy()

    # 2. Pre-fetch credentials from Secret Manager (validates access at startup)
    try:
        get_credentials()
        logger.info("Credentials loaded from Secret Manager")
    except Exception as exc:
        logger.error(f"Failed to load credentials: {exc!r}")
        # Do not abort — stream_client will fail gracefully and retry

    # 3. Start background streaming tasks
    await start_stream()
    logger.info("Betfair stream client started")

    yield

    # Shutdown
    logger.info(f"Shutting down {SERVICE_NAME}")
    await stop_stream()
    logger.info(f"{SERVICE_NAME} shutdown complete")


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title=SERVICE_NAME,
    version=SERVICE_VERSION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
    openapi_url="/openapi.json",
)

app.include_router(api.router,          prefix="/api",   tags=["api"])
app.include_router(admin.router,        prefix="/admin", tags=["admin"])
app.include_router(observability.router,                 tags=["observability"])


# ---------------------------------------------------------------------------
# Request tracking middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def _track_requests(request: Request, call_next):
    app_state.request_count += 1
    response = await call_next(request)
    if response.status_code >= 500:
        app_state.error_count += 1
    return response
