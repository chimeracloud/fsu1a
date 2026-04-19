"""
Credentials loader — fetches from Secret Manager once and caches in memory.
Cert/key PEM strings are written to temp files for the requests library.
Never call this module before the GCP service account ADC is available.
"""

import logging
import tempfile
from typing import Optional

from google.cloud import secretmanager

from app.config import (
    GCP_PROJECT,
    SECRET_BETFAIR_APP_KEY,
    SECRET_BETFAIR_CERT_PEM,
    SECRET_BETFAIR_KEY_PEM,
    SECRET_BETFAIR_PASSWORD,
    SECRET_BETFAIR_USERNAME,
)

logger = logging.getLogger(__name__)

_cached: Optional[dict] = None
# Temp file paths are kept in module scope so files are not GC'd during process lifetime
_cert_file: Optional[object] = None
_key_file: Optional[object] = None


def _fetch(client: secretmanager.SecretManagerServiceClient, secret_id: str) -> str:
    path = f"projects/{GCP_PROJECT}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=path)
    return response.payload.data.decode("utf-8")


def get_credentials() -> dict:
    """Return cached credentials dict. Loads from Secret Manager on first call."""
    global _cached, _cert_file, _key_file

    if _cached is not None:
        return _cached

    logger.info("Loading credentials from Secret Manager")
    client = secretmanager.SecretManagerServiceClient()

    username = _fetch(client, SECRET_BETFAIR_USERNAME)
    password = _fetch(client, SECRET_BETFAIR_PASSWORD)
    app_key = _fetch(client, SECRET_BETFAIR_APP_KEY)
    cert_pem = _fetch(client, SECRET_BETFAIR_CERT_PEM)
    key_pem = _fetch(client, SECRET_BETFAIR_KEY_PEM)

    # Write cert and key to temp files so requests can reference them as file paths.
    # delete=False keeps them alive for the process lifetime; we hold module-level refs.
    _cert_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
    _cert_file.write(cert_pem)
    _cert_file.flush()

    _key_file = tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False)
    _key_file.write(key_pem)
    _key_file.flush()

    _cached = {
        "username": username,
        "password": password,
        "app_key": app_key,
        "cert_pem": cert_pem,
        "key_pem": key_pem,
        "cert_path": _cert_file.name,
        "key_path": _key_file.name,
    }

    logger.info("Credentials loaded and cert/key written to temp files")
    return _cached
