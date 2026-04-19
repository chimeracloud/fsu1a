"""
Firestore settings client — FSU1A is the sole owner of writes to its document.
A new client is created per call (Cloud Run best practice for connection pooling).
"""

import logging

from google.cloud import firestore

from app.config import (
    DEFAULT_SETTINGS,
    FIRESTORE_COLLECTION,
    FIRESTORE_DOCUMENT,
    GCP_PROJECT,
)

logger = logging.getLogger(__name__)


def load_settings() -> dict:
    """
    Load settings from Firestore. Auto-bootstraps the document from DEFAULT_SETTINGS
    if it does not exist yet.
    """
    client = firestore.Client(project=GCP_PROJECT)
    doc_ref = client.collection(FIRESTORE_COLLECTION).document(FIRESTORE_DOCUMENT)
    doc = doc_ref.get()

    if not doc.exists:
        logger.info("No settings document found — bootstrapping defaults")
        bootstrap = DEFAULT_SETTINGS.copy()
        doc_ref.set(bootstrap)
        return bootstrap

    data = doc.to_dict()
    # Merge with defaults so any keys added in a new deployment are present
    result = DEFAULT_SETTINGS.copy()
    result.update(data)
    return result


def save_settings(settings: dict) -> dict:
    """
    Persist settings to Firestore, incrementing the version counter.
    Returns the saved dict (with updated version).
    """
    settings = settings.copy()
    settings["version"] = settings.get("version", 0) + 1

    client = firestore.Client(project=GCP_PROJECT)
    doc_ref = client.collection(FIRESTORE_COLLECTION).document(FIRESTORE_DOCUMENT)
    doc_ref.set(settings)

    logger.info(f"Settings saved (version={settings['version']})")
    return settings
