from __future__ import annotations

from typing import Any

from temporalio.client import Client

from trusted_friends.settings import (
    TEMPORAL_ADDRESS,
    TEMPORAL_API_KEY,
    TEMPORAL_NAMESPACE,
    TEMPORAL_TLS,
)


def temporal_client_options() -> dict[str, Any]:
    """Build the shared Temporal client options for both API and worker.

    The API starts/signals/queries workflows; the worker polls and executes
    them. Using one helper prevents subtle namespace/TLS/API-key drift between
    those two processes, which is especially important in Temporal Cloud.
    """
    options: dict[str, Any] = {
        "namespace": TEMPORAL_NAMESPACE,
        "tls": TEMPORAL_TLS,
    }
    if TEMPORAL_API_KEY:
        options["api_key"] = TEMPORAL_API_KEY
    return options


async def connect_temporal_client() -> Client:
    """Connect to either Temporal Cloud or local dev based on environment."""
    return await Client.connect(TEMPORAL_ADDRESS, **temporal_client_options())
