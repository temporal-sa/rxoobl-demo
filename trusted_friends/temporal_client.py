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
    options: dict[str, Any] = {
        "namespace": TEMPORAL_NAMESPACE,
        "tls": TEMPORAL_TLS,
    }
    if TEMPORAL_API_KEY:
        options["api_key"] = TEMPORAL_API_KEY
    return options


async def connect_temporal_client() -> Client:
    return await Client.connect(TEMPORAL_ADDRESS, **temporal_client_options())
