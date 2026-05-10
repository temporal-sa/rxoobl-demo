from __future__ import annotations

import os


def _env_bool(name: str, *, default: bool) -> bool:
    """Parse deployment flags from env without making every caller repeat it."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


# These defaults intentionally target Temporal Cloud. Local development can
# override them with `TEMPORAL_NAMESPACE=default`, `TEMPORAL_ADDRESS=localhost:7233`,
# and `TEMPORAL_TLS=false`.
TEMPORAL_NAMESPACE = os.getenv("TEMPORAL_NAMESPACE", "tf-demo.zsvab")
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", f"{TEMPORAL_NAMESPACE}.tmprl.cloud:7233")
TEMPORAL_API_KEY = os.getenv("TEMPORAL_API_KEY")
TEMPORAL_TLS = _env_bool(
    "TEMPORAL_TLS",
    default=bool(TEMPORAL_API_KEY) or TEMPORAL_ADDRESS.endswith(".tmprl.cloud:7233"),
)
TASK_QUEUE = os.getenv("TASK_QUEUE", "trusted-friends-demo")

# Worker Deployment Versioning is opt-in. When enabled, workers advertise their
# deployment/build ID, and workflow starts use versioned routing so Temporal
# Cloud can route AutoUpgrade executions to the deployment's current version.
TEMPORAL_WORKER_DEPLOYMENT_NAME = os.getenv("TEMPORAL_WORKER_DEPLOYMENT_NAME", "")
TEMPORAL_WORKER_BUILD_ID = os.getenv("TEMPORAL_WORKER_BUILD_ID", "")
TEMPORAL_WORKER_VERSIONING = _env_bool(
    "TEMPORAL_WORKER_VERSIONING",
    default=bool(TEMPORAL_WORKER_DEPLOYMENT_NAME and TEMPORAL_WORKER_BUILD_ID),
)
DEFAULT_CONSENT_TTL_SECONDS = int(os.getenv("DEFAULT_CONSENT_TTL_SECONDS", "120"))
