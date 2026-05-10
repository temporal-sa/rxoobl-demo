from __future__ import annotations

from temporalio.common import AutoUpgradeVersioningOverride, VersioningBehavior, VersioningOverride

from trusted_friends.settings import TEMPORAL_WORKER_VERSIONING


def workflow_versioning_behavior() -> VersioningBehavior:
    """Tell the SDK how workflow code should participate in Worker Versioning.

    Local tests and dev-server runs often use unversioned workers. In that mode
    the workflow decorator must stay UNSPECIFIED; Temporal rejects versioned
    workflow behavior if the worker itself is not in versioned mode.
    """
    if not TEMPORAL_WORKER_VERSIONING:
        return VersioningBehavior.UNSPECIFIED
    return VersioningBehavior.AUTO_UPGRADE


def workflow_versioning_override() -> VersioningOverride | None:
    """Route new starts to the Worker Deployment current version when enabled."""
    if not TEMPORAL_WORKER_VERSIONING:
        return None
    return AutoUpgradeVersioningOverride()
