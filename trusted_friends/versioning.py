from __future__ import annotations

from temporalio.common import AutoUpgradeVersioningOverride, VersioningBehavior, VersioningOverride

from trusted_friends.settings import TEMPORAL_WORKER_VERSIONING


def workflow_versioning_behavior() -> VersioningBehavior:
    if not TEMPORAL_WORKER_VERSIONING:
        return VersioningBehavior.UNSPECIFIED
    return VersioningBehavior.AUTO_UPGRADE


def workflow_versioning_override() -> VersioningOverride | None:
    if not TEMPORAL_WORKER_VERSIONING:
        return None
    return AutoUpgradeVersioningOverride()
