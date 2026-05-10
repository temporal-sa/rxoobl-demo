from __future__ import annotations

import importlib

from temporalio.common import AutoUpgradeVersioningOverride, VersioningBehavior

import trusted_friends.settings as settings


# Keep the list explicit so each test starts from a known environment instead of
# inheriting local .env or shell values.
TEMPORAL_ENV_KEYS = {
    "TEMPORAL_ADDRESS",
    "TEMPORAL_API_KEY",
    "TEMPORAL_NAMESPACE",
    "TEMPORAL_TLS",
    "TEMPORAL_WORKER_BUILD_ID",
    "TEMPORAL_WORKER_DEPLOYMENT_NAME",
    "TEMPORAL_WORKER_VERSIONING",
    "TASK_QUEUE",
}


def reload_settings(monkeypatch, **values: str):
    """Reload settings with only the requested Temporal environment values."""

    for key in TEMPORAL_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(settings)


def test_temporal_cloud_defaults_target_tf_demo_namespace(monkeypatch) -> None:
    reloaded = reload_settings(monkeypatch)

    assert reloaded.TEMPORAL_NAMESPACE == "tf-demo.zsvab"
    assert reloaded.TEMPORAL_ADDRESS == "tf-demo.zsvab.tmprl.cloud:7233"
    assert reloaded.TEMPORAL_TLS is True
    assert reloaded.TASK_QUEUE == "trusted-friends-demo"


def test_local_temporal_override_can_disable_tls(monkeypatch) -> None:
    reloaded = reload_settings(
        monkeypatch,
        TEMPORAL_NAMESPACE="default",
        TEMPORAL_ADDRESS="localhost:7233",
        TEMPORAL_TLS="false",
    )

    assert reloaded.TEMPORAL_NAMESPACE == "default"
    assert reloaded.TEMPORAL_ADDRESS == "localhost:7233"
    assert reloaded.TEMPORAL_TLS is False


def test_worker_versioning_defaults_on_when_deployment_version_is_set(monkeypatch) -> None:
    reloaded = reload_settings(
        monkeypatch,
        TEMPORAL_WORKER_DEPLOYMENT_NAME="trusted-friends-demo",
        TEMPORAL_WORKER_BUILD_ID="0.1.0",
    )

    assert reloaded.TEMPORAL_WORKER_VERSIONING is True


def test_workflow_start_uses_auto_upgrade_override_when_worker_versioning_enabled(
    monkeypatch,
) -> None:
    reload_settings(
        monkeypatch,
        TEMPORAL_WORKER_DEPLOYMENT_NAME="trusted-friends-demo",
        TEMPORAL_WORKER_BUILD_ID="0.1.0",
    )
    import trusted_friends.versioning as versioning

    reloaded = importlib.reload(versioning)

    # The API applies this override when starting workflows so new executions are
    # assigned to the current compatible Worker Deployment version in Cloud.
    assert reloaded.workflow_versioning_behavior() == VersioningBehavior.AUTO_UPGRADE
    assert isinstance(reloaded.workflow_versioning_override(), AutoUpgradeVersioningOverride)


def test_workflow_versioning_is_unspecified_when_worker_versioning_disabled(
    monkeypatch,
) -> None:
    reload_settings(monkeypatch)
    import trusted_friends.versioning as versioning

    reloaded = importlib.reload(versioning)

    assert reloaded.workflow_versioning_behavior() == VersioningBehavior.UNSPECIFIED
    assert reloaded.workflow_versioning_override() is None
