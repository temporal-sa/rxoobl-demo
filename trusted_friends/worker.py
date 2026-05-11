from __future__ import annotations

import asyncio
import concurrent.futures

from temporalio.client import Client
from temporalio.common import VersioningBehavior
from temporalio.worker import Worker, WorkerDeploymentConfig, WorkerDeploymentVersion
from temporalio.runtime import Runtime, TelemetryConfig, PrometheusConfig

from trusted_friends.activities import (
    emit_tc_change_event,
    evaluate_event_eligibility,
    evaluate_initial_eligibility,
)
from trusted_friends.settings import (
    TASK_QUEUE,
    TEMPORAL_WORKER_BUILD_ID,
    TEMPORAL_WORKER_DEPLOYMENT_NAME,
    TEMPORAL_WORKER_VERSIONING,
)
from trusted_friends.temporal_client import connect_temporal_client
from trusted_friends.workflows import EligibilityEvaluationWorkflow, TrustedConnectionWorkflow


def _worker_deployment_config() -> WorkerDeploymentConfig | None:
    """Return Worker Deployment configuration when versioning is enabled.

    Worker Versioning has two halves:
    1. Workers advertise `deployment_name + build_id` while polling.
    2. Workflow starts opt into versioned routing and Temporal Cloud routes them
       to the deployment's current version.

    If any of the required env vars are missing, this worker intentionally runs
    as an unversioned poller so local development and tests remain simple.
    """
    if not (
        TEMPORAL_WORKER_VERSIONING
        and TEMPORAL_WORKER_DEPLOYMENT_NAME
        and TEMPORAL_WORKER_BUILD_ID
    ):
        return None
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(
            deployment_name=TEMPORAL_WORKER_DEPLOYMENT_NAME,
            build_id=TEMPORAL_WORKER_BUILD_ID,
        ),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.AUTO_UPGRADE,
    )


async def main() -> None:
    """Connect to Temporal and poll the task queue until the process exits."""

    # Prometheus metrics are especially useful in Cloud demos because they show
    # whether the worker is polling, executing workflow tasks, and draining
    # activity work. This must be set before the SDK runtime is first used.
    
    Runtime.set_default(
        Runtime(
            telemetry=TelemetryConfig(
                metrics=PrometheusConfig(bind_address="0.0.0.0:9090")
            )
        ),
        error_if_already_set=True,
    )

    # The same task queue is used for workflow and activity polling. Temporal
    # creates separate workflow/activity partitions behind the scenes, but the
    # application-level routing key remains `TASK_QUEUE`.
    client: Client = await connect_temporal_client()
    deployment_config = _worker_deployment_config()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as activity_executor:
        # Activities run in a thread pool because they are allowed to perform
        # blocking or non-deterministic work. Workflow code itself stays on the
        # SDK's deterministic workflow runner.
        worker_options = {
            "task_queue": TASK_QUEUE,
            "workflows": [TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
            "activities": [
                evaluate_initial_eligibility,
                evaluate_event_eligibility,
                emit_tc_change_event,
            ],
            "activity_executor": activity_executor,
        }
        if deployment_config is not None:
            # Passing `deployment_config` makes this a versioned worker. The
            # Build ID must match the Worker Deployment current version in
            # Temporal Cloud, otherwise AutoUpgrade workflows will not route to
            # this process.
            worker_options["deployment_config"] = deployment_config

        worker = Worker(
            client,
            **worker_options,
        )
        await worker.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
