from __future__ import annotations

import asyncio
import concurrent.futures

from temporalio.client import Client
from temporalio.common import VersioningBehavior
from temporalio.worker import Worker, WorkerDeploymentConfig, WorkerDeploymentVersion

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
    client: Client = await connect_temporal_client()
    deployment_config = _worker_deployment_config()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as activity_executor:
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
