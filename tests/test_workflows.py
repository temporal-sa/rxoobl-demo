from __future__ import annotations

import asyncio
import concurrent.futures
import shutil
import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from trusted_friends.activities import (
    emit_tc_change_event,
    evaluate_event_eligibility,
    evaluate_initial_eligibility,
)
from trusted_friends.models import (
    ConnectionStatus,
    ConsentStatus,
    EligibilityEvent,
    ParentalConsent,
    RelationshipEvent,
    RelationshipEventType,
    SourceChannel,
    TrustedConnectionRequest,
    UserEligibilitySnapshot,
)
from trusted_friends.rules import workflow_id_for_pair
from trusted_friends.workflows import EligibilityEvaluationWorkflow, TrustedConnectionWorkflow


ACTIVITIES = [evaluate_initial_eligibility, evaluate_event_eligibility, emit_tc_change_event]
TEMPORAL_CLI_PATH = shutil.which("temporal")


async def start_temporal_environment() -> WorkflowEnvironment:
    kwargs = {}
    if TEMPORAL_CLI_PATH:
        kwargs["dev_server_existing_path"] = TEMPORAL_CLI_PATH
    return await WorkflowEnvironment.start_local(**kwargs)


async def wait_for_status(handle, expected: ConnectionStatus, timeout: float = 5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    last_state = None
    while asyncio.get_running_loop().time() < deadline:
        last_state = await handle.query(TrustedConnectionWorkflow.get_state)
        if last_state.status == expected:
            return last_state
        await asyncio.sleep(0.05)
    raise AssertionError(f"expected {expected}, got {last_state}")


def adult_request(user_id_a: str = "alice", user_id_b: str = "bob") -> TrustedConnectionRequest:
    return TrustedConnectionRequest(
        requester_user_id=user_id_a,
        target_user_id=user_id_b,
        source_channel=SourceChannel.STANDARD,
        requester_snapshot=UserEligibilitySnapshot(user_id_a, age=18),
        target_snapshot=UserEligibilitySnapshot(user_id_b, age=18),
        consent_ttl_seconds=5,
    )


async def test_normal_send_accept_becomes_trusted() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                request = adult_request()
                workflow_id = workflow_id_for_pair(
                    request.requester_user_id,
                    request.target_user_id,
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                pending = await wait_for_status(handle, ConnectionStatus.PENDING)
                assert not pending.accepted

                await handle.signal(TrustedConnectionWorkflow.accept)
                trusted = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert trusted.accepted
                assert trusted.reason == "USER_ACCEPTED"


async def test_generic_relationship_event_can_bypass_approval() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                request = adult_request("bypass-a", "bypass-b")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("bypass-a", "bypass-b"),
                    task_queue=task_queue,
                )
                await wait_for_status(handle, ConnectionStatus.PENDING)

                await handle.signal(
                    TrustedConnectionWorkflow.apply_relationship_event,
                    RelationshipEvent(
                        event_type=RelationshipEventType.ACCEPTED,
                        reason="IRL_AUTO_UPGRADE",
                        bypass_approval=True,
                    ),
                )

                trusted = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert trusted.accepted


async def test_close_signal_completes_pair_workflow() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                request = adult_request("close-a", "close-b")
                workflow_id = workflow_id_for_pair(
                    request.requester_user_id,
                    request.target_user_id,
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id,
                    task_queue=task_queue,
                )
                await wait_for_status(handle, ConnectionStatus.PENDING)

                await handle.signal(TrustedConnectionWorkflow.close)
                assert await handle.result() is None


async def test_vpc_approval_then_acceptance_becomes_trusted() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                request = TrustedConnectionRequest(
                    requester_user_id="child",
                    target_user_id="friend",
                    source_channel=SourceChannel.STANDARD,
                    requester_snapshot=UserEligibilitySnapshot("child", age=12),
                    target_snapshot=UserEligibilitySnapshot("friend", age=12),
                    consent_ttl_seconds=5,
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("child", "friend"),
                    task_queue=task_queue,
                )

                waiting = await wait_for_status(
                    handle,
                    ConnectionStatus.WAITING_FOR_PARENTAL_CONSENT,
                )
                assert waiting.consent_required

                await handle.signal(TrustedConnectionWorkflow.accept)
                await handle.signal(
                    TrustedConnectionWorkflow.parental_consent,
                    ParentalConsent("consent-1", ConsentStatus.APPROVED),
                )
                trusted = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert trusted.consent_status == ConsentStatus.APPROVED


async def test_vpc_denial_marks_denied() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                request = TrustedConnectionRequest(
                    requester_user_id="child-denied",
                    target_user_id="friend-denied",
                    source_channel=SourceChannel.STANDARD,
                    requester_snapshot=UserEligibilitySnapshot("child-denied", age=12),
                    target_snapshot=UserEligibilitySnapshot("friend-denied", age=12),
                    consent_ttl_seconds=5,
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("child-denied", "friend-denied"),
                    task_queue=task_queue,
                )

                await wait_for_status(
                    handle,
                    ConnectionStatus.WAITING_FOR_PARENTAL_CONSENT,
                )
                await handle.signal(
                    TrustedConnectionWorkflow.parental_consent,
                    ParentalConsent("consent-denied", ConsentStatus.DENIED),
                )
                denied = await wait_for_status(handle, ConnectionStatus.DENIED)
                assert denied.reason == "PARENTAL_CONSENT_DENIED"


async def test_vpc_timeout_marks_expired() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                request = TrustedConnectionRequest(
                    requester_user_id="child-timeout",
                    target_user_id="friend-timeout",
                    source_channel=SourceChannel.STANDARD,
                    requester_snapshot=UserEligibilitySnapshot("child-timeout", age=12),
                    target_snapshot=UserEligibilitySnapshot("friend-timeout", age=12),
                    consent_ttl_seconds=1,
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("child-timeout", "friend-timeout"),
                    task_queue=task_queue,
                )

                await wait_for_status(
                    handle,
                    ConnectionStatus.WAITING_FOR_PARENTAL_CONSENT,
                )
                await asyncio.sleep(1.5)
                expired = await wait_for_status(handle, ConnectionStatus.EXPIRED)
                assert expired.reason == "PARENTAL_CONSENT_TIMEOUT"


async def test_eligibility_workflow_suspends_and_restores_pair() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                pair_workflow_id = workflow_id_for_pair("alice-event", "bob-event")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    adult_request("alice-event", "bob-event"),
                    id=pair_workflow_id,
                    task_queue=task_queue,
                )
                await handle.signal(TrustedConnectionWorkflow.accept)
                await wait_for_status(handle, ConnectionStatus.TRUSTED)

                suspended_event = EligibilityEvent(
                    event_id="event-suspend",
                    user_id_a="alice-event",
                    user_id_b="bob-event",
                    changed_user_id="alice-event",
                    snapshot=UserEligibilitySnapshot(
                        "alice-event",
                        age=18,
                        is_age_verified=False,
                        is_on_watchlist=True,
                    ),
                    pair_workflow_id=pair_workflow_id,
                )
                update = await env.client.execute_workflow(
                    EligibilityEvaluationWorkflow.run,
                    suspended_event,
                    id="eligibility-eval-event-suspend",
                    task_queue=task_queue,
                )
                assert not update.eligible
                suspended = await wait_for_status(handle, ConnectionStatus.SUSPENDED)
                assert suspended.last_eligibility_event_id == "event-suspend"

                restored_event = EligibilityEvent(
                    event_id="event-restore",
                    user_id_a="alice-event",
                    user_id_b="bob-event",
                    changed_user_id="alice-event",
                    snapshot=UserEligibilitySnapshot(
                        "alice-event",
                        age=18,
                        is_age_verified=True,
                        is_on_watchlist=False,
                    ),
                    pair_workflow_id=pair_workflow_id,
                )
                update = await env.client.execute_workflow(
                    EligibilityEvaluationWorkflow.run,
                    restored_event,
                    id="eligibility-eval-event-restore",
                    task_queue=task_queue,
                )
                assert update.eligible
                restored = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert restored.last_eligibility_event_id == "event-restore"


async def test_eligibility_workflow_completes_if_pair_was_terminated() -> None:
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=ACTIVITIES,
                activity_executor=activity_executor,
            ):
                pair_workflow_id = workflow_id_for_pair("terminated-a", "terminated-b")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    adult_request("terminated-a", "terminated-b"),
                    id=pair_workflow_id,
                    task_queue=task_queue,
                )
                await wait_for_status(handle, ConnectionStatus.PENDING)
                await handle.terminate(reason="demo cleanup")

                update = await env.client.execute_workflow(
                    EligibilityEvaluationWorkflow.run,
                    EligibilityEvent(
                        event_id="event-after-terminate",
                        user_id_a="terminated-a",
                        user_id_b="terminated-b",
                        changed_user_id="terminated-a",
                        snapshot=UserEligibilitySnapshot(
                            "terminated-a",
                            age=18,
                            is_age_verified=False,
                            is_on_watchlist=True,
                        ),
                        pair_workflow_id=pair_workflow_id,
                    ),
                    id="eligibility-eval-event-after-terminate",
                    task_queue=task_queue,
                )

                assert not update.eligible
                assert update.reason == "AGE_VERIFICATION_REQUIRED"
