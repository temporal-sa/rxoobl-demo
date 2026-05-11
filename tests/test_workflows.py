from __future__ import annotations

import asyncio
import concurrent.futures
import shutil
import uuid

from temporalio.testing import WorkflowEnvironment
from temporalio import activity
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
    EligibilityEventType,
    ParentalConsent,
    RelationshipEvent,
    RelationshipEventType,
    SourceChannel,
    TCChangeEvent,
    TrustedConnectionRequest,
    UserEligibilitySnapshot,
)
from trusted_friends.rules import workflow_id_for_pair
from trusted_friends.workflows import (
    CONTINUE_AS_NEW_EVENT_LIMIT,
    EligibilityEvaluationWorkflow,
    TrustedConnectionWorkflow,
)


# Workflow tests run against the Temporal test server so they exercise real task
# polling, timers, signals, activities, and workflow queries instead of only
# calling workflow methods directly.
ACTIVITIES = [evaluate_initial_eligibility, evaluate_event_eligibility, emit_tc_change_event]
TEMPORAL_CLI_PATH = shutil.which("temporal")
RECORDED_TC_EVENTS: list[TCChangeEvent] = []


@activity.defn(name="emit_tc_change_event")
def record_tc_change_event(event: TCChangeEvent) -> str:
    """Test activity that records downstream TC publications."""

    RECORDED_TC_EVENTS.append(event)
    return event.event_id


async def start_temporal_environment() -> WorkflowEnvironment:
    """Start a local Temporal test environment, reusing the CLI when installed."""

    kwargs = {}
    if TEMPORAL_CLI_PATH:
        kwargs["dev_server_existing_path"] = TEMPORAL_CLI_PATH
    return await WorkflowEnvironment.start_local(**kwargs)


async def wait_for_status(handle, expected: ConnectionStatus, timeout: float = 5.0):
    """Poll get_state until the workflow reaches the expected durable status."""

    deadline = asyncio.get_running_loop().time() + timeout
    last_state = None
    while asyncio.get_running_loop().time() < deadline:
        last_state = await handle.query(TrustedConnectionWorkflow.get_state)
        if last_state.status == expected:
            return last_state
        await asyncio.sleep(0.05)
    raise AssertionError(f"expected {expected}, got {last_state}")


async def wait_for_state(handle, predicate, timeout: float = 5.0):
    """Poll get_state until a caller-supplied predicate matches the state."""

    deadline = asyncio.get_running_loop().time() + timeout
    last_state = None
    while asyncio.get_running_loop().time() < deadline:
        last_state = await handle.query(TrustedConnectionWorkflow.get_state)
        if predicate(last_state):
            return last_state
        await asyncio.sleep(0.05)
    raise AssertionError(f"state predicate was not satisfied, got {last_state}")


async def wait_for_recorded_tc_event(reason: str, timeout: float = 5.0) -> TCChangeEvent:
    """Poll recorded activity output until a TC event with the reason appears."""

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        for event in RECORDED_TC_EVENTS:
            if event.reason == reason:
                return event
        await asyncio.sleep(0.05)
    raise AssertionError(f"TC change event {reason} was not emitted: {RECORDED_TC_EVENTS}")


def adult_request(user_id_a: str = "alice", user_id_b: str = "bob") -> TrustedConnectionRequest:
    """Build the common eligible adult pair used by most workflow tests."""

    return TrustedConnectionRequest(
        requester_user_id=user_id_a,
        target_user_id=user_id_b,
        source_channel=SourceChannel.STANDARD,
        requester_snapshot=UserEligibilitySnapshot(user_id_a, age=18),
        target_snapshot=UserEligibilitySnapshot(user_id_b, age=18),
        consent_ttl_seconds=5,
    )


def edited_request(
    base: TrustedConnectionRequest,
    *,
    requester_snapshot: UserEligibilitySnapshot | None = None,
    target_snapshot: UserEligibilitySnapshot | None = None,
    source_channel: SourceChannel | None = None,
    consent_ttl_seconds: int | None = None,
    are_friends: bool | None = None,
    parent_child_relationship: bool | None = None,
    auto_accept: bool | None = None,
    trigger: str = "OPERATOR_CONFIGURATION",
) -> TrustedConnectionRequest:
    """Build a full operator configuration update for an existing pair."""

    return TrustedConnectionRequest(
        requester_user_id=base.requester_user_id,
        target_user_id=base.target_user_id,
        source_channel=source_channel or base.source_channel,
        requester_snapshot=requester_snapshot or base.requester_snapshot,
        target_snapshot=target_snapshot or base.target_snapshot,
        consent_ttl_seconds=consent_ttl_seconds or base.consent_ttl_seconds,
        are_friends=base.are_friends if are_friends is None else are_friends,
        parent_child_relationship=base.parent_child_relationship
        if parent_child_relationship is None
        else parent_child_relationship,
        auto_accept=base.auto_accept if auto_accept is None else auto_accept,
        trigger=trigger,
        metadata=base.metadata,
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


async def test_configuration_update_suspends_and_restores_pair() -> None:
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
                request = adult_request("config-a", "config-b")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("config-a", "config-b"),
                    task_queue=task_queue,
                )
                await handle.signal(TrustedConnectionWorkflow.accept)
                await wait_for_status(handle, ConnectionStatus.TRUSTED)

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(
                        request,
                        requester_snapshot=UserEligibilitySnapshot(
                            "config-a",
                            age=18,
                            is_age_verified=False,
                            is_on_watchlist=True,
                        ),
                    ),
                )
                suspended = await wait_for_state(
                    handle,
                    lambda state: state.status == ConnectionStatus.SUSPENDED
                    and state.reason == "REQUESTER_AGE_VERIFICATION_REQUIRED",
                )
                assert suspended.reason == "REQUESTER_AGE_VERIFICATION_REQUIRED"
                assert not suspended.requester_snapshot.is_age_verified

                await handle.signal(TrustedConnectionWorkflow.apply_configuration, request)
                restored = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert restored.reason == "USER_ACCEPTED"
                assert restored.accepted
                assert restored.requester_snapshot.is_age_verified


async def test_configuration_update_can_require_parental_consent() -> None:
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
                request = adult_request("config-child-a", "config-child-b")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("config-child-a", "config-child-b"),
                    task_queue=task_queue,
                )
                await wait_for_status(handle, ConnectionStatus.PENDING)

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(
                        request,
                        requester_snapshot=UserEligibilitySnapshot("config-child-a", age=12),
                        target_snapshot=UserEligibilitySnapshot("config-child-b", age=12),
                        consent_ttl_seconds=5,
                    ),
                )
                waiting = await wait_for_status(
                    handle,
                    ConnectionStatus.WAITING_FOR_PARENTAL_CONSENT,
                )
                assert waiting.consent_required
                assert waiting.reason == "PARENTAL_CONSENT_REQUIRED"
                assert waiting.requester_snapshot.age == 12
                assert waiting.target_snapshot.age == 12


async def test_configuration_updates_emit_tc_change_events() -> None:
    RECORDED_TC_EVENTS.clear()
    task_queue = str(uuid.uuid4())
    async with await start_temporal_environment() as env:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as activity_executor:
            async with Worker(
                env.client,
                task_queue=task_queue,
                workflows=[TrustedConnectionWorkflow, EligibilityEvaluationWorkflow],
                activities=[
                    evaluate_initial_eligibility,
                    evaluate_event_eligibility,
                    record_tc_change_event,
                ],
                activity_executor=activity_executor,
            ):
                request = adult_request("emit-a", "emit-b")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("emit-a", "emit-b"),
                    task_queue=task_queue,
                )
                await wait_for_status(handle, ConnectionStatus.PENDING)
                RECORDED_TC_EVENTS.clear()

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(
                        request,
                        requester_snapshot=UserEligibilitySnapshot("emit-a", age=19),
                    ),
                )
                age_event = await wait_for_recorded_tc_event("REQUESTER_AGE_UP")
                assert age_event.event_type == "CONFIGURATION_CHANGED"
                assert age_event.changed_fields == ["requester_snapshot.age"]
                assert age_event.subject_user_id == "emit-a"

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(
                        request,
                        requester_snapshot=UserEligibilitySnapshot("emit-a", age=19),
                        are_friends=False,
                    ),
                )
                friendship_event = await wait_for_recorded_tc_event("FRIENDSHIP_CHANGED")
                assert friendship_event.event_type == "CONFIGURATION_CHANGED"
                assert friendship_event.changed_fields == ["are_friends"]
                suspended_for_friendship = await wait_for_status(
                    handle,
                    ConnectionStatus.SUSPENDED,
                )
                assert suspended_for_friendship.reason == "FRIENDSHIP_REQUIRED"

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(
                        request,
                        requester_snapshot=UserEligibilitySnapshot(
                            "emit-a",
                            age=19,
                            is_age_verified=False,
                        ),
                        are_friends=True,
                    ),
                )
                verification_event = await wait_for_recorded_tc_event(
                    "REQUESTER_AGE_VERIFICATION_CHANGED"
                )
                assert verification_event.event_type == "CONFIGURATION_CHANGED"
                assert verification_event.changed_fields == [
                    "requester_snapshot.is_age_verified"
                ]
                suspended = await wait_for_status(handle, ConnectionStatus.SUSPENDED)
                assert suspended.reason == "REQUESTER_AGE_VERIFICATION_REQUIRED"
                status_event = await wait_for_recorded_tc_event(
                    "REQUESTER_AGE_VERIFICATION_REQUIRED"
                )
                assert status_event.event_type == "STATUS_CHANGED"


async def test_configuration_update_recomputes_pair_level_fields() -> None:
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
                    requester_user_id="config-parent",
                    target_user_id="config-child",
                    source_channel=SourceChannel.PARENT_CHILD,
                    requester_snapshot=UserEligibilitySnapshot("config-parent", age=42),
                    target_snapshot=UserEligibilitySnapshot("config-child", age=14),
                    consent_ttl_seconds=5,
                    parent_child_relationship=True,
                    auto_accept=True,
                    trigger="PARENT_CHILD_AUTO_UPGRADE",
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id_for_pair("config-parent", "config-child"),
                    task_queue=task_queue,
                )
                trusted = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert trusted.parent_child_relationship
                assert trusted.source_channel == SourceChannel.PARENT_CHILD

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(request, parent_child_relationship=False),
                )
                missing_relationship = await wait_for_status(
                    handle,
                    ConnectionStatus.SUSPENDED,
                )
                assert missing_relationship.reason == "PARENT_CHILD_RELATIONSHIP_REQUIRED"
                assert not missing_relationship.parent_child_relationship

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(
                        request,
                        source_channel=SourceChannel.STANDARD,
                        parent_child_relationship=True,
                    ),
                )
                standard_channel = await wait_for_state(
                    handle,
                    lambda state: state.status == ConnectionStatus.SUSPENDED
                    and state.reason == "SAME_AGE_GROUP_REQUIRED",
                )
                assert standard_channel.reason == "SAME_AGE_GROUP_REQUIRED"
                assert standard_channel.source_channel == SourceChannel.STANDARD

                await handle.signal(
                    TrustedConnectionWorkflow.apply_configuration,
                    edited_request(request, parent_child_relationship=True),
                )
                restored = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert restored.reason == "PARENT_CHILD_AUTO_UPGRADE"
                assert restored.parent_child_relationship


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


async def test_pair_workflow_continues_as_new_after_event_limit() -> None:
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
                workflow_id = workflow_id_for_pair("continue-a", "continue-b")
                await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    adult_request("continue-a", "continue-b"),
                    id=workflow_id,
                    task_queue=task_queue,
                )
                latest_handle = env.client.get_workflow_handle(workflow_id)
                await wait_for_status(latest_handle, ConnectionStatus.PENDING)

                for _ in range(CONTINUE_AS_NEW_EVENT_LIMIT):
                    await latest_handle.signal(TrustedConnectionWorkflow.accept)

                continued = await wait_for_state(
                    latest_handle,
                    lambda state: state.status == ConnectionStatus.TRUSTED
                    and state.continue_as_new_count >= 1,
                    timeout=10.0,
                )
                assert continued.accepted
                assert continued.requester_user_id == "continue-a"
                assert continued.target_user_id == "continue-b"
                assert continued.transitions[-1].status == ConnectionStatus.TRUSTED


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


async def test_age_update_clears_parental_consent_requirement() -> None:
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
                workflow_id = workflow_id_for_pair("age-consent-child", "age-consent-peer")
                request = TrustedConnectionRequest(
                    requester_user_id="age-consent-child",
                    target_user_id="age-consent-peer",
                    source_channel=SourceChannel.STANDARD,
                    requester_snapshot=UserEligibilitySnapshot("age-consent-child", age=12),
                    target_snapshot=UserEligibilitySnapshot("age-consent-peer", age=13),
                    consent_ttl_seconds=5,
                )
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    request,
                    id=workflow_id,
                    task_queue=task_queue,
                )

                waiting = await wait_for_status(
                    handle,
                    ConnectionStatus.WAITING_FOR_PARENTAL_CONSENT,
                )
                assert waiting.consent_required

                await env.client.execute_workflow(
                    EligibilityEvaluationWorkflow.run,
                    EligibilityEvent(
                        event_id="event-age-out-consent",
                        user_id_a="age-consent-child",
                        user_id_b="age-consent-peer",
                        changed_user_id="age-consent-child",
                        snapshot=UserEligibilitySnapshot("age-consent-child", age=13),
                        pair_workflow_id=workflow_id,
                        event_type=EligibilityEventType.AGE_CHANGED,
                    ),
                    id="eligibility-eval-event-age-out-consent",
                    task_queue=task_queue,
                )

                cleared = await wait_for_state(
                    handle,
                    lambda state: state.status == ConnectionStatus.PENDING
                    and not state.consent_required,
                )
                assert cleared.reason == "NATURAL_AGE_UP_RETAINED"
                assert cleared.consent_status is None
                assert cleared.requester_snapshot.age == 13


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
                # This exercises the durable workflow sleep that expires pending
                # consent when no approval/denial signal arrives.
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
                assert suspended.requester_snapshot.is_on_watchlist

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
                assert restored.requester_snapshot.is_age_verified


async def test_parent_child_fact_updates_pair_relationship_state() -> None:
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
                pair_workflow_id = workflow_id_for_pair("parent-event", "child-event")
                handle = await env.client.start_workflow(
                    TrustedConnectionWorkflow.run,
                    TrustedConnectionRequest(
                        requester_user_id="parent-event",
                        target_user_id="child-event",
                        source_channel=SourceChannel.PARENT_CHILD,
                        requester_snapshot=UserEligibilitySnapshot("parent-event", age=42),
                        target_snapshot=UserEligibilitySnapshot("child-event", age=14),
                        parent_child_relationship=True,
                        auto_accept=True,
                        trigger="PARENT_CHILD_AUTO_UPGRADE",
                    ),
                    id=pair_workflow_id,
                    task_queue=task_queue,
                )
                trusted = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert trusted.parent_child_relationship

                await env.client.execute_workflow(
                    EligibilityEvaluationWorkflow.run,
                    EligibilityEvent(
                        event_id="event-parent-child-removed",
                        user_id_a="parent-event",
                        user_id_b="child-event",
                        changed_user_id="parent-event",
                        snapshot=UserEligibilitySnapshot("parent-event", age=42),
                        pair_workflow_id=pair_workflow_id,
                        event_type=EligibilityEventType.PARENT_CHILD_REMOVED,
                    ),
                    id="eligibility-eval-event-parent-child-removed",
                    task_queue=task_queue,
                )
                suspended = await wait_for_status(handle, ConnectionStatus.SUSPENDED)
                assert not suspended.parent_child_relationship

                await env.client.execute_workflow(
                    EligibilityEvaluationWorkflow.run,
                    EligibilityEvent(
                        event_id="event-parent-child-formed",
                        user_id_a="parent-event",
                        user_id_b="child-event",
                        changed_user_id="parent-event",
                        snapshot=UserEligibilitySnapshot("parent-event", age=42),
                        pair_workflow_id=pair_workflow_id,
                        event_type=EligibilityEventType.PARENT_CHILD_FORMED,
                    ),
                    id="eligibility-eval-event-parent-child-formed",
                    task_queue=task_queue,
                )
                restored = await wait_for_status(handle, ConnectionStatus.TRUSTED)
                assert restored.parent_child_relationship


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

                # The event workflow should still finish and return its
                # eligibility decision even if the target pair workflow is gone.
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
