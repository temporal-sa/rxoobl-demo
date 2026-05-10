from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from temporalio.client import (
    Client,
    RPCError,
    RPCStatusCode,
    WorkflowExecutionStatus,
    WorkflowHandle,
    WorkflowQueryFailedError,
    WorkflowQueryRejectedError,
)
from temporalio.common import QueryRejectCondition
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from trusted_friends.models import (
    ConsentStatus,
    EligibilityEvent,
    EligibilityEventType,
    ParentalConsent,
    SourceChannel,
    TrustedConnectionRequest,
    UserEligibilitySnapshot,
)
from trusted_friends.rules import workflow_id_for_pair
from trusted_friends.settings import DEFAULT_CONSENT_TTL_SECONDS, TASK_QUEUE
from trusted_friends.temporal_client import connect_temporal_client
from trusted_friends.versioning import workflow_versioning_override
from trusted_friends.workflows import EligibilityEvaluationWorkflow, TrustedConnectionWorkflow


app = FastAPI(title="Trusted Friends Temporal Demo")

# The frontend is a Vite dev server during demos. Restricting CORS to local
# origins keeps the API convenient for live demos without making it an open
# cross-origin endpoint.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5173",
        "http://localhost:5173",
    ],
    allow_origin_regex=r"http://(127\.0\.0\.1|localhost):517[0-9]",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class DemoUserSnapshotPayload(BaseModel):
    """Small API payload for demo user attributes.

    The workflow consumes `UserEligibilitySnapshot` dataclasses. Pydantic stays
    at the HTTP boundary where it can validate untrusted JSON before converting
    to the workflow payload shape.
    """

    age: int = Field(18, ge=0, le=130)
    country_code: str = "US"
    is_age_verified: bool = True
    is_on_watchlist: bool = False


class SendTrustedFriendPayload(BaseModel):
    requester_user_id: str
    target_user_id: str
    source_channel: SourceChannel = SourceChannel.STANDARD
    requester_snapshot: DemoUserSnapshotPayload | None = None
    target_snapshot: DemoUserSnapshotPayload | None = None
    consent_ttl_seconds: int = Field(DEFAULT_CONSENT_TTL_SECONDS, gt=0)
    are_friends: bool = True
    parent_child_relationship: bool = False
    auto_accept: bool = False
    trigger: str = "DOUBLE_OPT_IN"
    metadata: dict[str, str] = Field(default_factory=dict)


class ParentalConsentPayload(BaseModel):
    consent_id: str
    status: ConsentStatus
    timestamp: str | None = None


class EligibilityEventPayload(BaseModel):
    event_id: str
    user_id_a: str
    user_id_b: str
    changed_user_id: str
    snapshot: DemoUserSnapshotPayload
    pair_workflow_id: str | None = None
    event_type: EligibilityEventType = EligibilityEventType.ELIGIBILITY_CHANGED


async def get_temporal_client() -> Client:
    """Reuse one Temporal client per API process.

    Temporal clients are safe to reuse and maintain their own connection pool.
    Creating one per request would add avoidable Cloud connection overhead.
    """
    client = getattr(app.state, "temporal_client", None)
    if client is None:
        client = await connect_temporal_client()
        app.state.temporal_client = client
    return client


@app.post("/trusted-friends/send", status_code=202)
async def send_trusted_friend(
    payload: SendTrustedFriendPayload,
    client: Client = Depends(get_temporal_client),
) -> dict[str, object]:
    """Start the long-lived workflow for a normalized trusted-friend pair."""
    workflow_id = workflow_id_for_pair(payload.requester_user_id, payload.target_user_id)
    request = TrustedConnectionRequest(
        requester_user_id=payload.requester_user_id,
        target_user_id=payload.target_user_id,
        source_channel=payload.source_channel,
        requester_snapshot=_snapshot(
            payload.requester_user_id,
            payload.requester_snapshot,
        ),
        target_snapshot=_snapshot(payload.target_user_id, payload.target_snapshot),
        consent_ttl_seconds=payload.consent_ttl_seconds,
        are_friends=payload.are_friends,
        parent_child_relationship=payload.parent_child_relationship,
        auto_accept=payload.auto_accept,
        trigger=payload.trigger,
        metadata=payload.metadata,
    )

    try:
        # `ALLOW_DUPLICATE` allows a new demo run after a prior workflow with
        # the same ID has closed. While a workflow is still open, Temporal still
        # rejects another start with the same workflow ID.
        await client.start_workflow(
            TrustedConnectionWorkflow.run,
            request,
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            # When Worker Versioning is enabled, starts are routed to the
            # deployment's current version. If disabled, this helper returns
            # None and the workflow starts on the unversioned task queue.
            versioning_override=workflow_versioning_override(),
        )
    except WorkflowAlreadyStartedError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Trusted connection workflow is already running: {workflow_id}",
        ) from exc

    return {"workflow_id": workflow_id, "status_url": f"/trusted-friends/{workflow_id}"}


@app.post("/trusted-friends/{workflow_id}/accept", status_code=202)
async def accept_trusted_friend(
    workflow_id: str,
    client: Client = Depends(get_temporal_client),
) -> dict[str, object]:
    """Send the recipient acceptance signal and acknowledge it immediately."""
    handle = client.get_workflow_handle(workflow_id)
    await _signal_pair_workflow(handle, workflow_id, TrustedConnectionWorkflow.accept)
    return _signal_response(workflow_id, "accept")


@app.post("/trusted-friends/{workflow_id}/parental-consent", status_code=202)
async def parental_consent(
    workflow_id: str,
    payload: ParentalConsentPayload,
    client: Client = Depends(get_temporal_client),
) -> dict[str, object]:
    """Send VPC approval/denial as a workflow signal.

    The endpoint intentionally does not query after signaling. In Temporal
    Cloud, consistent queries can backpressure when workflows are catching up;
    the UI polls later at a slower cadence.
    """
    handle = client.get_workflow_handle(workflow_id)
    await _signal_pair_workflow(
        handle,
        workflow_id,
        TrustedConnectionWorkflow.parental_consent,
        ParentalConsent(
            consent_id=payload.consent_id,
            status=payload.status,
            timestamp=payload.timestamp,
        ),
    )
    return _signal_response(workflow_id, "parental_consent")


@app.post("/trusted-friends/{workflow_id}/close", status_code=202)
async def close_trusted_friend(
    workflow_id: str,
    client: Client = Depends(get_temporal_client),
) -> dict[str, object]:
    """Ask the workflow to complete gracefully."""
    handle = client.get_workflow_handle(workflow_id)
    await _signal_pair_workflow(handle, workflow_id, TrustedConnectionWorkflow.close)
    return {
        "workflow_id": workflow_id,
        "closed": True,
    }


@app.get("/trusted-friends/{workflow_id}")
async def get_trusted_friend(
    workflow_id: str,
    client: Client = Depends(get_temporal_client),
) -> dict[str, object]:
    """Query workflow-owned state for the UI."""
    handle = client.get_workflow_handle(workflow_id)
    return await _query_pair_workflow_state(handle, workflow_id)


@app.post("/events/eligibility", status_code=202)
async def eligibility_event(
    payload: EligibilityEventPayload,
    client: Client = Depends(get_temporal_client),
) -> dict[str, object]:
    """Start a short-lived workflow that processes one async eligibility event."""
    pair_workflow_id = payload.pair_workflow_id or workflow_id_for_pair(
        payload.user_id_a,
        payload.user_id_b,
    )
    pair_handle = client.get_workflow_handle(pair_workflow_id)
    # Avoid creating orphan event workflows. If the pair workflow is already
    # closed, the API returns a workflow-unavailable response instead.
    await _ensure_workflow_running(
        pair_handle,
        pair_workflow_id,
        "start eligibility evaluation",
    )
    event = EligibilityEvent(
        event_id=payload.event_id,
        user_id_a=payload.user_id_a,
        user_id_b=payload.user_id_b,
        changed_user_id=payload.changed_user_id,
        snapshot=_snapshot(payload.changed_user_id, payload.snapshot),
        pair_workflow_id=pair_workflow_id,
        event_type=payload.event_type,
    )
    workflow_id = f"eligibility-eval-{payload.event_id}"

    try:
        # Eligibility event IDs are expected to be unique. Rejecting duplicates
        # makes event replay/idempotency bugs visible in the demo.
        await client.start_workflow(
            EligibilityEvaluationWorkflow.run,
            event,
            id=workflow_id,
            task_queue=TASK_QUEUE,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
            versioning_override=workflow_versioning_override(),
        )
    except WorkflowAlreadyStartedError as exc:
        raise HTTPException(
            status_code=409,
            detail=f"Eligibility event workflow already exists: {workflow_id}",
        ) from exc

    return {
        "workflow_id": workflow_id,
        "pair_workflow_id": pair_workflow_id,
    }


def _snapshot(
    user_id: str,
    payload: DemoUserSnapshotPayload | None,
) -> UserEligibilitySnapshot:
    """Convert an HTTP payload into the workflow/activity dataclass shape."""
    payload = payload or DemoUserSnapshotPayload()
    return UserEligibilitySnapshot(
        user_id=user_id,
        age=payload.age,
        country_code=payload.country_code.upper(),
        is_age_verified=payload.is_age_verified,
        is_on_watchlist=payload.is_on_watchlist,
    )


def _state_response(state: object) -> dict[str, object]:
    return asdict(state)


def _signal_response(workflow_id: str, signal: str) -> dict[str, object]:
    """Consistent acknowledgment shape for fire-and-forget signal endpoints."""
    return {
        "workflow_id": workflow_id,
        "signal": signal,
        "accepted": True,
        "status_url": f"/trusted-friends/{workflow_id}",
    }


async def _query_pair_workflow_state(
    handle: WorkflowHandle[Any, Any],
    workflow_id: str,
) -> dict[str, object]:
    """Run a bounded consistent query against the pair workflow.

    Queries are useful for the UI but are not part of command delivery. A short
    timeout prevents backpressured Cloud queries from tying up API workers.
    """
    await _ensure_workflow_running(handle, workflow_id, "query")
    try:
        state = await handle.query(
            TrustedConnectionWorkflow.get_state,
            reject_condition=QueryRejectCondition.NOT_OPEN,
            rpc_timeout=timedelta(seconds=3),
        )
    except WorkflowQueryRejectedError as exc:
        raise _workflow_not_running(workflow_id, exc.status, "query") from exc
    except WorkflowQueryFailedError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "code": "WORKFLOW_QUERY_FAILED",
                "workflow_id": workflow_id,
                "message": f"Temporal query get_state failed: {exc.message}",
            },
        ) from exc
    except RPCError as exc:
        _raise_temporal_rpc_http_exception(exc, workflow_id, "query")

    return _state_response(state)


_SIGNAL_ARG_UNSET = object()


async def _signal_pair_workflow(
    handle: WorkflowHandle[Any, Any],
    workflow_id: str,
    signal: object,
    arg: object = _SIGNAL_ARG_UNSET,
) -> None:
    """Validate the workflow is open, then send a typed signal."""
    await _ensure_workflow_running(handle, workflow_id, "signal")
    try:
        if arg is _SIGNAL_ARG_UNSET:
            await handle.signal(signal)
        else:
            await handle.signal(signal, arg)
    except RPCError as exc:
        _raise_temporal_rpc_http_exception(exc, workflow_id, "signal")


async def _ensure_workflow_running(
    handle: WorkflowHandle[Any, Any],
    workflow_id: str,
    operation: str,
) -> None:
    """Turn closed/missing workflow executions into stable HTTP errors."""
    try:
        description = await handle.describe()
    except RPCError as exc:
        _raise_temporal_rpc_http_exception(exc, workflow_id, operation)

    if description.status and description.status != WorkflowExecutionStatus.RUNNING:
        raise _workflow_not_running(workflow_id, description.status, operation)


def _raise_temporal_rpc_http_exception(
    exc: RPCError,
    workflow_id: str,
    operation: str,
) -> None:
    """Map Temporal RPC failures to UI-friendly HTTP responses."""
    if exc.status == RPCStatusCode.NOT_FOUND:
        raise HTTPException(
            status_code=404,
            detail={
                "code": "WORKFLOW_NOT_FOUND",
                "workflow_id": workflow_id,
                "execution_status": "NOT_FOUND",
                "message": (
                    f"Temporal workflow {workflow_id} was not found; "
                    f"{operation} cannot be applied."
                ),
            },
        ) from exc

    if exc.status == RPCStatusCode.FAILED_PRECONDITION:
        raise _workflow_not_running(workflow_id, None, operation) from exc

    if exc.status in {
        RPCStatusCode.CANCELLED,
        RPCStatusCode.DEADLINE_EXCEEDED,
        RPCStatusCode.UNAVAILABLE,
        RPCStatusCode.RESOURCE_EXHAUSTED,
    }:
        # Cloud query/worker backpressure should be treated as transient. The
        # UI can keep the signal accepted and retry state refresh later.
        raise HTTPException(
            status_code=503,
            detail={
                "code": "TEMPORAL_UNAVAILABLE",
                "workflow_id": workflow_id,
                "message": f"Temporal could not complete {operation}: {exc.message}",
            },
        ) from exc

    raise HTTPException(
        status_code=502,
        detail={
            "code": "TEMPORAL_RPC_ERROR",
            "workflow_id": workflow_id,
            "message": f"Temporal could not complete {operation}: {exc.message}",
        },
    ) from exc


def _workflow_not_running(
    workflow_id: str,
    status: WorkflowExecutionStatus | None,
    operation: str,
) -> HTTPException:
    """Build the 410 response used for closed workflow executions."""
    status_name = _workflow_status_name(status)
    return HTTPException(
        status_code=410,
        detail={
            "code": "WORKFLOW_NOT_RUNNING",
            "workflow_id": workflow_id,
            "execution_status": status_name,
            "message": (
                f"Temporal workflow {workflow_id} is {status_name}; "
                f"{operation} cannot be applied."
            ),
        },
    )


def _workflow_status_name(status: WorkflowExecutionStatus | None) -> str:
    if status is None:
        return "UNKNOWN"
    return status.name
