from __future__ import annotations

from dataclasses import dataclass

import httpx
from temporalio.client import WorkflowExecutionStatus
from temporalio.common import WorkflowIDReusePolicy

from trusted_friends.api import app, get_temporal_client
from trusted_friends.models import (
    ConnectionStatus,
    SourceChannel,
    TrustedConnectionState,
)

# API tests use a small fake Temporal client so endpoint behavior can be checked
# without starting a Temporal server. The fake records workflow starts and
# signals while returning realistic query/describe shapes.

@dataclass
class RecordedSignal:
    name: str
    arg: object | None


@dataclass
class FakeWorkflowDescription:
    status: WorkflowExecutionStatus | None


class FakeHandle:
    def __init__(
        self,
        workflow_id: str,
        *,
        execution_status: WorkflowExecutionStatus | None = WorkflowExecutionStatus.RUNNING,
    ) -> None:
        self.workflow_id = workflow_id
        self.execution_status = execution_status
        self.signals: list[RecordedSignal] = []
        self.query_count = 0
        self.state = TrustedConnectionState(
            workflow_id=workflow_id,
            requester_user_id="alice",
            target_user_id="bob",
            normalized_user_ids=["alice", "bob"],
            source_channel=SourceChannel.STANDARD,
            trigger="DOUBLE_OPT_IN",
            are_friends=True,
            parent_child_relationship=False,
            status=ConnectionStatus.PENDING,
            reason="REQUEST_SENT",
            accepted=False,
            consent_required=False,
            consent_status=None,
            created_at="2026-04-30T00:00:00+00:00",
            updated_at="2026-04-30T00:00:00+00:00",
            last_eligibility_event_id=None,
        )

    async def describe(self):
        return FakeWorkflowDescription(self.execution_status)

    async def signal(self, signal, arg=None):
        # Store the SDK signal method name, not the bound function object, so
        # tests can assert that endpoints target the intended workflow signal.
        self.signals.append(RecordedSignal(getattr(signal, "__name__", str(signal)), arg))

    async def query(self, query, **kwargs):
        self.query_count += 1
        return self.state


class FakeTemporalClient:
    def __init__(self) -> None:
        self.started: list[dict[str, object]] = []
        self.handles: dict[str, FakeHandle] = {}

    async def start_workflow(self, workflow, arg, *, id: str, task_queue: str, **kwargs):
        self.started.append(
            {"workflow": workflow, "arg": arg, "id": id, "task_queue": task_queue, **kwargs}
        )
        self.handles.setdefault(id, FakeHandle(id))
        return self.handles[id]

    def get_workflow_handle(self, workflow_id: str) -> FakeHandle:
        return self.handles.setdefault(workflow_id, FakeHandle(workflow_id))


async def test_send_starts_pair_workflow() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/trusted-friends/send",
                json={
                    "requester_user_id": "bob",
                    "target_user_id": "alice",
                    "source_channel": "QR_CROSS_AGE",
                    "requester_snapshot": {
                        "age": 12,
                        "country_code": "BR",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                    "target_snapshot": {
                        "age": 17,
                        "country_code": "US",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                    "consent_ttl_seconds": 45,
                    "are_friends": False,
                    "parent_child_relationship": False,
                    "auto_accept": True,
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    # User order should not matter; the backend derives one stable workflow id
    # for the unordered pair and allows duplicate starts for repeated demos.
    assert response.json()["workflow_id"] == "trusted-connection-alice-bob"
    assert fake.started[0]["id"] == "trusted-connection-alice-bob"
    assert fake.started[0]["id_reuse_policy"] == WorkflowIDReusePolicy.ALLOW_DUPLICATE
    assert fake.started[0]["versioning_override"] is None
    request = fake.started[0]["arg"]
    assert request.source_channel == SourceChannel.QR_CROSS_AGE
    assert request.requester_snapshot.age == 12
    assert request.requester_snapshot.country_code == "BR"
    assert request.target_snapshot.age == 17
    assert request.consent_ttl_seconds == 45
    assert not request.are_friends
    assert request.auto_accept


async def test_accept_signals_and_queries_pair_workflow() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post("/trusted-friends/trusted-connection-alice-bob/accept")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json() == {
        "workflow_id": "trusted-connection-alice-bob",
        "signal": "accept",
        "accepted": True,
        "status_url": "/trusted-friends/trusted-connection-alice-bob",
    }
    handle = fake.get_workflow_handle("trusted-connection-alice-bob")
    assert handle.signals[0].name == "accept"


async def test_parental_consent_signals_pair_workflow() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/trusted-friends/trusted-connection-alice-bob/parental-consent",
                json={"consent_id": "consent-1", "status": "APPROVED"},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json() == {
        "workflow_id": "trusted-connection-alice-bob",
        "signal": "parental_consent",
        "accepted": True,
        "status_url": "/trusted-friends/trusted-connection-alice-bob",
    }
    handle = fake.get_workflow_handle("trusted-connection-alice-bob")
    assert handle.signals[0].name == "parental_consent"


async def test_close_signals_pair_workflow() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post("/trusted-friends/trusted-connection-alice-bob/close")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json() == {
        "workflow_id": "trusted-connection-alice-bob",
        "closed": True,
    }
    handle = fake.get_workflow_handle("trusted-connection-alice-bob")
    assert handle.signals[0].name == "close"


async def test_configuration_signals_pair_workflow_with_current_ids() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/trusted-friends/trusted-connection-alice-bob/configuration",
                json={
                    "source_channel": "STANDARD",
                    "requester_user_id": "alice",
                    "target_user_id": "bob",
                    "requester_snapshot": {
                        "age": 12,
                        "country_code": "US",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                    "target_snapshot": {
                        "age": 14,
                        "country_code": "US",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                    "consent_ttl_seconds": 90,
                    "are_friends": True,
                    "parent_child_relationship": False,
                    "auto_accept": False,
                    "trigger": "OPERATOR_CONFIGURATION",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json()["signal"] == "apply_configuration"
    handle = fake.get_workflow_handle("trusted-connection-alice-bob")
    assert handle.signals[0].name == "apply_configuration"
    request = handle.signals[0].arg
    assert request.requester_user_id == "alice"
    assert request.target_user_id == "bob"
    assert request.requester_snapshot.age == 12
    assert request.consent_ttl_seconds == 90
    assert handle.query_count == 0


async def test_configuration_rejects_terminated_pair_workflow() -> None:
    fake = FakeTemporalClient()
    fake.handles["trusted-connection-alice-bob"] = FakeHandle(
        "trusted-connection-alice-bob",
        execution_status=WorkflowExecutionStatus.TERMINATED,
    )
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/trusted-friends/trusted-connection-alice-bob/configuration",
                json={
                    "source_channel": "STANDARD",
                    "requester_user_id": "alice",
                    "target_user_id": "bob",
                    "requester_snapshot": {
                        "age": 18,
                        "country_code": "US",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                    "target_snapshot": {
                        "age": 18,
                        "country_code": "US",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                    "consent_ttl_seconds": 120,
                    "are_friends": True,
                    "parent_child_relationship": False,
                    "auto_accept": False,
                    "trigger": "OPERATOR_CONFIGURATION",
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 410
    handle = fake.get_workflow_handle("trusted-connection-alice-bob")
    assert handle.signals == []


async def test_eligibility_event_starts_short_lived_workflow() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/events/eligibility",
                json={
                    "event_id": "event-1",
                    "user_id_a": "bob",
                    "user_id_b": "alice",
                    "changed_user_id": "alice",
                    "snapshot": {
                        "age": 18,
                        "country_code": "US",
                        "is_age_verified": False,
                        "is_on_watchlist": True,
                    },
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json()["workflow_id"] == "eligibility-eval-event-1"
    assert response.json()["pair_workflow_id"] == "trusted-connection-alice-bob"
    assert fake.started[0]["id"] == "eligibility-eval-event-1"
    assert fake.started[0]["versioning_override"] is None


async def test_domain_fact_is_translated_before_starting_event_workflow() -> None:
    fake = FakeTemporalClient()
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/events/domain-facts",
                json={
                    "fact_id": "fact-parent-removed",
                    "fact_type": "PARENT_CHILD_RELATIONSHIP_REMOVED",
                    "user_id_a": "parent",
                    "user_id_b": "child",
                    "subject_user_id": "parent",
                    "snapshot": {
                        "age": 42,
                        "country_code": "US",
                        "is_age_verified": True,
                        "is_on_watchlist": False,
                    },
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 202
    assert response.json()["workflow_id"] == "eligibility-eval-fact-parent-removed"
    event = fake.started[0]["arg"]
    assert event.event_id == "fact-parent-removed"
    assert event.event_type == "PARENT_CHILD_REMOVED"
    assert event.pair_workflow_id == "trusted-connection-child-parent"


async def test_get_returns_gone_for_terminated_pair_workflow() -> None:
    fake = FakeTemporalClient()
    fake.handles["trusted-connection-alice-bob"] = FakeHandle(
        "trusted-connection-alice-bob",
        execution_status=WorkflowExecutionStatus.TERMINATED,
    )
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get("/trusted-friends/trusted-connection-alice-bob")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 410
    assert response.json()["detail"] == {
        "code": "WORKFLOW_NOT_RUNNING",
        "workflow_id": "trusted-connection-alice-bob",
        "execution_status": "TERMINATED",
        "message": (
            "Temporal workflow trusted-connection-alice-bob is TERMINATED; "
            "query cannot be applied."
        ),
    }


async def test_accept_does_not_signal_terminated_pair_workflow() -> None:
    fake = FakeTemporalClient()
    fake.handles["trusted-connection-alice-bob"] = FakeHandle(
        "trusted-connection-alice-bob",
        execution_status=WorkflowExecutionStatus.TERMINATED,
    )
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post("/trusted-friends/trusted-connection-alice-bob/accept")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 410
    handle = fake.get_workflow_handle("trusted-connection-alice-bob")
    # Lifecycle validation must happen before signal delivery so closed Cloud
    # executions do not accumulate failing signal attempts.
    assert handle.signals == []


async def test_eligibility_event_rejects_terminated_pair_workflow() -> None:
    fake = FakeTemporalClient()
    fake.handles["trusted-connection-alice-bob"] = FakeHandle(
        "trusted-connection-alice-bob",
        execution_status=WorkflowExecutionStatus.TERMINATED,
    )
    app.dependency_overrides[get_temporal_client] = lambda: fake
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/events/eligibility",
                json={
                    "event_id": "event-terminated",
                    "user_id_a": "bob",
                    "user_id_b": "alice",
                    "changed_user_id": "alice",
                    "snapshot": {
                        "age": 18,
                        "country_code": "US",
                        "is_age_verified": False,
                        "is_on_watchlist": True,
                    },
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 410
    assert fake.started == []
