from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

# Workflow modules run in Temporal's deterministic sandbox. Imports inside
# `imports_passed_through()` are treated as stable application modules rather
# than reloaded through the sandbox on every workflow task replay.
with workflow.unsafe.imports_passed_through():
    from trusted_friends.activities import (
        emit_tc_change_event,
        evaluate_event_eligibility,
        evaluate_initial_eligibility,
    )
    from trusted_friends.models import (
        ConfigurationChange,
        ConnectionStatus,
        ConsentStatus,
        EligibilityDecision,
        EligibilityEvent,
        EligibilityEventType,
        EligibilityUpdate,
        ParentalConsent,
        RelationshipEvent,
        RelationshipEventType,
        SourceChannel,
        StateTransition,
        TCChangeEvent,
        TrustedConnectionContinuation,
        TrustedConnectionRequest,
        TrustedConnectionState,
        UserEligibilitySnapshot,
    )
    from trusted_friends.rules import normalize_user_pair, requires_parental_consent
    from trusted_friends.versioning import workflow_versioning_behavior


ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)
CONTINUE_AS_NEW_EVENT_LIMIT = 100
MAX_CONTINUED_TRANSITIONS = 100


@dataclass
class _RelationshipRuntimeState:
    """Mutable state owned exclusively by the workflow execution.

    There is no database row for a trusted connection in this demo. Temporal
    replays history and reconstructs this object whenever a worker picks up the
    workflow, so every mutation must happen through deterministic workflow code.
    """

    workflow_id: str = ""
    requester_user_id: str = ""
    target_user_id: str = ""
    normalized_user_ids: list[str] = field(default_factory=list)
    source_channel: SourceChannel | None = None
    trigger: str = ""
    are_friends: bool = True
    parent_child_relationship: bool = False
    requester_snapshot: UserEligibilitySnapshot | None = None
    target_snapshot: UserEligibilitySnapshot | None = None
    consent_ttl_seconds: int = 120
    auto_accept: bool = False
    status: ConnectionStatus = ConnectionStatus.PENDING
    reason: str = "REQUEST_CREATED"
    accepted: bool = False
    consent_required: bool = False
    consent_status: ConsentStatus | None = None
    consent_timed_out: bool = False
    consent_deadline: datetime | None = None
    created_at: str = ""
    updated_at: str = ""
    last_eligibility_event_id: str | None = None
    eligibility_decision: EligibilityDecision = field(
        default_factory=lambda: EligibilityDecision(True, False, "PENDING"),
    )
    transitions: list[StateTransition] = field(default_factory=list)
    needs_validation: bool = False
    pending_configuration: TrustedConnectionRequest | None = None
    pending_change_events: list[ConfigurationChange] = field(default_factory=list)
    pending_reason: str = "REQUEST_CREATED"
    events_since_continue_as_new: int = 0
    continue_as_new_count: int = 0


class _RelationshipValidationStateMachine:
    """Pure state machine that turns relationship events into visible status.

    Signals and activities only record facts: accepted, consent received,
    eligibility changed, timer expired. `reconcile()` centralizes the priority
    order for those facts so the workflow loop stays easy to audit.
    """

    def apply_event(
        self,
        state: _RelationshipRuntimeState,
        event: RelationshipEvent,
        timestamp: str,
    ) -> None:
        """Apply an event to raw state and mark it for reconciliation."""
        if event.event_type == RelationshipEventType.REQUEST_CREATED:
            state.accepted = state.accepted or event.bypass_approval
            state.pending_reason = event.reason
        elif event.event_type == RelationshipEventType.ACCEPTED:
            state.accepted = True
            state.pending_reason = event.reason or "USER_ACCEPTED"
        elif event.event_type == RelationshipEventType.PARENTAL_CONSENT_RECEIVED:
            if self.is_terminal(state) or event.consent is None:
                return
            state.consent_status = event.consent.status
            state.pending_reason = (
                "PARENTAL_CONSENT_APPROVED"
                if event.consent.status == ConsentStatus.APPROVED
                else "PARENTAL_CONSENT_DENIED"
            )
        elif event.event_type == RelationshipEventType.ELIGIBILITY_UPDATED:
            if self.is_terminal(state) or event.eligibility_update is None:
                return
            update = event.eligibility_update
            state.last_eligibility_event_id = update.event_id
            if update.event_type == EligibilityEventType.PARENT_CHILD_FORMED:
                state.parent_child_relationship = True
            elif update.event_type == EligibilityEventType.PARENT_CHILD_REMOVED:
                state.parent_child_relationship = False
            if update.snapshot is not None:
                if update.changed_user_id == state.requester_user_id:
                    state.requester_snapshot = update.snapshot
                elif update.changed_user_id == state.target_user_id:
                    state.target_snapshot = update.snapshot
            if event.recompute_consent_required:
                self._refresh_consent_requirement_from_snapshots(state)
            state.eligibility_decision = EligibilityDecision(
                eligible=update.eligible,
                consent_required=state.consent_required,
                reason=update.reason,
            )
            state.pending_reason = update.reason
        elif event.event_type == RelationshipEventType.CONSENT_TIMEOUT:
            if self.is_terminal(state):
                return
            state.consent_timed_out = True
            state.pending_reason = "PARENTAL_CONSENT_TIMEOUT"
        else:
            state.pending_reason = event.reason

        if event.bypass_approval:
            state.accepted = True

        # Signals run during workflow task processing. Setting a flag lets the
        # main loop decide when to emit downstream side effects after it derives
        # a new state.
        state.needs_validation = True
        state.updated_at = timestamp

    def _refresh_consent_requirement_from_snapshots(
        self,
        state: _RelationshipRuntimeState,
    ) -> None:
        """Recompute VPC consent state from the latest workflow-owned snapshots."""
        if state.requester_snapshot is None or state.target_snapshot is None:
            return

        state.consent_required = requires_parental_consent(
            state.requester_snapshot,
        ) or requires_parental_consent(state.target_snapshot)
        if not state.consent_required:
            state.consent_timed_out = False
            state.consent_deadline = None

    def reconcile(self, state: _RelationshipRuntimeState, timestamp: str) -> bool:
        """Derive a public status from raw facts and record a transition if changed."""
        old_status = state.status
        new_status = self.desired_status(state)
        new_reason = self.reason_for_status(state, new_status, state.pending_reason)
        if new_status == old_status and new_reason == state.reason and state.transitions:
            return False

        state.status = new_status
        state.reason = new_reason
        state.updated_at = timestamp
        state.transitions.append(
            StateTransition(
                status=state.status,
                reason=state.reason,
                timestamp=state.updated_at,
            )
        )
        return True

    def desired_status(self, state: _RelationshipRuntimeState) -> ConnectionStatus:
        """Priority order matters: terminal/blocked states win over acceptance."""
        if state.consent_timed_out:
            return ConnectionStatus.EXPIRED
        if state.consent_status == ConsentStatus.DENIED:
            return ConnectionStatus.DENIED
        if not state.eligibility_decision.eligible:
            return ConnectionStatus.SUSPENDED
        if state.consent_required and state.consent_status != ConsentStatus.APPROVED:
            return ConnectionStatus.WAITING_FOR_PARENTAL_CONSENT
        if state.accepted:
            return ConnectionStatus.TRUSTED
        return ConnectionStatus.PENDING

    def reason_for_status(
        self,
        state: _RelationshipRuntimeState,
        status: ConnectionStatus,
        default_reason: str,
    ) -> str:
        if status == ConnectionStatus.EXPIRED:
            return "PARENTAL_CONSENT_TIMEOUT"
        if status == ConnectionStatus.DENIED:
            return "PARENTAL_CONSENT_DENIED"
        if status == ConnectionStatus.SUSPENDED:
            return state.eligibility_decision.reason
        if status == ConnectionStatus.TRUSTED:
            if default_reason in {
                "ELIGIBILITY_RESTORED",
                "PARENTAL_CONSENT_APPROVED",
                "NATURAL_AGE_UP_RETAINED",
                "PARENT_CHILD_RELATIONSHIP_FORMED",
            }:
                return default_reason
            if state.accepted:
                if default_reason in {
                    "IRL_AUTO_UPGRADE",
                    "PARENT_CHILD_AUTO_UPGRADE",
                    "QR_CROSS_AGE_RESCAN",
                    "SHARE_LINK_ACCEPTED",
                }:
                    return default_reason
                return "USER_ACCEPTED"
            return default_reason
        return default_reason

    def is_terminal(self, state: _RelationshipRuntimeState) -> bool:
        return state.status in {ConnectionStatus.EXPIRED, ConnectionStatus.DENIED}

    def consent_wait_timeout(
        self,
        state: _RelationshipRuntimeState,
        now: datetime,
    ) -> timedelta | None:
        """Return the remaining durable-timer delay for parental consent."""
        if not state.consent_required:
            return None
        if state.consent_status is not None or state.consent_timed_out:
            return None
        if state.consent_deadline is None:
            return None
        remaining = state.consent_deadline - now
        if remaining.total_seconds() <= 0:
            return timedelta(seconds=0)
        return remaining

    def is_consent_wait_expired(
        self,
        state: _RelationshipRuntimeState,
        now: datetime,
    ) -> bool:
        if state.consent_timed_out or self.is_terminal(state):
            return False
        if not state.consent_required or state.consent_status is not None:
            return False
        if state.consent_deadline is None:
            return False
        return now >= state.consent_deadline

# This state machine workflow owns the entire lifecycle of a trusted connection between two users. 
# It processes signals for user actions and async eligibility events, maintains the current state of the relationship, and emits status changes to downstream systems. 
# The workflow history serves as the source of truth for the relationship, and queries can be used to read the current state at any time.
@workflow.defn(versioning_behavior=workflow_versioning_behavior())
class TrustedConnectionWorkflow:
    """Long-lived workflow that owns one normalized trusted-friend pair.

    Clients address this workflow by deterministic workflow ID and send signals
    for user actions or async eligibility events. Queries read the current
    workflow-owned state, but the workflow history remains the source of truth.
    """

    def __init__(self) -> None:
        self._state = _RelationshipRuntimeState()
        self._state_machine = _RelationshipValidationStateMachine()
        self._close_requested = False

    @workflow.run
    async def run(
        self,
        request: TrustedConnectionRequest,
        continuation: TrustedConnectionContinuation | None = None,
    ) -> None:
        """Initialize the pair and wait for signals/timers until closed."""
        request = self._coerce_request(request)
        continuation = self._coerce_continuation(continuation)
        if continuation is None:
            self._initialize_state(request)

            # Eligibility checks are activities because real implementations would
            # call mutable external systems. Temporal records the activity result so
            # replay can use the recorded value instead of re-calling the service.
            initial_decision = await workflow.execute_activity(
                evaluate_initial_eligibility,
                request,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=ACTIVITY_RETRY_POLICY,
            )
            self._apply_configuration_decision(request, initial_decision)
            self._record_relationship_event(
                RelationshipEvent(
                    event_type=RelationshipEventType.REQUEST_CREATED,
                    reason=self._state.eligibility_decision.reason,
                    bypass_approval=request.auto_accept,
                )
            )
        else:
            self._restore_continued_state(continuation)

        while True:
            if self._close_requested:
                return

            if self._state.pending_configuration is not None:
                request = self._state.pending_configuration
                self._state.pending_configuration = None
                decision = await workflow.execute_activity(
                    evaluate_initial_eligibility,
                    request,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=ACTIVITY_RETRY_POLICY,
                )
                self._apply_configuration_decision(request, decision)
                continue

            if self._state.needs_validation:
                self._state.needs_validation = False
                changed = self._state_machine.reconcile(
                    self._state,
                    workflow.now().isoformat(),
                )
                if changed:
                    # Emitting is an activity so the external publication is
                    # retried and recorded, not duplicated by workflow replay.
                    await self._emit_status_change()
                while self._state.pending_change_events:
                    await self._emit_configuration_change(
                        self._state.pending_change_events[0],
                    )
                    self._state.pending_change_events.pop(0)
                continue

            if self._state_machine.is_consent_wait_expired(self._state, workflow.now()):
                self._record_relationship_event(
                    RelationshipEvent(
                        event_type=RelationshipEventType.CONSENT_TIMEOUT,
                        reason="PARENTAL_CONSENT_TIMEOUT",
                    )
                )
                continue

            if self._should_continue_as_new():
                self._continue_as_new()

            try:
                timeout = self._state_machine.consent_wait_timeout(
                    self._state,
                    workflow.now(),
                )
                if timeout is None:
                    # With no active consent timer, the workflow can sleep
                    # indefinitely. Signals wake the workflow by changing one
                    # of the values observed by this condition.
                    await workflow.wait_condition(
                        lambda: self._state.needs_validation
                        or self._state.pending_configuration is not None
                        or self._close_requested,
                    )
                else:
                    # This is a durable timer. The worker can crash or scale to
                    # zero and Temporal will still wake the workflow at expiry.
                    await workflow.wait_condition(
                        lambda: self._state.needs_validation
                        or self._state.pending_configuration is not None
                        or self._close_requested,
                        timeout=timeout,
                    )
            except asyncio.TimeoutError:
                self._record_relationship_event(
                    RelationshipEvent(
                        event_type=RelationshipEventType.CONSENT_TIMEOUT,
                        reason="PARENTAL_CONSENT_TIMEOUT",
                    )
                )

    @workflow.signal
    async def apply_relationship_event(self, event: RelationshipEvent) -> None:
        """Generic signal used by tests and future relationship event producers."""
        self._record_relationship_event(event)

    @workflow.signal
    async def accept(self) -> None:
        """Recipient acceptance signal for double opt-in style flows."""
        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.ACCEPTED,
                reason="USER_ACCEPTED",
            )
        )

    @workflow.signal
    async def parental_consent(self, consent: ParentalConsent) -> None:
        """Parent/guardian consent signal for VPC-gated requests."""
        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.PARENTAL_CONSENT_RECEIVED,
                reason="PARENTAL_CONSENT_RECEIVED",
                consent=consent,
            )
        )

    @workflow.signal
    async def close(self) -> None:
        """Complete the workflow gracefully after the demo user closes a run."""
        self._close_requested = True

    @workflow.signal
    async def apply_configuration(self, request: TrustedConnectionRequest) -> None:
        """Record a full operator-edited pair configuration for re-evaluation."""
        if self._state_machine.is_terminal(self._state):
            return
        self._state.pending_configuration = request
        self._state.events_since_continue_as_new += 1
        self._state.updated_at = workflow.now().isoformat()

    @workflow.signal
    async def apply_eligibility_update(self, update: EligibilityUpdate) -> None:
        """Signal sent by the short-lived eligibility workflow."""
        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.ELIGIBILITY_UPDATED,
                reason=update.reason,
                event_id=update.event_id,
                eligibility_update=update,
                recompute_consent_required=workflow.patched(
                    "eligibility-update-recomputes-consent-v1",
                ),
            )
        )

    @workflow.query
    def get_state(self) -> TrustedConnectionState:
        """Return a snapshot; queries must not mutate workflow state."""
        return self._snapshot_state()

    def _initialize_state(self, request: TrustedConnectionRequest) -> None:
        """Copy immutable start payload fields into workflow-owned state."""
        state = self._state
        state.workflow_id = workflow.info().workflow_id
        state.requester_user_id = request.requester_user_id
        state.target_user_id = request.target_user_id
        state.normalized_user_ids = list(
            normalize_user_pair(request.requester_user_id, request.target_user_id)
        )
        state.source_channel = request.source_channel
        state.trigger = request.trigger
        state.are_friends = request.are_friends
        state.parent_child_relationship = request.parent_child_relationship
        state.requester_snapshot = request.requester_snapshot
        state.target_snapshot = request.target_snapshot
        state.consent_ttl_seconds = request.consent_ttl_seconds
        state.auto_accept = request.auto_accept
        now = workflow.now().isoformat()
        state.created_at = now
        state.updated_at = now

    def _coerce_request(self, value) -> TrustedConnectionRequest:
        """Convert SDK-decoded payloads into the dataclass shape workflow code uses."""
        if isinstance(value, TrustedConnectionRequest):
            return value
        payload = dict(value)
        return TrustedConnectionRequest(
            requester_user_id=payload["requester_user_id"],
            target_user_id=payload["target_user_id"],
            source_channel=self._coerce_source_channel(payload["source_channel"]),
            requester_snapshot=self._coerce_user_snapshot(
                payload["requester_snapshot"],
            ),
            target_snapshot=self._coerce_user_snapshot(payload["target_snapshot"]),
            consent_ttl_seconds=payload.get("consent_ttl_seconds", 120),
            are_friends=payload.get("are_friends", True),
            parent_child_relationship=payload.get(
                "parent_child_relationship",
                False,
            ),
            auto_accept=payload.get("auto_accept", False),
            trigger=payload.get("trigger", "DOUBLE_OPT_IN"),
            metadata=dict(payload.get("metadata", {})),
        )

    def _coerce_continuation(
        self,
        value,
    ) -> TrustedConnectionContinuation | None:
        """Convert continued-run payloads into workflow dataclasses."""
        if value is None or isinstance(value, TrustedConnectionContinuation):
            return value
        payload = dict(value)
        return TrustedConnectionContinuation(
            state=self._coerce_trusted_connection_state(payload["state"]),
            eligibility_decision=self._coerce_eligibility_decision(
                payload["eligibility_decision"],
            ),
            consent_timed_out=payload.get("consent_timed_out", False),
            consent_deadline=payload.get("consent_deadline"),
            pending_reason=payload.get("pending_reason", "REQUEST_CREATED"),
            continue_as_new_count=payload.get("continue_as_new_count", 0),
        )

    def _coerce_trusted_connection_state(self, value) -> TrustedConnectionState:
        """Convert a continued public state snapshot into its query DTO."""
        if isinstance(value, TrustedConnectionState):
            return value
        payload = dict(value)
        return TrustedConnectionState(
            workflow_id=payload["workflow_id"],
            requester_user_id=payload["requester_user_id"],
            target_user_id=payload["target_user_id"],
            normalized_user_ids=list(payload["normalized_user_ids"]),
            source_channel=self._coerce_source_channel(payload["source_channel"]),
            trigger=payload["trigger"],
            are_friends=payload["are_friends"],
            parent_child_relationship=payload["parent_child_relationship"],
            status=self._coerce_connection_status(payload["status"]),
            reason=payload["reason"],
            accepted=payload["accepted"],
            consent_required=payload["consent_required"],
            consent_status=self._coerce_consent_status(payload["consent_status"]),
            created_at=payload["created_at"],
            updated_at=payload["updated_at"],
            last_eligibility_event_id=payload.get("last_eligibility_event_id"),
            requester_snapshot=self._coerce_optional_user_snapshot(
                payload.get("requester_snapshot"),
            ),
            target_snapshot=self._coerce_optional_user_snapshot(
                payload.get("target_snapshot"),
            ),
            consent_ttl_seconds=payload.get("consent_ttl_seconds", 120),
            auto_accept=payload.get("auto_accept", False),
            continue_as_new_count=payload.get("continue_as_new_count", 0),
            transitions=[
                self._coerce_transition(item)
                for item in payload.get("transitions", [])
            ],
        )

    def _coerce_user_snapshot(self, value) -> UserEligibilitySnapshot:
        if isinstance(value, UserEligibilitySnapshot):
            return value
        payload = dict(value)
        return UserEligibilitySnapshot(
            user_id=payload["user_id"],
            age=payload["age"],
            country_code=payload.get("country_code", "US"),
            is_age_verified=payload.get("is_age_verified", True),
            is_on_watchlist=payload.get("is_on_watchlist", False),
        )

    def _coerce_optional_user_snapshot(self, value) -> UserEligibilitySnapshot | None:
        return None if value is None else self._coerce_user_snapshot(value)

    def _coerce_eligibility_decision(self, value) -> EligibilityDecision:
        if isinstance(value, EligibilityDecision):
            return value
        payload = dict(value)
        return EligibilityDecision(
            eligible=payload["eligible"],
            consent_required=payload["consent_required"],
            reason=payload["reason"],
        )

    def _coerce_transition(self, value) -> StateTransition:
        if isinstance(value, StateTransition):
            return value
        payload = dict(value)
        return StateTransition(
            status=self._coerce_connection_status(payload["status"]),
            reason=payload["reason"],
            timestamp=payload["timestamp"],
        )

    def _coerce_source_channel(self, value) -> SourceChannel:
        return value if isinstance(value, SourceChannel) else SourceChannel(value)

    def _coerce_connection_status(self, value) -> ConnectionStatus:
        return value if isinstance(value, ConnectionStatus) else ConnectionStatus(value)

    def _coerce_consent_status(self, value) -> ConsentStatus | None:
        if value is None or isinstance(value, ConsentStatus):
            return value
        return ConsentStatus(value)

    def _restore_continued_state(
        self,
        continuation: TrustedConnectionContinuation,
    ) -> None:
        """Rehydrate workflow-owned state at the start of a continued run."""
        continued = continuation.state
        state = self._state
        state.workflow_id = workflow.info().workflow_id
        state.requester_user_id = continued.requester_user_id
        state.target_user_id = continued.target_user_id
        state.normalized_user_ids = list(continued.normalized_user_ids)
        state.source_channel = continued.source_channel
        state.trigger = continued.trigger
        state.are_friends = continued.are_friends
        state.parent_child_relationship = continued.parent_child_relationship
        state.requester_snapshot = continued.requester_snapshot
        state.target_snapshot = continued.target_snapshot
        state.consent_ttl_seconds = continued.consent_ttl_seconds
        state.auto_accept = continued.auto_accept
        state.status = continued.status
        state.reason = continued.reason
        state.accepted = continued.accepted
        state.consent_required = continued.consent_required
        state.consent_status = continued.consent_status
        state.consent_timed_out = continuation.consent_timed_out
        state.consent_deadline = (
            datetime.fromisoformat(continuation.consent_deadline)
            if continuation.consent_deadline
            else None
        )
        state.created_at = continued.created_at
        state.updated_at = continued.updated_at
        state.last_eligibility_event_id = continued.last_eligibility_event_id
        state.eligibility_decision = continuation.eligibility_decision
        state.transitions = list(continued.transitions)
        state.pending_reason = continuation.pending_reason
        state.continue_as_new_count = continuation.continue_as_new_count

    def _apply_configuration_decision(
        self,
        request: TrustedConnectionRequest,
        decision: EligibilityDecision,
    ) -> None:
        """Store latest editable config and schedule status reconciliation."""
        state = self._state
        state.pending_change_events.extend(self._configuration_changes_for(request))
        state.source_channel = request.source_channel
        state.trigger = request.trigger
        state.are_friends = request.are_friends
        state.parent_child_relationship = request.parent_child_relationship
        state.requester_snapshot = request.requester_snapshot
        state.target_snapshot = request.target_snapshot
        state.consent_ttl_seconds = request.consent_ttl_seconds
        state.auto_accept = request.auto_accept
        state.eligibility_decision = decision
        state.consent_required = decision.consent_required
        if request.auto_accept:
            state.accepted = True
        if state.consent_required and state.consent_status is None:
            state.consent_timed_out = False
            state.consent_deadline = workflow.now() + timedelta(
                seconds=request.consent_ttl_seconds
            )
        elif not state.consent_required:
            state.consent_timed_out = False
            state.consent_deadline = None
        state.pending_reason = decision.reason
        state.needs_validation = True
        state.updated_at = workflow.now().isoformat()

    def _configuration_changes_for(
        self,
        request: TrustedConnectionRequest,
    ) -> list[ConfigurationChange]:
        """Return semantic change events for a full edited configuration."""
        state = self._state
        changes: list[ConfigurationChange] = []
        if state.requester_snapshot is not None:
            changes.extend(
                self._snapshot_changes(
                    "REQUESTER",
                    state.requester_snapshot,
                    request.requester_snapshot,
                )
            )
        if state.target_snapshot is not None:
            changes.extend(
                self._snapshot_changes(
                    "TARGET",
                    state.target_snapshot,
                    request.target_snapshot,
                )
            )

        if state.source_channel is not None and state.source_channel != request.source_channel:
            changes.append(
                ConfigurationChange(
                    "SOURCE_CHANNEL_CHANGED",
                    ["source_channel"],
                )
            )
        if state.are_friends != request.are_friends:
            changes.append(ConfigurationChange("FRIENDSHIP_CHANGED", ["are_friends"]))
        if state.parent_child_relationship != request.parent_child_relationship:
            changes.append(
                ConfigurationChange(
                    "PARENT_CHILD_RELATIONSHIP_CHANGED",
                    ["parent_child_relationship"],
                )
            )
        if state.auto_accept != request.auto_accept:
            changes.append(ConfigurationChange("AUTO_ACCEPT_CHANGED", ["auto_accept"]))
        if state.consent_ttl_seconds != request.consent_ttl_seconds:
            changes.append(
                ConfigurationChange(
                    "CONSENT_TTL_CHANGED",
                    ["consent_ttl_seconds"],
                )
            )
        return changes

    def _snapshot_changes(
        self,
        role: str,
        before: UserEligibilitySnapshot,
        after: UserEligibilitySnapshot,
    ) -> list[ConfigurationChange]:
        """Return stable per-field user snapshot changes for event publication."""
        changes: list[ConfigurationChange] = []
        if before.age != after.age:
            reason = f"{role}_AGE_UP" if after.age > before.age else f"{role}_AGE_CHANGED"
            changes.append(
                ConfigurationChange(
                    reason,
                    [f"{role.lower()}_snapshot.age"],
                    after.user_id,
                )
            )
        if before.country_code != after.country_code:
            changes.append(
                ConfigurationChange(
                    f"{role}_COUNTRY_CHANGED",
                    [f"{role.lower()}_snapshot.country_code"],
                    after.user_id,
                )
            )
        if before.is_age_verified != after.is_age_verified:
            changes.append(
                ConfigurationChange(
                    f"{role}_AGE_VERIFICATION_CHANGED",
                    [f"{role.lower()}_snapshot.is_age_verified"],
                    after.user_id,
                )
            )
        if before.is_on_watchlist != after.is_on_watchlist:
            changes.append(
                ConfigurationChange(
                    f"{role}_WATCHLIST_CHANGED",
                    [f"{role.lower()}_snapshot.is_on_watchlist"],
                    after.user_id,
                )
            )
        return changes

    def _record_relationship_event(self, event: RelationshipEvent) -> None:
        """Record the event using deterministic workflow time."""
        self._state.events_since_continue_as_new += 1
        self._state_machine.apply_event(
            self._state,
            event,
            workflow.now().isoformat(),
        )

    def _should_continue_as_new(self) -> bool:
        """Return true only when all in-run work has been durably drained."""
        if (
            self._state.needs_validation
            or self._state.pending_configuration is not None
            or self._state.pending_change_events
            or self._close_requested
        ):
            return False
        if not workflow.patched("trusted-connection-continue-as-new-v1"):
            return False
        return (
            workflow.info().is_continue_as_new_suggested()
            or self._state.events_since_continue_as_new >= CONTINUE_AS_NEW_EVENT_LIMIT
        )

    def _continue_as_new(self) -> None:
        """Start the next run with compact state and a fresh event history."""
        state = self._state
        state.continue_as_new_count += 1
        state.events_since_continue_as_new = 0
        workflow.continue_as_new(
            args=[
                self._request_from_state(),
                self._continuation_from_state(),
            ],
        )

    def _request_from_state(self) -> TrustedConnectionRequest:
        """Build the stable run input required by the workflow signature."""
        state = self._state
        if state.requester_snapshot is None or state.target_snapshot is None:
            raise RuntimeError("Cannot continue as new without participant snapshots")
        return TrustedConnectionRequest(
            requester_user_id=state.requester_user_id,
            target_user_id=state.target_user_id,
            source_channel=state.source_channel or SourceChannel.STANDARD,
            requester_snapshot=state.requester_snapshot,
            target_snapshot=state.target_snapshot,
            consent_ttl_seconds=state.consent_ttl_seconds,
            are_friends=state.are_friends,
            parent_child_relationship=state.parent_child_relationship,
            auto_accept=state.auto_accept,
            trigger=state.trigger,
            metadata={"continued_from": state.workflow_id},
        )

    def _continuation_from_state(self) -> TrustedConnectionContinuation:
        """Capture the compact state payload passed to the next workflow run."""
        state = self._state
        snapshot = self._snapshot_state()
        snapshot.transitions = snapshot.transitions[-MAX_CONTINUED_TRANSITIONS:]
        return TrustedConnectionContinuation(
            state=snapshot,
            eligibility_decision=state.eligibility_decision,
            consent_timed_out=state.consent_timed_out,
            consent_deadline=state.consent_deadline.isoformat()
            if state.consent_deadline
            else None,
            pending_reason=state.pending_reason,
            continue_as_new_count=state.continue_as_new_count,
        )

    async def _emit_status_change(self) -> None:
        """Publish state transitions to downstream systems via an activity."""
        await self._emit_tc_change_event("STATUS_CHANGED", self._state.reason)

    async def _emit_configuration_change(self, change: ConfigurationChange) -> None:
        """Publish configuration mutations even when status stays unchanged."""
        await self._emit_tc_change_event(
            "CONFIGURATION_CHANGED",
            change.reason,
            changed_fields=change.changed_fields,
            subject_user_id=change.subject_user_id,
        )

    async def _emit_tc_change_event(
        self,
        event_type: str,
        reason: str,
        *,
        changed_fields: list[str] | None = None,
        subject_user_id: str | None = None,
    ) -> None:
        """Publish a typed TC change event to downstream systems via an activity."""
        first, second = self._state.normalized_user_ids
        event = TCChangeEvent(
            event_id=str(workflow.uuid4()),
            timestamp=workflow.now().isoformat(),
            workflow_id=self._state.workflow_id,
            user_id_a=first,
            user_id_b=second,
            status=self._state.status,
            reason=reason,
            event_type=event_type,
            changed_fields=changed_fields or [],
            subject_user_id=subject_user_id,
        )
        await workflow.execute_activity(
            emit_tc_change_event,
            event,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )

    def _snapshot_state(self) -> TrustedConnectionState:
        """Create an immutable query DTO from internal mutable state."""
        state = self._state
        return TrustedConnectionState(
            workflow_id=state.workflow_id,
            requester_user_id=state.requester_user_id,
            target_user_id=state.target_user_id,
            normalized_user_ids=list(state.normalized_user_ids),
            source_channel=state.source_channel or SourceChannel.STANDARD,
            trigger=state.trigger,
            are_friends=state.are_friends,
            parent_child_relationship=state.parent_child_relationship,
            status=state.status,
            reason=state.reason,
            accepted=state.accepted,
            consent_required=state.consent_required,
            consent_status=state.consent_status,
            created_at=state.created_at,
            updated_at=state.updated_at,
            last_eligibility_event_id=state.last_eligibility_event_id,
            requester_snapshot=state.requester_snapshot,
            target_snapshot=state.target_snapshot,
            consent_ttl_seconds=state.consent_ttl_seconds,
            auto_accept=state.auto_accept,
            continue_as_new_count=state.continue_as_new_count,
            transitions=list(state.transitions),
        )

# This workflow handles async events independently of the primary pair workflow
# This architecture provides a number of benefits including:
# 1. Separation of concerns: The eligibility evaluation logic is decoupled from the main pair workflow, making it easier to maintain and evolve each component independently.
# 2. Scalability: The short-lived eligibility workflow can be scaled independently to handle varying loads of eligibility events without impacting the main pair workflow.
# 3. Idempotency and reliability: By using a separate workflow to process eligibility events, we can ensure that each event is processed exactly once and that any retries or failures are handled gracefully without affecting the main pair workflow.
# 4. Observability: This design allows for better monitoring and debugging of eligibility events, as they are processed in a dedicated workflow with its own history and logs, separate from the main pair workflow.
@workflow.defn(versioning_behavior=workflow_versioning_behavior())
class EligibilityEvaluationWorkflow:
    """Short-lived workflow that fans async eligibility events into the pair workflow."""

    @workflow.run
    async def run(self, event: EligibilityEvent) -> EligibilityUpdate:
        """Evaluate one event and signal the long-lived pair workflow."""
        update = await workflow.execute_activity(
            evaluate_event_eligibility,
            event,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        handle = workflow.get_external_workflow_handle(event.pair_workflow_id)
        try:
            # External workflow handles let one workflow signal another without
            # the API process being involved in the second hop.
            await handle.signal("apply_eligibility_update", update)
        except Exception as err:
            workflow.logger.warning(
                "Skipping eligibility update signal because pair workflow is unavailable",
                extra={
                    "pair_workflow_id": event.pair_workflow_id,
                    "event_id": event.event_id,
                    "error": str(err),
                },
            )
        return update
