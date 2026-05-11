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
        TrustedConnectionRequest,
        TrustedConnectionState,
    )
    from trusted_friends.rules import normalize_user_pair
    from trusted_friends.versioning import workflow_versioning_behavior


ACTIVITY_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)


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
    pending_reason: str = "REQUEST_CREATED"


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

    def reconcile(self, state: _RelationshipRuntimeState, timestamp: str) -> bool:
        """Derive a public status from raw facts and record a transition if changed."""
        old_status = state.status
        new_status = self.desired_status(state)
        if new_status == old_status and state.transitions:
            return False

        state.status = new_status
        state.reason = self.reason_for_status(state, new_status, state.pending_reason)
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
    async def run(self, request: TrustedConnectionRequest) -> None:
        """Initialize the pair and wait for signals/timers until closed."""
        self._initialize_state(request)

        # Eligibility checks are activities because real implementations would
        # call mutable external systems. Temporal records the activity result so
        # replay can use the recorded value instead of re-calling the service.
        self._state.eligibility_decision = await workflow.execute_activity(
            evaluate_initial_eligibility,
            request,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        self._state.consent_required = self._state.eligibility_decision.consent_required
        if self._state.consent_required:
            # `workflow.now()` is deterministic: during replay it returns the
            # same logical time represented by the workflow history.
            self._state.consent_deadline = workflow.now() + timedelta(
                seconds=request.consent_ttl_seconds
            )

        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.REQUEST_CREATED,
                reason=self._state.eligibility_decision.reason,
                bypass_approval=request.auto_accept,
            )
        )

        while True:
            if self._close_requested:
                return

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
                continue

            if self._state_machine.is_consent_wait_expired(self._state, workflow.now()):
                self._record_relationship_event(
                    RelationshipEvent(
                        event_type=RelationshipEventType.CONSENT_TIMEOUT,
                        reason="PARENTAL_CONSENT_TIMEOUT",
                    )
                )
                continue

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
                        lambda: self._state.needs_validation or self._close_requested,
                    )
                else:
                    # This is a durable timer. The worker can crash or scale to
                    # zero and Temporal will still wake the workflow at expiry.
                    await workflow.wait_condition(
                        lambda: self._state.needs_validation or self._close_requested,
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
    async def apply_eligibility_update(self, update: EligibilityUpdate) -> None:
        """Signal sent by the short-lived eligibility workflow."""
        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.ELIGIBILITY_UPDATED,
                reason=update.reason,
                event_id=update.event_id,
                eligibility_update=update,
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
        now = workflow.now().isoformat()
        state.created_at = now
        state.updated_at = now

    def _record_relationship_event(self, event: RelationshipEvent) -> None:
        """Record the event using deterministic workflow time."""
        self._state_machine.apply_event(
            self._state,
            event,
            workflow.now().isoformat(),
        )

    async def _emit_status_change(self) -> None:
        """Publish state transitions to downstream systems via an activity."""
        first, second = self._state.normalized_user_ids
        event = TCChangeEvent(
            event_id=str(workflow.uuid4()),
            timestamp=workflow.now().isoformat(),
            workflow_id=self._state.workflow_id,
            user_id_a=first,
            user_id_b=second,
            status=self._state.status,
            reason=self._state.reason,
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
