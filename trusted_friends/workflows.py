from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

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
    def apply_event(
        self,
        state: _RelationshipRuntimeState,
        event: RelationshipEvent,
        timestamp: str,
    ) -> None:
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
        state.needs_validation = True
        state.updated_at = timestamp

    def reconcile(self, state: _RelationshipRuntimeState, timestamp: str) -> bool:
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


@workflow.defn(versioning_behavior=workflow_versioning_behavior())
class TrustedConnectionWorkflow:
    def __init__(self) -> None:
        self._state = _RelationshipRuntimeState()
        self._state_machine = _RelationshipValidationStateMachine()
        self._close_requested = False

    @workflow.run
    async def run(self, request: TrustedConnectionRequest) -> None:
        self._initialize_state(request)

        self._state.eligibility_decision = await workflow.execute_activity(
            evaluate_initial_eligibility,
            request,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        self._state.consent_required = self._state.eligibility_decision.consent_required
        if self._state.consent_required:
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
                    await workflow.wait_condition(
                        lambda: self._state.needs_validation or self._close_requested,
                    )
                else:
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
        self._record_relationship_event(event)

    @workflow.signal
    async def accept(self) -> None:
        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.ACCEPTED,
                reason="USER_ACCEPTED",
            )
        )

    @workflow.signal
    async def parental_consent(self, consent: ParentalConsent) -> None:
        self._record_relationship_event(
            RelationshipEvent(
                event_type=RelationshipEventType.PARENTAL_CONSENT_RECEIVED,
                reason="PARENTAL_CONSENT_RECEIVED",
                consent=consent,
            )
        )

    @workflow.signal
    async def close(self) -> None:
        self._close_requested = True

    @workflow.signal
    async def apply_eligibility_update(self, update: EligibilityUpdate) -> None:
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
        return self._snapshot_state()

    def _initialize_state(self, request: TrustedConnectionRequest) -> None:
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
        self._state_machine.apply_event(
            self._state,
            event,
            workflow.now().isoformat(),
        )

    async def _emit_status_change(self) -> None:
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


@workflow.defn(versioning_behavior=workflow_versioning_behavior())
class EligibilityEvaluationWorkflow:
    @workflow.run
    async def run(self, event: EligibilityEvent) -> EligibilityUpdate:
        update = await workflow.execute_activity(
            evaluate_event_eligibility,
            event,
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        handle = workflow.get_external_workflow_handle(event.pair_workflow_id)
        try:
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
