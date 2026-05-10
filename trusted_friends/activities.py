from __future__ import annotations

from temporalio import activity

from trusted_friends.models import (
    EligibilityDecision,
    EligibilityEvent,
    EligibilityUpdate,
    TCChangeEvent,
    TrustedConnectionRequest,
)
from trusted_friends.rules import (
    evaluate_eligibility_event,
    evaluate_trusted_connection_request,
)

# Temporal activities are the "side-effect boundary" for a workflow. Workflows
# must be deterministic during replay, so anything that would normally call an
# external service, publish to a queue, read a database, or consult mutable
# business rules belongs in an activity instead of directly inside workflow code.


@activity.defn
def evaluate_initial_eligibility(request: TrustedConnectionRequest) -> EligibilityDecision:
    """Evaluate the initial request as if calling IDV/safety services."""
    return evaluate_trusted_connection_request(request)


@activity.defn
def evaluate_event_eligibility(event: EligibilityEvent) -> EligibilityUpdate:
    """Evaluate a later eligibility event before it is signaled to the pair workflow."""
    return evaluate_eligibility_event(event)


@activity.defn
def emit_tc_change_event(event: TCChangeEvent) -> str:
    """Represent downstream event publication.

    In production this is where a Kafka/SQS/EventBridge publish would happen.
    Keeping it as an activity means Temporal records the result and will not
    accidentally duplicate the side effect during workflow replay.
    """
    activity.logger.info(
        "TC change event emitted",
        extra={
            "event_id": event.event_id,
            "workflow_id": event.workflow_id,
            "status": event.status,
            "reason": event.reason,
        },
    )
    return event.event_id
