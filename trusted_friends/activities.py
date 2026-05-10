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


@activity.defn
def evaluate_initial_eligibility(request: TrustedConnectionRequest) -> EligibilityDecision:
    """Fake Identity Attributes and Safety Trustbox checks for the demo."""
    return evaluate_trusted_connection_request(request)


@activity.defn
def evaluate_event_eligibility(event: EligibilityEvent) -> EligibilityUpdate:
    """Fake event re-evaluation used by the short-lived eligibility workflow."""
    return evaluate_eligibility_event(event)


@activity.defn
def emit_tc_change_event(event: TCChangeEvent) -> str:
    """Fake Kafka/SQS emission. The Temporal history records every invocation."""
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
