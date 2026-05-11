from __future__ import annotations

from trusted_friends.models import (
    DomainFact,
    DomainFactType,
    EligibilityDecision,
    EligibilityEvent,
    EligibilityEventType,
    EligibilityUpdate,
    SourceChannel,
    TrustedConnectionRequest,
    UserEligibilitySnapshot,
)


VPC_U16_COUNTRIES = {"BR", "AU"}


def normalize_user_pair(user_id_a: str, user_id_b: str) -> tuple[str, str]:
    """Return a stable pair ordering used by IDs, queries, and signals.

    Temporal workflow IDs are unique within a namespace while a workflow is
    open. Sorting the pair makes `alice,bob` and `bob,alice` address the same
    durable relationship workflow instead of creating two independent histories.
    """
    if not user_id_a or not user_id_b:
        raise ValueError("Both user IDs are required")
    if user_id_a == user_id_b:
        raise ValueError("A user cannot add themselves as a trusted friend")
    return tuple(sorted((user_id_a, user_id_b)))


def workflow_id_for_pair(user_id_a: str, user_id_b: str) -> str:
    """Build the deterministic workflow ID for the long-lived pair workflow."""
    first, second = normalize_user_pair(user_id_a, user_id_b)
    return f"trusted-connection-{first}-{second}"


def requires_parental_consent(snapshot: UserEligibilitySnapshot) -> bool:
    """Centralize region-specific consent policy used by all request paths."""
    country_code = snapshot.country_code.upper()
    return snapshot.age < 13 or (country_code in VPC_U16_COUNTRIES and snapshot.age < 16)


def evaluate_user(snapshot: UserEligibilitySnapshot) -> EligibilityDecision:
    """Evaluate user-level eligibility before relationship-level rules run."""
    if snapshot.age < 0 or snapshot.age > 130:
        return EligibilityDecision(False, False, "INVALID_AGE")
    if not snapshot.is_age_verified:
        return EligibilityDecision(False, False, "AGE_VERIFICATION_REQUIRED")
    if snapshot.is_on_watchlist:
        return EligibilityDecision(False, False, "WATCHLIST_REQUIRES_EXPLICIT_IDV")
    return EligibilityDecision(True, requires_parental_consent(snapshot), "ELIGIBLE")


def evaluate_pair(
    requester: UserEligibilitySnapshot,
    target: UserEligibilitySnapshot,
) -> EligibilityDecision:
    """Combine requester and target eligibility into the pair-level baseline."""
    requester_decision = evaluate_user(requester)
    if not requester_decision.eligible:
        return EligibilityDecision(False, False, f"REQUESTER_{requester_decision.reason}")

    target_decision = evaluate_user(target)
    if not target_decision.eligible:
        return EligibilityDecision(False, False, f"TARGET_{target_decision.reason}")

    consent_required = requester_decision.consent_required or target_decision.consent_required
    reason = "PARENTAL_CONSENT_REQUIRED" if consent_required else "ELIGIBLE"
    return EligibilityDecision(True, consent_required, reason)


def same_trusted_friend_age_group(
    requester: UserEligibilitySnapshot,
    target: UserEligibilitySnapshot,
) -> bool:
    """Model the same-age-band rule for standard double opt-in requests."""
    return (requester.age >= 18 and target.age >= 18) or (
        16 <= requester.age <= 17 and 16 <= target.age <= 17
    )


def evaluate_trusted_connection_request(
    request: TrustedConnectionRequest,
) -> EligibilityDecision:
    """Apply entry-point-specific rules for a trusted friend request.

    The workflow calls this through an activity, which lets product policy
    evolve independently of the deterministic workflow loop. The returned
    reason string is intentionally user/auditor visible in the demo timeline.
    """
    pair_decision = evaluate_pair(request.requester_snapshot, request.target_snapshot)
    if not pair_decision.eligible:
        return pair_decision

    if request.source_channel != SourceChannel.SHARE_LINK and not request.are_friends:
        return EligibilityDecision(False, False, "FRIENDSHIP_REQUIRED")

    if request.source_channel == SourceChannel.PARENT_CHILD:
        if not request.parent_child_relationship:
            return EligibilityDecision(False, False, "PARENT_CHILD_RELATIONSHIP_REQUIRED")
        return EligibilityDecision(
            True,
            pair_decision.consent_required,
            "PARENT_CHILD_AUTO_UPGRADE",
        )

    if request.source_channel in {SourceChannel.QR_CODE, SourceChannel.CONTACT_LIST_IMPORTER}:
        if request.requester_snapshot.age < 13 or request.target_snapshot.age < 13:
            return EligibilityDecision(False, False, "IRL_AUTO_UPGRADE_REQUIRES_13_PLUS")
        return EligibilityDecision(True, False, "IRL_AUTO_UPGRADE")

    if request.source_channel == SourceChannel.QR_CROSS_AGE:
        return EligibilityDecision(
            True,
            pair_decision.consent_required,
            "QR_CROSS_AGE_RESCAN",
        )

    if request.source_channel == SourceChannel.SHARE_LINK:
        return EligibilityDecision(
            True,
            pair_decision.consent_required,
            "SHARE_LINK_ACCEPTED",
        )

    if pair_decision.consent_required:
        return pair_decision

    if not same_trusted_friend_age_group(
        request.requester_snapshot,
        request.target_snapshot,
    ):
        return EligibilityDecision(False, False, "SAME_AGE_GROUP_REQUIRED")

    return EligibilityDecision(True, False, "DOUBLE_OPT_IN_ELIGIBLE")


def evaluate_eligibility_event(event: EligibilityEvent) -> EligibilityUpdate:
    """Translate an async business event into a workflow eligibility update.

    Event workflows are short-lived: they validate one event, then signal the
    long-lived pair workflow. That keeps fan-in from external systems out of the
    pair workflow while preserving a durable audit trail for each event.
    """
    if event.event_type == EligibilityEventType.PARENT_CHILD_REMOVED:
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=False,
            reason="PARENT_CHILD_RELATIONSHIP_REMOVED",
            event_type=event.event_type,
            snapshot=event.snapshot,
        )

    if event.event_type == EligibilityEventType.PARENTAL_CONSENT_REJECTED:
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=False,
            reason="PARENTAL_CONSENT_REJECTED",
            event_type=event.event_type,
            snapshot=event.snapshot,
        )

    if event.event_type == EligibilityEventType.PARENT_CHILD_FORMED:
        decision = evaluate_user(event.snapshot)
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=decision.eligible,
            reason="PARENT_CHILD_RELATIONSHIP_FORMED"
            if decision.eligible
            else decision.reason,
            event_type=event.event_type,
            snapshot=event.snapshot,
        )

    if event.event_type == EligibilityEventType.PARENTAL_CONSENT_APPROVED:
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=True,
            reason="PARENTAL_CONSENT_APPROVED",
            event_type=event.event_type,
            snapshot=event.snapshot,
        )

    decision = evaluate_user(event.snapshot)
    if event.event_type == EligibilityEventType.AGE_CHANGED and decision.eligible:
        reason = "NATURAL_AGE_UP_RETAINED"
    else:
        reason = "ELIGIBILITY_RESTORED" if decision.eligible else decision.reason
    return EligibilityUpdate(
        event_id=event.event_id,
        changed_user_id=event.changed_user_id,
        eligible=decision.eligible,
        reason=reason,
        event_type=event.event_type,
        snapshot=event.snapshot,
    )


def eligibility_event_from_domain_fact(fact: DomainFact) -> EligibilityEvent:
    """Map an upstream fact to the TF-domain event consumed by Temporal.

    This is the anti-duplication boundary: Kafka producers only state what
    changed in their own domain. This function, owned by Trusted Friends,
    decides which workflow event type that fact represents.
    """
    event_type_by_fact_type = {
        DomainFactType.USER_ELIGIBILITY_CHANGED: EligibilityEventType.ELIGIBILITY_CHANGED,
        DomainFactType.USER_AGE_CHANGED: EligibilityEventType.AGE_CHANGED,
        DomainFactType.PARENT_CHILD_RELATIONSHIP_FORMED: (
            EligibilityEventType.PARENT_CHILD_FORMED
        ),
        DomainFactType.PARENT_CHILD_RELATIONSHIP_REMOVED: (
            EligibilityEventType.PARENT_CHILD_REMOVED
        ),
    }

    return EligibilityEvent(
        event_id=fact.fact_id,
        user_id_a=fact.user_id_a,
        user_id_b=fact.user_id_b,
        changed_user_id=fact.subject_user_id,
        snapshot=fact.snapshot,
        pair_workflow_id=fact.pair_workflow_id,
        event_type=event_type_by_fact_type[fact.fact_type],
    )
