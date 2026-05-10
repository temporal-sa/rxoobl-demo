from __future__ import annotations

from trusted_friends.models import (
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
    if not user_id_a or not user_id_b:
        raise ValueError("Both user IDs are required")
    if user_id_a == user_id_b:
        raise ValueError("A user cannot add themselves as a trusted friend")
    return tuple(sorted((user_id_a, user_id_b)))


def workflow_id_for_pair(user_id_a: str, user_id_b: str) -> str:
    first, second = normalize_user_pair(user_id_a, user_id_b)
    return f"trusted-connection-{first}-{second}"


def requires_parental_consent(snapshot: UserEligibilitySnapshot) -> bool:
    country_code = snapshot.country_code.upper()
    return snapshot.age < 13 or (country_code in VPC_U16_COUNTRIES and snapshot.age < 16)


def evaluate_user(snapshot: UserEligibilitySnapshot) -> EligibilityDecision:
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
    return (requester.age >= 18 and target.age >= 18) or (
        16 <= requester.age <= 17 and 16 <= target.age <= 17
    )


def evaluate_trusted_connection_request(
    request: TrustedConnectionRequest,
) -> EligibilityDecision:
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
    if event.event_type == EligibilityEventType.PARENT_CHILD_REMOVED:
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=False,
            reason="PARENT_CHILD_RELATIONSHIP_REMOVED",
        )

    if event.event_type == EligibilityEventType.PARENTAL_CONSENT_REJECTED:
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=False,
            reason="PARENTAL_CONSENT_REJECTED",
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
        )

    if event.event_type == EligibilityEventType.PARENTAL_CONSENT_APPROVED:
        return EligibilityUpdate(
            event_id=event.event_id,
            changed_user_id=event.changed_user_id,
            eligible=True,
            reason="PARENTAL_CONSENT_APPROVED",
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
    )
