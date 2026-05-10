from trusted_friends.models import (
    EligibilityEvent,
    EligibilityEventType,
    SourceChannel,
    TrustedConnectionRequest,
    UserEligibilitySnapshot,
)
from trusted_friends.rules import (
    evaluate_eligibility_event,
    evaluate_pair,
    evaluate_trusted_connection_request,
    evaluate_user,
    requires_parental_consent,
    workflow_id_for_pair,
)

# Rule tests pin the pure business logic separately from Temporal replay. This
# makes eligibility behavior easy to reason about before it is embedded in a
# workflow state machine.

def test_workflow_id_for_pair_is_deterministic() -> None:
    assert workflow_id_for_pair("bob", "alice") == "trusted-connection-alice-bob"
    assert workflow_id_for_pair("alice", "bob") == "trusted-connection-alice-bob"


def test_parental_consent_rules() -> None:
    assert requires_parental_consent(
        UserEligibilitySnapshot("u1", age=12, country_code="US")
    )
    assert requires_parental_consent(
        UserEligibilitySnapshot("u2", age=15, country_code="BR")
    )
    assert not requires_parental_consent(
        UserEligibilitySnapshot("u3", age=15, country_code="US")
    )


def test_age_verification_required() -> None:
    decision = evaluate_user(
        UserEligibilitySnapshot("u1", age=18, is_age_verified=False)
    )
    assert not decision.eligible
    assert decision.reason == "AGE_VERIFICATION_REQUIRED"


def test_pair_marks_consent_required() -> None:
    decision = evaluate_pair(
        UserEligibilitySnapshot("u1", age=18),
        UserEligibilitySnapshot("u2", age=12),
    )
    assert decision.eligible
    assert decision.consent_required
    assert decision.reason == "PARENTAL_CONSENT_REQUIRED"


def test_double_opt_in_requires_same_age_group() -> None:
    decision = evaluate_trusted_connection_request(
        TrustedConnectionRequest(
            requester_user_id="u1",
            target_user_id="u2",
            source_channel=SourceChannel.STANDARD,
            requester_snapshot=UserEligibilitySnapshot("u1", age=18),
            target_snapshot=UserEligibilitySnapshot("u2", age=17),
        )
    )
    assert not decision.eligible
    assert decision.reason == "SAME_AGE_GROUP_REQUIRED"


def test_share_link_does_not_require_friendship() -> None:
    decision = evaluate_trusted_connection_request(
        TrustedConnectionRequest(
            requester_user_id="u1",
            target_user_id="u2",
            source_channel=SourceChannel.SHARE_LINK,
            requester_snapshot=UserEligibilitySnapshot("u1", age=18),
            target_snapshot=UserEligibilitySnapshot("u2", age=18),
            are_friends=False,
        )
    )
    assert decision.eligible
    assert decision.reason == "SHARE_LINK_ACCEPTED"


def test_parent_child_requires_relationship_flag() -> None:
    decision = evaluate_trusted_connection_request(
        TrustedConnectionRequest(
            requester_user_id="parent",
            target_user_id="child",
            source_channel=SourceChannel.PARENT_CHILD,
            requester_snapshot=UserEligibilitySnapshot("parent", age=42),
            target_snapshot=UserEligibilitySnapshot("child", age=14),
            parent_child_relationship=False,
        )
    )
    assert not decision.eligible
    assert decision.reason == "PARENT_CHILD_RELATIONSHIP_REQUIRED"


def test_eligibility_event_rules_live_in_central_rule_set() -> None:
    update = evaluate_eligibility_event(
        EligibilityEvent(
            event_id="event-1",
            user_id_a="alice",
            user_id_b="bob",
            changed_user_id="alice",
            snapshot=UserEligibilitySnapshot(
                "alice",
                age=18,
                is_age_verified=False,
                is_on_watchlist=True,
            ),
            pair_workflow_id="trusted-connection-alice-bob",
            event_type=EligibilityEventType.ELIGIBILITY_CHANGED,
        )
    )

    assert not update.eligible
    assert update.reason == "AGE_VERIFICATION_REQUIRED"
