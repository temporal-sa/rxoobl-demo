from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

# This module intentionally contains plain dataclasses/enums only. Temporal's
# default JSON payload converter can serialize these small value objects, and
# keeping them side-effect-free makes them safe to import from workflow code.


class ConnectionStatus(StrEnum):
    """Externally visible lifecycle states for a trusted connection pair."""

    PENDING = "PENDING"
    WAITING_FOR_PARENTAL_CONSENT = "WAITING_FOR_PARENTAL_CONSENT"
    TRUSTED = "TRUSTED"
    SUSPENDED = "SUSPENDED"
    EXPIRED = "EXPIRED"
    DENIED = "DENIED"


class ConsentStatus(StrEnum):
    APPROVED = "APPROVED"
    DENIED = "DENIED"


class SourceChannel(StrEnum):
    """Entry points are business-significant because each one has different rules."""

    STANDARD = "STANDARD"
    SHARE_LINK = "SHARE_LINK"
    QR_CODE = "QR_CODE"
    QR_CROSS_AGE = "QR_CROSS_AGE"
    CONTACT_LIST_IMPORTER = "CONTACT_LIST_IMPORTER"
    PARENT_CHILD = "PARENT_CHILD"


class EligibilityEventType(StrEnum):
    ELIGIBILITY_CHANGED = "ELIGIBILITY_CHANGED"
    AGE_CHANGED = "AGE_CHANGED"
    PARENT_CHILD_FORMED = "PARENT_CHILD_FORMED"
    PARENT_CHILD_REMOVED = "PARENT_CHILD_REMOVED"
    PARENTAL_CONSENT_APPROVED = "PARENTAL_CONSENT_APPROVED"
    PARENTAL_CONSENT_REJECTED = "PARENTAL_CONSENT_REJECTED"


class DomainFactType(StrEnum):
    """Facts published by upstream systems before TF-specific interpretation."""

    USER_ELIGIBILITY_CHANGED = "USER_ELIGIBILITY_CHANGED"
    USER_AGE_CHANGED = "USER_AGE_CHANGED"
    PARENT_CHILD_RELATIONSHIP_FORMED = "PARENT_CHILD_RELATIONSHIP_FORMED"
    PARENT_CHILD_RELATIONSHIP_REMOVED = "PARENT_CHILD_RELATIONSHIP_REMOVED"


class RelationshipEventType(StrEnum):
    """Internal events consumed by the workflow state machine."""

    REQUEST_CREATED = "REQUEST_CREATED"
    ACCEPTED = "ACCEPTED"
    PARENTAL_CONSENT_RECEIVED = "PARENTAL_CONSENT_RECEIVED"
    ELIGIBILITY_UPDATED = "ELIGIBILITY_UPDATED"
    CONSENT_TIMEOUT = "CONSENT_TIMEOUT"


@dataclass
class UserEligibilitySnapshot:
    user_id: str
    age: int
    country_code: str = "US"
    is_age_verified: bool = True
    is_on_watchlist: bool = False


@dataclass
class TrustedConnectionRequest:
    """Start payload for the long-lived pair workflow."""

    requester_user_id: str
    target_user_id: str
    source_channel: SourceChannel
    requester_snapshot: UserEligibilitySnapshot
    target_snapshot: UserEligibilitySnapshot
    consent_ttl_seconds: int = 120
    are_friends: bool = True
    parent_child_relationship: bool = False
    auto_accept: bool = False
    trigger: str = "DOUBLE_OPT_IN"
    metadata: dict[str, str] = field(default_factory=dict)


@dataclass
class EligibilityDecision:
    """Decision returned by initial eligibility activities."""

    eligible: bool
    consent_required: bool
    reason: str


@dataclass
class ParentalConsent:
    consent_id: str
    status: ConsentStatus
    timestamp: str | None = None


@dataclass
class EligibilityEvent:
    event_id: str
    user_id_a: str
    user_id_b: str
    changed_user_id: str
    snapshot: UserEligibilitySnapshot
    pair_workflow_id: str
    event_type: EligibilityEventType = EligibilityEventType.ELIGIBILITY_CHANGED


@dataclass
class DomainFact:
    """Raw fact from another bounded context.

    Upstream producers should publish facts they own, such as a user profile or
    family-graph change. They should not decide trusted-friend state transitions;
    the Trusted Friends domain converts these facts into workflow events.
    """

    fact_id: str
    fact_type: DomainFactType
    user_id_a: str
    user_id_b: str
    subject_user_id: str
    snapshot: UserEligibilitySnapshot
    pair_workflow_id: str


@dataclass
class EligibilityUpdate:
    event_id: str
    changed_user_id: str
    eligible: bool
    reason: str
    event_type: EligibilityEventType = EligibilityEventType.ELIGIBILITY_CHANGED
    snapshot: UserEligibilitySnapshot | None = None


@dataclass
class RelationshipEvent:
    """Normalized event shape used inside the deterministic workflow loop."""

    event_type: RelationshipEventType
    reason: str
    event_id: str | None = None
    bypass_approval: bool = False
    consent: ParentalConsent | None = None
    eligibility_update: EligibilityUpdate | None = None
    recompute_consent_required: bool = False


@dataclass
class StateTransition:
    status: ConnectionStatus
    reason: str
    timestamp: str


@dataclass
class ConfigurationChange:
    """One operator-driven configuration field change captured by the workflow."""

    reason: str
    changed_fields: list[str]
    subject_user_id: str | None = None


@dataclass
class TrustedConnectionState:
    """Query response returned to the API/UI from workflow-owned state."""

    workflow_id: str
    requester_user_id: str
    target_user_id: str
    normalized_user_ids: list[str]
    source_channel: SourceChannel
    trigger: str
    are_friends: bool
    parent_child_relationship: bool
    status: ConnectionStatus
    reason: str
    accepted: bool
    consent_required: bool
    consent_status: ConsentStatus | None
    created_at: str
    updated_at: str
    last_eligibility_event_id: str | None
    requester_snapshot: UserEligibilitySnapshot | None = None
    target_snapshot: UserEligibilitySnapshot | None = None
    consent_ttl_seconds: int = 120
    auto_accept: bool = False
    continue_as_new_count: int = 0
    transitions: list[StateTransition] = field(default_factory=list)


@dataclass
class TrustedConnectionContinuation:
    """Private input used when the pair workflow rolls over with Continue-as-New."""

    state: TrustedConnectionState
    eligibility_decision: EligibilityDecision
    consent_timed_out: bool = False
    consent_deadline: str | None = None
    pending_reason: str = "REQUEST_CREATED"
    continue_as_new_count: int = 0


@dataclass
class TCChangeEvent:
    event_id: str
    timestamp: str
    workflow_id: str
    user_id_a: str
    user_id_b: str
    status: ConnectionStatus
    reason: str
    event_type: str = "STATUS_CHANGED"
    changed_fields: list[str] = field(default_factory=list)
    subject_user_id: str | None = None
