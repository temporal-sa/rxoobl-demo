// Frontend types mirror the FastAPI/Temporal DTOs, but use idiomatic camelCase
// only for UI-only concepts. Wire objects intentionally retain snake_case so
// fetch payloads can be serialized without lossy mapping code.
export type ConnectionStatus =
  | "PENDING"
  | "WAITING_FOR_PARENTAL_CONSENT"
  | "TRUSTED"
  | "SUSPENDED"
  | "EXPIRED"
  | "DENIED";

export type SourceChannel =
  | "STANDARD"
  | "SHARE_LINK"
  | "QR_CODE"
  | "QR_CROSS_AGE"
  | "CONTACT_LIST_IMPORTER"
  | "PARENT_CHILD";

export type EligibilityEventType =
  | "ELIGIBILITY_CHANGED"
  | "AGE_CHANGED"
  | "PARENT_CHILD_FORMED"
  | "PARENT_CHILD_REMOVED"
  | "PARENTAL_CONSENT_APPROVED"
  | "PARENTAL_CONSENT_REJECTED";

export interface UserSnapshot {
  // A point-in-time view of user safety attributes. Temporal workflows store
  // snapshots instead of fetching live user records so workflow decisions remain
  // reproducible after retries and replays.
  age: number;
  country_code: string;
  is_age_verified: boolean;
  is_on_watchlist: boolean;
}

export interface SendTrustedFriendPayload {
  // Start payload for the pair workflow. The backend normalizes the two user ids
  // into a deterministic workflow id so duplicate starts target the same pair.
  requester_user_id: string;
  target_user_id: string;
  source_channel: SourceChannel;
  requester_snapshot: UserSnapshot;
  target_snapshot: UserSnapshot;
  consent_ttl_seconds: number;
  are_friends: boolean;
  parent_child_relationship: boolean;
  auto_accept: boolean;
  trigger: string;
  metadata?: Record<string, string>;
}

export interface StateTransition {
  status: ConnectionStatus;
  reason: string;
  timestamp: string;
}

export interface TrustedConnectionState {
  // Query response from TrustedConnectionWorkflow.get_state(). Everything in
  // this object is workflow-owned state, not frontend-derived display state.
  workflow_id: string;
  requester_user_id: string;
  target_user_id: string;
  normalized_user_ids: string[];
  source_channel: SourceChannel;
  trigger: string;
  are_friends: boolean;
  parent_child_relationship: boolean;
  status: ConnectionStatus;
  reason: string;
  accepted: boolean;
  consent_required: boolean;
  consent_status: "APPROVED" | "DENIED" | null;
  created_at: string;
  updated_at: string;
  last_eligibility_event_id: string | null;
  transitions: StateTransition[];
}

export type WorkflowRuntimeStatus =
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "CANCELED"
  | "TERMINATED"
  | "CONTINUED_AS_NEW"
  | "TIMED_OUT"
  | "NOT_FOUND"
  | "UNKNOWN";

export interface WorkflowRuntimeIssue {
  // Runtime issues describe the Temporal execution itself, such as "not found"
  // or "completed", so the UI can distinguish closed workflows from failed API
  // calls.
  code: string;
  workflowId: string;
  executionStatus: WorkflowRuntimeStatus;
  message: string;
}

export interface ScenarioUser {
  id: string;
  label: string;
  snapshot: UserSnapshot;
}

export interface RolePreviewCopy {
  title: string;
  idle: string;
  pending: string;
  waiting: string;
  trusted: string;
  blocked: string;
  detail: string;
}

export interface ApproverPreviewCopy extends RolePreviewCopy {
  notRequired: string;
}

export interface ScenarioRolePreviews {
  requester: RolePreviewCopy;
  approver: ApproverPreviewCopy;
  recipient: RolePreviewCopy;
}

export interface Scenario {
  // Scenarios are executable documentation: each one drives a concrete Temporal
  // workflow path while also supplying the labels and role copy used in the UI.
  id: string;
  title: string;
  navLabel: string;
  group: string;
  objective: string;
  entryPoint: string;
  sourceChannel: SourceChannel;
  requester: ScenarioUser;
  target: ScenarioUser;
  areFriends: boolean;
  parentChildRelationship: boolean;
  autoAccept: boolean;
  consentTtlSeconds: number;
  trigger: string;
  expectedInitialStatus: ConnectionStatus;
  requirements: string[];
  previews: ScenarioRolePreviews;
  actions: ScenarioAction[];
}

export type ScenarioActionKind =
  | "start"
  | "accept"
  | "approveConsent"
  | "denyConsent"
  | "loseEligibility"
  | "restoreEligibility"
  | "ageUp"
  | "removeParentChild"
  | "restoreParentChild"
  | "refresh";

export interface ScenarioAction {
  kind: ScenarioActionKind;
  label: string;
  description: string;
}

export interface DemoEvent {
  // Local-only audit trail for presenter feedback. It is not persisted and does
  // not affect workflow state.
  id: string;
  at: string;
  level: "info" | "success" | "warning" | "error";
  title: string;
  detail: string;
}

export interface RunContext {
  // Runtime ids include a per-run token so each demo click creates fresh user
  // ids while preserving scenario-readable prefixes.
  workflowId: string;
  requesterUserId: string;
  targetUserId: string;
  runToken: string;
}
