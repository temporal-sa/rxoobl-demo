import type {
  EligibilityEventType,
  SendTrustedFriendPayload,
  TrustedConnectionState,
  WorkflowRuntimeIssue,
  WorkflowRuntimeStatus,
  UserSnapshot,
} from "./types";

// The UI is intentionally deployable without rebuilding: Vite can inject a
// Cloud/API URL through VITE_API_BASE_URL, while local development falls back to
// the FastAPI server started on port 8000.
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

// FastAPI returns structured details for workflow lifecycle problems. The UI
// keeps the fields optional because some failures may still be plain text from
// middleware, proxies, or browser-level fetch behavior.
interface ApiErrorDetail {
  code?: string;
  workflow_id?: string;
  execution_status?: string;
  message?: string;
}

export class ApiError extends Error {
  status: number;
  statusText: string;
  detail: unknown;

  constructor(status: number, statusText: string, detail: unknown) {
    super(formatApiErrorMessage(status, statusText, detail));
    this.name = "ApiError";
    this.status = status;
    this.statusText = statusText;
    this.detail = detail;
  }
}

// All API calls pass through this helper so JSON headers, structured error
// parsing, and URL prefixing remain consistent across start, signal, query, and
// event endpoints.
async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    ...options,
    headers: {
      "content-type": "application/json",
      ...(options?.headers ?? {}),
    },
  });

  if (!response.ok) {
    const detail = await readErrorDetail(response);
    throw new ApiError(response.status, response.statusText, detail);
  }

  return response.json() as Promise<T>;
}

// Error bodies can be JSON, FastAPI's {"detail": ...} envelope, empty, or plain
// text. Preserving the original shape lets higher-level code decide whether the
// failure is a workflow state issue or a generic transport problem.
async function readErrorDetail(response: Response) {
  const body = await response.text();
  if (!body) {
    return null;
  }
  try {
    const parsed = JSON.parse(body) as { detail?: unknown };
    return "detail" in parsed ? parsed.detail : parsed;
  } catch {
    return body;
  }
}

// Prefer the server's human-readable message when present, but always fall back
// to status text so rejected promises carry useful debugging context.
function formatApiErrorMessage(status: number, statusText: string, detail: unknown) {
  if (isApiErrorDetail(detail) && detail.message) {
    return detail.message;
  }
  if (typeof detail === "string" && detail.length > 0) {
    return `${status} ${statusText}: ${detail}`;
  }
  return `${status} ${statusText}`;
}

function isApiErrorDetail(detail: unknown): detail is ApiErrorDetail {
  return typeof detail === "object" && detail !== null;
}

export function workflowIssueFromError(
  error: unknown,
  fallbackWorkflowId?: string,
): WorkflowRuntimeIssue | null {
  // A completed or missing Temporal workflow is not the same as a broken UI.
  // Convert those API errors into displayable runtime state so the console can
  // explain why further signals are disabled.
  if (!(error instanceof ApiError) || !isApiErrorDetail(error.detail)) {
    return null;
  }

  const detail = error.detail;
  if (
    detail.code !== "WORKFLOW_NOT_RUNNING" &&
    detail.code !== "WORKFLOW_NOT_FOUND"
  ) {
    return null;
  }

  return {
    code: detail.code,
    workflowId: detail.workflow_id ?? fallbackWorkflowId ?? "unknown",
    executionStatus: normalizeRuntimeStatus(detail.execution_status),
    message: detail.message ?? error.message,
  };
}

export function isQueryBackpressureError(error: unknown): boolean {
  // Temporal Cloud returns transient unavailable/resource-exhausted failures
  // when a workflow's consistent query buffer is full. Signals may still be
  // accepted, so the UI treats query backpressure as a delayed refresh.
  if (!(error instanceof ApiError) || !isApiErrorDetail(error.detail)) {
    return false;
  }
  const detail = error.detail;
  return (
    detail.code === "TEMPORAL_UNAVAILABLE" &&
    typeof detail.message === "string" &&
    detail.message.includes("query")
  );
}

function normalizeRuntimeStatus(status: string | undefined): WorkflowRuntimeStatus {
  // Keep the UI's status union closed even if the server returns an unexpected
  // Temporal enum value from a newer SDK.
  switch (status) {
    case "RUNNING":
    case "COMPLETED":
    case "FAILED":
    case "CANCELED":
    case "TERMINATED":
    case "CONTINUED_AS_NEW":
    case "TIMED_OUT":
    case "NOT_FOUND":
      return status;
    default:
      return "UNKNOWN";
  }
}

export interface StartResponse {
  workflow_id: string;
  status_url: string;
}

export interface EligibilityStartResponse {
  workflow_id: string;
  pair_workflow_id: string;
}

export interface CloseResponse {
  workflow_id: string;
  closed: boolean;
}

export interface SignalAcceptedResponse {
  // Signal endpoints return as soon as Temporal accepts delivery. The workflow
  // may process the signal on a later task, so the UI waits before querying.
  workflow_id: string;
  signal: string;
  accepted: boolean;
  status_url: string;
}

export function startTrustedFriend(payload: SendTrustedFriendPayload) {
  return request<StartResponse>("/trusted-friends/send", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function acceptTrustedFriend(workflowId: string) {
  return request<SignalAcceptedResponse>(`/trusted-friends/${workflowId}/accept`, {
    method: "POST",
  });
}

export function sendParentalConsent(workflowId: string, status: "APPROVED" | "DENIED") {
  return request<SignalAcceptedResponse>(
    `/trusted-friends/${workflowId}/parental-consent`,
    {
      method: "POST",
      body: JSON.stringify({
        // The demo generates a client-side consent id so repeated approvals are
        // visible as separate Temporal signal payloads during presentations.
        consent_id: `consent-${Date.now().toString(36)}`,
        status,
        timestamp: new Date().toISOString(),
      }),
    },
  );
}

export function getTrustedFriend(workflowId: string) {
  return request<TrustedConnectionState>(`/trusted-friends/${workflowId}`);
}

export function closeTrustedFriend(workflowId: string) {
  return request<CloseResponse>(`/trusted-friends/${workflowId}/close`, {
    method: "POST",
  });
}

export function sendEligibilityEvent(payload: {
  eventId: string;
  workflowId: string;
  userIdA: string;
  userIdB: string;
  changedUserId: string;
  eventType: EligibilityEventType;
  snapshot: UserSnapshot;
}) {
  return request<EligibilityStartResponse>("/events/eligibility", {
    method: "POST",
    body: JSON.stringify({
      event_id: payload.eventId,
      user_id_a: payload.userIdA,
      user_id_b: payload.userIdB,
      changed_user_id: payload.changedUserId,
      pair_workflow_id: payload.workflowId,
      event_type: payload.eventType,
      snapshot: payload.snapshot,
    }),
  });
}
