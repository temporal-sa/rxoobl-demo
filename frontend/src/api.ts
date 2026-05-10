import type {
  EligibilityEventType,
  SendTrustedFriendPayload,
  TrustedConnectionState,
  WorkflowRuntimeIssue,
  WorkflowRuntimeStatus,
  UserSnapshot,
} from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

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
