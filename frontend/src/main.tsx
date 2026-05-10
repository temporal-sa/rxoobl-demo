import React, { useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock3,
  GitBranch,
  HeartHandshake,
  Inbox,
  Link2,
  Loader2,
  RefreshCw,
  Send,
  Shield,
  ShieldAlert,
  Timer,
  Users,
  XCircle,
} from "lucide-react";
import {
  acceptTrustedFriend,
  closeTrustedFriend,
  getTrustedFriend,
  isQueryBackpressureError,
  sendEligibilityEvent,
  sendParentalConsent,
  startTrustedFriend,
  workflowIssueFromError,
} from "./api";
import { scenarios } from "./scenarios";
import type {
  ConnectionStatus,
  DemoEvent,
  RunContext,
  Scenario,
  ScenarioAction,
  ScenarioActionKind,
  ScenarioRolePreviews,
  TrustedConnectionState,
  UserSnapshot,
  WorkflowRuntimeIssue,
} from "./types";
import "./styles.css";

const statusMeta: Record<
  ConnectionStatus,
  { label: string; tone: string; Icon: typeof Clock3 }
> = {
  PENDING: { label: "Pending", tone: "pending", Icon: Clock3 },
  WAITING_FOR_PARENTAL_CONSENT: {
    label: "Waiting for VPC",
    tone: "waiting",
    Icon: Timer,
  },
  TRUSTED: { label: "Trusted", tone: "trusted", Icon: CheckCircle2 },
  SUSPENDED: { label: "Suspended", tone: "suspended", Icon: ShieldAlert },
  EXPIRED: { label: "Expired", tone: "denied", Icon: XCircle },
  DENIED: { label: "Denied", tone: "denied", Icon: XCircle },
};

const actionIcons: Record<ScenarioActionKind, typeof Activity> = {
  start: GitBranch,
  accept: CheckCircle2,
  approveConsent: Shield,
  denyConsent: XCircle,
  loseEligibility: AlertTriangle,
  restoreEligibility: RefreshCw,
  ageUp: Timer,
  removeParentChild: ShieldAlert,
  restoreParentChild: HeartHandshake,
  refresh: RefreshCw,
};

const WORKFLOW_QUERY_INTERVAL_MS = 10000;
const INITIAL_QUERY_RETRY_DELAYS_MS = [
  WORKFLOW_QUERY_INTERVAL_MS,
  WORKFLOW_QUERY_INTERVAL_MS,
  WORKFLOW_QUERY_INTERVAL_MS,
];

interface RuntimeUserSnapshots {
  requester: UserSnapshot;
  target: UserSnapshot;
}

interface WorkflowRunRecord {
  workflowId: string;
  scenarioId: string;
  context: RunContext;
  state: TrustedConnectionState | null;
  runtimeSnapshots: RuntimeUserSnapshots;
  workflowIssue: WorkflowRuntimeIssue | null;
  createdAt: string;
  closedAt: string | null;
}

function App() {
  const [selectedId, setSelectedId] = useState(scenarios[0].id);
  const [state, setState] = useState<TrustedConnectionState | null>(null);
  const [runContext, setRunContext] = useState<RunContext | null>(null);
  const [workflowIssue, setWorkflowIssue] = useState<WorkflowRuntimeIssue | null>(null);
  const [events, setEvents] = useState<DemoEvent[]>([]);
  const [busyAction, setBusyAction] = useState<ScenarioActionKind | null>(null);
  const [busyWorkflowControl, setBusyWorkflowControl] = useState<"close" | "new" | null>(null);
  const [runtimeSnapshots, setRuntimeSnapshots] = useState<RuntimeUserSnapshots | null>(null);
  const [workflowRuns, setWorkflowRuns] = useState<WorkflowRunRecord[]>([]);
  const lastWorkflowQueryAt = useRef<Map<string, number>>(new Map());

  const scenario = useMemo(
    () => scenarios.find((item) => item.id === selectedId) ?? scenarios[0],
    [selectedId],
  );

  const scenarioWorkflowRuns = useMemo(
    () => workflowRuns.filter((record) => record.scenarioId === scenario.id),
    [scenario.id, workflowRuns],
  );

  function appendEvent(event: Omit<DemoEvent, "id" | "at">) {
    setEvents((current) => [
      {
        id: crypto.randomUUID(),
        at: new Date().toLocaleTimeString(),
        ...event,
      },
      ...current,
    ].slice(0, 12));
  }

  function selectScenario(nextId: string) {
    const nextScenario = scenarios.find((item) => item.id === nextId);
    const nextRun =
      workflowRuns.find((record) => record.scenarioId === nextId && !record.closedAt) ??
      workflowRuns.find((record) => record.scenarioId === nextId);

    setSelectedId(nextId);
    if (nextRun) {
      applyWorkflowRun(nextRun);
    } else {
      setState(null);
      setRunContext(null);
      setWorkflowIssue(null);
      setRuntimeSnapshots(null);
    }
    appendEvent({
      level: "info",
      title: "Scenario selected",
      detail: nextScenario?.title ?? nextId,
    });
  }

  function applyWorkflowRun(record: WorkflowRunRecord) {
    setRunContext(record.context);
    setState(record.state);
    setWorkflowIssue(record.workflowIssue);
    setRuntimeSnapshots(record.runtimeSnapshots);
  }

  function updateWorkflowRun(
    workflowId: string,
    update: Partial<WorkflowRunRecord>,
  ) {
    setWorkflowRuns((current) =>
      current.map((record) =>
        record.workflowId === workflowId ? { ...record, ...update } : record,
      ),
    );
  }

  function updateActiveWorkflowRun(update: Partial<WorkflowRunRecord>) {
    if (!runContext) {
      return;
    }
    updateWorkflowRun(runContext.workflowId, update);
  }

  async function startScenarioRun(forceNew: boolean) {
    if (runContext && !forceNew && !workflowIssue) {
      appendEvent({
        level: "info",
        title: "Active workflow reused",
        detail: runContext.workflowId,
      });
      await refreshState(runContext.workflowId);
      return;
    }

    setWorkflowIssue(null);
    const token = `${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 8)}`;
    const requesterUserId = `${scenario.requester.id}-${token}`;
    const targetUserId = `${scenario.target.id}-${token}`;
    const response = await startTrustedFriend({
      requester_user_id: requesterUserId,
      target_user_id: targetUserId,
      source_channel: scenario.sourceChannel,
      requester_snapshot: scenario.requester.snapshot,
      target_snapshot: scenario.target.snapshot,
      consent_ttl_seconds: scenario.consentTtlSeconds,
      are_friends: scenario.areFriends,
      parent_child_relationship: scenario.parentChildRelationship,
      auto_accept: scenario.autoAccept,
      trigger: scenario.trigger,
      metadata: {
        scenario_id: scenario.id,
        entry_point: scenario.entryPoint,
      },
    });
    const nextContext = {
      workflowId: response.workflow_id,
      requesterUserId,
      targetUserId,
      runToken: token,
    };
    const nextSnapshots = {
      requester: scenario.requester.snapshot,
      target: scenario.target.snapshot,
    };
    setRunContext(nextContext);
    setState(null);
    setRuntimeSnapshots(nextSnapshots);
    setWorkflowRuns((current) => [
      {
        workflowId: response.workflow_id,
        scenarioId: scenario.id,
        context: nextContext,
        state: null,
        runtimeSnapshots: nextSnapshots,
        workflowIssue: null,
        createdAt: new Date().toISOString(),
        closedAt: null,
      },
      ...current,
    ]);
    appendEvent({
      level: "info",
      title: forceNew ? "New active workflow accepted" : "Workflow accepted",
      detail: response.workflow_id,
    });
    try {
      await refreshInitialState(response.workflow_id);
      appendEvent({
        level: "success",
        title: forceNew ? "New active workflow ready" : "Workflow ready",
        detail: response.workflow_id,
      });
    } catch (error) {
      const issue = workflowIssueFromError(error, response.workflow_id);
      if (issue) {
        setWorkflowIssue(issue);
        updateWorkflowRun(response.workflow_id, { workflowIssue: issue });
      }
      appendEvent({
        level: "warning",
        title: "Initial query unavailable",
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  }

  async function createNewActiveWorkflow() {
    setBusyWorkflowControl("new");
    try {
      await startScenarioRun(true);
    } catch (error) {
      handleActionError(error);
    } finally {
      setBusyWorkflowControl(null);
    }
  }

  async function closeActiveWorkflow() {
    if (!runContext) {
      appendEvent({
        level: "warning",
        title: "No active workflow",
        detail: "Start or activate a workflow before closing it.",
      });
      return;
    }

    setBusyWorkflowControl("close");
    try {
      await closeTrustedFriend(runContext.workflowId);
      const issue: WorkflowRuntimeIssue = {
        code: "WORKFLOW_CLOSED",
        workflowId: runContext.workflowId,
        executionStatus: "COMPLETED",
        message: "Close signal sent; the trusted connection workflow completed.",
      };
      setWorkflowIssue(issue);
      updateActiveWorkflowRun({
        workflowIssue: issue,
        closedAt: new Date().toISOString(),
      });
      appendEvent({
        level: "success",
        title: "Close signal sent",
        detail: runContext.workflowId,
      });
    } catch (error) {
      handleActionError(error);
    } finally {
      setBusyWorkflowControl(null);
    }
  }

  async function setActiveWorkflow(workflowId: string) {
    const record = workflowRuns.find((item) => item.workflowId === workflowId);
    if (!record) {
      return;
    }
    const nextScenario = scenarios.find((item) => item.id === record.scenarioId);
    setSelectedId(record.scenarioId);
    applyWorkflowRun(record);
    appendEvent({
      level: "info",
      title: "Active workflow changed",
      detail: `${nextScenario?.navLabel ?? record.scenarioId} / ${record.workflowId}`,
    });
    if (!record.closedAt && !record.workflowIssue) {
      await refreshState(record.workflowId);
    }
  }

  async function runAction(action: ScenarioAction) {
    setBusyAction(action.kind);
    try {
      if (action.kind === "start") {
        await startScenarioRun(false);
        return;
      }

      if (!runContext) {
        appendEvent({
          level: "warning",
          title: "Start required",
          detail: "Start this scenario before sending signals or events.",
        });
        return;
      }

      if (action.kind === "accept") {
        await acceptTrustedFriend(runContext.workflowId);
        appendEvent({
          level: "success",
          title: "Signal sent",
          detail: "AcceptTrustedFriend -> accept()",
        });
        await refreshStateAfterAcceptedCommand(runContext.workflowId);
        return;
      }

      if (action.kind === "approveConsent" || action.kind === "denyConsent") {
        await sendParentalConsent(
          runContext.workflowId,
          action.kind === "approveConsent" ? "APPROVED" : "DENIED",
        );
        appendEvent({
          level: action.kind === "approveConsent" ? "success" : "warning",
          title: "Parental consent signal",
          detail: `ParentalConsentSignal -> ${
            action.kind === "approveConsent" ? "APPROVED" : "DENIED"
          }`,
        });
        await refreshStateAfterAcceptedCommand(runContext.workflowId);
        return;
      }

      if (action.kind === "refresh") {
        await refreshState(runContext.workflowId);
        return;
      }

      const changedSnapshot = await sendScenarioEvent(action.kind, scenario, runContext);
      if (changedSnapshot) {
        const nextSnapshots = {
          requester: changedSnapshot,
          target: runtimeSnapshots?.target ?? scenario.target.snapshot,
        };
        setRuntimeSnapshots(nextSnapshots);
        updateActiveWorkflowRun({ runtimeSnapshots: nextSnapshots });
      }
      appendEvent({
        level: "info",
        title: "Eligibility workflow started",
        detail: `${action.kind} -> eligibility-eval-${scenario.id}`,
      });
      await refreshStateAfterAcceptedCommand(runContext.workflowId);
    } catch (error) {
      handleActionError(error);
    } finally {
      setBusyAction(null);
    }
  }

  async function refreshState(workflowId: string) {
    const now = Date.now();
    const lastQueryAt = lastWorkflowQueryAt.current.get(workflowId) ?? 0;
    const remainingDelay = WORKFLOW_QUERY_INTERVAL_MS - (now - lastQueryAt);
    if (remainingDelay > 0) {
      await delay(remainingDelay);
    }
    lastWorkflowQueryAt.current.set(workflowId, Date.now());

    const nextState = await getTrustedFriend(workflowId);
    setState(nextState);
    setWorkflowIssue(null);
    updateWorkflowRun(workflowId, {
      state: nextState,
      workflowIssue: null,
    });
    appendEvent({
      level: "info",
      title: "Workflow queried",
      detail: `${nextState.status} / ${nextState.reason}`,
    });
  }

  async function refreshStateAfterAcceptedCommand(workflowId: string) {
    await delay(WORKFLOW_QUERY_INTERVAL_MS);
    try {
      await refreshState(workflowId);
    } catch (error) {
      if (!isQueryBackpressureError(error)) {
        throw error;
      }
      appendEvent({
        level: "warning",
        title: "State refresh delayed",
        detail: "Signal was accepted; Temporal query is still backpressured.",
      });
    }
  }

  async function refreshInitialState(workflowId: string) {
    let lastError: unknown = null;
    for (const retryDelay of INITIAL_QUERY_RETRY_DELAYS_MS) {
      await delay(retryDelay);
      try {
        await refreshState(workflowId);
        return;
      } catch (error) {
        lastError = error;
        if (workflowIssueFromError(error, workflowId)) {
          throw error;
        }
      }
    }

    throw lastError ?? new Error("Initial workflow state was not queryable.");
  }

  function handleActionError(error: unknown) {
    const issue = workflowIssueFromError(error, runContext?.workflowId);
    if (issue) {
      setWorkflowIssue(issue);
      updateWorkflowRun(issue.workflowId, { workflowIssue: issue });
      appendEvent({
        level: issue.executionStatus === "NOT_FOUND" ? "error" : "warning",
        title: "Workflow unavailable",
        detail: `${issue.workflowId} / ${issue.executionStatus}`,
      });
      return;
    }

    appendEvent({
      level: "error",
      title: "Action failed",
      detail: error instanceof Error ? error.message : String(error),
    });
  }

  return (
    <main className="app-shell">
      <TopBar state={state} workflowIssue={workflowIssue} />
      <section className="workspace">
        <ScenarioRail
          selectedId={selectedId}
          onSelect={selectScenario}
          activeStatus={state?.status ?? null}
        />
        <FlowCanvas
          scenario={scenario}
          state={state}
          runContext={runContext}
          workflowIssue={workflowIssue}
          busyAction={busyAction}
          onRunAction={runAction}
        />
        <Inspector
          scenario={scenario}
          state={state}
          runContext={runContext}
          workflowIssue={workflowIssue}
          busyAction={busyAction}
          busyWorkflowControl={busyWorkflowControl}
          runtimeSnapshots={runtimeSnapshots}
          workflowRuns={scenarioWorkflowRuns}
          onCloseActiveWorkflow={closeActiveWorkflow}
          onCreateNewWorkflow={createNewActiveWorkflow}
          onSetActiveWorkflow={setActiveWorkflow}
          onRunAction={runAction}
        />
      </section>
      <EventStream events={events} />
    </main>
  );
}

function TopBar({
  state,
  workflowIssue,
}: {
  state: TrustedConnectionState | null;
  workflowIssue: WorkflowRuntimeIssue | null;
}) {
  return (
    <header className="top-bar">
      <div className="brand-mark">
        <Shield size={22} />
      </div>
      <div>
        <h1>Trusted Friends Temporal Demo</h1>
        <p>Workflow state, timers, signals, and async eligibility processors in one console.</p>
      </div>
      <div className="server-status">
        <span className="pulse" />
        FastAPI :8000
      </div>
      <div className={`server-status secondary ${workflowIssue ? "closed" : ""}`}>
        <Activity size={16} />
        {workflowIssue
          ? `${workflowIssue.executionStatus}: ${workflowIssue.workflowId}`
          : state
            ? state.workflow_id
            : "Temporal workflow idle"}
      </div>
    </header>
  );
}

function ScenarioRail({
  selectedId,
  activeStatus,
  onSelect,
}: {
  selectedId: string;
  activeStatus: ConnectionStatus | null;
  onSelect: (id: string) => void;
}) {
  return (
    <aside className="scenario-rail">
      <div className="rail-heading">
        <span>Trusted Friend Flows</span>
        <strong>{scenarios.length}</strong>
      </div>
      {scenarios.map((scenario) => {
        const selected = scenario.id === selectedId;
        return (
          <button
            key={scenario.id}
            className={`scenario-tab ${selected ? "selected" : ""}`}
            onClick={() => onSelect(scenario.id)}
          >
            <span className="scenario-title">{scenario.navLabel}</span>
            <span className="scenario-group">{scenario.group}</span>
            {selected && activeStatus ? <StatusPill status={activeStatus} /> : null}
          </button>
        );
      })}
    </aside>
  );
}

function FlowCanvas({
  scenario,
  state,
  runContext,
  workflowIssue,
  busyAction,
  onRunAction,
}: {
  scenario: Scenario;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  busyAction: ScenarioActionKind | null;
  onRunAction: (action: ScenarioAction) => void;
}) {
  const displayedTransitions = state?.transitions?.length
    ? state.transitions
    : [{ status: scenario.expectedInitialStatus, reason: "Expected first state", timestamp: "" }];

  return (
    <section className="flow-canvas">
      <div className="canvas-header">
        <div>
          <h2>{scenario.title}</h2>
          <p>{scenario.objective}</p>
        </div>
        <StatusPill status={state?.status ?? scenario.expectedInitialStatus} />
      </div>

      {workflowIssue ? <WorkflowRuntimeAlert issue={workflowIssue} /> : null}

      <div className="flow-grid">
        <InfoBlock label="Entry point" value={scenario.entryPoint} icon={<GitBranch />} />
        <InfoBlock label="Source channel" value={scenario.sourceChannel} icon={<Link2 />} />
        <InfoBlock label="Friendship prerequisite" value={scenario.areFriends ? "Required / satisfied" : "Not required"} icon={<Users />} />
        <InfoBlock label="Temporal owner" value={runContext?.workflowId ?? "Not started"} icon={<Activity />} />
      </div>

      <RolePreviewDeck
        scenario={scenario}
        state={state}
        runContext={runContext}
        workflowIssue={workflowIssue}
        busyAction={busyAction}
        onRunAction={onRunAction}
      />

      <div className="timeline-panel">
        <div className="panel-title">
          <span>Workflow timeline</span>
          <small>query get_state()</small>
        </div>
        <div className="timeline">
          {displayedTransitions.map((transition, index) => (
            <div className="timeline-row" key={`${transition.status}-${index}`}>
              <div className="timeline-node" />
              <div>
                <StatusPill status={transition.status as ConnectionStatus} />
                <strong>{transition.reason}</strong>
                <span>{transition.timestamp ? shortTime(transition.timestamp) : "awaiting workflow start"}</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      <section className="requirements">
        <div className="panel-title">
          <span>Requirement coverage</span>
          <small>visible demo contract</small>
        </div>
        <div className="requirement-list">
          {scenario.requirements.map((requirement) => (
            <div className="requirement-row" key={requirement}>
              <CheckCircle2 size={16} />
              <span>{requirement}</span>
            </div>
          ))}
        </div>
      </section>
    </section>
  );
}

function WorkflowRuntimeAlert({ issue }: { issue: WorkflowRuntimeIssue }) {
  return (
    <div className="runtime-alert">
      <AlertTriangle size={18} />
      <strong>{issue.executionStatus}</strong>
      <span>{issue.message}</span>
    </div>
  );
}

function InfoBlock({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
}) {
  return (
    <div className="info-block">
      <div className="info-icon">{icon}</div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

type RolePreviewRole = keyof ScenarioRolePreviews;
type RolePreviewTone =
  | "idle"
  | "pending"
  | "waiting"
  | "trusted"
  | "blocked"
  | "neutral"
  | "unavailable";
type RoleActionVariant = "primary" | "secondary" | "danger";

interface RolePreviewAction {
  action: ScenarioAction;
  disabled: boolean;
  loading: boolean;
  variant: RoleActionVariant;
}

interface RolePreviewModel {
  role: RolePreviewRole;
  title: string;
  userLabel: string;
  runtimeId?: string;
  body: string;
  detail: string;
  statusLabel: string;
  tone: RolePreviewTone;
  Icon: typeof Shield;
  actions: RolePreviewAction[];
}

function RolePreviewDeck({
  scenario,
  state,
  runContext,
  workflowIssue,
  busyAction,
  onRunAction,
}: {
  scenario: Scenario;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  busyAction: ScenarioActionKind | null;
  onRunAction: (action: ScenarioAction) => void;
}) {
  const workflowUnavailable = Boolean(
    workflowIssue && runContext && workflowIssue.workflowId === runContext.workflowId,
  );
  const startAction = getScenarioAction(scenario, "start");
  const acceptAction = getScenarioAction(scenario, "accept");
  const approveAction = getScenarioAction(scenario, "approveConsent");
  const denyAction = getScenarioAction(scenario, "denyConsent");
  const actionLocked = busyAction !== null || workflowUnavailable;
  const currentStatus = state?.status ?? scenario.expectedInitialStatus;
  const approvalRequired = Boolean(state?.consent_required) || Boolean(approveAction || denyAction);
  const parentAuthority = scenario.parentChildRelationship || scenario.sourceChannel === "PARENT_CHILD";
  const canActOnWorkflow = Boolean(runContext) && !workflowUnavailable && !isClosedStatus(state?.status);
  const canAccept = Boolean(
    acceptAction && canActOnWorkflow && !state?.accepted && state?.consent_status !== "DENIED",
  );
  const canApprove = Boolean(
    approveAction && canActOnWorkflow && state?.consent_status !== "APPROVED" && state?.consent_status !== "DENIED",
  );
  const canDeny = Boolean(
    denyAction && canActOnWorkflow && state?.consent_status !== "APPROVED" && state?.consent_status !== "DENIED",
  );

  const models: RolePreviewModel[] = [
    {
      role: "requester",
      title: scenario.previews.requester.title,
      userLabel: scenario.requester.label,
      runtimeId: runContext?.requesterUserId,
      body: previewBody(scenario.previews.requester, scenario, state, runContext, workflowIssue),
      detail: scenario.previews.requester.detail,
      statusLabel: previewStatusLabel(scenario, state, runContext, workflowIssue),
      tone: previewTone(scenario, state, runContext, workflowIssue),
      Icon: Send,
      actions:
        startAction && !runContext
          ? [
              {
                action: startAction,
                disabled: actionLocked,
                loading: busyAction === startAction.kind,
                variant: "primary",
              },
            ]
          : [],
    },
    {
      role: "approver",
      title: scenario.previews.approver.title,
      userLabel: approvalRequired ? "Parent / approver" : parentAuthority ? scenario.requester.label : "Approver",
      runtimeId: parentAuthority ? runContext?.requesterUserId : undefined,
      body: approverBody({
        copy: scenario.previews.approver,
        scenario,
        state,
        runContext,
        workflowIssue,
        approvalRequired,
        parentAuthority,
      }),
      detail: scenario.previews.approver.detail,
      statusLabel: approverStatusLabel({
        approvalRequired,
        parentAuthority,
        state,
        runContext,
        workflowIssue,
        currentStatus,
      }),
      tone:
        approvalRequired || parentAuthority || workflowIssue
          ? previewTone(scenario, state, runContext, workflowIssue)
          : "neutral",
      Icon: Shield,
      actions:
        approvalRequired && (approveAction || denyAction)
          ? [
              ...(approveAction
                ? [
                    {
                      action: approveAction,
                      disabled: actionLocked || !canApprove,
                      loading: busyAction === approveAction.kind,
                      variant: "primary" as const,
                    },
                  ]
                : []),
              ...(denyAction
                ? [
                    {
                      action: denyAction,
                      disabled: actionLocked || !canDeny,
                      loading: busyAction === denyAction.kind,
                      variant: "danger" as const,
                    },
                  ]
                : []),
            ]
          : [],
    },
    {
      role: "recipient",
      title: scenario.previews.recipient.title,
      userLabel: scenario.target.label,
      runtimeId: runContext?.targetUserId,
      body: previewBody(scenario.previews.recipient, scenario, state, runContext, workflowIssue),
      detail: scenario.previews.recipient.detail,
      statusLabel:
        canAccept && currentStatus === "PENDING"
          ? "Ready to accept"
          : previewStatusLabel(scenario, state, runContext, workflowIssue),
      tone: previewTone(scenario, state, runContext, workflowIssue),
      Icon: Inbox,
      actions:
        acceptAction && !state?.accepted
          ? [
              {
                action: acceptAction,
                disabled: actionLocked || !canAccept,
                loading: busyAction === acceptAction.kind,
                variant: "primary",
              },
            ]
          : [],
    },
  ];

  return (
    <section className="role-preview-section">
      <div className="panel-title">
        <span>Role previews</span>
        <small>requester / approver / recipient</small>
      </div>
      <div className="role-preview-grid">
        {models.map((model) => (
          <RolePreviewPane key={model.role} model={model} onRunAction={onRunAction} />
        ))}
      </div>
    </section>
  );
}

function RolePreviewPane({
  model,
  onRunAction,
}: {
  model: RolePreviewModel;
  onRunAction: (action: ScenarioAction) => void;
}) {
  const Icon = model.Icon;
  return (
    <article className={`role-preview-card ${model.role}`}>
      <div className="role-device-bar">
        <span className="role-device-dot" />
        <span className="role-device-dot" />
        <span className="role-device-dot" />
      </div>
      <div className="role-preview-header">
        <div className="role-avatar">
          <Icon size={18} />
        </div>
        <div>
          <strong>{model.userLabel}</strong>
          <span>{model.runtimeId ?? "demo preview"}</span>
        </div>
      </div>
      <div className={`role-state ${model.tone}`}>
        <span>{model.statusLabel}</span>
      </div>
      <h3>{model.title}</h3>
      <p>{model.body}</p>
      <small>{model.detail}</small>
      {model.actions.length > 0 ? (
        <div className="role-actions">
          {model.actions.map((item) => {
            const ActionIcon = actionIcons[item.action.kind];
            return (
              <button
                key={item.action.kind}
                className={`role-action ${item.variant}`}
                disabled={item.disabled}
                onClick={() => onRunAction(item.action)}
                title={item.action.description}
              >
                {item.loading ? <Loader2 className="spin" /> : <ActionIcon />}
                <span>{item.action.label}</span>
              </button>
            );
          })}
        </div>
      ) : null}
    </article>
  );
}

function getScenarioAction(
  scenario: Scenario,
  kind: ScenarioActionKind,
): ScenarioAction | undefined {
  return scenario.actions.find((action) => action.kind === kind);
}

function previewBody(
  copy: ScenarioRolePreviews[RolePreviewRole],
  scenario: Scenario,
  state: TrustedConnectionState | null,
  runContext: RunContext | null,
  workflowIssue: WorkflowRuntimeIssue | null,
) {
  if (workflowIssue) {
    return copy.blocked;
  }
  if (!runContext) {
    return copy.idle;
  }

  switch (state?.status ?? scenario.expectedInitialStatus) {
    case "TRUSTED":
      return copy.trusted;
    case "WAITING_FOR_PARENTAL_CONSENT":
      return copy.waiting;
    case "PENDING":
      return copy.pending;
    case "SUSPENDED":
    case "EXPIRED":
    case "DENIED":
      return copy.blocked;
  }
}

function approverBody({
  copy,
  scenario,
  state,
  runContext,
  workflowIssue,
  approvalRequired,
  parentAuthority,
}: {
  copy: ScenarioRolePreviews["approver"];
  scenario: Scenario;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  approvalRequired: boolean;
  parentAuthority: boolean;
}) {
  if (workflowIssue) {
    return copy.blocked;
  }
  if (!approvalRequired && !parentAuthority) {
    return copy.notRequired;
  }
  if (!runContext) {
    return copy.idle;
  }
  if (approvalRequired && state?.consent_status === "APPROVED") {
    return copy.trusted;
  }
  if (approvalRequired && state?.consent_status === "DENIED") {
    return copy.blocked;
  }
  return previewBody(copy, scenario, state, runContext, workflowIssue);
}

function previewTone(
  scenario: Scenario,
  state: TrustedConnectionState | null,
  runContext: RunContext | null,
  workflowIssue: WorkflowRuntimeIssue | null,
): RolePreviewTone {
  if (workflowIssue) {
    return "unavailable";
  }
  if (!runContext) {
    return "idle";
  }

  switch (state?.status ?? scenario.expectedInitialStatus) {
    case "TRUSTED":
      return "trusted";
    case "WAITING_FOR_PARENTAL_CONSENT":
      return "waiting";
    case "PENDING":
      return "pending";
    case "SUSPENDED":
    case "EXPIRED":
    case "DENIED":
      return "blocked";
  }
}

function previewStatusLabel(
  scenario: Scenario,
  state: TrustedConnectionState | null,
  runContext: RunContext | null,
  workflowIssue: WorkflowRuntimeIssue | null,
) {
  if (workflowIssue) {
    return "Workflow unavailable";
  }
  if (!runContext) {
    return "Preview idle";
  }
  return statusMeta[state?.status ?? scenario.expectedInitialStatus].label;
}

function approverStatusLabel({
  approvalRequired,
  parentAuthority,
  state,
  runContext,
  workflowIssue,
  currentStatus,
}: {
  approvalRequired: boolean;
  parentAuthority: boolean;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  currentStatus: ConnectionStatus;
}) {
  if (workflowIssue) {
    return "Workflow unavailable";
  }
  if (!approvalRequired && !parentAuthority) {
    return "No approval required";
  }
  if (!runContext) {
    return "Preview idle";
  }
  if (parentAuthority && !approvalRequired) {
    return currentStatus === "SUSPENDED" ? "Family link missing" : "Family authority";
  }
  if (state?.consent_status === "APPROVED") {
    return "Consent approved";
  }
  if (state?.consent_status === "DENIED") {
    return "Consent denied";
  }
  return currentStatus === "WAITING_FOR_PARENTAL_CONSENT"
    ? "Approval pending"
    : statusMeta[currentStatus].label;
}

function isClosedStatus(status: ConnectionStatus | undefined) {
  return status === "SUSPENDED" || status === "EXPIRED" || status === "DENIED";
}

function WorkflowManager({
  runContext,
  workflowIssue,
  workflowRuns,
  busyWorkflowControl,
  onCloseActiveWorkflow,
  onCreateNewWorkflow,
  onSetActiveWorkflow,
}: {
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  workflowRuns: WorkflowRunRecord[];
  busyWorkflowControl: "close" | "new" | null;
  onCloseActiveWorkflow: () => void;
  onCreateNewWorkflow: () => void;
  onSetActiveWorkflow: (workflowId: string) => void;
}) {
  const activeWorkflowId = runContext?.workflowId ?? null;
  const activeClosed = Boolean(
    workflowIssue && activeWorkflowId && workflowIssue.workflowId === activeWorkflowId,
  );

  return (
    <section className="inspector-section workflow-manager">
      <div className="panel-title">
        <span>Active workflow</span>
        <small>close / create / switch</small>
      </div>
      <div className={`active-workflow-card ${activeClosed ? "closed" : ""}`}>
        <span>{activeWorkflowId ? "Current trusted connection" : "No workflow active"}</span>
        <strong>{activeWorkflowId ?? "Start a scenario or create a run"}</strong>
        {workflowIssue && activeWorkflowId ? (
          <small>{workflowIssue.executionStatus}: {workflowIssue.message}</small>
        ) : null}
      </div>
      <div className="workflow-control-grid">
        <button
          className="workflow-control-button danger"
          disabled={!activeWorkflowId || activeClosed || busyWorkflowControl !== null}
          onClick={onCloseActiveWorkflow}
          title="Send the Temporal close signal to the active trusted connection workflow."
        >
          {busyWorkflowControl === "close" ? <Loader2 className="spin" /> : <XCircle />}
          <span>Close active</span>
        </button>
        <button
          className="workflow-control-button primary"
          disabled={busyWorkflowControl !== null}
          onClick={onCreateNewWorkflow}
          title="Create a new trusted connection workflow run and make it active."
        >
          {busyWorkflowControl === "new" ? <Loader2 className="spin" /> : <GitBranch />}
          <span>New active</span>
        </button>
      </div>
      <div className="workflow-run-list">
        {workflowRuns.length === 0 ? (
          <div className="empty-workflow-list">No workflow runs for this scenario yet.</div>
        ) : (
          workflowRuns.map((record) => (
            <button
              key={record.workflowId}
              className={`workflow-run-row ${record.workflowId === activeWorkflowId ? "active" : ""}`}
              disabled={record.workflowId === activeWorkflowId || busyWorkflowControl !== null}
              onClick={() => onSetActiveWorkflow(record.workflowId)}
              title="Set this trusted connection workflow as the active run."
            >
              <span>
                <strong>{shortWorkflowId(record.workflowId)}</strong>
                <small>{shortTime(record.createdAt)}</small>
              </span>
              {record.closedAt || record.workflowIssue ? (
                <em>{record.workflowIssue?.executionStatus ?? "Closed"}</em>
              ) : record.state ? (
                <StatusPill status={record.state.status} />
              ) : (
                <em>Started</em>
              )}
            </button>
          ))
        )}
      </div>
    </section>
  );
}

function Inspector({
  scenario,
  state,
  runContext,
  workflowIssue,
  busyAction,
  busyWorkflowControl,
  runtimeSnapshots,
  workflowRuns,
  onCloseActiveWorkflow,
  onCreateNewWorkflow,
  onSetActiveWorkflow,
  onRunAction,
}: {
  scenario: Scenario;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  busyAction: ScenarioActionKind | null;
  busyWorkflowControl: "close" | "new" | null;
  runtimeSnapshots: RuntimeUserSnapshots | null;
  workflowRuns: WorkflowRunRecord[];
  onCloseActiveWorkflow: () => void;
  onCreateNewWorkflow: () => void;
  onSetActiveWorkflow: (workflowId: string) => void;
  onRunAction: (action: ScenarioAction) => void;
}) {
  const actions = [...scenario.actions, { kind: "refresh", label: "Refresh query", description: "Query workflow state." } as ScenarioAction];
  const workflowUnavailable = Boolean(
    workflowIssue && runContext && workflowIssue.workflowId === runContext.workflowId,
  );
  return (
    <aside className="inspector">
      <WorkflowManager
        runContext={runContext}
        workflowIssue={workflowIssue}
        workflowRuns={workflowRuns}
        busyWorkflowControl={busyWorkflowControl}
        onCloseActiveWorkflow={onCloseActiveWorkflow}
        onCreateNewWorkflow={onCreateNewWorkflow}
        onSetActiveWorkflow={onSetActiveWorkflow}
      />

      <section className="inspector-section">
        <div className="panel-title">
          <span>Actions</span>
          <small>start / signal / event</small>
        </div>
        <div className="action-stack">
          {actions.map((action) => {
            const Icon = actionIcons[action.kind];
            const disabled = action.kind !== "start" && (!runContext || workflowUnavailable);
            return (
              <button
                key={action.kind}
                className="action-button"
                disabled={disabled || busyAction !== null}
                onClick={() => onRunAction(action)}
                title={action.description}
              >
                {busyAction === action.kind ? <Loader2 className="spin" /> : <Icon />}
                <span>{action.label}</span>
              </button>
            );
          })}
        </div>
      </section>

      <section className="inspector-section">
        <div className="panel-title">
          <span>Current state</span>
          <small>Temporal query</small>
        </div>
        <div className="state-card">
          <StatusPill status={state?.status ?? scenario.expectedInitialStatus} />
          <dl>
            <div>
              <dt>Reason</dt>
              <dd>{state?.reason ?? "not started"}</dd>
            </div>
            <div>
              <dt>Accepted</dt>
              <dd>{state ? String(state.accepted) : String(scenario.autoAccept)}</dd>
            </div>
            <div>
              <dt>Consent</dt>
              <dd>{state?.consent_status ?? (state?.consent_required ? "required" : "not required")}</dd>
            </div>
            <div>
              <dt>Last event</dt>
              <dd>{state?.last_eligibility_event_id ?? "none"}</dd>
            </div>
            <div>
              <dt>Runtime</dt>
              <dd>{workflowIssue?.executionStatus ?? (runContext ? "RUNNING" : "not started")}</dd>
            </div>
          </dl>
        </div>
      </section>

      <section className="inspector-section">
        <div className="panel-title">
          <span>Users</span>
          <small>demo snapshots</small>
        </div>
        <UserCard
          user={scenario.requester}
          runtimeId={runContext?.requesterUserId}
          snapshot={runtimeSnapshots?.requester ?? scenario.requester.snapshot}
        />
        <UserCard
          user={scenario.target}
          runtimeId={runContext?.targetUserId}
          snapshot={runtimeSnapshots?.target ?? scenario.target.snapshot}
        />
      </section>
    </aside>
  );
}

function UserCard({
  user,
  runtimeId,
  snapshot,
}: {
  user: Scenario["requester"];
  runtimeId?: string;
  snapshot: UserSnapshot;
}) {
  return (
    <div className="user-card">
      <div>
        <strong>{user.label}</strong>
        <span>{runtimeId ?? user.id}</span>
      </div>
      <SnapshotMeter snapshot={snapshot} />
    </div>
  );
}

function SnapshotMeter({ snapshot }: { snapshot: UserSnapshot }) {
  return (
    <div className="snapshot-meter">
      <span>{snapshot.age}y</span>
      <span>{snapshot.country_code}</span>
      <span>{snapshot.is_age_verified ? "IDV" : "No IDV"}</span>
      <span>{snapshot.is_on_watchlist ? "Watchlist" : "Clear"}</span>
    </div>
  );
}

function EventStream({ events }: { events: DemoEvent[] }) {
  return (
    <section className="event-stream">
      <div className="panel-title">
        <span>Event stream</span>
        <small>API calls, signals, and workflow queries</small>
      </div>
      <div className="event-list">
        {events.length === 0 ? (
          <div className="empty-log">Select a flow and start a scenario.</div>
        ) : (
          events.map((event) => (
            <div className={`event-row ${event.level}`} key={event.id}>
              <span>{event.at}</span>
              <strong>{event.title}</strong>
              <p>{event.detail}</p>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function StatusPill({ status }: { status: ConnectionStatus }) {
  const meta = statusMeta[status];
  const Icon = meta.Icon;
  return (
    <span className={`status-pill ${meta.tone}`}>
      <Icon size={14} />
      {meta.label}
    </span>
  );
}

async function sendScenarioEvent(
  kind: ScenarioActionKind,
  scenario: Scenario,
  runContext: RunContext,
): Promise<UserSnapshot | null> {
  const eventTypeByKind = {
    loseEligibility: "ELIGIBILITY_CHANGED",
    restoreEligibility: "ELIGIBILITY_CHANGED",
    ageUp: "AGE_CHANGED",
    removeParentChild: "PARENT_CHILD_REMOVED",
    restoreParentChild: "PARENT_CHILD_FORMED",
  } as const;

  if (!(kind in eventTypeByKind)) {
    return null;
  }

  const changedUserId = runContext.requesterUserId;
  const snapshot = snapshotForEvent(kind, scenario);
  await sendEligibilityEvent({
    eventId: `${scenario.id}-${kind}-${Date.now().toString(36)}`,
    workflowId: runContext.workflowId,
    userIdA: runContext.requesterUserId,
    userIdB: runContext.targetUserId,
    changedUserId,
    eventType: eventTypeByKind[kind as keyof typeof eventTypeByKind],
    snapshot,
  });
  return snapshot;
}

function snapshotForEvent(kind: ScenarioActionKind, scenario: Scenario): UserSnapshot {
  if (kind === "loseEligibility") {
    return {
      ...scenario.requester.snapshot,
      is_age_verified: false,
      is_on_watchlist: true,
    };
  }
  if (kind === "ageUp") {
    return {
      ...scenario.requester.snapshot,
      age: Math.max(13, scenario.requester.snapshot.age + 1),
      is_age_verified: true,
      is_on_watchlist: false,
    };
  }
  return {
    ...scenario.requester.snapshot,
    is_age_verified: true,
    is_on_watchlist: false,
  };
}

function shortWorkflowId(workflowId: string) {
  if (workflowId.length <= 34) {
    return workflowId;
  }
  return `${workflowId.slice(0, 18)}...${workflowId.slice(-10)}`;
}

function shortTime(timestamp: string) {
  return new Date(timestamp).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
