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
  SlidersHorizontal,
  Timer,
  Users,
  XCircle,
} from "lucide-react";
import {
  acceptTrustedFriend,
  applyTrustedFriendConfiguration,
  closeTrustedFriend,
  getTrustedFriend,
  isQueryBackpressureError,
  sendDomainFact,
  sendParentalConsent,
  startTrustedFriend,
  workflowIssueFromError,
} from "./api";
import { scenarios } from "./scenarios";
import type {
  ConnectionStatus,
  DemoEvent,
  DomainFactType,
  RunContext,
  Scenario,
  ScenarioAction,
  ScenarioActionKind,
  ScenarioRolePreviews,
  SourceChannel,
  TrustedConnectionState,
  UserSnapshot,
  WorkflowRuntimeIssue,
} from "./types";
import "./styles.css";

// Maps durable workflow state into compact visual metadata. Keeping this table
// centralized prevents buttons, timeline rows, and status pills from drifting
// into different labels for the same Temporal status.
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
  ageUpRequester: Timer,
  ageUpTarget: Timer,
  blockRequester: AlertTriangle,
  blockTarget: AlertTriangle,
  restoreRequester: RefreshCw,
  restoreTarget: RefreshCw,
  emitChangedFacts: SlidersHorizontal,
  removeFriendship: Users,
  restoreFriendship: Users,
  removeParentChild: ShieldAlert,
  restoreParentChild: HeartHandshake,
  refresh: RefreshCw,
};

const SANDBOX_ID = "sandbox";
const WORKFLOW_QUERY_INTERVAL_MS = 500;
// Initial workflow tasks can take longer in Temporal Cloud than local dev,
// especially immediately after a worker deploy. These retries give the worker
// time to process the first task without hammering consistent queries.
const INITIAL_QUERY_RETRY_DELAYS_MS = [
  WORKFLOW_QUERY_INTERVAL_MS,
  WORKFLOW_QUERY_INTERVAL_MS,
  WORKFLOW_QUERY_INTERVAL_MS,
];

const sourceChannelOptions: SourceChannel[] = [
  "STANDARD",
  "SHARE_LINK",
  "QR_CODE",
  "QR_CROSS_AGE",
  "CONTACT_LIST_IMPORTER",
  "PARENT_CHILD",
];

const sandboxScenario: Scenario = {
  id: SANDBOX_ID,
  title: "Trusted Friends Sandbox",
  navLabel: "Sandbox",
  group: "Live system",
  objective: "Operate one trusted-friend pair directly and publish the same facts external systems would send.",
  entryPoint: "Sandbox operator controls / domain fact stream",
  sourceChannel: "STANDARD",
  requester: {
    id: "sandbox-a",
    label: "Requester",
    snapshot: {
      age: 18,
      country_code: "US",
      is_age_verified: true,
      is_on_watchlist: false,
    },
  },
  target: {
    id: "sandbox-b",
    label: "Target",
    snapshot: {
      age: 18,
      country_code: "US",
      is_age_verified: true,
      is_on_watchlist: false,
    },
  },
  areFriends: true,
  parentChildRelationship: false,
  autoAccept: false,
  consentTtlSeconds: 120,
  trigger: "SANDBOX_OPERATOR",
  expectedInitialStatus: "PENDING",
  requirements: [
    "Start from edited participants",
    "Publish domain facts for participant changes",
    "Recompute eligibility from the active workflow state",
  ],
  previews: {
    requester: {
      title: "Requester account",
      idle: "Configure both participants, then start an active trusted-friend workflow.",
      pending: "The requester has an active request in the workflow state machine.",
      waiting: "The requester is waiting while required approval is completed.",
      trusted: "The requester currently has trusted access to the target.",
      blocked: "The requester sees trusted access unavailable for the active pair.",
      detail: "Sandbox controls use the same start, signal, domain fact, and configuration APIs as the rest of the demo.",
    },
    approver: {
      title: "Approval authority",
      idle: "Approval appears when the active pair requires VPC or family authority.",
      pending: "No approval is required unless the workflow enters a consent-gated state.",
      waiting: "Approval actions become available while the workflow waits for consent.",
      trusted: "Approval is complete or not required for the current trusted state.",
      blocked: "Approval cannot override an ineligible or terminal pair.",
      notRequired: "No approval required for the current sandbox configuration.",
      detail: "The sandbox derives available buttons from live workflow state rather than scenario definitions.",
    },
    recipient: {
      title: "Target account",
      idle: "The target has no active trusted-friend workflow yet.",
      pending: "The target can accept when double opt-in is still required.",
      waiting: "The target is waiting on approval before trusted status can finish.",
      trusted: "The target currently has trusted access to the requester.",
      blocked: "The target sees trusted access suspended or closed.",
      detail: "Participant facts can be emitted for either side of the pair.",
    },
  },
  actions: [],
};

interface RuntimeUserSnapshots {
  // The UI updates snapshots after eligibility events so the visible user cards
  // match the payload most recently sent to Temporal.
  requester: UserSnapshot;
  target: UserSnapshot;
}

interface PairEditorUserDraft {
  idPrefix: string;
  label: string;
  snapshot: UserSnapshot;
}

interface PairEditorDraft {
  requester: PairEditorUserDraft;
  target: PairEditorUserDraft;
  sourceChannel: SourceChannel;
  areFriends: boolean;
  parentChildRelationship: boolean;
  autoAccept: boolean;
  consentTtlSeconds: number;
}

interface WorkflowRunRecord {
  // A scenario can have several workflow executions during a debugging session.
  // Keeping local records lets presenters switch between runs without losing the
  // last queried state or closed-workflow explanation.
  workflowId: string;
  scenarioId: string;
  context: RunContext;
  state: TrustedConnectionState | null;
  runtimeSnapshots: RuntimeUserSnapshots;
  editorDraft: PairEditorDraft;
  workflowIssue: WorkflowRuntimeIssue | null;
  createdAt: string;
  closedAt: string | null;
}

interface SandboxFactChange {
  factType: DomainFactType;
  subjectRole: "requester" | "target";
  subjectUserId: string;
  snapshot: UserSnapshot;
  label: string;
}

function draftFromScenario(scenario: Scenario): PairEditorDraft {
  return {
    requester: {
      idPrefix: scenario.requester.id,
      label: scenario.requester.label,
      snapshot: { ...scenario.requester.snapshot },
    },
    target: {
      idPrefix: scenario.target.id,
      label: scenario.target.label,
      snapshot: { ...scenario.target.snapshot },
    },
    sourceChannel: scenario.sourceChannel,
    areFriends: scenario.areFriends,
    parentChildRelationship: scenario.parentChildRelationship,
    autoAccept: scenario.autoAccept,
    consentTtlSeconds: scenario.consentTtlSeconds,
  };
}

function snapshotsFromDraft(draft: PairEditorDraft): RuntimeUserSnapshots {
  return {
    requester: { ...draft.requester.snapshot },
    target: { ...draft.target.snapshot },
  };
}

function snapshotsFromState(state: TrustedConnectionState): RuntimeUserSnapshots | null {
  if (!state.requester_snapshot || !state.target_snapshot) {
    return null;
  }
  return {
    requester: state.requester_snapshot,
    target: state.target_snapshot,
  };
}

function connectionDraftFromWorkflowState(
  draft: PairEditorDraft,
  state: TrustedConnectionState | null,
): PairEditorDraft {
  if (!state) {
    return draft;
  }

  return {
    ...draft,
    requester: {
      ...draft.requester,
      snapshot: state.requester_snapshot ?? draft.requester.snapshot,
    },
    target: {
      ...draft.target,
      snapshot: state.target_snapshot ?? draft.target.snapshot,
    },
    sourceChannel: state.source_channel,
    areFriends: state.are_friends,
    parentChildRelationship: state.parent_child_relationship,
    autoAccept: state.auto_accept,
    consentTtlSeconds: state.consent_ttl_seconds,
  };
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
  const [editorDraft, setEditorDraft] = useState<PairEditorDraft>(() =>
    draftFromScenario(scenarios[0]),
  );
  const [busyEditor, setBusyEditor] = useState(false);
  const [workflowRuns, setWorkflowRuns] = useState<WorkflowRunRecord[]>([]);
  // Temporal consistent queries are cheap in local dev but can backpressure in
  // Cloud. Track the last query per workflow and throttle every query path.
  const lastWorkflowQueryAt = useRef<Map<string, number>>(new Map());

  const isSandbox = selectedId === SANDBOX_ID;
  const scenario = useMemo(
    () =>
      selectedId === SANDBOX_ID
        ? sandboxScenario
        : scenarios.find((item) => item.id === selectedId) ?? scenarios[0],
    [selectedId],
  );

  const scenarioWorkflowRuns = useMemo(
    () => workflowRuns.filter((record) => record.scenarioId === scenario.id),
    [scenario.id, workflowRuns],
  );

  function appendEvent(event: Omit<DemoEvent, "id" | "at">) {
    // The event stream is presenter-facing telemetry. It intentionally keeps
    // only the most recent entries so the bottom panel stays readable.
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
    // Selecting a scenario restores its most recent run when available. This is
    // useful when comparing Cloud behavior across several workflow executions.
    const nextScenario =
      nextId === SANDBOX_ID ? sandboxScenario : scenarios.find((item) => item.id === nextId);
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
      setEditorDraft(nextScenario ? draftFromScenario(nextScenario) : draftFromScenario(scenarios[0]));
    }
    appendEvent({
      level: "info",
      title: nextId === SANDBOX_ID ? "Sandbox selected" : "Scenario selected",
      detail: nextScenario?.title ?? nextId,
    });
  }

  function applyWorkflowRun(record: WorkflowRunRecord) {
    setRunContext(record.context);
    setState(record.state);
    setWorkflowIssue(record.workflowIssue);
    setRuntimeSnapshots(record.runtimeSnapshots);
    setEditorDraft(record.editorDraft);
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
    // Reuse the active workflow unless the user explicitly asks for a new run.
    // Temporal workflows are durable, so repeated refreshes should query the
    // same execution instead of creating duplicate pair workflows.
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
    // Add a run token to user ids so each demo execution is isolated while still
    // preserving the scenario's readable user prefixes.
    const requesterUserId = `${editorDraft.requester.idPrefix}-${token}`;
    const targetUserId = `${editorDraft.target.idPrefix}-${token}`;
    const response = await startTrustedFriend({
      requester_user_id: requesterUserId,
      target_user_id: targetUserId,
      source_channel: editorDraft.sourceChannel,
      requester_snapshot: editorDraft.requester.snapshot,
      target_snapshot: editorDraft.target.snapshot,
      consent_ttl_seconds: editorDraft.consentTtlSeconds,
      are_friends: editorDraft.areFriends,
      parent_child_relationship: editorDraft.parentChildRelationship,
      auto_accept: editorDraft.autoAccept,
      trigger: scenario.trigger,
      metadata: {
        mode: isSandbox ? "sandbox" : "scenario",
        scenario_id: isSandbox ? "" : scenario.id,
        entry_point: scenario.entryPoint,
      },
    });
    const nextContext = {
      workflowId: response.workflow_id,
      requesterUserId,
      targetUserId,
      runToken: token,
    };
    const nextSnapshots = snapshotsFromDraft(editorDraft);
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
        editorDraft,
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
      // A close signal is terminal for this demo. Mark the local run closed
      // immediately so the UI stops offering signals that Temporal will reject.
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

  function updateEditorDraft(update: PairEditorDraft) {
    setEditorDraft(update);
    updateActiveWorkflowRun({ editorDraft: update });
  }

  function resetEditorDraft() {
    updateEditorDraft(draftFromScenario(scenario));
    appendEvent({
      level: "info",
      title: "Editor reset",
      detail: scenario.title,
    });
  }

  async function applyEditorConfiguration() {
    if (isSandbox) {
      await applySandboxEditorChanges(editorDraft);
      return;
    }
    if (!runContext) {
      appendEvent({
        level: "warning",
        title: "Start required",
        detail: "Start a workflow before applying live configuration changes.",
      });
      return;
    }
    if (state && isTerminalStatus(state.status)) {
      appendEvent({
        level: "warning",
        title: "New workflow required",
        detail: "Terminal workflows cannot be resurrected by editor changes.",
      });
      return;
    }

    setBusyEditor(true);
    try {
      await applyTrustedFriendConfiguration(runContext.workflowId, {
        sourceChannel: editorDraft.sourceChannel,
        requesterUserId: runContext.requesterUserId,
        targetUserId: runContext.targetUserId,
        requesterSnapshot: editorDraft.requester.snapshot,
        targetSnapshot: editorDraft.target.snapshot,
        consentTtlSeconds: editorDraft.consentTtlSeconds,
        areFriends: editorDraft.areFriends,
        parentChildRelationship: editorDraft.parentChildRelationship,
        autoAccept: editorDraft.autoAccept,
        trigger: "OPERATOR_CONFIGURATION",
        metadata: {
          scenario_id: scenario.id,
          entry_point: scenario.entryPoint,
        },
      });
      const nextSnapshots = snapshotsFromDraft(editorDraft);
      setRuntimeSnapshots(nextSnapshots);
      updateActiveWorkflowRun({
        runtimeSnapshots: nextSnapshots,
        editorDraft,
      });
      appendEvent({
        level: "success",
        title: "Configuration applied",
        detail: runContext.workflowId,
      });
      await refreshStateAfterAcceptedCommand(runContext.workflowId);
    } catch (error) {
      handleActionError(error);
    } finally {
      setBusyEditor(false);
    }
  }

  async function applySandboxEditorChanges(draft: PairEditorDraft) {
    if (!runContext) {
      appendEvent({
        level: "warning",
        title: "Start required",
        detail: "Start the sandbox workflow before emitting live fact changes.",
      });
      return;
    }
    if (state && isTerminalStatus(state.status)) {
      appendEvent({
        level: "warning",
        title: "New workflow required",
        detail: "Terminal workflows cannot receive sandbox fact changes.",
      });
      return;
    }

    const factChanges = sandboxFactChanges({
      draft,
      state,
      runtimeSnapshots,
      runContext,
    });
    const configurationLabels = sandboxConfigurationChangeLabels(draft, state);
    const configurationDirty = sandboxConfigurationDirty(draft, state);
    if (factChanges.length === 0 && !configurationDirty) {
      appendEvent({
        level: "info",
        title: "No sandbox changes",
        detail: "The editor matches the last queried workflow state.",
      });
      return;
    }

    setBusyEditor(true);
    try {
      for (const change of factChanges) {
        await sendDomainFact({
          factId: `${runContext.runToken}-sandbox-${change.factType}-${Date.now().toString(36)}-${crypto.randomUUID().slice(0, 6)}`,
          factType: change.factType,
          workflowId: runContext.workflowId,
          userIdA: runContext.requesterUserId,
          userIdB: runContext.targetUserId,
          subjectUserId: change.subjectUserId,
          snapshot: change.snapshot,
        });
      }

      await applyTrustedFriendConfiguration(runContext.workflowId, {
        sourceChannel: draft.sourceChannel,
        requesterUserId: runContext.requesterUserId,
        targetUserId: runContext.targetUserId,
        requesterSnapshot: draft.requester.snapshot,
        targetSnapshot: draft.target.snapshot,
        consentTtlSeconds: draft.consentTtlSeconds,
        areFriends: draft.areFriends,
        parentChildRelationship: draft.parentChildRelationship,
        autoAccept: draft.autoAccept,
        trigger: "SANDBOX_CONFIGURATION",
        metadata: {
          mode: "sandbox",
          fact_count: String(factChanges.length),
        },
      });

      const nextSnapshots = snapshotsFromDraft(draft);
      setEditorDraft(draft);
      setRuntimeSnapshots(nextSnapshots);
      updateActiveWorkflowRun({
        runtimeSnapshots: nextSnapshots,
        editorDraft: draft,
      });
      appendEvent({
        level: "success",
        title: "Sandbox changes emitted",
        detail: sandboxChangeSummary(factChanges, configurationLabels),
      });
      await refreshStateAfterAcceptedCommand(runContext.workflowId);
    } catch (error) {
      handleActionError(error);
    } finally {
      setBusyEditor(false);
    }
  }

  async function setActiveWorkflow(workflowId: string) {
    const record = workflowRuns.find((item) => item.workflowId === workflowId);
    if (!record) {
      return;
    }
    const nextScenario =
      record.scenarioId === SANDBOX_ID
        ? sandboxScenario
        : scenarios.find((item) => item.id === record.scenarioId);
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
        // Signal delivery and workflow processing are separate moments. Wait
        // before querying so Cloud has a chance to schedule the workflow task.
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

      if (isSandbox && isSandboxAction(action.kind)) {
        const actionBaseDraft =
          action.kind === "emitChangedFacts"
            ? editorDraft
            : connectionDraftFromWorkflowState(editorDraft, state);
        const nextDraft = draftForSandboxAction(action.kind, actionBaseDraft);
        updateEditorDraft(nextDraft);
        await applySandboxEditorChanges(nextDraft);
        return;
      }

      const changedSnapshot = await sendScenarioEvent(action.kind, editorDraft, runContext);
      if (changedSnapshot) {
        // Eligibility events mutate the requester's snapshot in this demo. Store
        // the changed snapshot locally so the user cards mirror the event.
        const nextSnapshots = {
          requester: changedSnapshot,
          target: runtimeSnapshots?.target ?? editorDraft.target.snapshot,
        };
        const nextDraft = {
          ...editorDraft,
          requester: {
            ...editorDraft.requester,
            snapshot: changedSnapshot,
          },
        };
        setEditorDraft(nextDraft);
        setRuntimeSnapshots(nextSnapshots);
        updateActiveWorkflowRun({
          runtimeSnapshots: nextSnapshots,
          editorDraft: nextDraft,
        });
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
    // Every visible refresh funnels through this throttle. It protects Temporal
    // Cloud from repeated consistent queries when a workflow is busy processing
    // signals or waiting for pollers.
    const now = Date.now();
    const lastQueryAt = lastWorkflowQueryAt.current.get(workflowId) ?? 0;
    const remainingDelay = WORKFLOW_QUERY_INTERVAL_MS - (now - lastQueryAt);
    if (remainingDelay > 0) {
      await delay(remainingDelay);
    }
    lastWorkflowQueryAt.current.set(workflowId, Date.now());

    const nextState = await getTrustedFriend(workflowId);
    const nextSnapshots = snapshotsFromState(nextState);
    setState(nextState);
    setWorkflowIssue(null);
    if (nextSnapshots) {
      setRuntimeSnapshots(nextSnapshots);
    }
    updateWorkflowRun(workflowId, {
      state: nextState,
      workflowIssue: null,
      ...(nextSnapshots ? { runtimeSnapshots: nextSnapshots } : {}),
    });
    appendEvent({
      level: "info",
      title: "Workflow queried",
      detail: `${nextState.status} / ${nextState.reason}`,
    });
  }

  async function refreshStateAfterAcceptedCommand(workflowId: string) {
    // API endpoints return once Temporal accepts a command. The workflow's query
    // state usually lags behind that acknowledgment, so delay before refreshing.
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
    // Newly started workflows may not have completed their first workflow task
    // when the POST /send response reaches the browser. Retry on a slow cadence.
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
    // Closed/missing workflow errors are normal while demoing restart behavior,
    // so surface them as runtime state instead of a generic failed toast.
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
          isSandbox={isSandbox}
          editorDraft={editorDraft}
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
          busyEditor={busyEditor}
          runtimeSnapshots={runtimeSnapshots}
          editorDraft={editorDraft}
          workflowRuns={scenarioWorkflowRuns}
          onCloseActiveWorkflow={closeActiveWorkflow}
          onCreateNewWorkflow={createNewActiveWorkflow}
          onSetActiveWorkflow={setActiveWorkflow}
          onUpdateEditorDraft={updateEditorDraft}
          onResetEditorDraft={resetEditorDraft}
          onApplyEditorConfiguration={applyEditorConfiguration}
          isSandbox={isSandbox}
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
        <strong>{scenarios.length + 1}</strong>
      </div>
      <button
        className={`scenario-tab sandbox-tab ${selectedId === SANDBOX_ID ? "selected" : ""}`}
        onClick={() => onSelect(SANDBOX_ID)}
      >
        <span className="scenario-title">Sandbox</span>
        <span className="scenario-group">Live system</span>
        {selectedId === SANDBOX_ID && activeStatus ? <StatusPill status={activeStatus} /> : null}
      </button>
      <div className="rail-divider">Scenarios</div>
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
  isSandbox,
  editorDraft,
  state,
  runContext,
  workflowIssue,
  busyAction,
  onRunAction,
}: {
  scenario: Scenario;
  isSandbox: boolean;
  editorDraft: PairEditorDraft;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  busyAction: ScenarioActionKind | null;
  onRunAction: (action: ScenarioAction) => void;
}) {
  const displayedTransitions = state?.transitions?.length
    ? state.transitions
    : [{ status: scenario.expectedInitialStatus, reason: "Expected first state", timestamp: "" }];
  const connectionDraft = isSandbox
    ? connectionDraftFromWorkflowState(editorDraft, state)
    : editorDraft;
  const sourceChannel = isSandbox ? connectionDraft.sourceChannel : scenario.sourceChannel;
  const friendshipValue = isSandbox
    ? connectionDraft.areFriends
      ? "Friends currently true"
      : "Friends currently false"
    : scenario.areFriends
      ? "Required / satisfied"
      : "Not required";
  const requirements = isSandbox
    ? sandboxRequirements(connectionDraft, state, runContext)
    : scenario.requirements;

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
        <InfoBlock label="Source channel" value={sourceChannel} icon={<Link2 />} />
        <InfoBlock label="Friendship prerequisite" value={friendshipValue} icon={<Users />} />
        <InfoBlock label="Temporal owner" value={runContext?.workflowId ?? "Not started"} icon={<Activity />} />
      </div>

      <RolePreviewDeck
        scenario={scenario}
        isSandbox={isSandbox}
        editorDraft={editorDraft}
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
          {requirements.map((requirement) => (
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
  isSandbox,
  editorDraft,
  state,
  runContext,
  workflowIssue,
  busyAction,
  onRunAction,
}: {
  scenario: Scenario;
  isSandbox: boolean;
  editorDraft: PairEditorDraft;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  busyAction: ScenarioActionKind | null;
  onRunAction: (action: ScenarioAction) => void;
}) {
  // Role preview models translate one workflow state into three product-facing
  // perspectives. Keeping this as data makes it easier to verify that signals
  // are only enabled for the role that should send them.
  const workflowUnavailable = Boolean(
    workflowIssue && runContext && workflowIssue.workflowId === runContext.workflowId,
  );
  const connectionDraft = isSandbox
    ? connectionDraftFromWorkflowState(editorDraft, state)
    : editorDraft;
  const availableActions = isSandbox
    ? sandboxActions(connectionDraft, state, runContext, workflowIssue)
    : scenario.actions;
  const startAction = getAvailableAction(availableActions, "start");
  const acceptAction = getAvailableAction(availableActions, "accept");
  const approveAction = getAvailableAction(availableActions, "approveConsent");
  const denyAction = getAvailableAction(availableActions, "denyConsent");
  const actionLocked = busyAction !== null || workflowUnavailable;
  const currentStatus = state?.status ?? scenario.expectedInitialStatus;
  const approvalRequired = Boolean(state?.consent_required) || Boolean(approveAction || denyAction);
  const parentAuthority = isSandbox
    ? connectionDraft.parentChildRelationship || connectionDraft.sourceChannel === "PARENT_CHILD"
    : scenario.parentChildRelationship || scenario.sourceChannel === "PARENT_CHILD";
  const canActOnWorkflow = Boolean(runContext) && !workflowUnavailable && !isClosedStatus(state?.status);
  const canSendSandboxFact = Boolean(runContext) && !workflowUnavailable && !isTerminalStatus(state?.status);
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
      userLabel: isSandbox ? editorDraft.requester.label : scenario.requester.label,
      runtimeId: runContext?.requesterUserId,
      body: previewBody(scenario.previews.requester, scenario, state, runContext, workflowIssue),
      detail: scenario.previews.requester.detail,
      statusLabel: previewStatusLabel(scenario, state, runContext, workflowIssue),
      tone: previewTone(scenario, state, runContext, workflowIssue),
      Icon: Send,
      actions:
        isSandbox
          ? sandboxRoleActions({
              role: "requester",
              actions: availableActions,
              actionLocked,
              canSendSandboxFact,
              busyAction,
            })
          : startAction && !runContext
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
      userLabel: approvalRequired
        ? "Parent / approver"
        : parentAuthority
          ? isSandbox
            ? editorDraft.requester.label
            : scenario.requester.label
          : "Approver",
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
        isSandbox
          ? sandboxRoleActions({
              role: "approver",
              actions: availableActions,
              actionLocked,
              canSendSandboxFact,
              busyAction,
              canApprove,
              canDeny,
            })
          : approvalRequired && (approveAction || denyAction)
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
      userLabel: isSandbox ? editorDraft.target.label : scenario.target.label,
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
        isSandbox
          ? sandboxRoleActions({
              role: "recipient",
              actions: availableActions,
              actionLocked,
              canSendSandboxFact,
              busyAction,
              canAccept,
              accepted: state?.accepted,
            })
          : acceptAction && !state?.accepted
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

function getAvailableAction(
  actions: ScenarioAction[],
  kind: ScenarioActionKind,
): ScenarioAction | undefined {
  return actions.find((action) => action.kind === kind);
}

function sandboxRoleActions({
  role,
  actions,
  actionLocked,
  canSendSandboxFact,
  busyAction,
  canAccept = false,
  accepted = false,
  canApprove = false,
  canDeny = false,
}: {
  role: RolePreviewRole;
  actions: ScenarioAction[];
  actionLocked: boolean;
  canSendSandboxFact: boolean;
  busyAction: ScenarioActionKind | null;
  canAccept?: boolean;
  accepted?: boolean;
  canApprove?: boolean;
  canDeny?: boolean;
}): RolePreviewAction[] {
  const kindsByRole: Record<RolePreviewRole, ScenarioActionKind[]> = {
    requester: [
      "start",
      "removeFriendship",
      "restoreFriendship",
      "blockRequester",
      "restoreRequester",
      "ageUpRequester",
      "emitChangedFacts",
    ],
    approver: [
      "approveConsent",
      "denyConsent",
      "removeParentChild",
      "restoreParentChild",
    ],
    recipient: ["accept", "blockTarget", "restoreTarget", "ageUpTarget"],
  };

  return kindsByRole[role]
    .map((kind) => getAvailableAction(actions, kind))
    .filter((action): action is ScenarioAction => Boolean(action))
    .map((action) => {
      let disabled = actionLocked;
      let variant: RoleActionVariant = "secondary";
      if (action.kind === "start") {
        variant = "primary";
      } else if (action.kind === "accept") {
        variant = "primary";
        disabled = actionLocked || accepted || !canAccept;
      } else if (action.kind === "approveConsent") {
        variant = "primary";
        disabled = actionLocked || !canApprove;
      } else if (action.kind === "denyConsent") {
        variant = "danger";
        disabled = actionLocked || !canDeny;
      } else if (
        action.kind === "blockRequester" ||
        action.kind === "blockTarget" ||
        action.kind === "removeFriendship" ||
        action.kind === "removeParentChild"
      ) {
        variant = "danger";
        disabled = actionLocked || !canSendSandboxFact;
      } else {
        disabled = actionLocked || !canSendSandboxFact;
      }

      return {
        action,
        disabled,
        loading: busyAction === action.kind,
        variant,
      };
    });
}

function previewBody(
  copy: ScenarioRolePreviews[RolePreviewRole],
  scenario: Scenario,
  state: TrustedConnectionState | null,
  runContext: RunContext | null,
  workflowIssue: WorkflowRuntimeIssue | null,
) {
  // Copy selection is intentionally state-machine-like: the same Temporal state
  // should always produce the same requester/recipient wording.
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
  // The approver pane has extra branches because some flows require VPC, some
  // use parent-child authority, and many require no approval at all.
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

function isTerminalStatus(status: ConnectionStatus | undefined) {
  return status === "EXPIRED" || status === "DENIED";
}

function sandboxActions(
  draft: PairEditorDraft,
  state: TrustedConnectionState | null,
  runContext: RunContext | null,
  workflowIssue: WorkflowRuntimeIssue | null,
): ScenarioAction[] {
  const actions: ScenarioAction[] = [];
  const workflowUnavailable = Boolean(
    workflowIssue && runContext && workflowIssue.workflowId === runContext.workflowId,
  );
  const canUseWorkflow = Boolean(runContext) && !workflowUnavailable && !isTerminalStatus(state?.status);

  if (!runContext || workflowUnavailable) {
    actions.push({
      kind: "start",
      label: "Start sandbox",
      description: "Start a trusted connection from the current sandbox participants.",
    });
    if (workflowUnavailable) {
      actions.push({
        kind: "refresh",
        label: "Refresh query",
        description: "Query workflow state.",
      });
    }
    return actions;
  }

  if (!state) {
    actions.push({
      kind: "refresh",
      label: "Refresh query",
      description: "Query workflow state.",
    });
    return actions;
  }

  if (canUseWorkflow && !state.accepted && state.status !== "SUSPENDED") {
    actions.push({
      kind: "accept",
      label: "Accept",
      description: "Signal recipient acceptance for the active pair.",
    });
  }
  if (canUseWorkflow && state.consent_required && state.consent_status == null) {
    actions.push(
      {
        kind: "approveConsent",
        label: "Approve consent",
        description: "Signal approved parental consent.",
      },
      {
        kind: "denyConsent",
        label: "Deny consent",
        description: "Signal denied parental consent.",
      },
    );
  }

  const requesterBlocked =
    !draft.requester.snapshot.is_age_verified || draft.requester.snapshot.is_on_watchlist;
  const targetBlocked =
    !draft.target.snapshot.is_age_verified || draft.target.snapshot.is_on_watchlist;
  actions.push(
    {
      kind: draft.areFriends ? "removeFriendship" : "restoreFriendship",
      label: draft.areFriends ? "Remove friendship" : "Form friendship",
      description: draft.areFriends
        ? "Change the pair friendship fact to false and recompute eligibility."
        : "Change the pair friendship fact to true and recompute eligibility.",
    },
    {
      kind: requesterBlocked ? "restoreRequester" : "blockRequester",
      label: requesterBlocked ? "Restore requester" : "Block requester",
      description: requesterBlocked
        ? "Emit a requester eligibility-restored fact."
        : "Emit a requester eligibility-loss fact.",
    },
    {
      kind: targetBlocked ? "restoreTarget" : "blockTarget",
      label: targetBlocked ? "Restore target" : "Block target",
      description: targetBlocked
        ? "Emit a target eligibility-restored fact."
        : "Emit a target eligibility-loss fact.",
    },
    {
      kind: "ageUpRequester",
      label: "Age up requester",
      description: "Emit a requester age-change fact.",
    },
    {
      kind: "ageUpTarget",
      label: "Age up target",
      description: "Emit a target age-change fact.",
    },
    {
      kind: draft.parentChildRelationship ? "removeParentChild" : "restoreParentChild",
      label: draft.parentChildRelationship ? "Remove family link" : "Form family link",
      description: draft.parentChildRelationship
        ? "Emit a parent-child relationship removed fact."
        : "Emit a parent-child relationship formed fact.",
    },
  );

  if (canUseWorkflow) {
    actions.push({
      kind: "emitChangedFacts",
      label: "Emit editor changes",
      description: "Publish domain facts for changed editor settings and recompute the workflow.",
    });
  }
  actions.push({
    kind: "refresh",
    label: "Refresh query",
    description: "Query workflow state.",
  });
  return actions;
}

function sandboxRequirements(
  draft: PairEditorDraft,
  state: TrustedConnectionState | null,
  runContext: RunContext | null,
) {
  const requirements = [
    runContext ? "Active workflow owns the pair state" : "Start a sandbox workflow",
    draft.areFriends ? "Friendship fact: true" : "Friendship fact: false",
    draft.parentChildRelationship ? "Family relationship: present" : "Family relationship: absent",
  ];
  if (state?.consent_required) {
    requirements.push("Consent currently required");
  }
  if (!draft.requester.snapshot.is_age_verified || !draft.target.snapshot.is_age_verified) {
    requirements.push("Age verification fact can suspend eligibility");
  }
  if (draft.requester.snapshot.is_on_watchlist || draft.target.snapshot.is_on_watchlist) {
    requirements.push("Watchlist fact can suspend eligibility");
  }
  return requirements;
}

function isSandboxAction(kind: ScenarioActionKind) {
  return [
    "ageUpRequester",
    "ageUpTarget",
    "blockRequester",
    "blockTarget",
    "restoreRequester",
    "restoreTarget",
    "emitChangedFacts",
    "removeFriendship",
    "restoreFriendship",
    "removeParentChild",
    "restoreParentChild",
  ].includes(kind);
}

function draftForSandboxAction(
  kind: ScenarioActionKind,
  draft: PairEditorDraft,
): PairEditorDraft {
  if (kind === "emitChangedFacts") {
    return draft;
  }
  if (kind === "removeFriendship" || kind === "restoreFriendship") {
    return {
      ...draft,
      areFriends: kind === "restoreFriendship",
    };
  }
  if (kind === "removeParentChild" || kind === "restoreParentChild") {
    return {
      ...draft,
      parentChildRelationship: kind === "restoreParentChild",
    };
  }

  const role =
    kind === "ageUpTarget" || kind === "blockTarget" || kind === "restoreTarget"
      ? "target"
      : "requester";
  const snapshot = draft[role].snapshot;
  let nextSnapshot = snapshot;
  if (kind === "ageUpRequester" || kind === "ageUpTarget") {
    nextSnapshot = {
      ...snapshot,
      age: Math.min(130, snapshot.age + 1),
    };
  } else if (kind === "blockRequester" || kind === "blockTarget") {
    nextSnapshot = {
      ...snapshot,
      is_age_verified: false,
      is_on_watchlist: true,
    };
  } else if (kind === "restoreRequester" || kind === "restoreTarget") {
    nextSnapshot = {
      ...snapshot,
      is_age_verified: true,
      is_on_watchlist: false,
    };
  }

  return {
    ...draft,
    [role]: {
      ...draft[role],
      snapshot: nextSnapshot,
    },
  };
}

function sandboxFactChanges({
  draft,
  state,
  runtimeSnapshots,
  runContext,
}: {
  draft: PairEditorDraft;
  state: TrustedConnectionState | null;
  runtimeSnapshots: RuntimeUserSnapshots | null;
  runContext: RunContext;
}): SandboxFactChange[] {
  const currentRequester =
    state?.requester_snapshot ?? runtimeSnapshots?.requester ?? draft.requester.snapshot;
  const currentTarget =
    state?.target_snapshot ?? runtimeSnapshots?.target ?? draft.target.snapshot;
  const changes: SandboxFactChange[] = [];
  if (!snapshotsEqual(currentRequester, draft.requester.snapshot)) {
    changes.push({
      factType:
        currentRequester.age !== draft.requester.snapshot.age
          ? "USER_AGE_CHANGED"
          : "USER_ELIGIBILITY_CHANGED",
      subjectRole: "requester",
      subjectUserId: runContext.requesterUserId,
      snapshot: draft.requester.snapshot,
      label: requesterFactLabel(currentRequester, draft.requester.snapshot),
    });
  }
  if (!snapshotsEqual(currentTarget, draft.target.snapshot)) {
    changes.push({
      factType:
        currentTarget.age !== draft.target.snapshot.age
          ? "USER_AGE_CHANGED"
          : "USER_ELIGIBILITY_CHANGED",
      subjectRole: "target",
      subjectUserId: runContext.targetUserId,
      snapshot: draft.target.snapshot,
      label: targetFactLabel(currentTarget, draft.target.snapshot),
    });
  }
  if (state && state.parent_child_relationship !== draft.parentChildRelationship) {
    changes.push({
      factType: draft.parentChildRelationship
        ? "PARENT_CHILD_RELATIONSHIP_FORMED"
        : "PARENT_CHILD_RELATIONSHIP_REMOVED",
      subjectRole: "requester",
      subjectUserId: runContext.requesterUserId,
      snapshot: draft.requester.snapshot,
      label: draft.parentChildRelationship
        ? "Parent-child relationship formed"
        : "Parent-child relationship removed",
    });
  }
  return changes;
}

function sandboxConfigurationDirty(
  draft: PairEditorDraft,
  state: TrustedConnectionState | null,
) {
  if (!state) {
    return true;
  }
  return (
    state.source_channel !== draft.sourceChannel ||
    state.are_friends !== draft.areFriends ||
    state.parent_child_relationship !== draft.parentChildRelationship ||
    state.auto_accept !== draft.autoAccept ||
    state.consent_ttl_seconds !== draft.consentTtlSeconds ||
    !state.requester_snapshot ||
    !state.target_snapshot ||
    !snapshotsEqual(state.requester_snapshot, draft.requester.snapshot) ||
    !snapshotsEqual(state.target_snapshot, draft.target.snapshot)
  );
}

function sandboxConfigurationChangeLabels(
  draft: PairEditorDraft,
  state: TrustedConnectionState | null,
) {
  if (!state) {
    return [];
  }
  const labels: string[] = [];
  if (state.source_channel !== draft.sourceChannel) {
    labels.push("Source channel changed");
  }
  if (state.are_friends !== draft.areFriends) {
    labels.push(draft.areFriends ? "Friendship formed" : "Friendship removed");
  }
  if (state.parent_child_relationship !== draft.parentChildRelationship) {
    labels.push(
      draft.parentChildRelationship
        ? "Parent-child relationship formed"
        : "Parent-child relationship removed",
    );
  }
  if (state.auto_accept !== draft.autoAccept) {
    labels.push("Auto-accept changed");
  }
  if (state.consent_ttl_seconds !== draft.consentTtlSeconds) {
    labels.push("Consent TTL changed");
  }
  return labels;
}

function sandboxChangeSummary(
  factChanges: SandboxFactChange[],
  configurationLabels: string[],
) {
  const labels = Array.from(
    new Set([...factChanges.map((change) => change.label), ...configurationLabels]),
  );
  return labels.join(", ") || "Pair configuration changed";
}

function snapshotsEqual(left: UserSnapshot, right: UserSnapshot) {
  return (
    left.age === right.age &&
    left.country_code === right.country_code &&
    left.is_age_verified === right.is_age_verified &&
    left.is_on_watchlist === right.is_on_watchlist
  );
}

function requesterFactLabel(before: UserSnapshot, after: UserSnapshot) {
  return userFactLabel("Requester", before, after);
}

function targetFactLabel(before: UserSnapshot, after: UserSnapshot) {
  return userFactLabel("Target", before, after);
}

function userFactLabel(role: string, before: UserSnapshot, after: UserSnapshot) {
  if (before.age !== after.age) {
    return `${role} age changed`;
  }
  if (
    before.is_age_verified !== after.is_age_verified ||
    before.is_on_watchlist !== after.is_on_watchlist
  ) {
    return `${role} eligibility changed`;
  }
  return `${role} profile changed`;
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
  // Workflow management is local UI state layered over durable Temporal state:
  // switching a run only changes which execution the console queries/signals.
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

function PairEditor({
  draft,
  title,
  subtitle,
  resetLabel,
  applyLabel,
  runContext,
  workflowIssue,
  state,
  busy,
  onChange,
  onReset,
  onApply,
}: {
  draft: PairEditorDraft;
  title: string;
  subtitle: string;
  resetLabel: string;
  applyLabel: string;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  state: TrustedConnectionState | null;
  busy: boolean;
  onChange: (draft: PairEditorDraft) => void;
  onReset: () => void;
  onApply: () => void;
}) {
  const idsLocked = Boolean(runContext);
  const applyDisabled =
    !runContext || Boolean(workflowIssue) || isTerminalStatus(state?.status) || busy;

  function updateUser(
    role: keyof Pick<PairEditorDraft, "requester" | "target">,
    update: Partial<PairEditorUserDraft>,
  ) {
    onChange({
      ...draft,
      [role]: {
        ...draft[role],
        ...update,
      },
    });
  }

  function updateSnapshot(
    role: keyof Pick<PairEditorDraft, "requester" | "target">,
    update: Partial<UserSnapshot>,
  ) {
    updateUser(role, {
      snapshot: {
        ...draft[role].snapshot,
        ...update,
      },
    });
  }

  return (
    <section className="inspector-section pair-editor">
      <div className="panel-title">
        <span>{title}</span>
        <small>{subtitle}</small>
      </div>

      <div className="editor-users">
        <EditableUserCard
          title="Requester"
          user={draft.requester}
          runtimeId={runContext?.requesterUserId}
          idsLocked={idsLocked}
          onUserChange={(update) => updateUser("requester", update)}
          onSnapshotChange={(update) => updateSnapshot("requester", update)}
        />
        <EditableUserCard
          title="Target"
          user={draft.target}
          runtimeId={runContext?.targetUserId}
          idsLocked={idsLocked}
          onUserChange={(update) => updateUser("target", update)}
          onSnapshotChange={(update) => updateSnapshot("target", update)}
        />
      </div>

      <div className="editor-field-grid">
        <label>
          <span>Source</span>
          <select
            value={draft.sourceChannel}
            onChange={(event) =>
              onChange({ ...draft, sourceChannel: event.target.value as SourceChannel })
            }
          >
            {sourceChannelOptions.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <label>
          <span>Consent TTL</span>
          <input
            type="number"
            min={1}
            value={draft.consentTtlSeconds}
            onChange={(event) =>
              onChange({
                ...draft,
                consentTtlSeconds: Math.max(1, Number(event.target.value) || 1),
              })
            }
          />
        </label>
      </div>

      <div className="editor-toggle-grid">
        <EditorCheckbox
          label="Friends"
          checked={draft.areFriends}
          onChange={(checked) => onChange({ ...draft, areFriends: checked })}
        />
        <EditorCheckbox
          label="Parent-child"
          checked={draft.parentChildRelationship}
          onChange={(checked) => onChange({ ...draft, parentChildRelationship: checked })}
        />
        <EditorCheckbox
          label="Auto-accept"
          checked={draft.autoAccept}
          onChange={(checked) => onChange({ ...draft, autoAccept: checked })}
        />
      </div>

      <div className="editor-actions">
        <button className="editor-button secondary" onClick={onReset} disabled={busy}>
          <RefreshCw />
          <span>{resetLabel}</span>
        </button>
        <button
          className="editor-button primary"
          onClick={onApply}
          disabled={applyDisabled}
          title={
            runContext
              ? "Apply the edited pair configuration to the active workflow."
              : "Start a workflow before applying live configuration."
          }
        >
          {busy ? <Loader2 className="spin" /> : <SlidersHorizontal />}
          <span>{applyLabel}</span>
        </button>
      </div>
    </section>
  );
}

function EditableUserCard({
  title,
  user,
  runtimeId,
  idsLocked,
  onUserChange,
  onSnapshotChange,
}: {
  title: string;
  user: PairEditorUserDraft;
  runtimeId?: string;
  idsLocked: boolean;
  onUserChange: (update: Partial<PairEditorUserDraft>) => void;
  onSnapshotChange: (update: Partial<UserSnapshot>) => void;
}) {
  return (
    <div className="editor-user-card">
      <div className="editor-user-heading">
        <strong>{title}</strong>
        <span>{runtimeId ?? "not started"}</span>
      </div>
      <label>
        <span>Label</span>
        <input
          value={user.label}
          onChange={(event) => onUserChange({ label: event.target.value })}
        />
      </label>
      <label>
        <span>User ID prefix</span>
        <input
          value={user.idPrefix}
          disabled={idsLocked}
          onChange={(event) => onUserChange({ idPrefix: event.target.value })}
        />
      </label>
      <div className="editor-field-grid compact">
        <label>
          <span>Age</span>
          <input
            type="number"
            min={0}
            max={130}
            value={user.snapshot.age}
            onChange={(event) =>
              onSnapshotChange({ age: Math.max(0, Number(event.target.value) || 0) })
            }
          />
        </label>
        <label>
          <span>Country</span>
          <input
            value={user.snapshot.country_code}
            maxLength={2}
            onChange={(event) =>
              onSnapshotChange({ country_code: event.target.value.toUpperCase() })
            }
          />
        </label>
      </div>
      <div className="editor-toggle-grid compact">
        <EditorCheckbox
          label="Age verified"
          checked={user.snapshot.is_age_verified}
          onChange={(checked) => onSnapshotChange({ is_age_verified: checked })}
        />
        <EditorCheckbox
          label="Watchlist"
          checked={user.snapshot.is_on_watchlist}
          onChange={(checked) => onSnapshotChange({ is_on_watchlist: checked })}
        />
      </div>
    </div>
  );
}

function EditorCheckbox({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <label className="editor-checkbox">
      <input
        type="checkbox"
        checked={checked}
        onChange={(event) => onChange(event.target.checked)}
      />
      <span>{label}</span>
    </label>
  );
}

function Inspector({
  scenario,
  isSandbox,
  state,
  runContext,
  workflowIssue,
  busyAction,
  busyWorkflowControl,
  busyEditor,
  runtimeSnapshots,
  editorDraft,
  workflowRuns,
  onCloseActiveWorkflow,
  onCreateNewWorkflow,
  onSetActiveWorkflow,
  onUpdateEditorDraft,
  onResetEditorDraft,
  onApplyEditorConfiguration,
  onRunAction,
}: {
  scenario: Scenario;
  isSandbox: boolean;
  state: TrustedConnectionState | null;
  runContext: RunContext | null;
  workflowIssue: WorkflowRuntimeIssue | null;
  busyAction: ScenarioActionKind | null;
  busyWorkflowControl: "close" | "new" | null;
  busyEditor: boolean;
  runtimeSnapshots: RuntimeUserSnapshots | null;
  editorDraft: PairEditorDraft;
  workflowRuns: WorkflowRunRecord[];
  onCloseActiveWorkflow: () => void;
  onCreateNewWorkflow: () => void;
  onSetActiveWorkflow: (workflowId: string) => void;
  onUpdateEditorDraft: (draft: PairEditorDraft) => void;
  onResetEditorDraft: () => void;
  onApplyEditorConfiguration: () => void;
  onRunAction: (action: ScenarioAction) => void;
}) {
  // Add refresh as a UI-only action so scenarios do not need to include it in
  // their business-flow definitions.
  const actions = isSandbox
    ? runContext
      ? [{ kind: "refresh", label: "Refresh query", description: "Query workflow state." } as ScenarioAction]
      : []
    : [...scenario.actions, { kind: "refresh", label: "Refresh query", description: "Query workflow state." } as ScenarioAction];
  const workflowUnavailable = Boolean(
    workflowIssue && runContext && workflowIssue.workflowId === runContext.workflowId,
  );
  const requesterSnapshot =
    state?.requester_snapshot ?? runtimeSnapshots?.requester ?? editorDraft.requester.snapshot;
  const targetSnapshot =
    state?.target_snapshot ?? runtimeSnapshots?.target ?? editorDraft.target.snapshot;
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

      <PairEditor
        draft={editorDraft}
        title={isSandbox ? "Sandbox editor" : "Pair editor"}
        subtitle={isSandbox ? "fact source" : "operator configuration"}
        resetLabel={isSandbox ? "Reset sandbox" : "Reset to scenario"}
        applyLabel={isSandbox ? "Emit changed facts" : "Apply configuration"}
        runContext={runContext}
        workflowIssue={workflowIssue}
        state={state}
        busy={busyEditor}
        onChange={onUpdateEditorDraft}
        onReset={onResetEditorDraft}
        onApply={onApplyEditorConfiguration}
      />

      {actions.length > 0 ? (
        <section className="inspector-section">
          <div className="panel-title">
            <span>{isSandbox ? "System" : "Actions"}</span>
            <small>{isSandbox ? "query" : "start / signal / event"}</small>
          </div>
          <div className="action-stack">
            {actions.map((action) => {
              const Icon = actionIcons[action.kind];
              const disabled =
                action.kind !== "start" &&
                action.kind !== "emitChangedFacts" &&
                (!runContext || workflowUnavailable);
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
      ) : null}

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
              <dd>{state ? String(state.accepted) : String(editorDraft.autoAccept)}</dd>
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
          label={editorDraft.requester.label}
          fallbackId={editorDraft.requester.idPrefix}
          runtimeId={runContext?.requesterUserId}
          snapshot={requesterSnapshot}
        />
        <UserCard
          label={editorDraft.target.label}
          fallbackId={editorDraft.target.idPrefix}
          runtimeId={runContext?.targetUserId}
          snapshot={targetSnapshot}
        />
      </section>
    </aside>
  );
}

function UserCard({
  label,
  fallbackId,
  runtimeId,
  snapshot,
}: {
  label: string;
  fallbackId: string;
  runtimeId?: string;
  snapshot: UserSnapshot;
}) {
  return (
    <div className="user-card">
      <div>
        <strong>{label}</strong>
        <span>{runtimeId ?? fallbackId}</span>
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
  draft: PairEditorDraft,
  runContext: RunContext,
): Promise<UserSnapshot | null> {
  // Demo buttons produce upstream-style facts. The API translates those facts
  // into TF-domain eligibility events so transition logic stays in one place.
  const factTypeByKind: Partial<Record<ScenarioActionKind, DomainFactType>> = {
    loseEligibility: "USER_ELIGIBILITY_CHANGED",
    restoreEligibility: "USER_ELIGIBILITY_CHANGED",
    ageUp: "USER_AGE_CHANGED",
    removeParentChild: "PARENT_CHILD_RELATIONSHIP_REMOVED",
    restoreParentChild: "PARENT_CHILD_RELATIONSHIP_FORMED",
  };
  const factType = factTypeByKind[kind];
  if (!factType) {
    return null;
  }

  const subjectUserId = runContext.requesterUserId;
  // For this demo all facts are scoped to the requester. A production consumer
  // would discover affected pair ids from an index/read model and submit one
  // fact per affected pair to this Trusted Friends boundary.
  const snapshot = snapshotForEvent(kind, draft);
  await sendDomainFact({
    factId: `${runContext.runToken}-${kind}-${Date.now().toString(36)}`,
    factType,
    workflowId: runContext.workflowId,
    userIdA: runContext.requesterUserId,
    userIdB: runContext.targetUserId,
    subjectUserId,
    snapshot,
  });
  return snapshot;
}

function snapshotForEvent(kind: ScenarioActionKind, draft: PairEditorDraft): UserSnapshot {
  // Build the changed snapshot from scenario data instead of mutating the
  // original object, which keeps scenario definitions reusable across runs.
  if (kind === "loseEligibility") {
    return {
      ...draft.requester.snapshot,
      is_age_verified: false,
      is_on_watchlist: true,
    };
  }
  if (kind === "ageUp") {
    return {
      ...draft.requester.snapshot,
      age: Math.max(13, draft.requester.snapshot.age + 1),
      is_age_verified: true,
      is_on_watchlist: false,
    };
  }
  return {
    ...draft.requester.snapshot,
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
