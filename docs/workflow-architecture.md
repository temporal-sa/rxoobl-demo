# Trusted Friends Workflow Architecture

This document explains how the Trusted Friends demo uses Temporal to own a
trusted-connection lifecycle, and how that design compares with a Kafka-based
event architecture.

The short version: Temporal is the source of truth for each pair relationship.
Kafka is excellent for distributing facts to many consumers, but a trusted
friend relationship is not just a stream of facts. It is a long-running,
interactive state machine with human approvals, timers, async eligibility
events, recovery behavior, and a need for direct queries. Temporal gives that
state machine a durable execution context.

## Core Design

Each trusted connection pair gets one long-lived Temporal workflow:

- Workflow type: `TrustedConnectionWorkflow`
- Workflow ID: `trusted-connection-<normalized-user-a>-<normalized-user-b>`
- Runtime owner: Temporal event history
- Query surface: `get_state`
- Signal surface: `accept`, `parental_consent`, `apply_eligibility_update`,
  `apply_relationship_event`, and `close`

Short-lived eligibility checks run in separate workflows:

- Workflow type: `EligibilityEvaluationWorkflow`
- Workflow ID: `eligibility-eval-<event_id>`
- Purpose: evaluate an external eligibility event and signal the pair workflow

The pair workflow owns the canonical relationship status:

- `PENDING`
- `WAITING_FOR_PARENTAL_CONSENT`
- `TRUSTED`
- `SUSPENDED`
- `EXPIRED`
- `DENIED`

The FastAPI app does not maintain a lifecycle database. It starts workflows,
sends signals, and queries workflow-owned state. The React app is a demo console
and presentation layer over those API calls.

## System Diagram

```mermaid
flowchart LR
  Presenter["Presenter / demo user"] --> Console["React + Vite console"]
  Console --> API["FastAPI API"]

  API -->|start_workflow| Temporal["Temporal server"]
  API -->|signal workflow| Temporal
  API -->|query get_state| Temporal

  Temporal -->|dispatch workflow tasks| Worker["Temporal worker"]
  Worker --> PairWorkflow["TrustedConnectionWorkflow"]
  Worker --> EligibilityWorkflow["EligibilityEvaluationWorkflow"]

  PairWorkflow --> Rules["trusted_friends.rules"]
  EligibilityWorkflow --> Rules

  PairWorkflow --> Activities["Activities"]
  EligibilityWorkflow --> Activities

  Activities --> External["Fake external boundaries"]
  External --> Identity["identity / safety checks"]
  External --> ChangeEvent["TC change event emission"]
```

### Component Responsibilities

| Component | Responsibility | Source |
| --- | --- | --- |
| React console | Scenario selection, role previews, action buttons, active workflow controls | `frontend/src/main.tsx` |
| FastAPI app | HTTP boundary, request validation, Temporal client calls, runtime error mapping | `trusted_friends/api.py` |
| Temporal server | Durable event history, workflow execution coordination, timers, task queues | local `temporal server start-dev` |
| Temporal worker | Hosts workflow definitions and activity implementations | `trusted_friends/worker.py` |
| Pair workflow | Long-lived relationship state machine for one normalized user pair | `trusted_friends/workflows.py` |
| Eligibility workflow | Short-lived async event processor that signals the pair workflow | `trusted_friends/workflows.py` |
| Rules module | Centralized eligibility and relationship policy decisions | `trusted_friends/rules.py` |
| Activities | Fake side-effect boundaries for evaluation and event emission | `trusted_friends/activities.py` |

## The Pair Workflow

`TrustedConnectionWorkflow` is the central lifecycle owner. It keeps an internal
runtime state object with:

- pair identity and normalized user IDs
- source channel and trigger
- friendship and parent-child flags
- accepted flag
- consent requirement, status, and deadline
- current eligibility decision
- current status and reason
- transition history
- last eligibility event ID

The workflow starts with a `TrustedConnectionRequest`, evaluates initial
eligibility through an activity, records `REQUEST_CREATED`, and then runs an
event loop.

```mermaid
flowchart TB
  Start["run(request)"] --> Init["Initialize workflow-owned state"]
  Init --> InitialActivity["Activity: evaluate_initial_eligibility"]
  InitialActivity --> Consent{"Consent required?"}
  Consent -->|yes| Deadline["Set consent_deadline"]
  Consent -->|no| RequestEvent["Record REQUEST_CREATED"]
  Deadline --> RequestEvent

  RequestEvent --> Loop["Workflow event loop"]
  Loop --> Close{"close requested?"}
  Close -->|yes| Complete["Complete workflow"]
  Close -->|no| NeedsValidation{"needs_validation?"}
  NeedsValidation -->|yes| Reconcile["Reconcile state machine"]
  Reconcile --> Changed{"Status changed?"}
  Changed -->|yes| Emit["Activity: emit_tc_change_event"]
  Changed -->|no| Loop
  Emit --> Loop

  NeedsValidation -->|no| ConsentExpired{"Consent wait expired?"}
  ConsentExpired -->|yes| TimeoutEvent["Record CONSENT_TIMEOUT"]
  TimeoutEvent --> Loop
  ConsentExpired -->|no| Wait["wait_condition for signal or timer"]
  Wait --> Loop
```

The workflow does not push every incoming signal directly into an external
store. Instead, every signal mutates workflow-owned runtime state, sets
`needs_validation`, and lets the state machine reconcile the desired status.
That makes the status derivation explicit and testable.

## State Machine

The state machine is implemented by `_RelationshipValidationStateMachine`.
Signals and internal timeout events update inputs such as `accepted`,
`consent_status`, `consent_timed_out`, and `eligibility_decision`. The
reconciliation step derives the public status from those inputs.

```mermaid
stateDiagram-v2
  [*] --> PENDING: eligible request created
  [*] --> WAITING_FOR_PARENTAL_CONSENT: eligible request needs VPC
  [*] --> SUSPENDED: initial eligibility fails

  PENDING --> TRUSTED: recipient accepts
  PENDING --> SUSPENDED: eligibility lost
  PENDING --> WAITING_FOR_PARENTAL_CONSENT: consent required

  WAITING_FOR_PARENTAL_CONSENT --> TRUSTED: consent approved and accepted
  WAITING_FOR_PARENTAL_CONSENT --> DENIED: consent denied
  WAITING_FOR_PARENTAL_CONSENT --> EXPIRED: consent deadline passes
  WAITING_FOR_PARENTAL_CONSENT --> SUSPENDED: eligibility lost

  TRUSTED --> SUSPENDED: eligibility lost or family link removed
  SUSPENDED --> TRUSTED: eligibility restored

  DENIED --> [*]
  EXPIRED --> [*]
```

The public status priority is:

1. Consent timeout produces `EXPIRED`.
2. Consent denial produces `DENIED`.
3. Failed eligibility produces `SUSPENDED`.
4. Required but unapproved consent produces `WAITING_FOR_PARENTAL_CONSENT`.
5. Acceptance produces `TRUSTED`.
6. Otherwise the relationship remains `PENDING`.

This priority matters. For example, a denied parental consent should stay
terminal even if a later eligibility event arrives. The state machine enforces
that by ignoring consent and eligibility updates after terminal states.

## Start And Query Flow

The standard start flow begins with the React console and ends when the UI can
query the workflow-owned state.

```mermaid
sequenceDiagram
  participant UI as React console
  participant API as FastAPI
  participant TS as Temporal server
  participant Worker as Temporal worker
  participant WF as TrustedConnectionWorkflow
  participant Rules as rules.py

  UI->>API: POST /trusted-friends/send
  API->>API: Normalize pair and build TrustedConnectionRequest
  API->>TS: start_workflow(TrustedConnectionWorkflow.run)
  TS-->>API: Workflow accepted
  API-->>UI: 202 workflow_id

  TS->>Worker: Dispatch workflow task
  Worker->>WF: run(request)
  WF->>Rules: evaluate initial eligibility via activity
  Rules-->>WF: EligibilityDecision
  WF->>WF: Record REQUEST_CREATED and reconcile

  UI->>API: GET /trusted-friends/{workflow_id}
  API->>TS: query get_state
  TS->>Worker: Query workflow state
  Worker->>WF: get_state()
  WF-->>Worker: TrustedConnectionState
  Worker-->>TS: Query result
  TS-->>API: Query result
  API-->>UI: Current state
```

The UI treats the Temporal start response as "accepted" rather than
immediately "ready". It then retries the initial query because a cold worker may
need a moment to pick up and execute the first workflow task.

## Double Opt-In Flow

In the basic flow, the requester sends a trusted friend request and the
recipient accepts. No parental approval is needed.

```mermaid
sequenceDiagram
  participant Requester
  participant UI as React console
  participant API as FastAPI
  participant WF as TrustedConnectionWorkflow

  Requester->>UI: Send request
  UI->>API: POST /trusted-friends/send
  API->>WF: start workflow
  WF->>WF: Initial eligibility = DOUBLE_OPT_IN_ELIGIBLE
  WF-->>UI: Query shows PENDING

  Requester->>UI: Recipient accepts
  UI->>API: POST /trusted-friends/{id}/accept
  API->>WF: signal accept()
  WF->>WF: Record ACCEPTED
  WF->>WF: Reconcile to TRUSTED
  WF-->>UI: Query shows TRUSTED / USER_ACCEPTED
```

## VPC Approval Flow

VPC means verified parental consent. For under-13 users, or under-16 users in
configured countries, the pair remains waiting until parental consent arrives.
The workflow stores the consent deadline durably and can expire the request
without an external scheduler.

```mermaid
sequenceDiagram
  participant Child as Requester or recipient
  participant UI as React console
  participant API as FastAPI
  participant WF as TrustedConnectionWorkflow
  participant Timer as Durable timer

  Child->>UI: Send request
  UI->>API: POST /trusted-friends/send
  API->>WF: start workflow
  WF->>WF: Initial eligibility requires VPC
  WF->>Timer: wait_condition with consent timeout
  WF-->>UI: Query shows WAITING_FOR_PARENTAL_CONSENT

  alt Parent approves
    UI->>API: POST /parental-consent APPROVED
    API->>WF: signal parental_consent(APPROVED)
    UI->>API: POST /accept
    API->>WF: signal accept()
    WF->>WF: Reconcile to TRUSTED
  else Parent denies
    UI->>API: POST /parental-consent DENIED
    API->>WF: signal parental_consent(DENIED)
    WF->>WF: Reconcile to DENIED
  else No response before deadline
    Timer-->>WF: timeout
    WF->>WF: Record CONSENT_TIMEOUT
    WF->>WF: Reconcile to EXPIRED
  end
```

## Async Eligibility Events

Eligibility changes are modeled as short-lived workflows. The API starts an
`EligibilityEvaluationWorkflow` with a deterministic event workflow ID, and
that workflow signals the long-lived pair workflow after evaluating the event.

```mermaid
sequenceDiagram
  participant UI as React console
  participant API as FastAPI
  participant Eval as EligibilityEvaluationWorkflow
  participant Rules as rules.py
  participant Pair as TrustedConnectionWorkflow

  UI->>API: POST /events/eligibility
  API->>API: Ensure pair workflow is running
  API->>Eval: start workflow eligibility-eval-<event_id>
  API-->>UI: 202 event workflow accepted

  Eval->>Rules: evaluate_eligibility_event(event)
  Rules-->>Eval: EligibilityUpdate
  Eval->>Pair: signal apply_eligibility_update(update)
  Pair->>Pair: Reconcile status
  Pair-->>UI: Later query shows SUSPENDED, TRUSTED, or other result
```

This split is useful because eligibility events have their own identity and
deduplication semantics. The event workflow ID uses `event_id`, and the API
uses `REJECT_DUPLICATE` for those workflows. A duplicate eligibility event
cannot start a second event processor with the same ID.

## Workflow Identity And Reuse

Pair workflow IDs are deterministic and based on the normalized user pair:

```text
trusted-connection-<first-sorted-user-id>-<second-sorted-user-id>
```

That gives the API one stable address for the pair. It also means the system can
reject accidental duplicate starts while the pair workflow is already running.

The API uses `WorkflowIDReusePolicy.ALLOW_DUPLICATE` for pair workflows. That
allows the same deterministic pair ID to be started again after the previous
workflow has completed or been closed, while Temporal still rejects another
start if the same workflow ID is currently running.

Eligibility workflow IDs are different:

```text
eligibility-eval-<event_id>
```

Those use `REJECT_DUPLICATE`, because a duplicate event ID should not be
processed twice.

## Failure Handling

Temporal records workflow progress in event history. If the worker process
dies, the server still has the workflow history. When a worker comes back, the
SDK replays history and reconstructs the workflow state before processing the
next signal, timer, activity completion, or query.

The demo uses activity retry policy for side-effect boundaries:

```text
initial interval: 1 second
maximum interval: 10 seconds
maximum attempts: 3
```

API error handling separates runtime states:

- `404 WORKFLOW_NOT_FOUND`: the workflow ID does not exist.
- `410 WORKFLOW_NOT_RUNNING`: the workflow exists but is completed,
  terminated, canceled, timed out, or otherwise not open.
- `503 TEMPORAL_UNAVAILABLE`: Temporal could not complete the operation due to
  availability, deadline, or resource issues.
- `502 TEMPORAL_RPC_ERROR` or `WORKFLOW_QUERY_FAILED`: unexpected Temporal
  query or RPC failure.

The UI surfaces these as workflow runtime issues rather than treating closed
workflows as generic broken UI state.

## Why Temporal Fits This Problem

The trusted friend lifecycle has several properties that match Temporal well:

- It is long-running. A VPC request may wait on a parent instead of completing
  inside one request-response cycle.
- It is interactive. Users and approvers can signal the workflow at different
  times.
- It has durable timers. Consent timeout belongs in the lifecycle state
  machine, not in an external cron job that must rediscover pending rows.
- It needs direct reads. The UI wants the current authoritative status for a
  pair.
- It has ordered per-pair state changes. Signals to one workflow instance are
  processed against that workflow's history.
- It has side effects at specific points. Activities isolate non-deterministic
  work such as external checks or event emission.
- It benefits from replay. State can be rebuilt from workflow history instead
  of reverse-engineering a database row from several consumers.

## Kafka-Based Alternative

A Kafka design would model the relationship lifecycle as events on topics, with
one or more consumers building state in databases or compacted topics.

```mermaid
flowchart LR
  UI["React console"] --> API["API service"]
  API --> Outbox["Transactional outbox"]
  Outbox --> Kafka["Kafka cluster"]

  Kafka --> RequestConsumer["Request consumer"]
  Kafka --> ConsentConsumer["Consent consumer"]
  Kafka --> EligibilityConsumer["Eligibility consumer"]
  Kafka --> TimerService["Timer / scheduler service"]

  RequestConsumer --> StateDB["Relationship state DB"]
  ConsentConsumer --> StateDB
  EligibilityConsumer --> StateDB
  TimerService --> StateDB

  StateDB --> ReadAPI["Read API / projection"]
  ReadAPI --> UI

  StateDB --> EventOutbox["Change event outbox"]
  EventOutbox --> Kafka
  Kafka --> Downstream["Notifications, analytics, moderation, audit"]
```

A practical Kafka version would likely need:

- `trusted_friend_requests` topic
- `trusted_friend_acceptances` topic
- `parental_consent_events` topic
- `eligibility_events` topic
- `trusted_connection_state_changed` topic
- consumer groups for request, consent, eligibility, timeout, projection, and
  notification processing
- a relationship state database keyed by normalized pair
- an idempotency or inbox table for processed event IDs
- an outbox table for publishing state changes transactionally
- a scheduler or delay system for consent deadlines
- dead-letter topics for malformed or repeatedly failing events
- compaction or projections for current state reads

Kafka can support this, but Kafka itself does not own the relationship state
machine. The ownership moves into consumer code plus databases plus
deduplication tables plus scheduler infrastructure.

## Kafka Flow Diagram

```mermaid
sequenceDiagram
  participant UI as React console
  participant API as API service
  participant Kafka as Kafka topics
  participant Consumer as Lifecycle consumer
  participant DB as Relationship DB
  participant Timer as Scheduler
  participant Read as Read API

  UI->>API: Send trusted friend request
  API->>DB: Insert request command or outbox record
  API->>Kafka: Publish TrustedFriendRequested
  Kafka->>Consumer: Consume event
  Consumer->>DB: Load pair state, evaluate rules, update row
  Consumer->>Kafka: Publish TrustedConnectionStateChanged

  alt VPC required
    Consumer->>Timer: Schedule consent deadline
    UI->>API: Parent approves
    API->>Kafka: Publish ParentalConsentReceived
    Kafka->>Consumer: Consume consent event
    Consumer->>DB: Update consent and recompute state
  else Timer fires
    Timer->>Kafka: Publish ConsentTimedOut
    Kafka->>Consumer: Consume timeout event
    Consumer->>DB: Mark EXPIRED if still waiting
  end

  UI->>Read: Query current state
  Read->>DB: Read latest projection
  DB-->>Read: Current pair row
  Read-->>UI: Current state
```

The key architectural difference is where orchestration lives. In Temporal, the
orchestration is explicit in workflow code. In Kafka, orchestration emerges from
multiple consumers, topics, database updates, and scheduled events.

## Temporal vs Kafka Comparison

| Concern | Temporal workflow design | Kafka-based design |
| --- | --- | --- |
| Source of truth | One workflow history per pair owns lifecycle state | State is usually in a DB projection maintained by consumers |
| Command handling | API starts workflows and sends signals directly to the pair owner | API publishes commands/events, consumers eventually process them |
| Per-pair ordering | Signals are processed against one workflow instance | Requires partitioning by pair key and careful consumer design |
| Human-in-the-loop | Native fit through signals and durable waiting | Requires persisted pending state plus event consumers |
| Timers and deadlines | Durable workflow timers are part of the execution | Requires scheduler, delay topic, cron, or delayed queue pattern |
| Current state query | Workflow query returns authoritative in-memory reconstructed state | Requires read model or compacted topic projection |
| Failure recovery | Worker replay reconstructs state from Temporal history | Consumers replay events, but must rebuild projections and handle side effects |
| Side effects | Activities provide retry and isolation boundaries | Consumers must implement retries, idempotency, and transactional outbox logic |
| Duplicate handling | Workflow IDs and event workflow IDs provide clear dedupe boundaries | Requires message keys, processed-event tables, idempotent consumers |
| Debugging | Inspect one workflow history for the pair | Trace across topics, partitions, consumers, DB rows, and scheduler records |
| Operational footprint | Temporal server plus workers | Kafka cluster plus consumers, DB, scheduler, outbox, DLQ, projection services |
| Fanout | Possible through activities or emitted events, but not Temporal's main job | Strong fit for many independent downstream consumers |
| Analytics streams | Use emitted events downstream | Strong fit through retained topics and stream processing |

## Where Kafka Is Still Useful

The best architecture is often not "Temporal or Kafka". For this domain, a
hybrid is natural:

```mermaid
flowchart LR
  API["FastAPI"] --> Temporal["Temporal workflow"]
  Temporal --> Pair["TrustedConnectionWorkflow"]
  Pair --> Activity["emit_tc_change_event activity"]
  Activity --> Kafka["Kafka: trusted_connection_state_changed"]
  Kafka --> Notifications["Notifications"]
  Kafka --> Analytics["Analytics"]
  Kafka --> Audit["Audit sink"]
  Kafka --> Safety["Safety systems"]
```

Temporal should own the lifecycle decision because it is the durable state
machine. Kafka can distribute the resulting facts to consumers that do not own
the pair lifecycle.

Good Kafka use cases around this workflow:

- notify other services when a connection becomes trusted or suspended
- feed analytics and experimentation pipelines
- maintain audit and compliance exports
- broadcast state changes to safety, messaging, or ranking systems
- integrate with systems that already consume Kafka topics

Less ideal Kafka use cases here:

- waiting for parental approval with a timeout
- answering "what is the authoritative state of this pair right now?"
- coordinating accept, approval, eligibility loss, restoration, and close
  behavior across several asynchronous consumers
- debugging one pair's lifecycle from request through final state

## Design Tradeoffs

Temporal is the stronger fit when the central problem is orchestration and
durable state progression. It makes the long-running lifecycle explicit and
keeps the per-pair state machine in one place.

Kafka is the stronger fit when the central problem is durable event
distribution, broad fanout, and independent downstream processing. It gives
many consumers a shared log, but it does not remove the need to design a state
machine, a read model, idempotency, and timeout handling.

For this demo, Temporal is the right primary abstraction because the core
business object is not a topic. It is a relationship that moves through a
well-defined lifecycle over time.

## Implementation Notes

- `TrustedConnectionWorkflow` should stay deterministic. Side effects belong in
  activities, not directly in workflow code.
- Rules that product or policy teams may change should stay in
  `trusted_friends.rules` and be called through activities where appropriate.
- New signals should be expressed as typed relationship events when they affect
  relationship lifecycle state.
- New async processors can follow the `EligibilityEvaluationWorkflow` pattern:
  start a short-lived workflow with a deterministic event ID, evaluate the
  event, and signal the pair workflow.
- If this grows beyond a demo, continue-as-new should be considered for very
  long-lived pair workflows with large histories.
- Kafka or another event bus should receive state-change facts from an activity
  after the workflow has reconciled state, not before the workflow owns the
  decision.
