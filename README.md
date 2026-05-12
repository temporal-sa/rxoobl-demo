# Trusted Friends Temporal Demo

This repo is a local demo of Trusted Friend lifecycle management with Temporal
as the source of truth for relationship state. A FastAPI backend starts and
signals workflows, a Temporal worker runs the relationship logic, and a
React/Vite console drives the demo scenarios.

There is no database for trusted-connection lifecycle state. The API starts
long-lived pair workflows, starts short-lived eligibility event workflows, and
queries workflow-owned state for the UI.

## What It Shows

The frontend at `http://127.0.0.1:5173` is the main demo surface. It provides
scenario buttons for:

- Same-age double opt-in through `SendTrustedFriend` and `AcceptTrustedFriend`
- IRL auto-upgrade for 13+ users through QR / Contact List Importer-style entry
  points
- Parent-child auto-upgrade, plus relationship removal/restoration events
- QR cross-age rescan for existing friends
- Trusted Friend share link flow without a friendship prerequisite
- U13 VPC approval and denial through Temporal signals
- Eligibility loss and restoration through async event workflows
- Natural age-up retention after VPC approval

The UI shows the selected flow, entry point, source channel, current Temporal
state, signal/event activity, user snapshots, state transitions, and workflow
runtime availability.

## Prerequisites

- Python 3.12+
- `uv`
- Node.js/npm
- Temporal CLI

On macOS, Temporal CLI can be installed with:

```bash
brew install temporal
```

## Run Locally

Use four terminals.

1. Start Temporal:

```bash
temporal server start-dev
```

2. Start the worker:

```bash
export TEMPORAL_NAMESPACE=default
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TLS=false
unset TEMPORAL_API_KEY
uv run python -m trusted_friends.worker
```

3. Start the API:

```bash
export TEMPORAL_NAMESPACE=default
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TLS=false
unset TEMPORAL_API_KEY
uv run uvicorn trusted_friends.api:app --reload
```

4. Start the frontend:

```bash
cd frontend
npm install
npm run dev
```

Open `http://127.0.0.1:5173`.

The API listens on `http://127.0.0.1:8000`. The frontend can be pointed at a
different API with `VITE_API_BASE_URL`.

## Run Against Temporal Cloud

The default Temporal settings now target the Temporal Cloud namespace
`tf-demo.zsvab` at `tf-demo.zsvab.tmprl.cloud:7233`. Provide a Temporal Cloud
API key before starting the worker or API. You can either export it in the
shell or put it in a repo-local `.env` copied from `.env.example`; the Python
worker/API load `.env` automatically without overriding exported variables.

```bash
export TEMPORAL_API_KEY=<temporal-cloud-api-key>
uv run python -m trusted_friends.worker
```

For local development, override the Cloud defaults:

```bash
export TEMPORAL_NAMESPACE=default
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TLS=false
unset TEMPORAL_API_KEY
```

To build a deployment bundle for the Cloud worker/API host:

```bash
./scripts/build_deployment_package.sh
```

See [`docs/temporal-cloud.md`](docs/temporal-cloud.md) for package contents,
launcher scripts, and Worker Deployment version setup.

## Reset Local Demo

Reset the demo back to a clean local state:

```bash
./scripts/reset_demo.sh
```

The reset script stops the local frontend, API, worker, and Temporal dev server.
It also checks the default demo listener ports `5173`, `8000`, `7233`, and
`8233`, then removes generated build/test/browser artifacts and Python bytecode.
It preserves dependencies by default, including `.venv` and
`frontend/node_modules`.

Useful options:

- `--dry-run`: show what would be stopped or removed without changing anything.
- `--keep-services`: remove local artifacts without stopping running processes.
- `--keep-temporal`: stop app services but leave `temporal server start-dev`
  running.
- `--deps`: also remove `.venv` and `frontend/node_modules`.

With the default `temporal server start-dev`, workflow executions are lost when
the Temporal process stops. If you started Temporal with `--db-filename`, remove
that local database file separately when you want to clear persisted workflow
history.

After running the default reset, start the four local services again before
using the UI to start workflows.

## Architecture

- `TrustedConnectionWorkflow`: long-lived relationship workflow, one per
  normalized user pair. It keeps internal runtime state, accepts typed
  relationship events through signal handlers, exposes query handlers, and runs
  a validation state machine that owns `PENDING`,
  `WAITING_FOR_PARENTAL_CONSENT`, `TRUSTED`, `SUSPENDED`, `EXPIRED`, and
  `DENIED`.
- `EligibilityEvaluationWorkflow`: short-lived workflow started with
  deterministic IDs like `eligibility-eval-<event_id>`. It evaluates an
  eligibility event and signals the pair workflow.
- `trusted_friends.rules`: centralized rule set used by the workflow-facing
  activities. Put rules that change often here instead of directly in workflow
  code.
- Activities are fake external boundaries for identity/safety evaluation,
  eligibility event evaluation, and TC event emission.
- FastAPI exposes the start/signal/query endpoints used by the frontend.

For a deeper architecture breakdown, state diagrams, sequence diagrams, and a
comparison with a Kafka-based design, see
[`docs/workflow-architecture.md`](docs/workflow-architecture.md).

## Workflow Lifecycle Notes

- If you manually terminate or close a pair workflow, the GUI shows the runtime
  as unavailable instead of breaking.
- Start a new scenario run in the UI to continue the demo after a workflow has
  been terminated.
- API calls against closed pair workflows return workflow-unavailable details
  such as `WORKFLOW_NOT_RUNNING` or `WORKFLOW_NOT_FOUND`.
- Restart the worker and API after changing workflow, activity, rule, or API
  code.

## API Quick Start

Start a double opt-in connection:

```bash
curl -X POST http://127.0.0.1:8000/trusted-friends/send \
  -H 'content-type: application/json' \
  -d '{
    "requester_user_id": "alice",
    "target_user_id": "bob",
    "source_channel": "STANDARD",
    "requester_snapshot": {"age": 18, "country_code": "US", "is_age_verified": true},
    "target_snapshot": {"age": 18, "country_code": "US", "is_age_verified": true},
    "are_friends": true,
    "auto_accept": false,
    "trigger": "DOUBLE_OPT_IN"
  }'
```

Accept it:

```bash
curl -X POST http://127.0.0.1:8000/trusted-friends/trusted-connection-alice-bob/accept
```

Query workflow state:

```bash
curl http://127.0.0.1:8000/trusted-friends/trusted-connection-alice-bob
```

Trigger an eligibility suspension:

```bash
curl -X POST http://127.0.0.1:8000/events/eligibility \
  -H 'content-type: application/json' \
  -d '{
    "event_id": "event-1",
    "user_id_a": "alice",
    "user_id_b": "bob",
    "changed_user_id": "alice",
    "event_type": "ELIGIBILITY_CHANGED",
    "snapshot": {
      "age": 18,
      "country_code": "US",
      "is_age_verified": false,
      "is_on_watchlist": true
    }
  }'
```

The `/events/eligibility` endpoint targets the pair workflow derived from
`user_id_a` and `user_id_b`, unless `pair_workflow_id` is provided explicitly.
If that pair workflow has already been closed, the API reports it as
workflow-unavailable instead of starting a new event workflow.

Send VPC approval:

```bash
curl -X POST http://127.0.0.1:8000/trusted-friends/<workflow_id>/parental-consent \
  -H 'content-type: application/json' \
  -d '{"consent_id": "consent-1", "status": "APPROVED"}'
```

## Demo Rules

- Users must be age verified to become trusted friends.
- A watchlisted user is ineligible unless the demo snapshot is explicitly age
  verified.
- Standard double opt-in requires users to already be friends and to be in the
  same eligible age group: both 18+, or both 16-17.
- Share links do not require an existing friendship.
- IRL auto-upgrade requires both users to be 13+ and friends through an IRL
  channel.
- Parent-child auto-upgrade requires friendship, a parent-child relationship,
  and age verification.
- U13 users require VPC globally. U16 users require VPC in Brazil (`BR`) and
  Australia (`AU`).
- Eligibility events suspend or restore the long-lived pair workflow by signal.
- Natural age-up events can retain an already approved trusted connection.

## Tests

Run the backend, rules, and workflow tests:

```bash
uv run pytest
```

Build the frontend:

```bash
cd frontend
npm install
npm run build
```

## Useful Paths

- Backend API and workflow runtime error handling: `trusted_friends/api.py`
- Temporal workflows and relationship state machine: `trusted_friends/workflows.py`
- Centralized demo rules: `trusted_friends/rules.py`
- Fake external boundaries: `trusted_friends/activities.py`
- React console: `frontend/src/main.tsx`
- Scenario definitions: `frontend/src/scenarios.ts`
- Architecture deep dive: `docs/workflow-architecture.md`
