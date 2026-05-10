# Temporal Cloud Deployment

This package is configured for the Temporal Cloud namespace `tf-demo.zsvab`.

## Connection Settings

The API and worker read the same Temporal settings:

```bash
export TEMPORAL_NAMESPACE=tf-demo.zsvab
export TEMPORAL_ADDRESS=tf-demo.zsvab.tmprl.cloud:7233
export TEMPORAL_TLS=true
export TEMPORAL_API_KEY=<temporal-cloud-api-key>
export TASK_QUEUE=trusted-friends-demo
```

For local development, override the Cloud defaults:

```bash
export TEMPORAL_NAMESPACE=default
export TEMPORAL_ADDRESS=localhost:7233
export TEMPORAL_TLS=false
unset TEMPORAL_API_KEY
```

## Build Package

From the repository root:

```bash
./scripts/build_deployment_package.sh
```

The script creates:

```text
output/temporal-cloud-deployment/trusted-friends-temporal-cloud.tar.gz
```

The archive contains the Python wheel, lockfile, Cloud run scripts, and this
README.

## Run From Package

Unpack the archive on the host that will run the worker/API, then install the
wheel:

```bash
python -m venv .venv
source .venv/bin/activate
pip install dist/*.whl
```

Start at least one worker:

```bash
TEMPORAL_API_KEY=<temporal-cloud-api-key> ./scripts/run_temporal_cloud_worker.sh
```

Start the API where the frontend can reach it:

```bash
TEMPORAL_API_KEY=<temporal-cloud-api-key> ./scripts/run_temporal_cloud_api.sh
```

## Worker Deployment Version

The worker launcher sets these defaults:

```bash
TEMPORAL_WORKER_DEPLOYMENT_NAME=trusted-friends-demo
TEMPORAL_WORKER_BUILD_ID=latest-8b4f
TEMPORAL_WORKER_VERSIONING=true
```

After the worker starts polling, make that version current if the namespace is
using Worker Deployment routing:

```bash
temporal worker deployment set-current-version \
  --address tf-demo.zsvab.tmprl.cloud:7233 \
  --namespace tf-demo.zsvab \
  --api-key "$TEMPORAL_API_KEY" \
  --tls \
  --deployment-name trusted-friends-demo \
  --build-id "$TEMPORAL_WORKER_BUILD_ID"
```

## Smoke Check

With the worker and API running:

```bash
curl -X POST http://127.0.0.1:8000/trusted-friends/send \
  -H 'content-type: application/json' \
  -d '{"requester_user_id":"alice","target_user_id":"bob"}'
```

The workflow should appear in the Temporal Cloud `tf-demo.zsvab` namespace on task
queue `trusted-friends-demo`.
