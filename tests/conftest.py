from __future__ import annotations

import os


# Tests should not inherit local developer Cloud credentials or worker
# versioning flags from a repo-local .env file.
os.environ.setdefault("TRUSTED_FRIENDS_LOAD_ENV_FILE", "false")
