"""Healthcheck entrypoint for containers."""

from __future__ import annotations

import json
import sys

from .bootstrap import boot_or_raise


def main() -> int:
    try:
        context = boot_or_raise()
        payload = {
            "status": "ok",
            "environment": context.environment,
            "mode": context.operating_mode.value,
            "redis_prefix": context.redis_prefix,
            "db_schema": context.db_schema,
        }
        print(json.dumps(payload))
        return 0
    except Exception as exc:  # pragma: no cover - exercised by container healthcheck only
        print(json.dumps({"status": "error", "detail": str(exc)}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
