"""Application configuration and API authentication helpers."""

import os
import secrets

APP_VERSION = os.getenv("JAVDB_SPIDER_VERSION", "1.10.0")
AUTH_HEADER = "X-JavDB-Token"
PUBLIC_API_PATHS = {"/api/version"}


def _env_truthy(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def is_auth_required() -> bool:
    return _env_truthy("JAVDB_AUTH_REQUIRED") or bool(os.getenv("JAVDB_AUTH_TOKEN", "").strip())


def is_api_authorized(provided_token: str | None) -> bool:
    expected_token = os.getenv("JAVDB_AUTH_TOKEN", "").strip()
    if not is_auth_required():
        return True
    if not expected_token or not provided_token:
        return False
    return secrets.compare_digest(provided_token, expected_token)
