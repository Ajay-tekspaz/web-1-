"""
Snowflake connection settings from environment variables, key-pair auth only.

    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
    SNOWFLAKE_ROLE (default DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE),
    SNOWFLAKE_WAREHOUSE (default DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS),
    SNOWFLAKE_DATABASE (default DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV),
    SNOWFLAKE_PRIVATE_KEY (PEM text) or SNOWFLAKE_PRIVATE_KEY_PATH, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE (optional)

Nothing here prints or logs a secret.
"""

from __future__ import annotations

import os
from typing import Any

DEFAULTS = {
    "role": "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_ROLE",
    "warehouse": "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_PIPELINE_XS",
    "database": "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV",
}


class MissingSnowflakeConfig(RuntimeError):
    pass


def _private_key_der(env: dict[str, str]) -> bytes:
    from cryptography.hazmat.primitives import serialization

    pem = env.get("SNOWFLAKE_PRIVATE_KEY")
    path = env.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if not pem and path:
        with open(path, "rb") as f:
            pem_bytes = f.read()
    elif pem:
        pem_bytes = pem.replace("\\n", "\n").encode()
    else:
        raise MissingSnowflakeConfig("set SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH (key-pair auth)")
    passphrase = env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None
    key = serialization.load_pem_private_key(pem_bytes, password=passphrase.encode() if passphrase else None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connection_kwargs(env: dict[str, str] | None = None) -> dict[str, Any]:
    """kwargs for snowflake.connector.connect(), built from the environment."""
    env = dict(os.environ if env is None else env)
    missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER") if not env.get(k)]
    if missing:
        raise MissingSnowflakeConfig(f"missing environment variables: {', '.join(missing)}")
    return {
        "account": env["SNOWFLAKE_ACCOUNT"],
        "user": env["SNOWFLAKE_USER"],
        "role": env.get("SNOWFLAKE_ROLE", DEFAULTS["role"]),
        "warehouse": env.get("SNOWFLAKE_WAREHOUSE", DEFAULTS["warehouse"]),
        "database": env.get("SNOWFLAKE_DATABASE", DEFAULTS["database"]),
        "private_key": _private_key_der(env),
    }
