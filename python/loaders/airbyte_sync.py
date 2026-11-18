"""
Raw data from the customer's own Airbyte: start a sync of one connection and wait for it to finish.

Where the customer already runs Airbyte (Cloud or self-hosted), its connection loads the raw tables into the
project's raw schema (docs/ingestion.md, "Your own Airbyte") and the pipeline only has to start it and wait:
the dbt build that follows reads what it landed. Nothing here touches Snowflake.

The platform that deploys a project writes into the CI environment (and the hosted Airflow passes the same):
    AIRBYTE_CONNECTION_ID   the connection to sync
    AIRBYTE_API_URL         https://api.airbyte.com/v1 (Cloud, the default) or https://<host>/api/public/v1
    AIRBYTE_CLIENT_ID       an Airbyte application's client id and secret (user settings > Applications); each
    AIRBYTE_CLIENT_SECRET   call gets a fresh access token, since Airbyte's expire within minutes
    AIRBYTE_TOKEN           or an access token instead (only for short runs: it isn't renewed)
    AIRBYTE_SYNC_TIMEOUT_MINUTES   how long to wait (default 180)

Standard library only, so it runs before the project's requirements matter.

Usage:
    python -m loaders.airbyte_sync            # sync AIRBYTE_CONNECTION_ID and wait
    python -m loaders.airbyte_sync --check    # only check the credentials and the connection
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

CLOUD_API = "https://api.airbyte.com/v1"
DONE_OK = {"succeeded"}
DONE_BAD = {"failed", "cancelled", "incomplete"}
POLL_SECONDS = 15
TOKEN_SECONDS = 120  # Airbyte's access tokens last about three minutes: renew well before

# (method, url, headers, body) -> (status, parsed JSON or None)
Http = Callable[[str, str, Mapping[str, str], Any], "tuple[int, Any]"]


class AirbyteError(Exception):
    """The sync couldn't start or didn't succeed; the message says why, without any credential."""


@dataclass(frozen=True)
class Settings:
    api_url: str
    connection_id: str
    client_id: str | None = None
    client_secret: str | None = None
    token: str | None = None
    timeout_minutes: float = 180

    @staticmethod
    def from_env(env: Mapping[str, str] | None = None) -> "Settings | None":
        """None when no Airbyte connection is configured: the pipeline then loads its raw data another way."""
        env = os.environ if env is None else env
        connection_id = (env.get("AIRBYTE_CONNECTION_ID") or "").strip()
        if not connection_id:
            return None
        client_id = (env.get("AIRBYTE_CLIENT_ID") or "").strip() or None
        client_secret = (env.get("AIRBYTE_CLIENT_SECRET") or "").strip() or None
        token = (env.get("AIRBYTE_TOKEN") or "").strip() or None
        if not token and not (client_id and client_secret):
            raise AirbyteError(
                "AIRBYTE_CONNECTION_ID is set, but there are no credentials: "
                "set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET (or AIRBYTE_TOKEN)."
            )
        minutes = float(env.get("AIRBYTE_SYNC_TIMEOUT_MINUTES") or 180)
        api_url = ((env.get("AIRBYTE_API_URL") or "").strip() or CLOUD_API).rstrip("/")
        if not api_url.startswith(("https://", "http://")):
            raise AirbyteError("AIRBYTE_API_URL must start with https://")
        return Settings(api_url, connection_id, client_id, client_secret, token, minutes)


def urllib_http(method: str, url: str, headers: Mapping[str, str], body: Any) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    content = {"Content-Type": "application/json"} if data else {}
    request = urllib.request.Request(
        url, data=data, method=method, headers={"Accept": "application/json", **content, **headers}
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            return error.code, json.loads(raw) if raw else None
        except ValueError:
            return error.code, None


def _detail(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("detail", "message", "title"):
            if isinstance(payload.get(key), str):
                return payload[key][:300]
    return ""


class Airbyte:
    def __init__(self, settings: Settings, http: Http = urllib_http, clock: Callable[[], float] = time.monotonic):
        self.settings = settings
        self.http = http
        self.clock = clock
        self._token: str | None = settings.token
        self._token_at = clock() if settings.token else -1e9

    def _bearer(self) -> str:
        s = self.settings
        if s.client_id and s.client_secret and (self._token is None or self.clock() - self._token_at > TOKEN_SECONDS):
            grant = {"client_id": s.client_id, "client_secret": s.client_secret, "grant-type": "client_credentials"}
            status, payload = self.http("POST", f"{s.api_url}/applications/token", {}, grant)
            token = payload.get("access_token") if isinstance(payload, dict) else None
            if status != 200 or not token:
                message = f"Airbyte refused the application's client id and secret ({status}). {_detail(payload)}"
                raise AirbyteError(message.strip())
            self._token, self._token_at = token, self.clock()
        assert self._token
        return self._token

    def call(self, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        auth = {"Authorization": f"Bearer {self._bearer()}"}
        status, payload = self.http(method, f"{self.settings.api_url}{path}", auth, body)
        if status in (401, 403):
            refused = "the credentials were refused or can't use this connection"
            raise AirbyteError(f"Airbyte answered {status}: {refused}. {_detail(payload)}".strip())
        return status, payload

    def connection(self) -> dict:
        status, payload = self.call("GET", f"/connections/{self.settings.connection_id}")
        if status == 404:
            connection_id = self.settings.connection_id
            raise AirbyteError(f"Airbyte has no connection {connection_id} that these credentials can see.")
        if status != 200 or not isinstance(payload, dict):
            raise AirbyteError(f"Airbyte answered {status} for the connection. {_detail(payload)}".strip())
        return payload

    def start(self) -> int:
        """Starts a sync, or follows the one already running (Airbyte runs one at a time per connection)."""
        status, payload = self.call("POST", "/jobs", {"connectionId": self.settings.connection_id, "jobType": "sync"})
        if status in (200, 201) and isinstance(payload, dict) and payload.get("jobId") is not None:
            return int(payload["jobId"])
        if status == 409:
            running = self.running_job()
            if running is not None:
                return running
        raise AirbyteError(f"Airbyte didn't start the sync ({status}). {_detail(payload)}".strip())

    def running_job(self) -> int | None:
        query = f"connectionId={self.settings.connection_id}&jobType=sync&limit=5&orderBy=createdAt|DESC"
        status, payload = self.call("GET", f"/jobs?{query}")
        jobs = payload.get("data", []) if status == 200 and isinstance(payload, dict) else []
        for job in jobs:
            if job.get("status") in ("pending", "running"):
                return int(job["jobId"])
        return None

    def wait(
        self, job_id: int, sleep: Callable[[float], None] = time.sleep, log: Callable[[str], None] = print
    ) -> dict:
        deadline = self.clock() + self.settings.timeout_minutes * 60
        last = None
        while True:
            status, payload = self.call("GET", f"/jobs/{job_id}")
            if status != 200 or not isinstance(payload, dict):
                raise AirbyteError(f"Airbyte answered {status} for sync job {job_id}. {_detail(payload)}".strip())
            state = payload.get("status")
            if state != last:
                log(f"airbyte job {job_id}: {state}")
                last = state
            if state in DONE_OK:
                return payload
            if state in DONE_BAD:
                raise AirbyteError(
                    f"The Airbyte sync (job {job_id}) ended {state}. "
                    "Its logs are in Airbyte, under the connection's Job history."
                )
            if self.clock() > deadline:
                raise AirbyteError(
                    f"The Airbyte sync (job {job_id}) was still {state} after "
                    f"{self.settings.timeout_minutes:g} minutes. It goes on in Airbyte; this run stops waiting."
                )
            sleep(POLL_SECONDS)


def sync(
    settings: Settings,
    http: Http = urllib_http,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    log: Callable[[str], None] = print,
) -> dict:
    airbyte = Airbyte(settings, http, clock)
    name = airbyte.connection().get("name") or settings.connection_id
    job_id = airbyte.start()
    log(f"airbyte: syncing {name} (job {job_id})")
    job = airbyte.wait(job_id, sleep, log)
    log(f"airbyte: {name} synced: {job.get('rowsSynced', '?')} rows, {job.get('bytesSynced', '?')} bytes")
    return job


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync the customer's Airbyte connection and wait for it")
    parser.add_argument("--check", action="store_true", help="only check the credentials and the connection")
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        if settings is None:
            print("AIRBYTE_CONNECTION_ID isn't set: nothing to sync.", file=sys.stderr)
            return 2
        if args.check:
            connection = Airbyte(settings).connection()
            name = connection.get("name") or settings.connection_id
            print(f"airbyte: {name} is reachable ({connection.get('status', 'unknown status')})")
            return 0
        sync(settings)
        return 0
    except AirbyteError as error:
        print(f"airbyte: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
