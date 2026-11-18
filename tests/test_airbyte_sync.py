import pytest

from loaders.airbyte_sync import Airbyte, AirbyteError, Settings, main, sync

API = "https://api.airbyte.com/v1"


class FakeAirbyte:
    """Airbyte's public API as far as the sync uses it; records every call."""

    def __init__(self, job_states=("running", "running", "succeeded"), start_status=200, token_status=200):
        self.calls = []
        self.job_states = list(job_states)
        self.start_status = start_status
        self.token_status = token_status
        self.tokens = 0

    def __call__(self, method, url, headers, body):
        path = url.removeprefix(API)
        self.calls.append((method, path, dict(headers), body))
        if path == "/applications/token":
            self.tokens += 1
            if self.token_status != 200:
                return self.token_status, {"detail": "bad client"}
            return 200, {"access_token": f"t{self.tokens}"}
        if path == "/connections/c-1":
            return 200, {"connectionId": "c-1", "name": "Shop Postgres → Snowflake", "status": "active"}
        if path == "/jobs" and method == "POST":
            if self.start_status == 409:
                return 409, {"detail": "A sync is already running"}
            return self.start_status, {"jobId": 77, "status": "pending"}
        if path.startswith("/jobs?"):
            return 200, {"data": [{"jobId": 76, "status": "running"}]}
        if path == "/jobs/77" or path == "/jobs/76":
            state = self.job_states.pop(0) if len(self.job_states) > 1 else self.job_states[0]
            job_id = int(path.rsplit("/", 1)[1])
            return 200, {"jobId": job_id, "status": state, "rowsSynced": 1200, "bytesSynced": 34000}
        return 404, None


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


ENV = {"AIRBYTE_CONNECTION_ID": "c-1", "AIRBYTE_CLIENT_ID": "id", "AIRBYTE_CLIENT_SECRET": "secret"}


def test_settings_come_from_the_environment_and_nothing_is_configured_without_a_connection():
    assert Settings.from_env({}) is None
    s = Settings.from_env({**ENV, "AIRBYTE_API_URL": "https://airbyte.acme.com/api/public/v1/"})
    assert s.api_url == "https://airbyte.acme.com/api/public/v1"
    assert Settings.from_env(ENV).api_url == API
    with pytest.raises(AirbyteError, match="no credentials"):
        Settings.from_env({"AIRBYTE_CONNECTION_ID": "c-1"})


def test_a_sync_starts_waits_and_reports_what_it_moved():
    fake, clock, lines = FakeAirbyte(), Clock(), []
    job = sync(Settings.from_env(ENV), fake, clock.sleep, clock, lines.append)
    assert job["status"] == "succeeded"
    started = [c for c in fake.calls if c[0] == "POST" and c[1] == "/jobs"]
    assert started[0][3] == {"connectionId": "c-1", "jobType": "sync"}
    assert started[0][2]["Authorization"] == "Bearer t1"
    assert lines[0] == "airbyte: syncing Shop Postgres → Snowflake (job 77)"
    assert "1200 rows" in lines[-1]
    # The client secret only ever goes to the token endpoint.
    assert all("secret" not in str(c[3]) for c in fake.calls if c[1] != "/applications/token")


def test_the_access_token_is_renewed_before_it_expires():
    fake, clock = FakeAirbyte(job_states=["running"] * 30 + ["succeeded"]), Clock()
    sync(Settings.from_env(ENV), fake, clock.sleep, clock, lambda _: None)
    assert fake.tokens >= 3  # 30 polls of 15 s outlast several two-minute tokens


def test_a_running_sync_is_followed_rather_than_refused():
    fake, clock = FakeAirbyte(start_status=409), Clock()
    job = sync(Settings.from_env(ENV), fake, clock.sleep, clock, lambda _: None)
    assert job["jobId"] == 76


def test_a_failed_or_endless_sync_fails_the_step():
    clock = Clock()
    with pytest.raises(AirbyteError, match="ended failed"):
        sync(Settings.from_env(ENV), FakeAirbyte(job_states=["running", "failed"]), clock.sleep, clock, lambda _: None)
    clock = Clock()
    settings = Settings.from_env({**ENV, "AIRBYTE_SYNC_TIMEOUT_MINUTES": "1"})
    with pytest.raises(AirbyteError, match="still running after 1 minutes"):
        sync(settings, FakeAirbyte(job_states=["running"]), clock.sleep, clock, lambda _: None)


def test_refused_credentials_say_so_without_the_secret():
    clock = Clock()
    with pytest.raises(AirbyteError) as error:
        Airbyte(Settings.from_env(ENV), FakeAirbyte(token_status=401), clock).connection()
    assert "client id and secret" in str(error.value) and "secret'" not in str(error.value)


def test_main_exits_2_without_a_connection(monkeypatch):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)
    assert main([]) == 2
