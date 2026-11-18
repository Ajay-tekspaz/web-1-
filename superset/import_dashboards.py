"""
Recreate the K-13 dashboards from the YAML in superset/assets on any Superset (4.1).

    # local DuckDB stand-in (path as Superset sees it)
    python superset/import_dashboards.py --url http://localhost:8088 --duckdb-uri duckdb:////app/data/lakehouse.duckdb

    # Snowflake DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX through the read-only BI role
    # (snowflake/migrations/003_bi_readonly_role.sql)
    SNOWFLAKE_ACCOUNT=... SUPERSET_SNOWFLAKE_PRIVATE_KEY_PATH=.secrets/superset_svc.p8 \
    python superset/import_dashboards.py --url http://localhost:8088 --target snowflake

    # only build the bundle (docker-init.sh imports it with the superset CLI)
    python superset/import_dashboards.py --zip-only /tmp/dashboards.zip

The YAML is Superset's own export format (Dashboards > Export). After changing a dashboard in the
UI, export it again and replace the files under superset/assets/.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

import yaml

ASSETS = Path(__file__).resolve().parent / "assets"
DATABASE_FILE = "databases/Retail_Lakehouse.yaml"
QUERY_TAG = {"tool": "superset", "project": "daily-expense-tracker-fintech-application", "environment": "dev"}


def database_settings(target: str, duckdb_uri: str, env: dict[str, str]) -> dict:
    """sqlalchemy_uri + extra for the single 'Daily Expense Tracker Fintech Application' connection."""
    if target == "duckdb":
        return {"sqlalchemy_uri": duckdb_uri,
                "extra": {"engine_params": {"connect_args": {"read_only": True}}, "metadata_params": {},
                          "schemas_allowed_for_file_upload": []}}
    account = env.get("SNOWFLAKE_ACCOUNT")
    if not account:
        raise SystemExit("SNOWFLAKE_ACCOUNT is required for --target snowflake")
    user = env.get("SUPERSET_SNOWFLAKE_USER", "DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SUPERSET")
    uri = (f"snowflake://{user}@{account}/DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_DEV/GOLD"
           "?role=DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_READER&warehouse=DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_XS")
    return {
        "sqlalchemy_uri": uri,
        "extra": {
            # Every Superset query is tagged so TeksOptima can attribute BI cost to the tool
            "engine_params": {"connect_args": {"session_parameters": {"QUERY_TAG": json.dumps(QUERY_TAG)}}},
            "metadata_params": {}, "schemas_allowed_for_file_upload": [],
        },
    }


def build_bundle(out_zip: Path, settings: dict) -> Path:
    """Copy assets, point the database at the target, zip in Superset's import layout."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "dashboard_export"
        shutil.copytree(ASSETS, root)
        db_file = root / DATABASE_FILE
        db = yaml.safe_load(db_file.read_text(encoding="utf-8"))
        db.update(settings)
        db_file.write_text(yaml.safe_dump(db, sort_keys=False), encoding="utf-8")
        with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in sorted(root.rglob("*.yaml")):
                zf.write(f, f.relative_to(root.parent).as_posix())
    return out_zip


def api_session(url: str, username: str, password: str):
    import requests

    s = requests.Session()
    r = s.post(f"{url}/api/v1/security/login",
               json={"username": username, "password": password, "provider": "db", "refresh": True})
    r.raise_for_status()
    s.headers.update({"Authorization": f"Bearer {r.json()['access_token']}", "Referer": url})
    s.headers["X-CSRFToken"] = s.get(f"{url}/api/v1/security/csrf_token/").json()["result"]
    return s


def import_bundle(url: str, username: str, password: str, bundle: Path, snowflake_key: str | None) -> list[str]:
    s = api_session(url, username, password)
    with bundle.open("rb") as f:
        r = s.post(f"{url}/api/v1/dashboard/import/", files={"formData": (bundle.name, f, "application/zip")},
                   data={"overwrite": "true"})
    if r.status_code >= 400:
        raise SystemExit(f"import failed: {r.status_code} {r.text[:1000]}")

    if snowflake_key:  # key-pair auth is stored encrypted on the connection, never in the YAML
        dbs = s.get(f"{url}/api/v1/database/", params={"q": "(page_size:100)"}).json()["result"]
        db_id = next(d["id"] for d in dbs if d["database_name"] == "Daily Expense Tracker Fintech Application")
        r = s.put(f"{url}/api/v1/database/{db_id}", json={"masked_encrypted_extra": json.dumps(
            {"auth_method": "keypair", "auth_params": {"privatekey_body": snowflake_key, "privatekey_pass": ""}})})
        r.raise_for_status()

    dashboards = s.get(f"{url}/api/v1/dashboard/", params={"q": "(page_size:100)"}).json()["result"]
    return sorted(d["dashboard_title"] for d in dashboards)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default=os.environ.get("SUPERSET_URL", "http://localhost:8088"))
    parser.add_argument("--username", default=os.environ.get("SUPERSET_ADMIN_USER", "admin"))
    parser.add_argument("--password", default=os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin"))
    parser.add_argument("--target", choices=["duckdb", "snowflake"], default="duckdb")
    parser.add_argument("--duckdb-uri", default="duckdb:////app/data/lakehouse.duckdb")
    parser.add_argument("--zip-only", type=Path, help="write the bundle here and stop")
    args = parser.parse_args(argv)

    settings = database_settings(args.target, args.duckdb_uri, dict(os.environ))
    if args.zip_only:
        build_bundle(args.zip_only, settings)
        print(f"[ok] bundle written to {args.zip_only}")
        return 0

    key = None
    if args.target == "snowflake":
        key_path = os.environ.get("SUPERSET_SNOWFLAKE_PRIVATE_KEY_PATH")
        if not key_path:
            raise SystemExit("SUPERSET_SNOWFLAKE_PRIVATE_KEY_PATH is required for --target snowflake")
        key = Path(key_path).read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory() as tmp:
        bundle = build_bundle(Path(tmp) / "dashboards.zip", settings)
        titles = import_bundle(args.url, args.username, args.password, bundle, key)
    print(f"[ok] imported into {args.url}: {', '.join(titles)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
