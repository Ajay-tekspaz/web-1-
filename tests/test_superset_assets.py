"""The dashboards in superset/assets must import cleanly on a fresh Superset (K-13)."""

import importlib.util
import json
import zipfile
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "superset" / "assets"
spec = importlib.util.spec_from_file_location("import_dashboards", ROOT / "superset" / "import_dashboards.py")
importer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(importer)


def _load(sub):
    return {f.name: yaml.safe_load(f.read_text(encoding="utf-8")) for f in sorted((ASSETS / sub).rglob("*.yaml"))}


def test_two_dashboards_on_gold_only():
    dashboards = _load("dashboards")
    assert {d["dashboard_title"] for d in dashboards.values()} == {"Sales performance", "Inventory health"}
    assert {d["schema"] for d in _load("datasets").values()} == {"gold"}


def test_every_layout_slot_points_at_an_exported_chart():
    # Superset maps layout slots to charts by uuid on import; a missing uuid scrambles the layout
    charts = {c["uuid"] for c in _load("charts").values()}
    for name, d in _load("dashboards").items():
        slots = [n for n in d["position"].values() if isinstance(n, dict) and n.get("type") == "CHART"]
        assert slots, name
        assert all(s["meta"].get("uuid") in charts for s in slots), name
        assert len({s["meta"]["uuid"] for s in slots}) == len(slots), f"{name}: chart placed twice"


def test_no_local_paths_or_secrets_in_assets():
    text = "\n".join(f.read_text(encoding="utf-8") for f in ASSETS.rglob("*.yaml"))
    assert ":/" not in text.replace("duckdb:////app/", "").replace("://", ""), "absolute local path in assets"
    assert "PRIVATE KEY" not in text and "password" not in text.lower()


def test_snowflake_connection_is_read_only_role_with_query_tag():
    settings = importer.database_settings("snowflake", "", {"SNOWFLAKE_ACCOUNT": "acme-dev"})
    uri = settings["sqlalchemy_uri"]
    assert "role=DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_READER" in uri and "warehouse=DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_BI_XS" in uri
    tag = json.loads(settings["extra"]["engine_params"]["connect_args"]["session_parameters"]["QUERY_TAG"])
    assert tag["tool"] == "superset"


def test_bundle_points_database_at_target(tmp_path):
    bundle = importer.build_bundle(tmp_path / "b.zip", importer.database_settings("duckdb", "duckdb:////x.db", {}))
    with zipfile.ZipFile(bundle) as zf:
        names = zf.namelist()
        db = yaml.safe_load(zf.read("dashboard_export/databases/Retail_Lakehouse.yaml"))
    assert "dashboard_export/metadata.yaml" in names
    assert db["sqlalchemy_uri"] == "duckdb:////x.db"
