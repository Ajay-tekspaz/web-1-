"""
Run the whole lakehouse locally, end to end, on the DuckDB stand-in:

    generate -> load raw -> dbt build (staging + marts) -> forecast -> dbt build (forecast)

The same steps run in CI (dbt Test workflow) and in the Airflow DAG. With --incremental_check the
raw layer is first loaded only up to a cut-off date and built, then fully loaded and built again,
which proves the incremental marts pick up late-arriving rows.

Usage (from the repo root):
    python scripts/run_pipeline.py                         # full-size data (26 weeks, 10 stores, 50 SKUs)
    python scripts/run_pipeline.py --small                 # CI size (14 weeks, 3 stores, 12 SKUs)
    python scripts/run_pipeline.py --incremental_check
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from data_generation.deterministic_generator import GeneratorConfig, generate  # noqa: E402
from loaders import duckdb_loader  # noqa: E402

SMALL = {"days": 98, "num_stores": 3, "num_products": 12, "renamed_column_day": 60}


def step(name: str) -> float:
    print(f"\n=== {name}", flush=True)
    return time.monotonic()


def dbt(args: list[str], env: dict[str, str], save_as: str | None = None) -> None:
    """Run dbt from dbt/; keep a copy of run_results.json under data/artifacts/ as run evidence."""
    exe = shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt") or "dbt"
    run_results = ROOT / "dbt" / "target" / "run_results.json"
    run_results.unlink(missing_ok=True)  # never report an earlier run's results as this one's
    result = subprocess.run([exe, *args], cwd=ROOT / "dbt", env=env)
    if save_as and run_results.exists():
        artifacts = ROOT / "data" / "artifacts"
        artifacts.mkdir(parents=True, exist_ok=True)
        shutil.copy(run_results, artifacts / f"run_results_{save_as}.json")
    if result.returncode != 0:
        raise SystemExit(f"dbt {' '.join(args)} failed with exit code {result.returncode}")


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--small", action="store_true", help="small dataset for CI")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--data_dir", type=Path, default=ROOT / "data" / "generated")
    parser.add_argument("--db_path", type=Path, default=ROOT / "data" / "lakehouse.duckdb")
    parser.add_argument("--incremental_check", action="store_true")
    parser.add_argument("--skip_generate", action="store_true", help="reuse files already in --data_dir")
    args = parser.parse_args(argv)

    env = {**os.environ, "DBT_TARGET": "local", "DBT_PROFILES_DIR": str(ROOT / "dbt"),
           "LAKEHOUSE_DUCKDB_PATH": str(args.db_path.resolve()), "PYTHONIOENCODING": "utf-8"}
    cfg = GeneratorConfig(seed=args.seed, **(SMALL if args.small else {}))
    started = time.monotonic()

    if not args.skip_generate:
        step(f"generate (seed={cfg.seed}, {cfg.days} days, {cfg.num_stores} stores, {cfg.num_products} SKUs)")
        if args.data_dir.exists():
            shutil.rmtree(args.data_dir)
        truth = generate(cfg, args.data_dir)
        print(json.dumps(truth.row_counts), json.dumps(truth.defects))

    if args.db_path.exists():
        args.db_path.unlink()
    step("dbt deps")
    dbt(["deps", "--quiet"], env)

    if args.incremental_check:
        cutoff = cfg.end_date() - timedelta(days=14)
        step(f"load raw through {cutoff} and build (first, partial run)")
        duckdb_loader.load(args.data_dir, args.db_path, through=cutoff)
        dbt(["build", "--exclude", "tag:forecast"], env, save_as="partial")

    step("load raw (all files)")
    counts = duckdb_loader.load(args.data_dir, args.db_path)
    print(json.dumps(counts))

    step("dbt build: staging + marts" + (" (incremental)" if args.incremental_check else ""))
    dbt(["build", "--exclude", "tag:forecast"], env, save_as="marts")

    step("forecast (Holt-Winters)")
    subprocess.run([sys.executable, "-m", "forecasts.demand_forecast", "--db_path", str(args.db_path),
                    "--metrics_path", str(args.db_path.parent / "forecast_metrics.json")],
                   cwd=ROOT, env={**env, "PYTHONPATH": str(ROOT / "python")}, check=True)

    step("dbt build: forecast")
    dbt(["build", "--select", "tag:forecast"], env, save_as="forecast")

    print(f"\n[ok] pipeline finished in {time.monotonic() - started:.0f}s; database: {args.db_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
