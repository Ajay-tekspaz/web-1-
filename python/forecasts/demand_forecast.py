"""
[Issue K-16] Weekly demand forecast per store x SKU (Holt-Winters / ETS, statsmodels).

Method (chosen by the lab because it ports to TypeScript; Prophet is only a benchmark):
  * one model per store x SKU on *daily* units sold, zero-filled
  * additive Holt-Winters: damped additive trend + additive weekly seasonality (period 7)
  * the point forecast is summed into Monday-start weeks; the 80% band is the 10th/90th
    percentile of weekly sums over simulated future paths
  * accuracy: refit on everything except the last `holdout_weeks`, forecast them, and report
    MAPE over store x SKU x week (weeks with zero actual sales are skipped)

Reads GOLD.FCT_SALES_DAILY, writes ML.DEMAND_FORECAST_OUTPUT (dbt's fct_demand_forecast reads it).

Usage:
    python -m forecasts.demand_forecast --db_path data/lakehouse.duckdb   # local DuckDB
    python -m forecasts.demand_forecast --target snowflake [--apply]      # DAILY_EXPENSE_TRACKER_FINTECH_APPLICATION_SANDBOX
The Snowflake target is a dry run unless --apply is given.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

MODEL_NAME = "holt_winters_add_damped_s7"
OUTPUT_COLUMNS = ["forecast_week", "store_id", "sku", "forecast_units", "lower_80", "upper_80",
                  "model_name", "holdout_mape", "trained_through", "generated_at"]
SALES_SQL = "select sale_date, store_id, sku, units_sold from gold.fct_sales_daily"


@dataclass(frozen=True)
class ForecastConfig:
    horizon_weeks: int = 13
    holdout_weeks: int = 4
    simulations: int = 500
    interval: float = 0.80
    seasonal_periods: int = 7
    seed: int = 42


# ------------------------------------------------------------------- shaping
def daily_series(sales: pd.DataFrame) -> tuple[dict[tuple[int, str], pd.Series], pd.Timestamp]:
    """Zero-filled daily series per (store_id, sku), truncated to end on the last full week (Sunday)."""
    sales = sales.copy()
    sales["sale_date"] = pd.to_datetime(sales["sale_date"])
    last = sales["sale_date"].max()
    last_sunday = last - pd.Timedelta(days=(last.weekday() + 1) % 7)
    first = sales["sale_date"].min()
    index = pd.date_range(first, last_sunday, freq="D")
    pivot = (sales[sales["sale_date"] <= last_sunday]
             .pivot_table(index="sale_date", columns=["store_id", "sku"], values="units_sold", aggfunc="sum")
             .reindex(index).fillna(0.0))
    series = {(int(store), str(sku)): pivot[(store, sku)].astype(float) for store, sku in pivot.columns}
    return series, last_sunday


def weekly_sums(daily: np.ndarray) -> np.ndarray:
    """Sum consecutive 7-day blocks along axis 0 (len must be a multiple of 7)."""
    return daily.reshape(-1, 7, *daily.shape[1:]).sum(axis=1)


# -------------------------------------------------------------------- model
def fit(y: pd.Series, cfg: ForecastConfig):
    from statsmodels.tsa.holtwinters import ExponentialSmoothing

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # convergence chatter on short or flat series
        model = ExponentialSmoothing(
            y.to_numpy(), trend="add", damped_trend=True, seasonal="add",
            seasonal_periods=cfg.seasonal_periods, initialization_method="estimated",
        )
        return model.fit(optimized=True)


def forecast_weeks(y: pd.Series, weeks: int, cfg: ForecastConfig,
                   rng: np.random.RandomState | None = None) -> dict[str, np.ndarray]:
    """Point forecast per future week, plus the band when an rng is given; all clipped at zero."""
    days = weeks * 7
    res = fit(y, cfg)
    point = weekly_sums(np.clip(res.forecast(days), 0, None))
    if rng is None:
        return {"point": point}
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        sims = res.simulate(days, repetitions=cfg.simulations, error="add", random_state=rng)
    sims = weekly_sums(np.clip(np.asarray(sims).reshape(days, -1), 0, None))
    tail = (1 - cfg.interval) / 2
    lower = np.quantile(sims, tail, axis=1)
    upper = np.quantile(sims, 1 - tail, axis=1)
    return {"point": point, "lower": np.minimum(lower, point), "upper": np.maximum(upper, point)}


def holdout_errors(y: pd.Series, cfg: ForecastConfig) -> np.ndarray:
    """Absolute percentage error per holdout week (NaN where the actual week had no sales)."""
    n = cfg.holdout_weeks * 7
    actual = weekly_sums(y.to_numpy()[-n:])
    predicted = forecast_weeks(y.iloc[:-n], cfg.holdout_weeks, cfg)["point"]
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(actual > 0, np.abs(actual - predicted) / actual, np.nan)


def _rng(cfg: ForecastConfig, store_id: int, sku: str) -> np.random.RandomState:
    """Per-series seed, so results don't depend on which other series ran (statsmodels needs RandomState)."""
    sku_number = int("".join(ch for ch in sku if ch.isdigit()) or 0)
    return np.random.RandomState((cfg.seed * 1_000_003 + store_id * 10_007 + sku_number) % 2**32)


def run(sales: pd.DataFrame, cfg: ForecastConfig, generated_at: datetime | None = None,
        max_series: int | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Forecast every store x SKU. Returns (output rows, accuracy metrics)."""
    series, trained_through = daily_series(sales)
    min_days = (cfg.holdout_weeks + 4) * 7
    generated_at = generated_at or datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    keys = sorted(series)[:max_series] if max_series else sorted(series)

    rows, errors, wape_num, wape_den = [], [], 0.0, 0.0
    for store_id, sku in keys:
        y = series[(store_id, sku)]
        if len(y) < min_days or y.sum() == 0:
            continue
        ape = holdout_errors(y, cfg)
        series_mape = float(np.nanmean(ape)) if np.isfinite(ape).any() else None
        errors.extend(ape[np.isfinite(ape)].tolist())
        actual = weekly_sums(y.to_numpy()[-cfg.holdout_weeks * 7:])
        wape_num += float(np.nansum(ape * actual))
        wape_den += float(actual.sum())

        fc = forecast_weeks(y, cfg.horizon_weeks, cfg, _rng(cfg, store_id, sku))
        for w in range(cfg.horizon_weeks):
            rows.append({
                "forecast_week": (trained_through + pd.Timedelta(days=1 + 7 * w)).date(),
                "store_id": store_id,
                "sku": sku,
                "forecast_units": round(float(fc["point"][w]), 2),
                "lower_80": round(float(fc["lower"][w]), 2),
                "upper_80": round(float(fc["upper"][w]), 2),
                "model_name": MODEL_NAME,
                "holdout_mape": None if series_mape is None else round(series_mape, 4),
                "trained_through": trained_through.date(),
                "generated_at": generated_at,
            })

    output = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    metrics = {
        "model": MODEL_NAME,
        "config": asdict(cfg),
        "trained_through": str(trained_through.date()),
        "series": int(output[["store_id", "sku"]].drop_duplicates().shape[0]) if len(output) else 0,
        "rows": int(len(output)),
        "holdout_mape": round(float(np.mean(errors)), 4) if errors else None,
        "holdout_mape_median": round(float(np.median(errors)), 4) if errors else None,
        "holdout_wape": round(wape_num / wape_den, 4) if wape_den else None,
        "holdout_points": len(errors),
    }
    return output, metrics


# ----------------------------------------------------------- prophet benchmark
def prophet_benchmark(sales: pd.DataFrame, cfg: ForecastConfig, max_series: int = 30) -> float | None:
    """Holdout MAPE of Prophet on a sample of series, for comparison only (pip install prophet)."""
    try:
        from prophet import Prophet
    except ImportError:
        return None
    series, _ = daily_series(sales)
    n = cfg.holdout_weeks * 7
    errors = []
    for key in sorted(series)[:max_series]:
        y = series[key]
        train = pd.DataFrame({"ds": y.index[:-n], "y": y.to_numpy()[:-n]})
        model = Prophet(weekly_seasonality=True, yearly_seasonality=False, daily_seasonality=False)
        model.fit(train)
        pred = model.predict(model.make_future_dataframe(n, include_history=False))["yhat"].clip(lower=0)
        actual = weekly_sums(y.to_numpy()[-n:])
        predicted = weekly_sums(pred.to_numpy())
        errors.extend((np.abs(actual - predicted) / actual)[actual > 0].tolist())
    return round(float(np.mean(errors)), 4) if errors else None


# ------------------------------------------------------------------------ IO
def write_duckdb(output: pd.DataFrame, db_path: Path) -> None:
    import duckdb

    with duckdb.connect(str(db_path)) as con:
        con.execute("create schema if not exists ml")
        con.register("forecast_df", output)
        con.execute("create or replace table ml.demand_forecast_output as select * from forecast_df")


def snowflake_statements() -> list[str]:
    return [
        "CREATE SCHEMA IF NOT EXISTS ML",
        "CREATE OR REPLACE TABLE ML.DEMAND_FORECAST_OUTPUT (FORECAST_WEEK DATE, STORE_ID INTEGER, SKU VARCHAR, "
        "FORECAST_UNITS FLOAT, LOWER_80 FLOAT, UPPER_80 FLOAT, MODEL_NAME VARCHAR, HOLDOUT_MAPE FLOAT, "
        "TRAINED_THROUGH DATE, GENERATED_AT TIMESTAMP_NTZ)",
    ]


def write_snowflake(output: pd.DataFrame, execute: Callable[[str], Any],
                    insert_rows: Callable[[str, Iterable[tuple], list[str]], Any]) -> None:
    """Write through any executor, e.g. TrackedSnowflakeHook.run / .insert_rows in Airflow."""
    def sql_value(v: Any) -> Any:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return v.isoformat() if hasattr(v, "isoformat") else v

    for stmt in snowflake_statements():
        execute(stmt)
    rows = [tuple(sql_value(v) for v in r) for r in output[OUTPUT_COLUMNS].itertuples(index=False)]
    insert_rows("ML.DEMAND_FORECAST_OUTPUT", rows, [c.upper() for c in OUTPUT_COLUMNS])


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    parser = argparse.ArgumentParser(description="Holt-Winters weekly demand forecast")
    parser.add_argument("--target", choices=["duckdb", "snowflake"], default="duckdb")
    parser.add_argument("--db_path", type=Path, default=Path("data/lakehouse.duckdb"))
    parser.add_argument("--apply", action="store_true", help="snowflake only: write results (default: dry run)")
    parser.add_argument("--max_series", type=int, default=None, help="limit series (smoke tests)")
    parser.add_argument("--metrics_path", type=Path, default=Path("data/forecast_metrics.json"))
    parser.add_argument("--benchmark_prophet", action="store_true")
    args = parser.parse_args(argv)
    cfg = ForecastConfig()

    if args.target == "duckdb":
        import duckdb

        with duckdb.connect(str(args.db_path), read_only=True) as con:
            sales = con.execute(SALES_SQL).df()
    else:
        import snowflake.connector

        from lakehouse.snowflake_auth import connection_kwargs

        conn = snowflake.connector.connect(**connection_kwargs(), session_parameters={
            "QUERY_TAG": '{"project":"daily-expense-tracker-fintech-application","environment":"dev","tool":"forecast"}'})
        cursor = conn.cursor()
        cursor.execute(SALES_SQL)
        sales = cursor.fetch_pandas_all()
        sales.columns = [c.lower() for c in sales.columns]

    output, metrics = run(sales, cfg, max_series=args.max_series)
    if args.benchmark_prophet:
        metrics["prophet_benchmark_mape"] = prophet_benchmark(sales, cfg)

    if args.target == "duckdb":
        write_duckdb(output, args.db_path)
        print(f"[ok] wrote {len(output):,} rows to ml.demand_forecast_output in {args.db_path}")
    elif args.apply:
        cur = conn.cursor()
        write_snowflake(output, cur.execute,
                        lambda table, rows, cols: cur.executemany(
                            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
                            list(rows)))
        print(f"[ok] wrote {len(output):,} rows to ML.DEMAND_FORECAST_OUTPUT")
    else:
        print("-- DRY RUN: would run\n" + ";\n".join(snowflake_statements()) +
              f";\n-- and insert {len(output):,} rows. Re-run with --apply to write.")

    args.metrics_path.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_path.write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
