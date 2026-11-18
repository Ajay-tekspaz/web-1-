import hashlib
import os
import subprocess
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data_generation.deterministic_generator import GeneratorConfig, generate
from conftest import SMALL

ROOT = Path(__file__).resolve().parents[1]


def _digest(folder: Path) -> dict[str, str]:
    return {p.relative_to(folder).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(folder.rglob("*")) if p.is_file()}


def _orders(folder: Path) -> pd.DataFrame:
    frames = []
    for f in sorted((folder / "orders").glob("*.csv")):
        df = pd.read_csv(f, dtype={"customer_id": str})
        frames.append(df.rename(columns={"order_ts": "order_timestamp"}))
    return pd.concat(frames, ignore_index=True)


def _lines(folder: Path) -> pd.DataFrame:
    return pd.concat([pd.read_csv(f) for f in sorted((folder / "order_lines").glob("*.csv"))], ignore_index=True)


def _inventory(folder: Path) -> pd.DataFrame:
    return pd.concat([pd.read_csv(f) for f in sorted((folder / "inventory").glob("*.csv"))], ignore_index=True)


def test_same_seed_gives_byte_identical_files(tmp_path, small_data):
    folder, _ = small_data
    generate(SMALL, tmp_path / "again")
    assert _digest(folder) == _digest(tmp_path / "again")


def test_different_seed_gives_different_data(tmp_path, small_data):
    folder, _ = small_data
    generate(replace(SMALL, seed=SMALL.seed + 1), tmp_path / "other")
    name = "orders/orders_2026-05-01.csv"
    assert _digest(folder)[name] != _digest(tmp_path / "other")[name]


def test_order_totals_are_lines_minus_promotions(small_data):
    folder, _ = small_data
    orders, lines = _orders(folder).drop_duplicates("order_id"), _lines(folder)
    products = pd.read_csv(folder / "products.csv").set_index("sku")

    complete = lines.groupby("order_id")["quantity"].apply(lambda q: q.notna().all())
    ok_lines = lines[lines["order_id"].isin(complete[complete].index)]
    # each line: quantity x list price - discount
    list_price = ok_lines["sku"].map(products["unit_price"])
    expected = (ok_lines["quantity"] * list_price - ok_lines["discount_amount"]).round(2)
    assert (abs(expected - ok_lines["line_total"]) <= 0.011).all()
    # each order: sum of its lines
    sums = ok_lines.groupby("order_id").agg(lines_total=("line_total", "sum"), n=("line_total", "size"))
    merged = orders.set_index("order_id").join(sums, how="inner")
    assert (abs(merged["total_amount"] - merged["lines_total"]) <= 0.01 * merged["n"]).all()
    # many distinct totals, not the 10 the modulo generator produced
    assert orders["total_amount"].nunique() > 1000
    # promotions really discount something
    assert (lines["discount_amount"] > 0).sum() > 0


def test_promotion_discounts_only_inside_promotion_windows(small_data):
    folder, _ = small_data
    orders, lines = _orders(folder).drop_duplicates("order_id"), _lines(folder)
    products = pd.read_csv(folder / "products.csv").set_index("sku")
    promos = pd.read_csv(folder / "promotions.csv")
    discounted = lines[lines["discount_amount"] > 0].merge(orders[["order_id", "order_timestamp"]], on="order_id")
    for _, row in discounted.head(200).iterrows():
        day = row["order_timestamp"][:10]
        active = promos[(promos["start_date"] <= day) & (promos["end_date"] >= day)]
        hits = active[((active["scope"] == "sku") & (active["target"] == row["sku"])) |
                      ((active["scope"] == "category") & (active["target"] == products.loc[row["sku"], "category"]))]
        assert len(hits) > 0, f"discount without promotion: {row.to_dict()}"


def test_default_history_is_at_least_90_days():
    assert GeneratorConfig().days >= 90


def test_weekly_and_holiday_seasonality(small_data):
    folder, _ = small_data
    orders = _orders(folder).drop_duplicates("order_id")
    orders["day"] = pd.to_datetime(orders["order_timestamp"]).dt.normalize()
    daily = orders.groupby("day")["total_amount"].sum()
    by_weekday = daily.groupby(daily.index.dayofweek).mean()
    assert by_weekday[5] > 1.2 * by_weekday[1]  # Saturday vs Tuesday
    july4 = daily[pd.Timestamp("2026-07-04")]
    other_saturdays = daily[(daily.index.dayofweek == 5) & (daily.index.month == 6)].mean()
    # demand is 1.7x; recorded sales are lower because stores sell out on the day (lost sales aren't recorded)
    assert july4 > 1.15 * other_saturdays


def test_inventory_is_consistent_with_sales(small_data):
    folder, truth = small_data
    inv = _inventory(folder).sort_values(["store_id", "sku", "snapshot_date"])
    inv["qoh"] = inv["quantity_on_hand"].abs()  # undo D5 sign errors
    prev = inv.groupby(["store_id", "sku"])["qoh"].shift()
    flow = inv.dropna(subset=["qoh"]).assign(prev=prev).dropna(subset=["prev"])
    assert (flow["prev"] + flow["units_received"] - flow["units_sold"] == flow["qoh"]).all()
    assert (inv["qoh"] == 0).sum() == truth.stockout_store_sku_days > 0  # stockouts happen
    assert inv["units_received"].sum() > 0                                # and are replenished

    lines = _lines(folder)
    sold_in_lines = lines["quantity"].sum()
    missing_qty_lines = lines["quantity"].isna().sum()
    assert sold_in_lines <= inv["units_sold"].sum() <= sold_in_lines + 12 * missing_qty_lines


def test_defects_match_ground_truth(small_data):
    folder, truth = small_data
    orders, lines, inv = _orders(folder), _lines(folder), _inventory(folder)
    assert orders["order_id"].duplicated().sum() == truth.defects["duplicate_order_rows"] == 1
    assert lines["quantity"].isna().sum() == truth.defects["null_quantity_lines"] > 0
    assert (inv["quantity_on_hand"] < 0).sum() == truth.defects.get("negative_inventory_rows", 0)
    late = orders.drop_duplicates("order_id")
    late = (late["ingested_at"].str[:10] != late["order_timestamp"].str[:10]).sum()
    assert late == truth.defects["late_arriving_orders"] > 0
    renamed = folder / truth.defects["renamed_column_file"]
    assert renamed.read_text().splitlines()[0].split(",")[2] == "order_ts"


@pytest.mark.parametrize("encoding", ["cp1252"])
def test_cli_runs_under_a_windows_console_encoding(tmp_path, encoding):
    env = {**os.environ, "PYTHONIOENCODING": encoding, "PYTHONPATH": str(ROOT / "python")}
    result = subprocess.run(
        [sys.executable, str(ROOT / "python/data_generation/deterministic_generator.py"), "--days", "21",
         "--num_stores", "1", "--num_products", "6", "--start_date", date(2026, 4, 6).isoformat(),
         "--output_path", str(tmp_path)],
        env=env, capture_output=True, text=True, encoding=encoding)
    assert result.returncode == 0, result.stderr
    assert "[ok]" in result.stdout
