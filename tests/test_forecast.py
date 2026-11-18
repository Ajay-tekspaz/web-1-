from datetime import date

import numpy as np
import pandas as pd

from forecasts.demand_forecast import ForecastConfig, daily_series, forecast_weeks, run, weekly_sums

CFG = ForecastConfig(simulations=200)


def _sales(days: int = 140, stores=(1, 2), skus=("SKU0001", "SKU0002"), seed: int = 0) -> pd.DataFrame:
    """Daily sales with a weekend peak and Poisson noise, starting on a Monday."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2026-04-06", periods=days, freq="D")
    weekly = np.array([0.9, 0.8, 0.9, 1.0, 1.1, 1.4, 1.2])
    rows = []
    for s in stores:
        for i, sku in enumerate(skus):
            base = 20 + 10 * i + 5 * s
            for d in dates:
                rows.append({"sale_date": d.date(), "store_id": s, "sku": sku,
                             "units_sold": int(rng.poisson(base * weekly[d.dayofweek]))})
    return pd.DataFrame(rows)


def test_weekly_sums_blocks_of_seven():
    assert weekly_sums(np.arange(14)).tolist() == [21, 70]


def test_series_are_zero_filled_and_end_on_a_sunday():
    sales = _sales(days=40)  # 2026-04-06 .. 2026-05-15 (a Friday)
    sales = sales[~((sales["store_id"] == 1) & (sales["sku"] == "SKU0001") & (sales["sale_date"] == date(2026, 4, 8)))]
    series, last = daily_series(sales)
    assert last == pd.Timestamp("2026-05-10") and last.dayofweek == 6
    y = series[(1, "SKU0001")]
    assert len(y) == 35 and y[pd.Timestamp("2026-04-08")] == 0


def test_holt_winters_is_accurate_on_seasonal_data():
    _, metrics = run(_sales(), CFG)
    assert metrics["holdout_mape"] < 0.10
    assert metrics["series"] == 4 and metrics["rows"] == 4 * CFG.horizon_weeks


def test_output_shape_band_and_weeks():
    output, metrics = run(_sales(), CFG)
    assert (output["lower_80"] <= output["forecast_units"]).all()
    assert (output["forecast_units"] <= output["upper_80"]).all()
    assert (output["lower_80"] >= 0).all()
    weeks = pd.to_datetime(output["forecast_week"])
    assert (weeks.dt.dayofweek == 0).all()                         # Monday-start weeks
    assert weeks.min() == pd.Timestamp(metrics["trained_through"]) + pd.Timedelta(days=1)
    assert weeks.nunique() == CFG.horizon_weeks
    assert not output.duplicated(["forecast_week", "store_id", "sku"]).any()


def test_band_widens_with_horizon():
    y = daily_series(_sales())[0][(1, "SKU0001")]
    fc = forecast_weeks(y, 13, CFG, np.random.RandomState(0))
    width = fc["upper"] - fc["lower"]
    assert width[-1] > width[0]


def test_same_input_same_forecast():
    fixed = pd.Timestamp("2026-10-05 00:00:00").to_pydatetime()
    a, _ = run(_sales(), CFG, generated_at=fixed)
    b, _ = run(_sales(), CFG, generated_at=fixed)
    pd.testing.assert_frame_equal(a, b)


def test_short_series_are_skipped():
    output, metrics = run(_sales(days=35), CFG)
    assert output.empty and metrics["holdout_mape"] is None
