# Synthetic retail data (K-10)

All data in this project is synthetic. `python/data_generation/deterministic_generator.py` simulates a retail chain day by day. Every table is consistent with every other one: order totals match their lines, and stock levels follow from sales and deliveries.

```bash
python python/data_generation/deterministic_generator.py --output_path data/generated   # defaults below
python python/data_generation/deterministic_generator.py --seed 7 --days 98 --num_stores 3 --num_products 12
```

The same seed and parameters produce byte-identical files (`tests/test_generator.py::test_same_seed_gives_byte_identical_files`). Known defects are injected on purpose; see [data-ground-truth.md](data-ground-truth.md).

## Defaults

| Parameter | Default | Effect |
|---|---|---|
| `seed` | 42 | Everything random flows from one `numpy` PCG64 generator |
| `start_date` | 2026-04-06 (Mon) | First day of history |
| `days` | 182 | 26 full weeks, to 2026-10-04 (minimum 90 for forecasting) |
| `num_stores` | 10 | Up to 12 |
| `num_products` | 50 | Spread evenly over 6 categories |
| `mean_units_per_sku_day` | 6.0 | Average units per store per SKU per day, before seasonality |

With the defaults, the run produces 290,990 orders, 498,660 order lines and 91,000 inventory rows, about 60 MB of CSV. It takes about 90 seconds.

## How demand is built

For each store × SKU × day, expected units are:

```
store size factor (lognormal, sd 0.25)
× product popularity (lognormal, sd 0.35, normalised to mean 1)
× mean_units_per_sku_day
× day of week   Mon 0.92 · Tue 0.85 · Wed 0.93 · Thu 1.00 · Fri 1.12 · Sat 1.32 · Sun 1.18
× month end     1.15 on the last two days of each month (payday)
× holiday       Memorial Day 1.60 · Juneteenth 1.25 · 3 Jul 1.35 · 4 Jul 1.70 · Labor Day 1.55
× promotion     1 + 3 × discount (a 20% discount gives 1.6× demand)
× trend         +0.08% per day
```

Each day, the generator draws the number of orders per store from a Poisson distribution, so the expected units per store-day come out right. Each order is a basket of 1 to 6 lines (1 + Poisson(0.75)). SKUs are drawn in proportion to demand, and each line's quantity is 1 + Poisson(0.3). A line sells only what is on the shelf. If a SKU is out of stock, the sale is lost and nothing is recorded, as in a real POS feed.

Observed with the defaults: average order $94.66 (median $72.87), 28,230 distinct order totals, 1.71 lines per order, 1.32 units per line, 4.1% of lines discounted. Average daily revenue runs from $121k on Tuesday to $193k on Saturday.

## Inventory

Each store × SKU starts with 12 days of average demand on hand. When the end-of-day count falls to the reorder level (5 days of demand) or below, the store places a purchase order. The order brings stock back up to 16 days of demand and arrives after 2–3 days. 6% of deliveries slip by a further 3–6 days. Promotions and slipped deliveries cause stockouts: 1.8% of store-SKU-days end at zero (1,631 with the defaults).

For every row, `quantity_on_hand(day) = quantity_on_hand(day − 1) + units_received − units_sold`. `tests/test_generator.py::test_inventory_is_consistent_with_sales` checks this.

## Files and tables

Daily facts are partitioned by **ingestion** date, the day the row arrived, which is not always the day it happened (D3). Each file maps to one raw table (`python/lakehouse/raw_tables.py`).

### `stores.csv` → `STORES_RAW`

| Column | Type | Notes |
|---|---|---|
| store_id | integer | 1..num_stores |
| store_name | varchar | `Store 01 - Austin Downtown` |
| region | varchar | North, South, East, West |
| opened_date | date | 2015–2023 |

### `products.csv` → `PRODUCTS_RAW`

| Column | Type | Notes |
|---|---|---|
| sku | varchar | `SKU0001`… |
| product_name | varchar | Realistic per-category names |
| category | varchar | Electronics, Hardware, Clothing, Food, Books, Home & Garden |
| unit_price | number(10,2) | Uniform within a category range (Food $1.50–14, Electronics $15–180, …) |
| cost_price | number(10,2) | 45–70% of unit price |
| active | integer | 1 |

### `promotions.csv` → `PROMOTIONS_RAW`

| Column | Type | Notes |
|---|---|---|
| promo_id | varchar | `PROMO001`… |
| scope | varchar | `category` or `sku` |
| target | varchar | Category name or SKU |
| start_date, end_date | date | 4–10 days long; about one promotion every 18 days |
| discount_pct | integer | 10, 15, 20, 25 or 30 |

### `orders/orders_YYYY-MM-DD.csv` → `ORDERS_RAW`

| Column | Type | Notes |
|---|---|---|
| order_id | varchar | `ORD000000001`…, unique except D2 |
| store_id | integer | |
| order_timestamp | timestamp | 08:00–21:00, sorted within a store-day; named `order_ts` in one file (D4) |
| customer_id | varchar | `CUST000123`; empty for anonymous orders (25%, by design) |
| gross_amount | number(12,2) | Σ quantity × unit price |
| discount_amount | number(12,2) | Σ promotion discount on the lines |
| total_amount | number(12,2) | gross − discount = Σ line_total |
| ingested_at | timestamp | 23:30 on the file's date |

### `order_lines/order_lines_YYYY-MM-DD.csv` → `ORDER_LINES_RAW`

| Column | Type | Notes |
|---|---|---|
| order_id | varchar | |
| line_number | integer | 1.. within the order |
| sku | varchar | One line per SKU per order |
| quantity | integer | ≥ 1; empty for D1 |
| unit_price | number(10,2) | List price |
| discount_amount | number(12,2) | quantity × unit_price × discount_pct, rounded |
| line_total | number(12,2) | quantity × unit_price − discount_amount |
| ingested_at | timestamp | Same as the order |

### `inventory/inventory_YYYY-MM-DD.csv` → `INVENTORY_RAW`

| Column | Type | Notes |
|---|---|---|
| snapshot_date | date | End-of-day count, one row per store × SKU |
| store_id, sku | | |
| quantity_on_hand | integer | ≥ 0; negative for D5 |
| reorder_level | integer | 5 days of base demand |
| units_received | integer | Deliveries that arrived that morning |
| units_sold | integer | Units sold that day (includes lines whose quantity was later lost to D1) |
| last_updated | timestamp | 22:00 |
| ingested_at | timestamp | 23:30 |

### `ground_truth.json`

The generator's own record of the run: config, row counts, the exact number of each injected defect, stockout store-SKU-days and lost-sales units. dbt test warnings and InfraOps findings can be checked against it.

## Loaded columns

Both loaders add `_LOADED_AT` (load time) and `_SOURCE_FILE`. They union files by column name and keep unknown columns, so the D4 rename appears as an extra `ORDER_TS` column.
