# Data ground truth: known defects (K-10)

The generator injects these defects on purpose, at stated rates, so that dbt tests, InfraOps and anyone demoing the project have something real to catch. Exact counts for a run are in `data/generated/ground_truth.json`. The numbers below are for the default run (seed 42, 182 days, 10 stores, 50 SKUs).

| ID | Defect | Rate | Default run | Caught by | Handled in |
|---|---|---|---|---|---|
| D1 | Order line with an empty `quantity` | 0.2% of lines | 1,012 lines | `source_not_null_raw_ORDER_LINES_RAW_quantity` (warn) | `stg_order_lines` drops the line |
| D2 | Order re-sent by the POS feed (exact duplicate row) | 1 order | 1 row | `source_unique_raw_ORDERS_RAW_order_id` (warn) | `stg_orders` keeps the first copy |
| D3 | Late-arriving order: lands in the file 1–3 days after the sale | 1.5% of orders | 4,307 orders | `assert_sales_mart_complete` would fail if the incremental mart missed them | `fct_sales_daily` rebuilds every sale date touched by newly ingested rows |
| D4 | Upstream renamed `order_timestamp` → `order_ts` for one day | 1 file (day 100) | `orders_2026-07-15.csv`, 1,388 rows | `source_not_null_raw_ORDERS_RAW_order_timestamp` (warn) | `stg_orders`: `coalesce(order_timestamp, order_ts)` via `optional_column()` |
| D5 | Stock count recorded with the wrong sign | 0.05% of inventory rows | 41 rows | `dbt_utils_source_accepted_range_raw_INVENTORY_RAW_quantity_on_hand__0` (warn) | `stg_inventory` drops the row |

Behaviour that is **not** a defect but looks like one:

- **Anonymous orders.** 25% of orders have no `customer_id`.
- **Stockouts.** 1.8% of store-SKU-days end with zero stock (1,631 in the default run). These are real, caused by promotions and late deliveries, and they cap sales on peak days. Lost sales (9,714 units) are not in any file, exactly as a real POS wouldn't record them.
- **Order totals don't match the lines** for orders that lost a line to D1. The total was computed before the quantity went missing. `assert_order_totals_reconcile` excludes those orders and checks all the others.

## What a clean `dbt build` looks like

```
Done. PASS=70 WARN=4 ERROR=0 SKIP=0 TOTAL=74
  WARN  dbt_utils_source_accepted_range_raw_INVENTORY_RAW_quantity_on_hand__0   41 results   (D5)
  WARN  source_not_null_raw_ORDERS_RAW_order_timestamp                         1388 results  (D4)
  WARN  source_not_null_raw_ORDER_LINES_RAW_quantity                           1012 results  (D1)
  WARN  source_unique_raw_ORDERS_RAW_order_id                                     1 result   (D2)
```

The four warnings are expected, and their counts equal `ground_truth.json`. A different count, or a fifth warning, means something changed upstream.

## For InfraOps

D4 is the scenario for a schema-drift RCA. One day's file carries an unknown column, the not-null test on the raw source warns, and the loaded row count for that day is normal. D3 exercises freshness and late-data logic. D2 exercises duplicate detection.
