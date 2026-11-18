"""
[Issue K-10] Realistic, seeded synthetic retail data.

Simulates a retail chain day by day so that every table agrees with every other:

  * demand per store x product x day = store size x product popularity
    x day-of-week x month-end x holiday x promotion lift
  * orders are baskets drawn from that demand; order totals are the sum of
    quantity x list price minus the promotion discount on each line
  * inventory starts stocked, is depleted by sales, replenished by purchase
    orders with a lead time (some deliveries slip), and runs out when demand
    outpaces it (lost sales are not recorded, as in a real POS feed)

Known data-quality defects are injected at fixed rates and counted in
ground_truth.json; docs/data-ground-truth.md explains each one.

Same seed + same parameters => byte-identical files (tests/test_generator.py).

Usage:
    python python/data_generation/deterministic_generator.py --output_path data/generated
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

CATEGORIES = {
    # category: (price range, product names)
    "Electronics": ((15.0, 180.0), ["USB-C Cable", "Wireless Mouse", "Bluetooth Speaker", "Power Bank",
                                    "Earbuds", "Phone Charger", "HDMI Adapter", "Smart Plug", "LED Desk Lamp"]),
    "Hardware": ((4.0, 90.0), ["Cordless Drill", "Screwdriver Set", "Tape Measure", "Work Gloves",
                               "Wood Glue", "Hammer", "Utility Knife", "Wall Anchors"]),
    "Clothing": ((8.0, 70.0), ["Cotton T-Shirt", "Denim Jeans", "Rain Jacket", "Wool Socks",
                               "Baseball Cap", "Hoodie", "Running Shorts", "Leather Belt"]),
    "Food": ((1.5, 14.0), ["Ground Coffee", "Granola", "Olive Oil", "Dark Chocolate", "Pasta",
                           "Sparkling Water", "Peanut Butter", "Green Tea", "Rice"]),
    "Books": ((6.0, 35.0), ["Mystery Novel", "Cookbook", "Travel Guide", "Children's Picture Book",
                            "Sci-Fi Paperback", "Biography", "Puzzle Book", "Notebook Journal"]),
    "Home & Garden": ((5.0, 120.0), ["Potting Soil", "Garden Hose", "Scented Candle", "Throw Pillow",
                                     "Plant Pot", "Bath Towel", "Storage Bin", "Doormat"]),
}

STORE_LOCATIONS = [
    ("Austin Downtown", "South"), ("Boston Back Bay", "East"), ("Chicago Loop", "North"),
    ("Denver Union Station", "West"), ("Miami Brickell", "South"), ("Minneapolis North Loop", "North"),
    ("New York SoHo", "East"), ("Portland Pearl", "West"), ("San Diego Gaslamp", "West"),
    ("Atlanta Midtown", "South"), ("Detroit Corktown", "North"), ("Philadelphia Old City", "East"),
]

# Monday=0 ... Sunday=6. Weekend peak, Tuesday trough.
DAY_OF_WEEK = [0.92, 0.85, 0.93, 1.00, 1.12, 1.32, 1.18]
MONTH_END_LIFT = 1.15          # last two days of the month (payday)
HOLIDAY_LIFT = {               # US holidays inside the default window
    date(2026, 5, 25): 1.60,   # Memorial Day
    date(2026, 6, 19): 1.25,   # Juneteenth
    date(2026, 7, 3): 1.35,    # day before Independence Day
    date(2026, 7, 4): 1.70,    # Independence Day
    date(2026, 9, 7): 1.55,    # Labor Day
}


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 42
    start_date: date = date(2026, 4, 6)   # a Monday
    days: int = 182                       # 26 full weeks
    num_stores: int = 10
    num_products: int = 50
    mean_units_per_sku_day: float = 6.0   # per store, before seasonality
    # Data-quality defect rates (see docs/data-ground-truth.md)
    null_quantity_rate: float = 0.002     # D1: order lines with an empty quantity
    duplicate_orders: int = 1             # D2: order rows emitted twice
    late_arrival_rate: float = 0.015      # D3: orders delivered 1-3 days after they happened
    renamed_column_day: int = 100         # D4: day index whose orders file says order_ts
    negative_inventory_rate: float = 0.0005  # D5: inventory counts recorded as negative
    delivery_delay_rate: float = 0.06     # purchase orders that arrive late (cause stockouts)

    def end_date(self) -> date:
        return self.start_date + timedelta(days=self.days - 1)


@dataclass
class GroundTruth:
    config: dict
    row_counts: dict = field(default_factory=dict)
    defects: dict = field(default_factory=dict)
    stockout_store_sku_days: int = 0
    lost_sales_units: int = 0


def _money(x: float) -> str:
    return f"{x:.2f}"


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class RetailSimulator:
    """Day-by-day simulation of a retail chain. All randomness comes from one seeded generator."""

    def __init__(self, config: GeneratorConfig):
        if config.num_stores > len(STORE_LOCATIONS):
            raise ValueError(f"num_stores must be <= {len(STORE_LOCATIONS)}")
        self.cfg = config
        self.rng = np.random.Generator(np.random.PCG64(config.seed))
        self.stores = self._make_stores()
        self.products = self._make_products()
        self.promotions = self._make_promotions()
        self.truth = GroundTruth(config={k: str(v) for k, v in asdict(config).items()})

    # ------------------------------------------------------------------ dims
    def _make_stores(self) -> list[dict[str, Any]]:
        stores: list[dict[str, Any]] = []
        for i in range(self.cfg.num_stores):
            name, region = STORE_LOCATIONS[i]
            size = float(np.round(self.rng.lognormal(0.0, 0.25), 3))
            stores.append({
                "store_id": i + 1,
                "store_name": f"Store {i + 1:02d} - {name}",
                "region": region,
                "size_factor": size,
                "opened_date": (date(2015, 1, 1) + timedelta(days=int(self.rng.integers(0, 3000)))).isoformat(),
            })
        return stores

    def _make_products(self) -> list[dict[str, Any]]:
        cats = list(CATEGORIES)
        products: list[dict[str, Any]] = []
        for i in range(self.cfg.num_products):
            category = cats[i % len(cats)]
            (low, high), names = CATEGORIES[category]
            name = names[(i // len(cats)) % len(names)]
            variant = i // (len(cats) * len(names))
            price = float(np.round(self.rng.uniform(low, high), 2))
            cost = float(np.round(price * self.rng.uniform(0.45, 0.70), 2))
            popularity = float(self.rng.lognormal(0.0, 0.35))
            products.append({
                "sku": f"SKU{i + 1:04d}",
                "product_name": name if variant == 0 else f"{name} v{variant + 1}",
                "category": category,
                "unit_price": price,
                "cost_price": cost,
                "active": 1,
                "popularity": popularity,
            })
        # Normalise popularity so the mean SKU sells mean_units_per_sku_day per store
        mean_pop = np.mean([p["popularity"] for p in products])
        for p in products:
            p["popularity"] = p["popularity"] / mean_pop
        return products

    def _make_promotions(self) -> list[dict[str, Any]]:
        promos: list[dict[str, Any]] = []
        n = max(2, self.cfg.days // 18)
        for i in range(n):
            start = self.cfg.start_date + timedelta(days=int(self.rng.integers(7, max(8, self.cfg.days - 10))))
            length = int(self.rng.integers(4, 11))
            discount = int(self.rng.choice([10, 15, 20, 25, 30]))
            if self.rng.random() < 0.5:
                scope, target = "category", str(self.rng.choice(list(CATEGORIES)))
            else:
                scope, target = "sku", self.products[int(self.rng.integers(0, len(self.products)))]["sku"]
            promos.append({
                "promo_id": f"PROMO{i + 1:03d}",
                "scope": scope,
                "target": target,
                "start_date": start.isoformat(),
                "end_date": min(start + timedelta(days=length - 1), self.cfg.end_date()).isoformat(),
                "discount_pct": discount,
            })
        promos.sort(key=lambda p: (p["start_date"], p["promo_id"]))
        return promos

    # --------------------------------------------------------------- helpers
    def _day_multiplier(self, d: date) -> float:
        m = DAY_OF_WEEK[d.weekday()]
        next_month = (d.replace(day=28) + timedelta(days=4)).replace(day=1)
        if (next_month - d).days <= 2:
            m *= MONTH_END_LIFT
        m *= HOLIDAY_LIFT.get(d, 1.0)
        return m

    def _discounts_for_day(self, d: date) -> np.ndarray:
        """Discount fraction per product on day d (largest applicable promotion wins)."""
        disc = np.zeros(len(self.products))
        iso = d.isoformat()
        for promo in self.promotions:
            if not (promo["start_date"] <= iso <= promo["end_date"]):
                continue
            for j, p in enumerate(self.products):
                hit = p["category"] == promo["target"] if promo["scope"] == "category" else p["sku"] == promo["target"]
                if hit:
                    disc[j] = max(disc[j], promo["discount_pct"] / 100.0)
        return disc

    # ------------------------------------------------------------ simulation
    def run(self) -> dict:
        cfg = self.cfg
        n_sku = len(self.products)
        price = np.array([p["unit_price"] for p in self.products])
        popularity = np.array([p["popularity"] for p in self.products])
        base = np.outer([s["size_factor"] for s in self.stores], popularity) * cfg.mean_units_per_sku_day

        reorder_level = np.ceil(base * 5).astype(int)
        order_up_to = np.ceil(base * 16).astype(int)
        on_hand = np.ceil(base * 12).astype(int)
        pending: dict[tuple[int, int], tuple[int, int]] = {}  # (store, sku) -> (arrival_day, qty)

        orders_by_ingest: dict[int, list[dict]] = {}
        lines_by_ingest: dict[int, list[dict]] = {}
        inventory_by_day: dict[int, list[dict]] = {}
        order_seq = 0

        for day_idx in range(cfg.days):
            d = cfg.start_date + timedelta(days=day_idx)
            mult = self._day_multiplier(d) * (1 + 0.0008 * day_idx)  # gentle growth
            disc = self._discounts_for_day(d)
            lift = 1 + 3.0 * disc  # 20% off -> 1.6x demand
            received = np.zeros_like(on_hand)
            sold = np.zeros_like(on_hand)

            # Morning: purchase orders due today arrive
            for key in [k for k, (arr, _) in pending.items() if arr == day_idx]:
                _, qty = pending.pop(key)
                on_hand[key] += qty
                received[key] += qty

            for s_idx, store in enumerate(self.stores):
                demand = base[s_idx] * mult * lift
                total = demand.sum()
                units_per_order = 1.30 * 1.75  # mean qty x mean lines
                n_orders = int(self.rng.poisson(total / units_per_order))
                p_sku = demand / total
                seconds = np.sort(self.rng.integers(8 * 3600, 21 * 3600, size=n_orders))
                for o in range(n_orders):
                    n_lines = min(6, 1 + int(self.rng.poisson(0.75)))
                    skus = self.rng.choice(n_sku, size=n_lines, p=p_sku)
                    qtys = 1 + self.rng.poisson(0.30, size=n_lines)
                    basket: dict[int, int] = {}
                    for j, q in zip(skus.tolist(), qtys.tolist()):
                        basket[j] = basket.get(j, 0) + q
                    lines = []
                    for j in sorted(basket):
                        want = basket[j]
                        got = min(want, int(on_hand[s_idx, j]))
                        self.truth.lost_sales_units += want - got
                        if got == 0:
                            continue
                        on_hand[s_idx, j] -= got
                        sold[s_idx, j] += got
                        gross = got * price[j]
                        discount = round(gross * disc[j], 2)
                        lines.append((j, got, discount, round(gross - discount, 2)))
                    if not lines:
                        continue
                    order_seq += 1
                    order_id = f"ORD{order_seq:09d}"
                    ts = datetime.combine(d, datetime.min.time()) + timedelta(seconds=int(seconds[o]))
                    customer = None if self.rng.random() < 0.25 else f"CUST{int(self.rng.integers(1, 40000)):06d}"
                    gross_total = round(sum(q * price[j] for j, q, _, _ in lines), 2)
                    disc_total = round(sum(dc for _, _, dc, _ in lines), 2)
                    # D3: late-arriving rows land in a later day's file
                    lag = int(self.rng.integers(1, 4)) if self.rng.random() < cfg.late_arrival_rate else 0
                    ingest_idx = min(day_idx + lag, cfg.days - 1)
                    ingested_at = datetime.combine(cfg.start_date + timedelta(days=ingest_idx),
                                                   datetime.min.time()) + timedelta(hours=23, minutes=30)
                    if ingest_idx != day_idx:
                        self.truth.defects["late_arriving_orders"] = self.truth.defects.get(
                            "late_arriving_orders", 0) + 1
                    orders_by_ingest.setdefault(ingest_idx, []).append({
                        "order_id": order_id,
                        "store_id": store["store_id"],
                        "order_timestamp": _ts(ts),
                        "customer_id": customer or "",
                        "gross_amount": _money(gross_total),
                        "discount_amount": _money(disc_total),
                        "total_amount": _money(round(gross_total - disc_total, 2)),
                        "ingested_at": _ts(ingested_at),
                    })
                    for n, (j, q, dc, net) in enumerate(lines, start=1):
                        qty_out = str(q)
                        if self.rng.random() < cfg.null_quantity_rate:  # D1
                            qty_out = ""
                            self.truth.defects["null_quantity_lines"] = self.truth.defects.get(
                                "null_quantity_lines", 0) + 1
                        lines_by_ingest.setdefault(ingest_idx, []).append({
                            "order_id": order_id,
                            "line_number": n,
                            "sku": self.products[j]["sku"],
                            "quantity": qty_out,
                            "unit_price": _money(price[j]),
                            "discount_amount": _money(dc),
                            "line_total": _money(net),
                            "ingested_at": _ts(ingested_at),
                        })

            # Evening: count stock, raise purchase orders
            for s_idx, store in enumerate(self.stores):
                for j, product in enumerate(self.products):
                    qoh = int(on_hand[s_idx, j])
                    if qoh == 0:
                        self.truth.stockout_store_sku_days += 1
                    if qoh <= reorder_level[s_idx, j] and (s_idx, j) not in pending:
                        lead = int(self.rng.integers(2, 4))
                        if self.rng.random() < cfg.delivery_delay_rate:
                            lead += int(self.rng.integers(3, 7))
                        pending[(s_idx, j)] = (day_idx + lead, int(order_up_to[s_idx, j] - qoh))
                    recorded = qoh
                    if qoh > 0 and self.rng.random() < cfg.negative_inventory_rate:  # D5
                        recorded = -qoh
                        self.truth.defects["negative_inventory_rows"] = self.truth.defects.get(
                            "negative_inventory_rows", 0) + 1
                    counted_at = datetime.combine(d, datetime.min.time()) + timedelta(hours=22)
                    inventory_by_day.setdefault(day_idx, []).append({
                        "snapshot_date": d.isoformat(),
                        "store_id": store["store_id"],
                        "sku": product["sku"],
                        "quantity_on_hand": recorded,
                        "reorder_level": int(reorder_level[s_idx, j]),
                        "units_received": int(received[s_idx, j]),
                        "units_sold": int(sold[s_idx, j]),
                        "last_updated": _ts(counted_at),
                        "ingested_at": _ts(counted_at + timedelta(hours=1, minutes=30)),
                    })

        # D2: emit some orders twice (exact duplicate rows in the same file)
        all_days = sorted(orders_by_ingest)
        for k in range(cfg.duplicate_orders):
            day = all_days[(len(all_days) // 2 + k) % len(all_days)]
            orders_by_ingest[day].append(dict(orders_by_ingest[day][0]))
        self.truth.defects["duplicate_order_rows"] = cfg.duplicate_orders

        return {"orders": orders_by_ingest, "order_lines": lines_by_ingest, "inventory": inventory_by_day}


ORDER_COLUMNS = ["order_id", "store_id", "order_timestamp", "customer_id", "gross_amount",
                 "discount_amount", "total_amount", "ingested_at"]
LINE_COLUMNS = ["order_id", "line_number", "sku", "quantity", "unit_price", "discount_amount",
                "line_total", "ingested_at"]
INVENTORY_COLUMNS = ["snapshot_date", "store_id", "sku", "quantity_on_hand", "reorder_level",
                     "units_received", "units_sold", "last_updated", "ingested_at"]


def _write_csv(path: Path, columns: list[str], rows: list[dict], header: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(header or columns)
        for row in rows:
            writer.writerow([row[c] for c in columns])


def generate(config: GeneratorConfig, output_path: Path) -> GroundTruth:
    """Run the simulation and write every file under output_path. Returns the ground truth."""
    sim = RetailSimulator(config)
    facts = sim.run()
    out = Path(output_path)

    _write_csv(out / "stores.csv", ["store_id", "store_name", "region", "opened_date"], sim.stores)
    products_out = [{**p, "unit_price": _money(p["unit_price"]), "cost_price": _money(p["cost_price"])}
                    for p in sim.products]
    _write_csv(out / "products.csv", ["sku", "product_name", "category", "unit_price", "cost_price", "active"],
               products_out)
    _write_csv(out / "promotions.csv", ["promo_id", "scope", "target", "start_date", "end_date", "discount_pct"],
               sim.promotions)

    counts = {"stores": len(sim.stores), "products": len(sim.products), "promotions": len(sim.promotions),
              "orders": 0, "order_lines": 0, "inventory": 0}
    for day_idx in range(config.days):
        d = (config.start_date + timedelta(days=day_idx)).isoformat()
        orders = facts["orders"].get(day_idx, [])
        header = None
        if day_idx == config.renamed_column_day:  # D4: upstream renamed a column for one day
            header = [("order_ts" if c == "order_timestamp" else c) for c in ORDER_COLUMNS]
            sim.truth.defects["renamed_column_rows"] = len(orders)
            sim.truth.defects["renamed_column_file"] = f"orders/orders_{d}.csv"
        _write_csv(out / "orders" / f"orders_{d}.csv", ORDER_COLUMNS, orders, header)
        _write_csv(out / "order_lines" / f"order_lines_{d}.csv", LINE_COLUMNS, facts["order_lines"].get(day_idx, []))
        _write_csv(out / "inventory" / f"inventory_{d}.csv", INVENTORY_COLUMNS, facts["inventory"].get(day_idx, []))
        counts["orders"] += len(orders)
        counts["order_lines"] += len(facts["order_lines"].get(day_idx, []))
        counts["inventory"] += len(facts["inventory"].get(day_idx, []))

    sim.truth.row_counts = counts
    with (out / "ground_truth.json").open("w", newline="", encoding="utf-8") as f:
        json.dump(asdict(sim.truth), f, indent=2, sort_keys=True)
        f.write("\n")
    return sim.truth


def main(argv: list[str] | None = None) -> int:
    # Windows consoles default to cp1252; keep output plain ASCII and never crash on encoding.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")

    parser = argparse.ArgumentParser(description="Generate seeded synthetic retail data")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start_date", type=date.fromisoformat, default=GeneratorConfig.start_date)
    parser.add_argument("--days", type=int, default=GeneratorConfig.days)
    parser.add_argument("--num_stores", type=int, default=GeneratorConfig.num_stores)
    parser.add_argument("--num_products", type=int, default=GeneratorConfig.num_products)
    parser.add_argument("--output_path", type=Path, default=Path("data/generated"))
    args = parser.parse_args(argv)

    if args.days < 90:
        print("note: fewer than 90 days of history; forecasts and seasonality will be weak")
    config = GeneratorConfig(seed=args.seed, start_date=args.start_date, days=args.days,
                             num_stores=args.num_stores, num_products=args.num_products,
                             renamed_column_day=min(GeneratorConfig.renamed_column_day, args.days - 1))
    truth = generate(config, args.output_path)
    print(f"[ok] seed={config.seed} {config.start_date} .. {config.end_date()} -> {args.output_path}")
    for table, n in truth.row_counts.items():
        print(f"     {table:<12} {n:>9,} rows")
    print(f"     defects: {json.dumps(truth.defects, sort_keys=True)}")
    print(f"     stockout store-sku-days: {truth.stockout_store_sku_days:,}; "
          f"lost sales units: {truth.lost_sales_units:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
