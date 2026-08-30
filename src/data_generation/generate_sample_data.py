"""
Generate synthetic e-commerce CSV datasets with intentional data quality defects.

Uses Faker + pandas for local generation, then validates output with PySpark
to confirm Databricks-compatible ingestion.

AI rationale:
  - pandas/Faker are appropriate for bounded local generation (~110k rows).
  - PySpark validation ensures CSVs are readable with explicit schemas before
    Bronze ingestion.
  - Defects are injected deterministically via a fixed seed for reproducibility.
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Seed & volume parameters (documented in DATA_GENERATION_NOTES.md)
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
FAKER_SEED = 42

NUM_CUSTOMERS = 10_000
NUM_ORDERS = 100_000
NUM_PRODUCTS = 500

# Intentional defect counts
NULL_EMAILS = 50
DUPLICATE_CUSTOMER_IDS = 10
NULL_ORDER_CUSTOMER_IDS = 100
NULL_ORDER_PRODUCT_IDS = 200
ORPHAN_CUSTOMER_IDS = 50
ORPHAN_PRODUCT_IDS = 30
DUPLICATE_ORDER_IDS = 20

CUSTOMER_SEGMENTS = ["Bronze", "Silver", "Gold", "Platinum"]
PRODUCT_CATEGORIES = [
    "Electronics",
    "Clothing",
    "Home & Garden",
    "Sports",
    "Books",
    "Beauty",
    "Toys",
    "Food",
]
ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
COUNTRIES = ["US", "UK", "CA", "DE", "FR", "IN", "AU", "JP", "BR", "MX"]

ORPHAN_CUSTOMER_PREFIX = "ORPHAN-CUST"
ORPHAN_PRODUCT_PREFIX = "ORPHAN-PROD"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _init_faker() -> Faker:
    fake = Faker()
    Faker.seed(FAKER_SEED)
    fake.seed_instance(FAKER_SEED)
    return fake


def _random_date(fake: Faker, start: datetime, end: datetime) -> str:
    return fake.date_between(start_date=start, end_date=end).isoformat()


def generate_products(fake: Faker) -> pd.DataFrame:
    """Generate clean product catalog (no intentional defects in this phase)."""
    rows = []
    for i in range(1, NUM_PRODUCTS + 1):
        price = round(random.uniform(5.0, 500.0), 2)
        cost = round(price * random.uniform(0.3, 0.7), 2)
        rows.append(
            {
                "product_id": f"PROD-{i:05d}",
                "product_name": fake.catch_phrase(),
                "category": random.choice(PRODUCT_CATEGORIES),
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(0, 1000),
                "reorder_level": random.randint(10, 100),
            }
        )
    return pd.DataFrame(rows)


def generate_customers(fake: Faker) -> pd.DataFrame:
    """Generate customer records, then inject completeness and uniqueness defects."""
    rows = []
    signup_start = datetime(2018, 1, 1)
    signup_end = datetime(2025, 12, 31)

    for i in range(1, NUM_CUSTOMERS + 1):
        rows.append(
            {
                "customer_id": f"CUST-{i:05d}",
                "customer_name": fake.name(),
                "email": fake.email(),
                "country": random.choice(COUNTRIES),
                "signup_date": _random_date(fake, signup_start, signup_end),
                "customer_segment": random.choice(CUSTOMER_SEGMENTS),
                "lifetime_value": round(random.uniform(0, 25_000), 2),
            }
        )

    df = pd.DataFrame(rows)

    # Completeness: 50 NULL emails
    null_email_indices = random.sample(range(len(df)), NULL_EMAILS)
    df.loc[null_email_indices, "email"] = None

    # Uniqueness: 10 duplicate customer_ids (reuse existing IDs from first 10 rows)
    dup_source_ids = df.loc[:9, "customer_id"].tolist()
    dup_target_indices = random.sample(
        range(10, len(df)), DUPLICATE_CUSTOMER_IDS
    )
    for idx, source_id in zip(dup_target_indices, dup_source_ids):
        df.at[idx, "customer_id"] = source_id

    return df


def generate_orders(
    fake: Faker,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """Generate order records, then inject completeness, uniqueness, and FK defects."""
    valid_customer_ids = customers["customer_id"].unique().tolist()
    valid_product_ids = products["product_id"].tolist()

    order_start = datetime(2020, 1, 1)
    order_end = datetime(2025, 12, 31)

    rows = []
    for i in range(1, NUM_ORDERS + 1):
        customer_id = random.choice(valid_customer_ids)
        product_id = random.choice(valid_product_ids)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        total_amount = round(quantity * unit_price, 2)
        order_date = _random_date(fake, order_start, order_end)
        status = random.choice(ORDER_STATUSES)
        payment_date = order_date
        if status in {"Pending", "Cancelled"}:
            payment_date = None

        rows.append(
            {
                "order_id": f"ORD-{i:06d}",
                "customer_id": customer_id,
                "order_date": order_date,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "order_status": status,
                "payment_date": payment_date,
            }
        )

    df = pd.DataFrame(rows)

    # Track indices already modified to avoid overlapping defect categories
    used_indices: set[int] = set()

    def _pick_indices(count: int) -> list[int]:
        available = [i for i in range(len(df)) if i not in used_indices]
        chosen = random.sample(available, count)
        used_indices.update(chosen)
        return chosen

    # Completeness: 100 NULL customer_ids
    for idx in _pick_indices(NULL_ORDER_CUSTOMER_IDS):
        df.at[idx, "customer_id"] = None

    # Completeness: 200 NULL product_ids
    for idx in _pick_indices(NULL_ORDER_PRODUCT_IDS):
        df.at[idx, "product_id"] = None

    # Referential integrity: 50 orphan customer_ids
    orphan_customer_values = [
        f"{ORPHAN_CUSTOMER_PREFIX}-{i:03d}" for i in range(1, ORPHAN_CUSTOMER_IDS + 1)
    ]
    for idx, orphan_id in zip(_pick_indices(ORPHAN_CUSTOMER_IDS), orphan_customer_values):
        df.at[idx, "customer_id"] = orphan_id

    # Referential integrity: 30 orphan product_ids
    orphan_product_values = [
        f"{ORPHAN_PRODUCT_PREFIX}-{i:03d}" for i in range(1, ORPHAN_PRODUCT_IDS + 1)
    ]
    for idx, orphan_id in zip(_pick_indices(ORPHAN_PRODUCT_IDS), orphan_product_values):
        df.at[idx, "product_id"] = orphan_id

    # Uniqueness: 20 duplicate order_ids
    dup_source_ids = df.loc[:19, "order_id"].tolist()
    dup_target_indices = _pick_indices(DUPLICATE_ORDER_IDS)
    for idx, source_id in zip(dup_target_indices, dup_source_ids):
        df.at[idx, "order_id"] = source_id

    return df


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a single CSV file (empty strings represent NULLs)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, na_rep="")
    logger.info("Wrote %s (%d rows)", path.name, len(df))


def validate_with_pyspark(output_dir: Path) -> None:
    """
    Read generated CSVs with PySpark to confirm pipeline compatibility.

    Validation steps:
      1. Spark session starts in local mode.
      2. Each CSV is read with header inference disabled (string columns).
      3. Row counts are logged and compared to expected volumes.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        logger.warning("PySpark not installed; skipping Spark validation.")
        return

    try:
        spark = (
            SparkSession.builder.master("local[*]")
            .appName("generate_sample_data_validation")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
    except Exception as exc:
        logger.warning(
            "PySpark validation skipped (Java/Spark unavailable): %s", exc
        )
        return

    spark.sparkContext.setLogLevel("WARN")

    expected = {
        "customers.csv": NUM_CUSTOMERS,
        "products.csv": NUM_PRODUCTS,
        "orders.csv": NUM_ORDERS,
    }

    try:
        for filename, expected_count in expected.items():
            path = str(output_dir / filename)
            df = spark.read.option("header", True).option("inferSchema", False).csv(path)
            actual_count = df.count()
            if actual_count != expected_count:
                raise ValueError(
                    f"{filename}: expected {expected_count} rows, got {actual_count}"
                )
            logger.info(
                "PySpark validation passed: %s (%d rows, %d columns)",
                filename,
                actual_count,
                len(df.columns),
            )
    finally:
        spark.stop()


def log_defect_summary(customers: pd.DataFrame, orders: pd.DataFrame) -> dict[str, int]:
    """Compute and log defect counts for verification."""
    summary = {
        "null_emails": int(customers["email"].isna().sum()),
        "duplicate_customer_id_rows": int(
            customers["customer_id"].duplicated(keep=False).sum()
        ),
        "duplicate_customer_id_injections": DUPLICATE_CUSTOMER_IDS,
        "null_order_customer_ids": int(orders["customer_id"].isna().sum()),
        "null_order_product_ids": int(orders["product_id"].isna().sum()),
        "orphan_customer_ids": int(
            orders["customer_id"]
            .fillna("")
            .str.startswith(ORPHAN_CUSTOMER_PREFIX)
            .sum()
        ),
        "orphan_product_ids": int(
            orders["product_id"]
            .fillna("")
            .str.startswith(ORPHAN_PRODUCT_PREFIX)
            .sum()
        ),
        "duplicate_order_id_rows": int(orders["order_id"].duplicated(keep=False).sum()),
        "duplicate_order_id_injections": DUPLICATE_ORDER_IDS,
    }
    summary["total_defective_rows"] = (
        summary["null_emails"]
        + DUPLICATE_CUSTOMER_IDS
        + summary["null_order_customer_ids"]
        + summary["null_order_product_ids"]
        + summary["orphan_customer_ids"]
        + summary["orphan_product_ids"]
        + DUPLICATE_ORDER_IDS
    )

    logger.info("Defect summary:")
    for key, value in summary.items():
        logger.info("  %s: %d", key, value)

    return summary


def generate_all(output_dir: Path, validate: bool = True) -> dict[str, int]:
    """Generate all datasets, write CSVs, and optionally validate with PySpark."""
    random.seed(RANDOM_SEED)
    fake = _init_faker()

    logger.info("Generating products (%d rows)...", NUM_PRODUCTS)
    products = generate_products(fake)

    logger.info("Generating customers (%d rows)...", NUM_CUSTOMERS)
    customers = generate_customers(fake)

    logger.info("Generating orders (%d rows)...", NUM_ORDERS)
    orders = generate_orders(fake, customers, products)

    write_csv(products, output_dir / "products.csv")
    write_csv(customers, output_dir / "customers.csv")
    write_csv(orders, output_dir / "orders.csv")

    summary = log_defect_summary(customers, orders)

    if validate:
        validate_with_pyspark(output_dir)

    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic e-commerce CSVs with intentional defects."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_project_root() / "data",
        help="Directory for output CSV files (default: <project>/data/)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip PySpark validation step",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    generate_all(args.output_dir, validate=not args.no_validate)
    logger.info("Data generation complete. Output: %s", args.output_dir.resolve())


if __name__ == "__main__":
    main()
