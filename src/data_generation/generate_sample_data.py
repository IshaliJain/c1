"""
Generate synthetic e-commerce CSV datasets with intentional data quality defects.

Uses Faker + pandas for local generation, then validates output with PySpark
to confirm Databricks-compatible ingestion.

Injects exactly 700 intentional defect instances across four quality dimensions
and writes a defect manifest to data/manifest/defect_manifest.csv.
"""

from __future__ import annotations

import argparse
import logging
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Seed & volume parameters (documented in DATA_GENERATION_NOTES.md)
# ---------------------------------------------------------------------------
RANDOM_SEED = 42
FAKER_SEED = 42
TARGET_DEFECT_COUNT = 700

NUM_CUSTOMERS = 10_000
NUM_ORDERS = 100_000
NUM_PRODUCTS = 500

# Completeness defects (370 total)
NULL_EMAILS = 50
NULL_ORDER_CUSTOMER_IDS = 100
NULL_ORDER_PRODUCT_IDS = 200
EMPTY_PRODUCT_NAMES = 20

# Uniqueness defects (50 total)
DUPLICATE_CUSTOMER_IDS = 10
DUPLICATE_ORDER_IDS = 20
DUPLICATE_PRODUCT_IDS = 20

# Referential integrity defects (80 total)
ORPHAN_CUSTOMER_IDS = 50
ORPHAN_PRODUCT_IDS = 30

# Logic & type / business rule defects (200 total)
INVALID_EMAILS = 45
INVALID_SIGNUP_DATES = 25
NEGATIVE_LIFETIME_VALUES = 20
NEGATIVE_PRODUCT_PRICES = 25
NEGATIVE_ORDER_QUANTITIES = 25
NEGATIVE_UNIT_PRICES = 25
INVALID_ORDER_DATES = 35

# Removed INVALID_ORDER_STATUSES to keep total at exactly 700

CUSTOMER_SEGMENTS = ["Bronze", "Silver", "Gold", "Platinum"]
PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports",
    "Books", "Beauty", "Toys", "Food",
]
ORDER_STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned"]
COUNTRIES = ["US", "UK", "CA", "DE", "FR", "IN", "AU", "JP", "BR", "MX"]

ORPHAN_CUSTOMER_PREFIX = "ORPHAN-CUST"
ORPHAN_PRODUCT_PREFIX = "ORPHAN-PROD"

DEFECT_MANIFEST: list[dict[str, str]] = []

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path.cwd().parents[1]


def _init_faker() -> Faker:
    fake = Faker()
    Faker.seed(FAKER_SEED)
    fake.seed_instance(FAKER_SEED)
    return fake


def _random_date(fake: Faker, start: datetime, end: datetime) -> str:
    return fake.date_between(start_date=start, end_date=end).isoformat()


def _record_defect(
    entity: str,
    primary_key: str,
    category: str,
    error_code: str,
    field_name: str,
    description: str,
    injected_value: str,
) -> None:
    DEFECT_MANIFEST.append(
        {
            "defect_id": str(len(DEFECT_MANIFEST) + 1),
            "entity": entity,
            "primary_key_value": primary_key,
            "category": category,
            "error_code": error_code,
            "field_name": field_name,
            "defect_description": description,
            "injected_value": injected_value,
        }
    )


def _pick_indices(df: pd.DataFrame, count: int, used: set[int]) -> list[int]:
    available = [i for i in range(len(df)) if i not in used]
    chosen = random.sample(available, count)
    used.update(chosen)
    return chosen


def generate_products(fake: Faker) -> pd.DataFrame:
    """Generate product catalog with intentional uniqueness and logic defects."""
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

    df = pd.DataFrame(rows)
    used: set[int] = set()

    # Uniqueness: duplicate product_ids
    dup_source_ids = df.loc[: DUPLICATE_PRODUCT_IDS - 1, "product_id"].tolist()
    for idx, source_id in zip(
        _pick_indices(df, DUPLICATE_PRODUCT_IDS, used), dup_source_ids
    ):
        pk = df.at[idx, "product_id"]
        df.at[idx, "product_id"] = source_id
        _record_defect(
            "products", pk, "Uniqueness", "UNIQ_003", "product_id",
            "Duplicate product_id", source_id,
        )

    # Completeness: empty product_name
    for idx in _pick_indices(df, EMPTY_PRODUCT_NAMES, used):
        pk = df.at[idx, "product_id"]
        df.at[idx, "product_name"] = ""
        _record_defect(
            "products", pk, "Completeness", "COMP_004", "product_name",
            "Empty product_name", "",
        )

    # Logic: negative prices
    for idx in _pick_indices(df, NEGATIVE_PRODUCT_PRICES, used):
        pk = df.at[idx, "product_id"]
        df.at[idx, "price"] = round(random.uniform(-500.0, -1.0), 2)
        _record_defect(
            "products", pk, "Logic & Type", "BIZ_001", "price",
            "Negative product price", str(df.at[idx, "price"]),
        )

    return df


def generate_customers(fake: Faker) -> pd.DataFrame:
    """Generate customer records with completeness, uniqueness, and logic defects."""
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
    used: set[int] = set()

    for idx in _pick_indices(df, NULL_EMAILS, used):
        pk = df.at[idx, "customer_id"]
        df.at[idx, "email"] = None
        _record_defect(
            "customers", pk, "Completeness", "COMP_002", "email",
            "NULL email", "",
        )

    dup_source_ids = df.loc[: DUPLICATE_CUSTOMER_IDS - 1, "customer_id"].tolist()
    for idx, source_id in zip(
        _pick_indices(df, DUPLICATE_CUSTOMER_IDS, used), dup_source_ids
    ):
        pk = df.at[idx, "customer_id"]
        df.at[idx, "customer_id"] = source_id
        _record_defect(
            "customers", pk, "Uniqueness", "UNIQ_001", "customer_id",
            "Duplicate customer_id", source_id,
        )

    invalid_emails = ["not-an-email", "bad@", "@missing-local.com", "spaces in@email.com", "nodomain"]
    for i, idx in enumerate(_pick_indices(df, INVALID_EMAILS, used)):
        pk = df.at[idx, "customer_id"]
        bad_email = invalid_emails[i % len(invalid_emails)]
        df.at[idx, "email"] = bad_email
        _record_defect(
            "customers", pk, "Logic & Type", "TYPE_002", "email",
            "Invalid email format", bad_email,
        )

    bad_dates = ["2024-13-45", "not-a-date", "32/01/2020", "2020-02-30"]
    for i, idx in enumerate(_pick_indices(df, INVALID_SIGNUP_DATES, used)):
        pk = df.at[idx, "customer_id"]
        bad_date = bad_dates[i % len(bad_dates)]
        df.at[idx, "signup_date"] = bad_date
        _record_defect(
            "customers", pk, "Logic & Type", "TYPE_005", "signup_date",
            "Invalid signup_date", bad_date,
        )

    for idx in _pick_indices(df, NEGATIVE_LIFETIME_VALUES, used):
        pk = df.at[idx, "customer_id"]
        df.at[idx, "lifetime_value"] = round(random.uniform(-5000.0, -1.0), 2)
        _record_defect(
            "customers", pk, "Logic & Type", "BIZ_001", "lifetime_value",
            "Negative lifetime_value", str(df.at[idx, "lifetime_value"]),
        )

    return df


def generate_orders(
    fake: Faker,
    customers: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    """Generate order records with completeness, uniqueness, referential, and logic defects."""
    valid_customer_ids = customers["customer_id"].unique().tolist()
    valid_product_ids = products["product_id"].tolist()
    order_start = datetime(2020, 1, 1)
    order_end = datetime(2025, 12, 31)

    rows = []
    for i in range(1, NUM_ORDERS + 1):
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        order_date = _random_date(fake, order_start, order_end)
        status = random.choice(ORDER_STATUSES)
        payment_date = order_date if status not in {"Pending", "Cancelled"} else None
        rows.append(
            {
                "order_id": f"ORD-{i:06d}",
                "customer_id": random.choice(valid_customer_ids),
                "order_date": order_date,
                "product_id": random.choice(valid_product_ids),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": round(quantity * unit_price, 2),
                "order_status": status,
                "payment_date": payment_date,
            }
        )

    df = pd.DataFrame(rows)
    used: set[int] = set()

    for idx in _pick_indices(df, NULL_ORDER_CUSTOMER_IDS, used):
        pk = df.at[idx, "order_id"]
        df.at[idx, "customer_id"] = None
        _record_defect(
            "orders", pk, "Completeness", "COMP_007", "customer_id",
            "NULL customer_id", "",
        )

    for idx in _pick_indices(df, NULL_ORDER_PRODUCT_IDS, used):
        pk = df.at[idx, "order_id"]
        df.at[idx, "product_id"] = None
        _record_defect(
            "orders", pk, "Completeness", "COMP_003", "product_id",
            "NULL product_id", "",
        )

    orphan_customer_values = [
        f"{ORPHAN_CUSTOMER_PREFIX}-{i:03d}" for i in range(1, ORPHAN_CUSTOMER_IDS + 1)
    ]
    for idx, orphan_id in zip(
        _pick_indices(df, ORPHAN_CUSTOMER_IDS, used), orphan_customer_values
    ):
        pk = df.at[idx, "order_id"]
        df.at[idx, "customer_id"] = orphan_id
        _record_defect(
            "orders", pk, "Referential Integrity", "REF_001", "customer_id",
            "Orphan customer_id", orphan_id,
        )

    orphan_product_values = [
        f"{ORPHAN_PRODUCT_PREFIX}-{i:03d}" for i in range(1, ORPHAN_PRODUCT_IDS + 1)
    ]
    for idx, orphan_id in zip(
        _pick_indices(df, ORPHAN_PRODUCT_IDS, used), orphan_product_values
    ):
        pk = df.at[idx, "order_id"]
        df.at[idx, "product_id"] = orphan_id
        _record_defect(
            "orders", pk, "Referential Integrity", "REF_003", "product_id",
            "Orphan product_id", orphan_id,
        )

    dup_source_ids = df.loc[: DUPLICATE_ORDER_IDS - 1, "order_id"].tolist()
    for idx, source_id in zip(
        _pick_indices(df, DUPLICATE_ORDER_IDS, used), dup_source_ids
    ):
        pk = df.at[idx, "order_id"]
        df.at[idx, "order_id"] = source_id
        _record_defect(
            "orders", pk, "Uniqueness", "UNIQ_004", "order_id",
            "Duplicate order_id", source_id,
        )

    bad_dates = ["2024-13-45", "invalid-date", "2020-02-30"]
    for i, idx in enumerate(_pick_indices(df, INVALID_ORDER_DATES, used)):
        pk = df.at[idx, "order_id"]
        bad_date = bad_dates[i % len(bad_dates)]
        df.at[idx, "order_date"] = bad_date
        _record_defect(
            "orders", pk, "Logic & Type", "TYPE_005", "order_date",
            "Invalid order_date", bad_date,
        )

    for idx in _pick_indices(df, NEGATIVE_ORDER_QUANTITIES, used):
        pk = df.at[idx, "order_id"]
        df.at[idx, "quantity"] = random.randint(-10, -1)
        _record_defect(
            "orders", pk, "Logic & Type", "BIZ_002", "quantity",
            "Negative quantity", str(df.at[idx, "quantity"]),
        )

    for idx in _pick_indices(df, NEGATIVE_UNIT_PRICES, used):
        pk = df.at[idx, "order_id"]
        df.at[idx, "unit_price"] = round(random.uniform(-500.0, -1.0), 2)
        _record_defect(
            "orders", pk, "Logic & Type", "BIZ_001", "unit_price",
            "Negative unit_price", str(df.at[idx, "unit_price"]),
        )

    return df


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, na_rep="")
    logger.info("Wrote %s (%d rows)", path.name, len(df))


def write_manifest(output_dir: Path) -> None:
    manifest_dir = output_dir / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "defect_manifest.csv"
    pd.DataFrame(DEFECT_MANIFEST).to_csv(manifest_path, index=False)
    logger.info("Wrote defect manifest: %s (%d defects)", manifest_path, len(DEFECT_MANIFEST))


def validate_with_pyspark(output_dir: Path) -> None:
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
        logger.warning("PySpark validation skipped (Java/Spark unavailable): %s", exc)
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
            logger.info("PySpark validation passed: %s (%d rows)", filename, actual_count)
    finally:
        spark.stop()


def log_defect_summary() -> dict[str, int]:
    manifest_df = pd.DataFrame(DEFECT_MANIFEST)
    by_category = manifest_df.groupby("category").size().to_dict() if len(manifest_df) else {}
    summary = {"total_defects": len(DEFECT_MANIFEST), **by_category}

    logger.info("Defect summary:")
    for key, value in summary.items():
        logger.info("  %s: %d", key, value)

    if len(DEFECT_MANIFEST) != TARGET_DEFECT_COUNT:
        logger.warning(
            "Expected %d defects, got %d", TARGET_DEFECT_COUNT, len(DEFECT_MANIFEST)
        )

    return summary


def generate_all(output_dir: Path, validate: bool = True) -> dict[str, int]:
    global DEFECT_MANIFEST
    DEFECT_MANIFEST = []

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
    write_manifest(output_dir)

    summary = log_defect_summary()

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
