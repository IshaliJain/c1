"""
Execute Gold SQL aggregations and persist results as Delta tables.

Reads Silver Delta tables (via temp views), runs SQL scripts from src/gold/,
and writes Gold Delta tables for BI/dashboard consumption.

AI rationale:
  - SQL files separate business logic from orchestration for readability.
  - Only Silver rows with is_valid = true feed Gold aggregations.
  - Each Gold table is overwritten on each run for idempotent batch refresh.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from gold_common import get_spark, register_silver_views, write_gold_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

GOLD_QUERIES: list[tuple[str, str]] = [
    ("sales_by_product", "01_sales_by_product.sql"),
    ("revenue_by_customer", "02_revenue_by_customer.sql"),
    ("daily_weekly_trends", "03_daily_weekly_trends.sql"),
    ("customer_segmentation", "04_customer_segmentation.sql"),
]


def _gold_dir() -> Path:
    return Path(__file__).resolve().parent


def _read_sql(filename: str) -> str:
    sql_path = _gold_dir() / filename
    logger.info("Loading SQL: %s", sql_path.name)
    return sql_path.read_text(encoding="utf-8")


def create_gold_tables() -> dict[str, int]:
    spark = get_spark()
    register_silver_views(spark)

    results: dict[str, int] = {}
    for table_name, sql_file in GOLD_QUERIES:
        logger.info("=" * 60)
        logger.info("Building Gold table: %s", table_name)
        query = _read_sql(sql_file)
        gold_df = spark.sql(query)
        row_count = write_gold_table(gold_df, table_name)
        results[table_name] = row_count
        gold_df.show(5, truncate=False)

    logger.info("=" * 60)
    logger.info("Gold layer summary:")
    for table_name, count in results.items():
        logger.info("  %s: %d rows", table_name, count)

    return results


def main() -> dict[str, int]:
    return create_gold_tables()


if __name__ == "__main__":
    main()
