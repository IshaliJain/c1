"""
Silver transformation: orders Bronze → Silver.

Applies four quality check categories including referential integrity against
Silver customers and products, flags all rows, and writes to Silver Delta.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from quality_checks import (
    apply_completeness_checks,
    apply_logic_type_checks_orders,
    apply_referential_checks,
    apply_uniqueness_check,
)
from silver_common import (
    build_entity_summary,
    build_quality_result,
    finalize_silver_df,
    get_spark,
    read_bronze,
    resolve_layer_paths,
    write_silver,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ENTITY = "orders"


def _read_silver_parent(spark, entity: str) -> DataFrame:
    path = resolve_layer_paths("silver", entity)
    logger.info("Reading Silver parent for FK check: %s", path)
    return spark.read.format("delta").load(path)


def transform_orders(
    customers_df: DataFrame | None = None,
    products_df: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    spark = get_spark()
    bronze_df = read_bronze(spark, ENTITY)

    if customers_df is None:
        customers_df = _read_silver_parent(spark, "customers")
    if products_df is None:
        products_df = _read_silver_parent(spark, "products")

    df = apply_completeness_checks(bronze_df, ["customer_id", "product_id"])
    df = apply_uniqueness_check(df, "order_id")
    df = apply_referential_checks(
        df,
        [
            ("customer_id", customers_df, "customer_id"),
            ("product_id", products_df, "product_id"),
        ],
    )
    df = apply_logic_type_checks_orders(df)

    df = df.withColumn(
        "quality_check_result",
        build_quality_result(
            col("_failed_completeness"),
            col("_failed_uniqueness"),
            col("_failed_referential"),
            col("_failed_logic_type"),
        ),
    )
    silver_df = finalize_silver_df(df).drop("_key_count")

    write_silver(silver_df, ENTITY)
    summary_df = build_entity_summary(silver_df, ENTITY)

    passed = silver_df.filter("is_valid = true").count()
    failed = silver_df.filter("is_valid = false").count()
    logger.info(
        "Silver orders: %d total, %d passed, %d failed",
        passed + failed,
        passed,
        failed,
    )

    return silver_df, summary_df


def main() -> DataFrame:
    _, summary_df = transform_orders()
    return summary_df


if __name__ == "__main__":
    main()
