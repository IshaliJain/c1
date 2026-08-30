"""
Silver transformation: customers Bronze → Silver.

Applies four quality check categories, flags all rows, and writes to Silver Delta.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame

from quality_checks import (
    apply_completeness_checks,
    apply_logic_type_checks_customers,
    apply_no_referential_check,
    apply_uniqueness_check,
)
from silver_common import (
    build_entity_summary,
    build_quality_result,
    finalize_silver_df,
    get_spark,
    read_bronze,
    write_silver,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ENTITY = "customers"


def transform_customers() -> tuple[DataFrame, DataFrame]:
    spark = get_spark()
    bronze_df = read_bronze(spark, ENTITY)

    df = apply_completeness_checks(bronze_df, ["customer_id", "email"])
    df = apply_uniqueness_check(df, "customer_id")
    df = apply_no_referential_check(df)
    df = apply_logic_type_checks_customers(df)

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
        "Silver customers: %d total, %d passed, %d failed",
        passed + failed,
        passed,
        failed,
    )

    return silver_df, summary_df


def main() -> DataFrame:
    from pyspark.sql.functions import col

    _, summary_df = transform_customers()
    return summary_df


if __name__ == "__main__":
    main()
