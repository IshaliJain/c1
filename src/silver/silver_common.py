"""
Shared utilities for Silver layer transformations.

AI rationale:
  - Centralizes Bronze/Silver path resolution and quality-result column logic.
  - Quality checks are composable: each returns failure flags merged into
    quality_check_result without dropping rows.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    current_timestamp,
    lit,
    trim,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

CHECK_COMPLETENESS = "FAILED_COMPLETENESS"
CHECK_UNIQUENESS = "FAILED_UNIQUENESS"
CHECK_REFERENTIAL = "FAILED_REFERENTIAL"
CHECK_LOGIC_TYPE = "FAILED_LOGIC_TYPE"
PASSED = "PASSED"

QUALITY_SUMMARY_SCHEMA = StructType(
    [
        StructField("entity", StringType(), False),
        StructField("check_category", StringType(), False),
        StructField("total_rows", DoubleType(), False),
        StructField("passed_count", DoubleType(), False),
        StructField("failed_count", DoubleType(), False),
        StructField("pass_percentage", DoubleType(), False),
        StructField("_summary_generated_at", TimestampType(), False),
    ]
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def is_databricks_runtime() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def resolve_layer_paths(layer: str, entity: str) -> str:
    """Resolve Delta path for bronze, silver, or quality_summary."""
    if is_databricks_runtime():
        base = "/dbfs/tmp/databricks-medallion-pipeline"
    else:
        base = str(project_root() / "data" / "delta")

    if layer == "quality_summary":
        return f"{base}/silver/quality_summary"

    return f"{base}/{layer}/{entity}"


def read_bronze(spark: SparkSession, entity: str) -> DataFrame:
    path = resolve_layer_paths("bronze", entity)
    logger.info("Reading Bronze Delta: %s", path)
    return spark.read.format("delta").load(path)


def write_silver(df: DataFrame, entity: str) -> None:
    path = resolve_layer_paths("silver", entity)
    row_count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(path)
    )
    logger.info("Silver Delta written: %s (%d rows)", path, row_count)


def is_null_or_empty(column_name: str):
    """True when a string column is NULL or blank after trim."""
    return col(column_name).isNull() | (trim(col(column_name)) == "")


def build_quality_result(
    completeness_failed,
    uniqueness_failed,
    referential_failed,
    logic_type_failed,
):
    """Build pipe-delimited quality_check_result from boolean failure flags."""
    return (
        when(
            completeness_failed
            | uniqueness_failed
            | referential_failed
            | logic_type_failed,
            concat_ws(
                "|",
                when(completeness_failed, lit(CHECK_COMPLETENESS)),
                when(uniqueness_failed, lit(CHECK_UNIQUENESS)),
                when(referential_failed, lit(CHECK_REFERENTIAL)),
                when(logic_type_failed, lit(CHECK_LOGIC_TYPE)),
            ),
        ).otherwise(lit(PASSED))
    )


def finalize_silver_df(df: DataFrame) -> DataFrame:
    """Add is_valid flag and Silver processing timestamp."""
    return df.withColumn(
        "is_valid", col("quality_check_result") == lit(PASSED)
    ).withColumn("_silver_processed_at", current_timestamp())


def compute_check_metrics(
    df: DataFrame,
    entity: str,
    check_category: str,
    failed_col: str,
) -> DataFrame:
    """Compute passed/failed counts for a single check category."""
    spark = df.sparkSession
    total = df.count()
    failed = df.filter(col(failed_col)).count()
    passed = total - failed
    pass_pct = round((passed / total) * 100, 2) if total > 0 else 100.0

    return spark.createDataFrame(
        [
            (
                entity,
                check_category,
                float(total),
                float(passed),
                float(failed),
                float(pass_pct),
            )
        ],
        schema="entity string, check_category string, total_rows double, "
        "passed_count double, failed_count double, pass_percentage double",
    )


def build_entity_summary(df: DataFrame, entity: str) -> DataFrame:
    """Build quality summary metrics for all four check categories on an entity."""
    metrics = [
        compute_check_metrics(df, entity, "Completeness", "_failed_completeness"),
        compute_check_metrics(df, entity, "Uniqueness", "_failed_uniqueness"),
        compute_check_metrics(df, entity, "Referential Integrity", "_failed_referential"),
        compute_check_metrics(df, entity, "Logic & Type", "_failed_logic_type"),
    ]
    summary = metrics[0]
    for metric_df in metrics[1:]:
        summary = summary.union(metric_df)
    return summary.withColumn("_summary_generated_at", current_timestamp())


def write_quality_summary(summary_dfs: list[DataFrame]) -> None:
    """Write combined quality summary across all entities."""
    if not summary_dfs:
        return

    combined = summary_dfs[0]
    for summary_df in summary_dfs[1:]:
        combined = combined.union(summary_df)

    path = resolve_layer_paths("quality_summary", "")
    (
        combined.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(path)
    )
    logger.info("Quality summary written: %s", path)
    combined.show(truncate=False)
