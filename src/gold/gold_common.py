"""
Shared utilities for Gold layer aggregations.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def is_databricks_runtime() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def resolve_silver_path(entity: str) -> str:
    if is_databricks_runtime():
        base = "/dbfs/tmp/databricks-medallion-pipeline"
    else:
        base = str(project_root() / "data" / "delta")
    return f"{base}/silver/{entity}"


def resolve_gold_path(table_name: str) -> str:
    if is_databricks_runtime():
        base = "/dbfs/tmp/databricks-medallion-pipeline"
    else:
        base = str(project_root() / "data" / "delta")
    return f"{base}/gold/{table_name}"


def register_silver_views(spark: SparkSession) -> None:
    """Register Silver Delta tables as temp views for Gold SQL."""
    for entity in ("customers", "products", "orders"):
        path = resolve_silver_path(entity)
        logger.info("Registering temp view silver_%s from %s", entity, path)
        (
            spark.read.format("delta")
            .load(path)
            .createOrReplaceTempView(f"silver_{entity}")
        )


def write_gold_table(df: DataFrame, table_name: str) -> int:
    path = resolve_gold_path(table_name)
    row_count = df.count()
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(path)
    )
    logger.info("Gold table written: %s (%d rows)", path, row_count)
    return row_count
