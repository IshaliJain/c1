"""
Shared utilities for Bronze layer ingestion.

AI rationale:
  - Centralizes path resolution for local vs Databricks without hardcoded absolute paths.
  - Single ingest function ensures consistent metadata columns and append-only writes.
  - All source columns read as StringType to preserve raw values and intentional defects.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

logger = logging.getLogger(__name__)

METADATA_SCHEMA = StructType(
    [
        StructField("_ingestion_timestamp", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
    ]
)

CUSTOMER_SOURCE_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_value", StringType(), True),
    ]
)

ORDER_SOURCE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_date", StringType(), True),
    ]
)

PRODUCT_SOURCE_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("cost", StringType(), True),
        StructField("stock_quantity", StringType(), True),
        StructField("reorder_level", StringType(), True),
    ]
)


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def is_databricks_runtime() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def get_spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def resolve_paths(entity: str) -> tuple[str, str]:
    """
    Resolve source CSV and target Delta paths for the given entity.

    Local:
      source: <project>/data/{entity}.csv
      target: <project>/data/delta/bronze/{entity}

    Databricks:
      source: /dbfs/tmp/databricks-medallion-pipeline/data/{entity}.csv
      target: /dbfs/tmp/databricks-medallion-pipeline/bronze/{entity}
    """
    if is_databricks_runtime():
        base = "/dbfs/tmp/databricks-medallion-pipeline"
        source_path = f"{base}/data/{entity}.csv"
        target_path = f"{base}/bronze/{entity}"
    else:
        root = project_root()
        source_path = str(root / "data" / f"{entity}.csv")
        target_path = str(root / "data" / "delta" / "bronze" / entity)

    return source_path, target_path


def ingest_to_bronze(
    spark: SparkSession,
    entity: str,
    schema: StructType,
) -> int:
    """
    Read raw CSV, add metadata columns, append to Delta. No cleansing applied.

    Returns the number of rows ingested in this run.
    """
    source_path, target_path = resolve_paths(entity)

    logger.info("Starting Bronze ingestion for '%s'", entity)
    logger.info("  Source CSV: %s", source_path)
    logger.info("  Target Delta: %s", target_path)

    df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(source_path)
    )

    bronze_df: DataFrame = df.withColumn(
        "_ingestion_timestamp", current_timestamp()
    ).withColumn("_source_file", input_file_name())

    row_count = bronze_df.count()
    logger.info("  Rows read from source: %d", row_count)

    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(target_path)
    )

    logger.info("  Delta table created/updated at: %s", target_path)
    logger.info("  Bronze ingestion complete for '%s' (%d rows appended)", entity, row_count)

    return row_count
