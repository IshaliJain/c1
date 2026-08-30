"""
Bronze ingestion: customers.csv → Delta table.

Reads raw customer data with explicit schema, adds metadata columns,
and appends to the Bronze Delta table. No cleansing or transformations.

Validation:
  - Confirms source file exists before read (via Spark read error).
  - Logs row count and target Delta path after append.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from bronze_common import CUSTOMER_SOURCE_SCHEMA, get_spark, ingest_to_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

ENTITY = "customers"


def main() -> int:
    spark = get_spark()
    return ingest_to_bronze(spark, ENTITY, CUSTOMER_SOURCE_SCHEMA)


if __name__ == "__main__":
    rows = main()
    logger.info("01_ingest_customers finished: %d rows", rows)
