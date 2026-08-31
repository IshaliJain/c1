"""
Orchestrate all Bronze ingestion scripts in dependency-safe order.

Execution order:
  1. customers  (parent entity for order FK references)
  2. products   (parent entity for order FK references)
  3. orders     (references customers and products)

No transformations are applied at this layer — each script appends raw data
with metadata columns to its respective Delta table.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

try:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
except NameError:
    # In Databricks, __file__ is not defined; use current working directory
    sys.path.insert(0, str(Path.cwd()))

import importlib

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

INGEST_MODULES = [
    "01_ingest_customers",
    "03_ingest_products",
    "02_ingest_orders",
]


def main() -> dict[str, int]:
    results: dict[str, int] = {}

    for module_name in INGEST_MODULES:
        logger.info("=" * 60)
        logger.info("Running %s", module_name)
        module = importlib.import_module(module_name)
        row_count = module.main()
        results[module_name] = row_count

    logger.info("=" * 60)
    logger.info("Bronze ingest_all summary:")
    for module_name, count in results.items():
        logger.info("  %s: %d rows", module_name, count)
    logger.info("  Total rows ingested: %d", sum(results.values()))

    return results


if __name__ == "__main__":
    main()
