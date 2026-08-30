"""
Orchestrate all Silver transformations and write combined quality summary.

Execution order:
  1. customers  (no FK dependencies)
  2. products   (no FK dependencies)
  3. orders     (FK checks against Silver customers and products)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import importlib

from silver_common import write_quality_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    customers_mod = importlib.import_module("01_transform_customers")
    products_mod = importlib.import_module("03_transform_products")
    orders_mod = importlib.import_module("02_transform_orders")

    logger.info("=" * 60)
    logger.info("Transforming customers")
    customers_df, customers_summary = customers_mod.transform_customers()

    logger.info("=" * 60)
    logger.info("Transforming products")
    products_df, products_summary = products_mod.transform_products()

    logger.info("=" * 60)
    logger.info("Transforming orders")
    _, orders_summary = orders_mod.transform_orders(
        customers_df=customers_df,
        products_df=products_df,
    )

    write_quality_summary([customers_summary, products_summary, orders_summary])
    logger.info("Silver transform_all complete")


if __name__ == "__main__":
    main()
