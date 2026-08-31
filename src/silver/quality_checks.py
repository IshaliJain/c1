"""
Reusable data quality check functions for Silver layer.

Each function adds a boolean failure flag column; rows are never dropped.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, to_date, trim
from pyspark.sql.window import Window

from silver_common import is_null_or_empty


def apply_completeness_checks(df: DataFrame, critical_fields: list[str]) -> DataFrame:
    """Flag rows where any critical field is NULL or empty."""
    condition = None
    for field in critical_fields:
        field_failed = is_null_or_empty(field)
        condition = field_failed if condition is None else (condition | field_failed)

    return df.withColumn("_failed_completeness", condition)


def apply_uniqueness_check(df: DataFrame, key_column: str) -> DataFrame:
    """Flag rows participating in duplicate key groups using a window function."""
    window = Window.partitionBy(key_column)
    return df.withColumn("_key_count", count("*").over(window)).withColumn(
        "_failed_uniqueness",
        (col("_key_count") > 1) & col(key_column).isNotNull() & (col(key_column) != ""),
    )


def apply_referential_checks(
    df: DataFrame,
    references: list[tuple[str, DataFrame, str]],
) -> DataFrame:
    """
    Validate foreign keys via left joins; flag non-null orphans.

    Args:
        references: list of (fk_column, parent_df, parent_key_column)
    """
    result = df.withColumn("_failed_referential", lit(False))

    for idx, (fk_col, parent_df, parent_key) in enumerate(references):
        alias = f"_ref_parent_{idx}"
        valid = parent_df.select(col(parent_key).alias(f"{alias}_key")).distinct()
        result = result.join(
            valid,
            col(fk_col) == col(f"{alias}_key"),
            "left",
        )
        orphan = (
            col(fk_col).isNotNull()
            & (trim(col(fk_col)) != "")
            & col(f"{alias}_key").isNull()
        )
        result = result.withColumn(
            "_failed_referential",
            col("_failed_referential") | orphan,
        ).drop(f"{alias}_key")

    return result


def apply_no_referential_check(df: DataFrame) -> DataFrame:
    """Placeholder for entities with no FK checks."""
    return df.withColumn("_failed_referential", lit(False))


VALID_ORDER_STATUSES = [
    "Pending", "Processing", "Shipped", "Delivered", "Cancelled", "Returned",
]


def apply_logic_type_checks_customers(df: DataFrame) -> DataFrame:
    """Validate signup_date, email format, and non-negative lifetime_value."""
    signup_valid = col("signup_date").isNull() | to_date(
        col("signup_date"), "yyyy-MM-dd"
    ).isNotNull()
    email_valid = is_null_or_empty("email") | col("email").rlike(
        r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
    )
    ltv_numeric = col("lifetime_value").cast("double")
    ltv_valid = col("lifetime_value").isNull() | (
        ltv_numeric.isNotNull() & (ltv_numeric >= 0)
    )

    return df.withColumn(
        "_failed_logic_type",
        ~(signup_valid & email_valid & ltv_valid),
    )


def apply_logic_type_checks_products(df: DataFrame) -> DataFrame:
    """Validate non-negative numeric catalog fields."""
    condition = None
    for field in ["price", "cost", "stock_quantity", "reorder_level"]:
        casted = col(field).cast("double")
        field_valid = col(field).isNull() | (casted.isNotNull() & (casted >= 0))
        condition = field_valid if condition is None else (condition & field_valid)

    return df.withColumn("_failed_logic_type", ~condition)


def apply_logic_type_checks_orders(df: DataFrame) -> DataFrame:
    """Validate dates and positive numeric order fields."""
    order_date_valid = col("order_date").isNull() | to_date(
        col("order_date"), "yyyy-MM-dd"
    ).isNotNull()
    payment_date_valid = is_null_or_empty("payment_date") | to_date(
        col("payment_date"), "yyyy-MM-dd"
    ).isNotNull()

    quantity_valid = col("quantity").isNull() | (
        col("quantity").cast("double").isNotNull()
        & (col("quantity").cast("double") > 0)
    )
    unit_price_valid = col("unit_price").isNull() | (
        col("unit_price").cast("double").isNotNull()
        & (col("unit_price").cast("double") > 0)
    )
    total_amount_valid = col("total_amount").isNull() | (
        col("total_amount").cast("double").isNotNull()
        & (col("total_amount").cast("double") > 0)
    )
    status_valid = col("order_status").isNull() | col("order_status").isin(
        VALID_ORDER_STATUSES
    )

    return df.withColumn(
        "_failed_logic_type",
        ~(
            order_date_valid
            & payment_date_valid
            & quantity_valid
            & unit_price_valid
            & total_amount_valid
            & status_valid
        ),
    )
