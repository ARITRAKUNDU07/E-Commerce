# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Exploratory Data Profiling (Bronze Layer)
# MAGIC
# MAGIC Profiles all 9 Bronze Delta tables to surface data quality issues before Silver
# MAGIC transformations:
# MAGIC - Row and column counts
# MAGIC - Null value counts and percentages per column
# MAGIC - Primary key uniqueness and duplicate detection
# MAGIC - Order status distribution and timestamp ranges
# MAGIC - Numeric range anomalies (negative prices/freight, review scores)
# MAGIC - Referential integrity gaps (e.g., untranslated product categories)

# COMMAND ----------

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    BRONZE_TABLES,
    CATALOG,
    SCHEMA,
    get_bronze_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("02_data_profiling")

# Expected natural keys for uniqueness diagnostics across Bronze tables
KEY_COLUMNS_BY_DATASET: Dict[str, List[str]] = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "category_translation": ["product_category_name"],
}

# COMMAND ----------


def profile_nulls(df: DataFrame, table_name: str) -> DataFrame:
    """Compute null counts and null percentages for every non-audit column.

    Args:
        df: Spark DataFrame to profile.
        table_name: Label used in output summary rows.

    Returns:
        Spark DataFrame with columns ``table_name, column_name, total_rows, null_count, null_pct``.
    """
    business_cols = [c for c in df.columns if not c.startswith("_")]
    total_rows = df.count()

    if total_rows == 0:
        return df.sparkSession.createDataFrame(
            [(table_name, c, 0, 0, 0.0) for c in business_cols],
            ["table_name", "column_name", "total_rows", "null_count", "null_pct"],
        )

    agg_exprs = [
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), 1).otherwise(0)).alias(c)
        for c in business_cols
    ]
    null_counts_row = df.agg(*agg_exprs).collect()[0]

    rows: List[Tuple[str, str, int, int, float]] = []
    for col_name in business_cols:
        null_cnt = int(null_counts_row[col_name] or 0)
        null_pct = round((null_cnt / total_rows) * 100.0, 2)
        rows.append((table_name, col_name, total_rows, null_cnt, null_pct))

    return df.sparkSession.createDataFrame(
        rows,
        ["table_name", "column_name", "total_rows", "null_count", "null_pct"],
    )


def profile_key_duplicates(
    df: DataFrame,
    table_name: str,
    key_cols: List[str],
) -> Tuple[str, str, int, int, int]:
    """Measure duplicate key records in a Bronze table.

    Args:
        df: Source DataFrame.
        table_name: Table label.
        key_cols: Columns forming the candidate primary key.

    Returns:
        Tuple of ``(table_name, key_cols_str, total_rows, distinct_keys, duplicate_rows)``.
    """
    total_rows = df.count()
    distinct_keys = df.select(*key_cols).distinct().count()
    duplicate_rows = total_rows - distinct_keys
    return (table_name, ", ".join(key_cols), total_rows, distinct_keys, duplicate_rows)


def run_data_profiling(
    spark: SparkSession,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> None:
    """Execute end-to-end exploratory profiling on all 9 Bronze Delta tables.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.
    """
    logger.info("Starting Bronze data profiling in %s.%s", catalog, schema)

    duplicate_summary: List[Tuple[str, str, int, int, int]] = []
    null_profile_dfs: List[DataFrame] = []

    for dataset_key in BRONZE_TABLES:
        full_table = get_bronze_table(dataset_key, catalog=catalog, schema=schema)
        df = spark.table(full_table)

        dup_row = profile_key_duplicates(df, full_table, KEY_COLUMNS_BY_DATASET[dataset_key])
        duplicate_summary.append(dup_row)

        null_df = profile_nulls(df, full_table).filter(F.col("null_count") > 0)
        null_profile_dfs.append(null_df)

    # 1. Row counts & Key Uniqueness Summary
    dup_summary_df = spark.createDataFrame(
        duplicate_summary,
        ["table_name", "candidate_key", "total_rows", "distinct_keys", "duplicate_rows"],
    )
    print("\n=== 1. BRONZE TABLE ROW COUNTS & KEY DUPLICATES ===")
    dup_summary_df.show(truncate=False)

    # 2. Columns with Null / Blank Values
    all_nulls_df = null_profile_dfs[0]
    for next_df in null_profile_dfs[1:]:
        all_nulls_df = all_nulls_df.unionByName(next_df)

    print("\n=== 2. COLUMNS WITH NULL OR BLANK VALUES ===")
    all_nulls_df.orderBy(F.col("null_pct").desc()).show(50, truncate=False)

    # 3. Order Status Distribution
    orders_df = spark.table(get_bronze_table("orders", catalog=catalog, schema=schema))
    print("\n=== 3. ORDER STATUS DISTRIBUTION ===")
    (
        orders_df.groupBy("order_status")
        .agg(F.count("*").alias("order_count"))
        .withColumn("pct", F.round(F.col("order_count") / orders_df.count() * 100, 2))
        .orderBy(F.col("order_count").desc())
        .show(truncate=False)
    )

    # 4. Date Range & Delivery Anomaly Check
    print("\n=== 4. ORDER TIMESTAMP RANGES & DELIVERY ANOMALIES ===")
    orders_ts = orders_df.select(
        F.to_timestamp("order_purchase_timestamp").alias("purchase_ts"),
        F.to_timestamp("order_delivered_customer_date").alias("delivered_ts"),
        F.to_timestamp("order_estimated_delivery_date").alias("estimated_ts"),
    )
    orders_ts.agg(
        F.min("purchase_ts").alias("min_purchase_ts"),
        F.max("purchase_ts").alias("max_purchase_ts"),
        F.sum(F.when(F.col("delivered_ts") < F.col("purchase_ts"), 1).otherwise(0)).alias(
            "delivered_before_purchase"
        ),
        F.sum(F.when(F.col("delivered_ts").isNull(), 1).otherwise(0)).alias("missing_delivery_date"),
    ).show(truncate=False)

    # 5. Numeric Summary Statistics (Order Items & Payments)
    items_df = spark.table(get_bronze_table("order_items", catalog=catalog, schema=schema))
    print("\n=== 5. ORDER ITEMS PRICE & FREIGHT SUMMARY ===")
    items_df.select("price", "freight_value").summary("count", "min", "mean", "50%", "max").show()

    payments_df = spark.table(get_bronze_table("order_payments", catalog=catalog, schema=schema))
    print("\n=== 6. PAYMENT TYPES & MULTI-PAYMENT ORDERS ===")
    payments_df.groupBy("payment_type").agg(
        F.count("*").alias("payment_rows"),
        F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
        F.round(F.avg("payment_installments"), 2).alias("avg_installments"),
    ).orderBy(F.col("total_payment_value").desc()).show(truncate=False)

    multi_pay_orders = (
        payments_df.groupBy("order_id").count().filter(F.col("count") > 1).count()
    )
    print(f"Orders with multiple payment rows (require pre-aggregation in Silver): {multi_pay_orders:,d}")

    # 6. Product Categories Missing English Translation
    products_df = spark.table(get_bronze_table("products", catalog=catalog, schema=schema))
    translation_df = spark.table(
        get_bronze_table("category_translation", catalog=catalog, schema=schema)
    )
    untranslated_df = (
        products_df.select("product_category_name")
        .filter(F.col("product_category_name").isNotNull())
        .distinct()
        .join(translation_df, on="product_category_name", how="left_anti")
    )
    print("\n=== 7. PRODUCT CATEGORIES MISSING ENGLISH TRANSLATION ===")
    untranslated_df.show(truncate=False)

    logger.info("Bronze data profiling completed successfully.")


# COMMAND ----------

if __name__ == "__main__":
    active_spark = SparkSession.builder.getOrCreate()
    run_data_profiling(active_spark)
