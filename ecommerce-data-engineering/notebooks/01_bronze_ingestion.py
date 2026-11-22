# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer Ingestion
# MAGIC
# MAGIC Ingests all 9 raw Olist Brazilian E-Commerce CSV files from the Unity Catalog Volume into
# MAGIC Bronze Delta tables using:
# MAGIC - Explicit `StructType` schemas (preventing schema inference drift)
# MAGIC - Audit metadata columns: `_ingested_at` (`current_timestamp`) and `_source_file` (`_metadata.file_path`)
# MAGIC - Idempotent Delta overwrites (`mode("overwrite")` with `overwriteSchema=true`)

# COMMAND ----------

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Ensure project root is importable in both Databricks Repos and local Python runs
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    BRONZE_TABLES,
    CATALOG,
    RAW_DATA_PATH,
    SCHEMA,
    get_bronze_table,
    get_raw_file_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("01_bronze_ingestion")

# COMMAND ----------

# Explicit StructType schemas for all 9 Olist raw CSV files.
# Business Rule: Timestamps are ingested as StringType in Bronze so raw values are
# preserved faithfully without silent coercion failures; Silver handles to_timestamp().
# Zip code prefixes are ingested as StringType to preserve leading zeros in Brazilian CEPs.
BRONZE_SCHEMAS: Dict[str, StructType] = {
    "orders": StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", StringType(), True),
            StructField("order_approved_at", StringType(), True),
            StructField("order_delivered_carrier_date", StringType(), True),
            StructField("order_delivered_customer_date", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True),
        ]
    ),
    "order_items": StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
        ]
    ),
    "order_payments": StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("payment_sequential", IntegerType(), True),
            StructField("payment_type", StringType(), True),
            StructField("payment_installments", IntegerType(), True),
            StructField("payment_value", DoubleType(), True),
        ]
    ),
    "order_reviews": StructType(
        [
            StructField("review_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("review_score", IntegerType(), True),
            StructField("review_comment_title", StringType(), True),
            StructField("review_comment_message", StringType(), True),
            StructField("review_creation_date", StringType(), True),
            StructField("review_answer_timestamp", StringType(), True),
        ]
    ),
    "customers": StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("customer_unique_id", StringType(), True),
            StructField("customer_zip_code_prefix", StringType(), True),
            StructField("customer_city", StringType(), True),
            StructField("customer_state", StringType(), True),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_category_name", StringType(), True),
            # Note: Source Kaggle CSV spells 'length' as 'lenght'; normalized in Silver.
            StructField("product_name_lenght", IntegerType(), True),
            StructField("product_description_lenght", IntegerType(), True),
            StructField("product_photos_qty", IntegerType(), True),
            StructField("product_weight_g", DoubleType(), True),
            StructField("product_length_cm", DoubleType(), True),
            StructField("product_height_cm", DoubleType(), True),
            StructField("product_width_cm", DoubleType(), True),
        ]
    ),
    "sellers": StructType(
        [
            StructField("seller_id", StringType(), True),
            StructField("seller_zip_code_prefix", StringType(), True),
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True),
        ]
    ),
    "geolocation": StructType(
        [
            StructField("geolocation_zip_code_prefix", StringType(), True),
            StructField("geolocation_lat", DoubleType(), True),
            StructField("geolocation_lng", DoubleType(), True),
            StructField("geolocation_city", StringType(), True),
            StructField("geolocation_state", StringType(), True),
        ]
    ),
    "category_translation": StructType(
        [
            StructField("product_category_name", StringType(), True),
            StructField("product_category_name_english", StringType(), True),
        ]
    ),
}

# Dataset-specific CSV reader options.
# Business Rule: Olist review comments frequently contain embedded newlines and quotes;
# enabling multiLine=True and escape='"' prevents row splitting and column shifts.
CSV_READER_OPTIONS: Dict[str, Dict[str, str]] = {
    "order_reviews": {
        "multiLine": "true",
        "quote": '"',
        "escape": '"',
    }
}

# COMMAND ----------


def add_bronze_audit_columns(df: DataFrame, source_file_fallback: str | None = None) -> DataFrame:
    """Attach standard Bronze ingestion lineage columns to a DataFrame.

    Uses Unity Catalog's ``_metadata.file_path`` when available (required on
    Databricks Unity Catalog shared clusters where ``input_file_name()`` is
    disallowed), falling back gracefully to ``input_file_name()`` or a literal
    path for local unit-test DataFrames.

    Args:
        df: Raw ingested Spark DataFrame.
        source_file_fallback: Optional fallback file path if Spark metadata is absent.

    Returns:
        DataFrame with ``_ingested_at`` and ``_source_file`` appended.
    """
    try:
        source_col = F.col("_metadata.file_path")
        # Probe column resolution without triggering an action
        df_with_meta = df.withColumn("_source_file", source_col)
    except Exception:
        if source_file_fallback is not None:
            df_with_meta = df.withColumn(
                "_source_file",
                F.coalesce(F.nullif(F.input_file_name(), F.lit("")), F.lit(source_file_fallback)),
            )
        else:
            df_with_meta = df.withColumn("_source_file", F.input_file_name())

    return df_with_meta.withColumn("_ingested_at", F.current_timestamp())


def read_raw_csv(
    spark: SparkSession,
    dataset_key: str,
    raw_data_path: str = RAW_DATA_PATH,
) -> DataFrame:
    """Read a single Olist CSV file with its explicit StructType schema.

    Args:
        spark: Active SparkSession.
        dataset_key: Logical key in ``BRONZE_SCHEMAS``.
        raw_data_path: Base directory path for raw CSV files.

    Returns:
        DataFrame containing raw CSV rows plus ``_ingested_at`` and ``_source_file``.
    """
    file_path = get_raw_file_path(dataset_key, base_path=raw_data_path)
    schema = BRONZE_SCHEMAS[dataset_key]

    reader = (
        spark.read.format("csv")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .schema(schema)
    )

    for opt_key, opt_val in CSV_READER_OPTIONS.get(dataset_key, {}).items():
        reader = reader.option(opt_key, opt_val)

    logger.info("Reading raw CSV for '%s' from %s", dataset_key, file_path)
    raw_df = reader.load(file_path)
    return add_bronze_audit_columns(raw_df, source_file_fallback=file_path)


def write_bronze_delta_table(
    df: DataFrame,
    target_table: str,
) -> int:
    """Write a DataFrame to a Bronze Delta table idempotently using overwrite mode.

    Args:
        df: DataFrame to persist.
        target_table: Three-level Unity Catalog table name.

    Returns:
        Number of rows written to the Delta table.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    row_count = df.sparkSession.table(target_table).count()
    logger.info("Wrote Bronze table %-48s | rows: %10,d", target_table, row_count)
    print(f"[BRONZE] {target_table:<48} -> {row_count:>10,d} rows")
    return row_count


def run_bronze_ingestion(
    spark: SparkSession,
    raw_data_path: str = RAW_DATA_PATH,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> Dict[str, int]:
    """Ingest all 9 raw Olist CSV files into Bronze Delta tables.

    Args:
        spark: Active SparkSession.
        raw_data_path: Base path to raw CSV landing folder.
        catalog: Unity Catalog name.
        schema: Schema name.

    Returns:
        Mapping of Bronze table name to ingested row count.
    """
    logger.info("Starting Bronze ingestion from %s into %s.%s", raw_data_path, catalog, schema)
    summary: Dict[str, int] = {}
    summary_rows: List[Tuple[str, int]] = []

    for dataset_key in BRONZE_TABLES:
        target_table = get_bronze_table(dataset_key, catalog=catalog, schema=schema)
        bronze_df = read_raw_csv(spark, dataset_key=dataset_key, raw_data_path=raw_data_path)
        count = write_bronze_delta_table(bronze_df, target_table=target_table)
        summary[target_table] = count
        summary_rows.append((target_table, count))

    total_rows = sum(summary.values())
    print(f"\n{'=' * 66}")
    print(f"BRONZE INGESTION COMPLETE | Tables: {len(summary)} | Total Rows: {total_rows:,d}")
    print(f"{'=' * 66}\n")
    return summary


# COMMAND ----------

if __name__ == "__main__":
    active_spark = SparkSession.builder.getOrCreate()
    run_bronze_ingestion(active_spark)
