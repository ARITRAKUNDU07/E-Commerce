# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver Layer Cleaning & Enrichment
# MAGIC
# MAGIC Transforms raw Bronze Delta tables into conformed, deduplicated, typed Silver Delta tables:
# MAGIC - Explicit timestamp (`to_timestamp`) and numeric casting
# MAGIC - Deduplication: `order_reviews` by `review_id`, `geolocation` aggregated by zip prefix (avg lat/lng), `customers` by `customer_id`
# MAGIC - Product category cleaning (`"unknown"` for nulls) + English translation join
# MAGIC - Derived business columns: `delivery_days`, `delay_days`, `is_late`, `order_year`, `order_month`, `year_month`, `item_total`
# MAGIC - Order-level payment pre-aggregation (preventing row fan-out)
# MAGIC - Split delivered (`silver_orders`) vs non-delivered (`silver_orders_non_delivered`)
# MAGIC - Enriched analytical fact table (`silver_orders_enriched`) with post-join row-count verification

# COMMAND ----------

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    CATALOG,
    DELIVERED_STATUS,
    FALLBACK_CATEGORY_TRANSLATIONS,
    SCHEMA,
    UNKNOWN_CATEGORY,
    get_bronze_table,
    get_silver_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("03_silver_cleaning")

# COMMAND ----------


def clean_orders(orders_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Clean orders, derive logistics/date columns, and split delivered vs non-delivered.

    Business Rules:
        - Cast all 5 lifecycle timestamps using ``to_timestamp``.
        - Deduplicate by ``order_id`` keeping the most recent purchase record.
        - Enforce temporal validity: if ``order_delivered_customer_date`` precedes
          ``order_purchase_timestamp`` (corrupt source timestamp), nullify the
          delivery timestamp so downstream delivery metrics are never negative.
        - Derive ``order_date``, ``order_year``, ``order_month``, ``year_month``,
          ``delivery_days``, ``delay_days`` (delivered minus estimated, in days),
          and ``is_late`` (boolean: ``delay_days > 0``).
        - Separate ``delivered`` orders from non-delivered orders (canceled,
          unavailable, shipped, invoiced, processing, etc.) into two Silver tables.

    Args:
        orders_df: Raw ``bronze_orders`` DataFrame.

    Returns:
        Tuple of ``(silver_orders_delivered_df, silver_orders_non_delivered_df)``.
    """
    dedup_window = Window.partitionBy("order_id").orderBy(
        F.to_timestamp("order_purchase_timestamp").desc_nulls_last()
    )

    typed_df = (
        orders_df.filter(F.col("order_id").isNotNull() & (F.trim("order_id") != ""))
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn(
            "order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date")
        )
        .withColumn(
            "order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date")
        )
    )

    # Guard against impossible delivery timestamps earlier than purchase timestamp
    sanitized_df = typed_df.withColumn(
        "order_delivered_customer_date",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_purchase_timestamp").isNotNull()
            & (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp")),
            F.lit(None).cast("timestamp"),
        ).otherwise(F.col("order_delivered_customer_date")),
    )

    derived_df = (
        sanitized_df.withColumn("order_date", F.to_date("order_purchase_timestamp"))
        .withColumn("order_year", F.year("order_purchase_timestamp").cast(IntegerType()))
        .withColumn("order_month", F.month("order_purchase_timestamp").cast(IntegerType()))
        .withColumn("year_month", F.date_format("order_purchase_timestamp", "yyyy-MM"))
        # delivery_days: elapsed calendar days from purchase to customer delivery
        .withColumn(
            "delivery_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ).cast(IntegerType()),
        )
        # delay_days: positive = delivered late; 0 or negative = delivered on time or early
        .withColumn(
            "delay_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date"),
            ).cast(IntegerType()),
        )
        # is_late: True when delivered strictly after estimated delivery date
        .withColumn(
            "is_late",
            F.when(F.col("delay_days").isNotNull(), F.col("delay_days") > F.lit(0)).otherwise(
                F.lit(None).cast(BooleanType())
            ),
        )
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "order_date",
            "order_year",
            "order_month",
            "year_month",
            "delivery_days",
            "delay_days",
            "is_late",
        )
    )

    delivered_df = derived_df.filter(F.col("order_status") == F.lit(DELIVERED_STATUS))
    non_delivered_df = derived_df.filter(
        F.col("order_status").isNull() | (F.col("order_status") != F.lit(DELIVERED_STATUS))
    )

    return delivered_df, non_delivered_df


def clean_order_items(order_items_df: DataFrame) -> DataFrame:
    """Clean order line items, cast numeric/timestamp columns, and compute ``item_total``.

    Business Rules:
        - Enforce non-null composite key ``(order_id, order_item_id)`` and deduplicate.
        - Cast ``price`` and ``freight_value`` to DoubleType; reject negative amounts.
        - Compute ``item_total = round(price + freight_value, 2)``.

    Args:
        order_items_df: Raw ``bronze_order_items`` DataFrame.

    Returns:
        Cleaned ``silver_order_items`` DataFrame.
    """
    return (
        order_items_df.filter(
            F.col("order_id").isNotNull() & F.col("order_item_id").isNotNull()
        )
        .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("price", F.round(F.col("price").cast(DoubleType()), 2))
        .withColumn("freight_value", F.round(F.col("freight_value").cast(DoubleType()), 2))
        .filter((F.col("price") >= 0) & (F.col("freight_value") >= 0))
        .dropDuplicates(["order_id", "order_item_id"])
        .withColumn("item_total", F.round(F.col("price") + F.col("freight_value"), 2))
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "item_total",
        )
    )


def aggregate_order_payments(payments_df: DataFrame) -> DataFrame:
    """Aggregate payment rows to one row per ``order_id`` before joining to orders.

    Business Rules:
        - Orders in Olist can have multiple payment rows (e.g. voucher + credit card).
        - Pre-aggregating to order grain prevents order-row duplication on join.
        - ``total_payment_value`` is the sum of ``payment_value`` per order.
        - ``max_installments`` is the maximum ``payment_installments`` per order.
        - ``primary_payment_type`` is the ``payment_type`` contributing the highest
          total payment value for that order (tie-broken by earliest
          ``payment_sequential`` and then alphabetically for determinism).

    Args:
        payments_df: Raw ``bronze_order_payments`` DataFrame.

    Returns:
        Order-grain ``silver_order_payments`` DataFrame.
    """
    cleaned_df = (
        payments_df.filter(F.col("order_id").isNotNull())
        .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
        .withColumn(
            "payment_type",
            F.coalesce(F.nullif(F.lower(F.trim("payment_type")), F.lit("")), F.lit("not_defined")),
        )
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
        .filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))
    )

    # Determine primary payment type per order by highest summed payment_value
    type_totals_df = cleaned_df.groupBy("order_id", "payment_type").agg(
        F.sum("payment_value").alias("_type_value"),
        F.min("payment_sequential").alias("_first_seq"),
    )
    primary_window = Window.partitionBy("order_id").orderBy(
        F.col("_type_value").desc(),
        F.col("_first_seq").asc_nulls_last(),
        F.col("payment_type").asc(),
    )
    primary_type_df = (
        type_totals_df.withColumn("_rn", F.row_number().over(primary_window))
        .filter(F.col("_rn") == 1)
        .select("order_id", F.col("payment_type").alias("primary_payment_type"))
    )

    order_totals_df = cleaned_df.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
        F.max("payment_installments").alias("max_installments"),
        F.count("*").cast(IntegerType()).alias("payment_count"),
    )

    return order_totals_df.join(primary_type_df, on="order_id", how="left").select(
        "order_id",
        "total_payment_value",
        "max_installments",
        "payment_count",
        "primary_payment_type",
    )


def clean_order_reviews(reviews_df: DataFrame) -> DataFrame:
    """Clean and deduplicate customer reviews by ``review_id``.

    Business Rules:
        - Cast ``review_score`` to IntegerType (keeping valid scores in 1..5).
        - Cast ``review_creation_date`` and ``review_answer_timestamp`` via ``to_timestamp``.
        - Deduplicate by ``review_id``, keeping the latest answered/created record.

    Args:
        reviews_df: Raw ``bronze_order_reviews`` DataFrame.

    Returns:
        Deduplicated ``silver_order_reviews`` DataFrame (one row per ``review_id``).
    """
    dedup_window = Window.partitionBy("review_id").orderBy(
        F.col("review_answer_timestamp").desc_nulls_last(),
        F.col("review_creation_date").desc_nulls_last(),
        F.col("order_id").asc(),
    )

    return (
        reviews_df.filter(F.col("review_id").isNotNull() & (F.trim("review_id") != ""))
        .withColumn("review_score", F.col("review_score").cast(IntegerType()))
        .filter(F.col("review_score").between(1, 5))
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        )
    )


def aggregate_reviews_per_order(silver_reviews_df: DataFrame) -> DataFrame:
    """Aggregate deduplicated reviews to order grain before joining to orders.

    Business Rules:
        - A small fraction of Olist orders (~0.5%) have multiple distinct ``review_id``
          records. Aggregating to one row per ``order_id`` guarantees zero row
          multiplication in ``silver_orders_enriched``.

    Args:
        silver_reviews_df: Deduplicated ``silver_order_reviews`` DataFrame.

    Returns:
        Order-grain DataFrame with ``order_id``, ``review_score``, ``review_count``,
        and ``latest_review_id``.
    """
    latest_window = Window.partitionBy("order_id").orderBy(
        F.col("review_answer_timestamp").desc_nulls_last(),
        F.col("review_creation_date").desc_nulls_last(),
        F.col("review_id").asc(),
    )

    latest_review_df = (
        silver_reviews_df.withColumn("_rn", F.row_number().over(latest_window))
        .filter(F.col("_rn") == 1)
        .select("order_id", F.col("review_id").alias("latest_review_id"))
    )

    avg_review_df = silver_reviews_df.groupBy("order_id").agg(
        F.round(F.avg("review_score"), 2).alias("review_score"),
        F.count("review_id").cast(IntegerType()).alias("review_count"),
    )

    return avg_review_df.join(latest_review_df, on="order_id", how="left")


def clean_customers(customers_df: DataFrame) -> DataFrame:
    """Clean and deduplicate customer records by ``customer_id``.

    Business Rules:
        - Deduplicate by ``customer_id``.
        - Standardize ``customer_zip_code_prefix`` to 5-character zero-padded string,
          ``customer_city`` to lowercase trimmed, and ``customer_state`` to uppercase trimmed.

    Args:
        customers_df: Raw ``bronze_customers`` DataFrame.

    Returns:
        Deduplicated ``silver_customers`` DataFrame.
    """
    return (
        customers_df.filter(F.col("customer_id").isNotNull() & (F.trim("customer_id") != ""))
        .dropDuplicates(["customer_id"])
        .withColumn("customer_zip_code_prefix", F.lpad(F.trim("customer_zip_code_prefix"), 5, "0"))
        .withColumn("customer_city", F.lower(F.trim("customer_city")))
        .withColumn("customer_state", F.upper(F.trim("customer_state")))
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        )
    )


def clean_products(products_df: DataFrame, translation_df: DataFrame) -> DataFrame:
    """Clean product catalog, impute null categories with ``'unknown'``, and join English names.

    Business Rules:
        - Fix upstream Olist spelling typos (``product_name_lenght`` -> ``product_name_length``,
          ``product_description_lenght`` -> ``product_description_length``).
        - Replace null or blank ``product_category_name`` with ``"unknown"``.
        - Join ``bronze_category_translation`` to derive ``category_en``.
        - Apply deterministic fallback translations for known untranslated Portuguese
          categories so ``category_en`` is never null.

    Args:
        products_df: Raw ``bronze_products`` DataFrame.
        translation_df: Raw ``bronze_category_translation`` DataFrame.

    Returns:
        Cleaned ``silver_products`` DataFrame.
    """
    clean_translation_df = (
        translation_df.filter(F.col("product_category_name").isNotNull())
        .withColumn("product_category_name", F.lower(F.trim("product_category_name")))
        .withColumn(
            "product_category_name_english", F.lower(F.trim("product_category_name_english"))
        )
        .dropDuplicates(["product_category_name"])
    )

    fallback_map = F.create_map(
        *[
            item
            for pt_name, en_name in FALLBACK_CATEGORY_TRANSLATIONS.items()
            for item in (F.lit(pt_name), F.lit(en_name))
        ]
    )

    normalized_products = (
        products_df.filter(F.col("product_id").isNotNull() & (F.trim("product_id") != ""))
        .dropDuplicates(["product_id"])
        .withColumn(
            "product_category_name",
            F.coalesce(
                F.nullif(F.lower(F.trim("product_category_name")), F.lit("")),
                F.lit(UNKNOWN_CATEGORY),
            ),
        )
        .withColumn("product_name_length", F.col("product_name_lenght").cast(IntegerType()))
        .withColumn(
            "product_description_length", F.col("product_description_lenght").cast(IntegerType())
        )
        .withColumn("product_photos_qty", F.col("product_photos_qty").cast(IntegerType()))
        .withColumn("product_weight_g", F.col("product_weight_g").cast(DoubleType()))
        .withColumn("product_length_cm", F.col("product_length_cm").cast(DoubleType()))
        .withColumn("product_height_cm", F.col("product_height_cm").cast(DoubleType()))
        .withColumn("product_width_cm", F.col("product_width_cm").cast(DoubleType()))
    )

    return (
        normalized_products.join(
            clean_translation_df.select("product_category_name", "product_category_name_english"),
            on="product_category_name",
            how="left",
        )
        .withColumn(
            "category_en",
            F.coalesce(
                F.col("product_category_name_english"),
                fallback_map[F.col("product_category_name")],
                F.col("product_category_name"),
            ),
        )
        .select(
            "product_id",
            "product_category_name",
            "category_en",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
    )


def clean_sellers(sellers_df: DataFrame) -> DataFrame:
    """Clean and deduplicate seller records by ``seller_id``.

    Args:
        sellers_df: Raw ``bronze_sellers`` DataFrame.

    Returns:
        Deduplicated ``silver_sellers`` DataFrame.
    """
    return (
        sellers_df.filter(F.col("seller_id").isNotNull() & (F.trim("seller_id") != ""))
        .dropDuplicates(["seller_id"])
        .withColumn("seller_zip_code_prefix", F.lpad(F.trim("seller_zip_code_prefix"), 5, "0"))
        .withColumn("seller_city", F.lower(F.trim("seller_city")))
        .withColumn("seller_state", F.upper(F.trim("seller_state")))
        .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    )


def clean_geolocation(geolocation_df: DataFrame) -> DataFrame:
    """Deduplicate geolocation coordinates by zip prefix using average latitude/longitude.

    Business Rules:
        - Raw Olist geolocation contains ~1M duplicate rows per zip code prefix.
        - Aggregate to one row per 5-digit ``geolocation_zip_code_prefix`` using
          average ``geolocation_lat`` and ``geolocation_lng``.

    Args:
        geolocation_df: Raw ``bronze_geolocation`` DataFrame.

    Returns:
        Zip-prefix-grain ``silver_geolocation`` DataFrame.
    """
    return (
        geolocation_df.filter(
            F.col("geolocation_zip_code_prefix").isNotNull()
            & F.col("geolocation_lat").isNotNull()
            & F.col("geolocation_lng").isNotNull()
        )
        .withColumn(
            "geolocation_zip_code_prefix",
            F.lpad(F.trim("geolocation_zip_code_prefix"), 5, "0"),
        )
        .withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
        .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType()))
        .withColumn("geolocation_city", F.lower(F.trim("geolocation_city")))
        .withColumn("geolocation_state", F.upper(F.trim("geolocation_state")))
        .groupBy("geolocation_zip_code_prefix")
        .agg(
            F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
            F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
            F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
            F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
            F.count("*").cast(IntegerType()).alias("coordinate_samples"),
        )
    )


def build_orders_enriched(
    silver_orders_df: DataFrame,
    silver_order_items_df: DataFrame,
    silver_order_payments_df: DataFrame,
    silver_customers_df: DataFrame,
    silver_products_df: DataFrame,
    silver_sellers_df: DataFrame,
    silver_order_reviews_df: DataFrame,
) -> DataFrame:
    """Build the conformed Silver fact table ``silver_orders_enriched`` at order-item grain.

    Joins delivered orders with line items, pre-aggregated payments, customers,
    products (with English categories), sellers, and order-level aggregated reviews.
    Verifies post-join row count equals the exact delivered order-item count so any
    unexpected join fan-out fails fast.

    Args:
        silver_orders_df: Delivered orders (``silver_orders``).
        silver_order_items_df: Cleaned order line items (``silver_order_items``).
        silver_order_payments_df: Order-grain aggregated payments (``silver_order_payments``).
        silver_customers_df: Deduplicated customers (``silver_customers``).
        silver_products_df: Cleaned products with English categories (``silver_products``).
        silver_sellers_df: Deduplicated sellers (``silver_sellers``).
        silver_order_reviews_df: Deduplicated reviews (``silver_order_reviews``).

    Returns:
        Conformed ``silver_orders_enriched`` DataFrame at ``(order_id, order_item_id)`` grain.

    Raises:
        ValueError: If post-join row count differs from delivered order-item row count.
    """
    # Base join: delivered orders x order items (defines the analytical item grain)
    base_items_df = silver_orders_df.join(silver_order_items_df, on="order_id", how="inner")
    expected_row_count = base_items_df.count()

    order_reviews_agg_df = aggregate_reviews_per_order(silver_order_reviews_df)

    enriched_df = (
        base_items_df.join(silver_order_payments_df, on="order_id", how="left")
        .join(silver_customers_df, on="customer_id", how="left")
        .join(silver_products_df, on="product_id", how="left")
        .join(silver_sellers_df, on="seller_id", how="left")
        .join(order_reviews_agg_df, on="order_id", how="left")
        .withColumn(
            "category_en",
            F.coalesce(F.col("category_en"), F.lit(UNKNOWN_CATEGORY)),
        )
        .withColumn(
            "primary_payment_type",
            F.coalesce(F.col("primary_payment_type"), F.lit("not_defined")),
        )
        .select(
            "order_id",
            "order_item_id",
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "order_date",
            "order_year",
            "order_month",
            "year_month",
            "delivery_days",
            "delay_days",
            "is_late",
            "product_id",
            "product_category_name",
            "category_en",
            "seller_id",
            "seller_city",
            "seller_state",
            "shipping_limit_date",
            "price",
            "freight_value",
            "item_total",
            "total_payment_value",
            "max_installments",
            "payment_count",
            "primary_payment_type",
            "latest_review_id",
            "review_score",
            "review_count",
        )
    )

    actual_row_count = enriched_df.count()
    logger.info(
        "Join verification | delivered_items=%d | silver_orders_enriched=%d",
        expected_row_count,
        actual_row_count,
    )
    print(
        f"[SILVER JOIN CHECK] Expected delivered items: {expected_row_count:,d} | "
        f"Enriched rows: {actual_row_count:,d}"
    )

    if actual_row_count != expected_row_count:
        raise ValueError(
            f"Row count mismatch after enrichment joins! Expected {expected_row_count:,d} "
            f"rows but got {actual_row_count:,d}. Check dimension keys for fan-out."
        )

    return enriched_df


# COMMAND ----------


def write_silver_delta_table(df: DataFrame, target_table: str) -> int:
    """Persist a Silver DataFrame as a Delta table using idempotent overwrite mode.

    Args:
        df: Cleaned Spark DataFrame.
        target_table: Three-level Unity Catalog table name.

    Returns:
        Persisted row count.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    row_count = df.sparkSession.table(target_table).count()
    logger.info("Wrote Silver table %-48s | rows: %10s", target_table, f"{row_count:,d}")
    print(f"[SILVER] {target_table:<48} -> {row_count:>10,d} rows")
    return row_count


def run_silver_pipeline(
    spark: SparkSession,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> Dict[str, int]:
    """Execute the complete Bronze -> Silver cleaning and enrichment pipeline.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.

    Returns:
        Mapping of Silver table names to written row counts.
    """
    logger.info("Starting Silver cleaning pipeline in %s.%s", catalog, schema)

    # Read Bronze tables
    bronze_orders = spark.table(get_bronze_table("orders", catalog, schema))
    bronze_order_items = spark.table(get_bronze_table("order_items", catalog, schema))
    bronze_order_payments = spark.table(get_bronze_table("order_payments", catalog, schema))
    bronze_order_reviews = spark.table(get_bronze_table("order_reviews", catalog, schema))
    bronze_customers = spark.table(get_bronze_table("customers", catalog, schema))
    bronze_products = spark.table(get_bronze_table("products", catalog, schema))
    bronze_sellers = spark.table(get_bronze_table("sellers", catalog, schema))
    bronze_geolocation = spark.table(get_bronze_table("geolocation", catalog, schema))
    bronze_translation = spark.table(get_bronze_table("category_translation", catalog, schema))

    # Transform entities
    silver_orders_df, silver_orders_non_delivered_df = clean_orders(bronze_orders)
    silver_order_items_df = clean_order_items(bronze_order_items)
    silver_order_payments_df = aggregate_order_payments(bronze_order_payments)
    silver_order_reviews_df = clean_order_reviews(bronze_order_reviews)
    silver_customers_df = clean_customers(bronze_customers)
    silver_products_df = clean_products(bronze_products, bronze_translation)
    silver_sellers_df = clean_sellers(bronze_sellers)
    silver_geolocation_df = clean_geolocation(bronze_geolocation)

    # Build enriched analytical table with row-count validation
    silver_orders_enriched_df = build_orders_enriched(
        silver_orders_df=silver_orders_df,
        silver_order_items_df=silver_order_items_df,
        silver_order_payments_df=silver_order_payments_df,
        silver_customers_df=silver_customers_df,
        silver_products_df=silver_products_df,
        silver_sellers_df=silver_sellers_df,
        silver_order_reviews_df=silver_order_reviews_df,
    )

    silver_outputs: Dict[str, DataFrame] = {
        "orders": silver_orders_df,
        "orders_non_delivered": silver_orders_non_delivered_df,
        "order_items": silver_order_items_df,
        "order_payments": silver_order_payments_df,
        "order_reviews": silver_order_reviews_df,
        "customers": silver_customers_df,
        "products": silver_products_df,
        "sellers": silver_sellers_df,
        "geolocation": silver_geolocation_df,
        "orders_enriched": silver_orders_enriched_df,
    }

    summary: Dict[str, int] = {}
    for table_key, df in silver_outputs.items():
        target_table = get_silver_table(table_key, catalog=catalog, schema=schema)
        summary[target_table] = write_silver_delta_table(df, target_table)

    print(f"\n{'=' * 66}")
    print(f"SILVER CLEANING COMPLETE | Tables Written: {len(summary)}")
    print(f"{'=' * 66}\n")
    return summary


# COMMAND ----------

if __name__ == "__main__":
    active_spark = SparkSession.builder.getOrCreate()
    run_silver_pipeline(active_spark)
