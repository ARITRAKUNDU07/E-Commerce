# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold Layer Analytical Aggregations
# MAGIC
# MAGIC Builds 9 business-ready Gold Delta tables from `silver_orders_enriched` using PySpark
# MAGIC `groupBy`/`agg` and `Window` functions:
# MAGIC 1. `gold_daily_sales`
# MAGIC 2. `gold_monthly_sales` (with `lag` MoM growth %)
# MAGIC 3. `gold_category_performance`
# MAGIC 4. `gold_customer_rfm` (with `ntile(5)` scoring & 5 customer segments)
# MAGIC 5. `gold_state_sales`
# MAGIC 6. `gold_delivery_performance`
# MAGIC 7. `gold_payment_mix`
# MAGIC 8. `gold_seller_performance`
# MAGIC 9. `gold_review_vs_delay`

# COMMAND ----------

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    CATALOG,
    RFM_NUM_BINS,
    SCHEMA,
    get_gold_table,
    get_silver_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("04_gold_aggregations")

# COMMAND ----------


def build_gold_daily_sales(enriched_df: DataFrame) -> DataFrame:
    """Aggregate daily order volume, GMV revenue, AOV, and freight totals.

    Business Rules:
        - ``revenue`` = sum of ``item_total`` (``price + freight_value``) so all Gold
          revenue tables reconcile cleanly against ``silver_orders_enriched``.
        - ``orders`` = distinct count of ``order_id``.
        - ``aov`` = ``revenue / orders`` rounded to 2 decimals.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``order_date, orders, revenue, aov, freight``.
    """
    return (
        enriched_df.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("item_total"), 2).alias("revenue"),
            F.round(F.sum("freight_value"), 2).alias("freight"),
        )
        .withColumn("aov", F.round(F.col("revenue") / F.col("orders"), 2))
        .select("order_date", "orders", "revenue", "aov", "freight")
        .orderBy("order_date")
    )


def build_gold_monthly_sales(enriched_df: DataFrame) -> DataFrame:
    """Aggregate monthly revenue and orders with Month-over-Month growth percentage.

    Business Rules:
        - Uses ``F.lag("revenue", 1)`` ordered by ``year_month`` to calculate
          ``mom_growth_pct = ((revenue - prev_revenue) / prev_revenue) * 100``.
        - First month in the series has ``mom_growth_pct = NULL``.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``year_month, revenue, orders, mom_growth_pct``.
    """
    month_window = Window.orderBy("year_month")

    monthly_df = enriched_df.groupBy("year_month").agg(
        F.round(F.sum("item_total"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
    )

    return (
        monthly_df.withColumn("_prev_revenue", F.lag("revenue", 1).over(month_window))
        .withColumn(
            "mom_growth_pct",
            F.when(
                F.col("_prev_revenue").isNotNull() & (F.col("_prev_revenue") > 0),
                F.round(
                    ((F.col("revenue") - F.col("_prev_revenue")) / F.col("_prev_revenue")) * 100.0,
                    2,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select("year_month", "revenue", "orders", "mom_growth_pct")
        .orderBy("year_month")
    )


def build_gold_category_performance(enriched_df: DataFrame) -> DataFrame:
    """Compute sales volume, revenue, average item price, and review score by category.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``category_en, units, revenue, avg_price, avg_review``.
    """
    return (
        enriched_df.groupBy("category_en")
        .agg(
            F.count("order_item_id").alias("units"),
            F.round(F.sum("item_total"), 2).alias("revenue"),
            F.round(F.avg("price"), 2).alias("avg_price"),
            F.round(F.avg("review_score"), 2).alias("avg_review"),
        )
        .orderBy(F.col("revenue").desc())
    )


def build_gold_customer_rfm(enriched_df: DataFrame, num_bins: int = RFM_NUM_BINS) -> DataFrame:
    """Build Customer RFM segmentation table using ``ntile(5)`` window scoring.

    Business Rules:
        - Evaluated at ``customer_unique_id`` grain (true person identifier in Olist,
          whereas ``customer_id`` is unique per order).
        - Reference snapshot date is ``max(order_date) + 1 day`` across the dataset
          so historical Olist runs are deterministic and repeatable.
        - ``recency_days``: days since customer's most recent delivered order.
        - ``frequency``: count of distinct delivered orders.
        - ``monetary``: lifetime sum of ``item_total``.
        - Scoring via ``ntile(5)``:
            - ``r_score``: lower ``recency_days`` earns higher score (5 = most recent).
            - ``f_score``: higher ``frequency`` earns higher score (5 = most frequent).
            - ``m_score``: higher ``monetary`` earns higher score (5 = highest spend).
        - Segments:
            - **Champions**: ``r_score >= 4`` and ``f_score >= 4`` and ``m_score >= 4``
            - **Loyal**: ``r_score >= 3`` and ``f_score >= 3``
            - **Potential Loyalist**: ``r_score >= 4`` or (``r_score == 3`` and ``m_score >= 3``)
            - **At Risk**: ``r_score == 2`` or (``r_score == 1`` and (``f_score >= 3`` or ``m_score >= 3``))
            - **Lost**: all remaining low-recency / low-engagement customers

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.
        num_bins: Number of quantile bins for RFM scoring (default 5).

    Returns:
        DataFrame with ``customer_unique_id, recency_days, frequency, monetary,
        r_score, f_score, m_score, rfm_score, rfm_segment``.
    """
    snapshot_row = enriched_df.agg(F.date_add(F.max("order_date"), 1).alias("ref_date")).collect()[0]
    ref_date = snapshot_row["ref_date"]

    base_rfm_df = (
        enriched_df.filter(F.col("customer_unique_id").isNotNull())
        .groupBy("customer_unique_id")
        .agg(
            F.datediff(F.lit(ref_date), F.max("order_date")).cast(IntegerType()).alias("recency_days"),
            F.countDistinct("order_id").alias("frequency"),
            F.round(F.sum("item_total"), 2).alias("monetary"),
        )
    )

    # Order windows deterministically so tie-breaking is stable across runs
    r_window = Window.orderBy(F.col("recency_days").desc(), F.col("customer_unique_id").asc())
    f_window = Window.orderBy(
        F.col("frequency").asc(), F.col("monetary").asc(), F.col("customer_unique_id").asc()
    )
    m_window = Window.orderBy(F.col("monetary").asc(), F.col("customer_unique_id").asc())

    scored_df = (
        base_rfm_df.withColumn("r_score", F.ntile(num_bins).over(r_window))
        .withColumn("f_score", F.ntile(num_bins).over(f_window))
        .withColumn("m_score", F.ntile(num_bins).over(m_window))
        .withColumn(
            "rfm_score",
            F.concat(
                F.col("r_score").cast("string"),
                F.col("f_score").cast("string"),
                F.col("m_score").cast("string"),
            ),
        )
        .withColumn(
            "rfm_segment",
            F.when(
                (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
                F.lit("Champions"),
            )
            .when((F.col("r_score") >= 3) & (F.col("f_score") >= 3), F.lit("Loyal"))
            .when(
                (F.col("r_score") >= 4) | ((F.col("r_score") == 3) & (F.col("m_score") >= 3)),
                F.lit("Potential Loyalist"),
            )
            .when(
                (F.col("r_score") == 2)
                | ((F.col("r_score") == 1) & ((F.col("f_score") >= 3) | (F.col("m_score") >= 3))),
                F.lit("At Risk"),
            )
            .otherwise(F.lit("Lost")),
        )
    )

    return scored_df.select(
        "customer_unique_id",
        "recency_days",
        "frequency",
        "monetary",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "rfm_segment",
    )


def build_gold_state_sales(enriched_df: DataFrame) -> DataFrame:
    """Aggregate orders, revenue, and unique customers by Brazilian state.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``state, orders, revenue, customers``.
    """
    return (
        enriched_df.groupBy(F.col("customer_state").alias("state"))
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.round(F.sum("item_total"), 2).alias("revenue"),
            F.countDistinct("customer_unique_id").alias("customers"),
        )
        .orderBy(F.col("revenue").desc())
    )


def build_gold_delivery_performance(enriched_df: DataFrame) -> DataFrame:
    """Measure logistics SLA performance and customer satisfaction by state.

    Business Rules:
        - Deduplicates to order grain before aggregating so multi-item orders are
          counted once when computing average delivery days, delay days, late %,
          and average review score.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``state, avg_delivery_days, avg_delay_days, pct_late, avg_review``.
    """
    order_grain_df = enriched_df.select(
        "order_id",
        F.col("customer_state").alias("state"),
        "delivery_days",
        "delay_days",
        "is_late",
        "review_score",
    ).dropDuplicates(["order_id"])

    return (
        order_grain_df.groupBy("state")
        .agg(
            F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
            F.round(F.avg("delay_days"), 2).alias("avg_delay_days"),
            F.round(
                F.avg(F.when(F.col("is_late").isNotNull(), F.col("is_late").cast("double"))) * 100.0,
                2,
            ).alias("pct_late"),
            F.round(F.avg("review_score"), 2).alias("avg_review"),
        )
        .orderBy(F.col("pct_late").desc())
    )


def build_gold_payment_mix(enriched_df: DataFrame) -> DataFrame:
    """Summarize orders, revenue, and average installments by primary payment method.

    Business Rules:
        - ``revenue`` is summed from ``item_total`` so it reconciles exactly with
          ``silver_orders_enriched``.
        - ``avg_installments`` is computed at order grain so multi-item orders do
          not weight installment averages disproportionately.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``payment_type, orders, revenue, avg_installments``.
    """
    revenue_df = enriched_df.groupBy(F.col("primary_payment_type").alias("payment_type")).agg(
        F.countDistinct("order_id").alias("orders"),
        F.round(F.sum("item_total"), 2).alias("revenue"),
    )

    installments_df = (
        enriched_df.select(
            "order_id",
            F.col("primary_payment_type").alias("payment_type"),
            "max_installments",
        )
        .dropDuplicates(["order_id"])
        .groupBy("payment_type")
        .agg(F.round(F.avg("max_installments"), 2).alias("avg_installments"))
    )

    return (
        revenue_df.join(installments_df, on="payment_type", how="left")
        .select("payment_type", "orders", "revenue", "avg_installments")
        .orderBy(F.col("revenue").desc())
    )


def build_gold_seller_performance(enriched_df: DataFrame) -> DataFrame:
    """Rank marketplace sellers by total revenue, order count, and average review score.

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``seller_id, revenue, orders, avg_review``.
    """
    return (
        enriched_df.groupBy("seller_id")
        .agg(
            F.round(F.sum("item_total"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders"),
            F.round(F.avg("review_score"), 2).alias("avg_review"),
        )
        .orderBy(F.col("revenue").desc())
    )


def build_gold_review_vs_delay(enriched_df: DataFrame) -> DataFrame:
    """Quantify correlation between delivery delay buckets and customer review scores.

    Business Rules:
        - Evaluated at order grain (one row per delivered order with both
          ``delay_days`` and ``review_score`` present).
        - Delay buckets:
            1. ``On Time`` (``delay_days <= 0``)
            2. ``1-3 Days Late`` (``1 <= delay_days <= 3``)
            3. ``4-7 Days Late`` (``4 <= delay_days <= 7``)
            4. ``8+ Days Late`` (``delay_days >= 8``)

    Args:
        enriched_df: ``silver_orders_enriched`` DataFrame.

    Returns:
        DataFrame with columns ``delay_bucket, bucket_order, avg_review, orders``.
    """
    order_grain_df = (
        enriched_df.select("order_id", "delay_days", "review_score")
        .dropDuplicates(["order_id"])
        .filter(F.col("delay_days").isNotNull() & F.col("review_score").isNotNull())
    )

    bucketed_df = order_grain_df.withColumn(
        "delay_bucket",
        F.when(F.col("delay_days") <= 0, F.lit("On Time"))
        .when(F.col("delay_days").between(1, 3), F.lit("1-3 Days Late"))
        .when(F.col("delay_days").between(4, 7), F.lit("4-7 Days Late"))
        .otherwise(F.lit("8+ Days Late")),
    ).withColumn(
        "bucket_order",
        F.when(F.col("delay_days") <= 0, F.lit(1))
        .when(F.col("delay_days").between(1, 3), F.lit(2))
        .when(F.col("delay_days").between(4, 7), F.lit(3))
        .otherwise(F.lit(4)),
    )

    return (
        bucketed_df.groupBy("delay_bucket", "bucket_order")
        .agg(
            F.round(F.avg("review_score"), 2).alias("avg_review"),
            F.count("order_id").alias("orders"),
        )
        .orderBy("bucket_order")
    )


# COMMAND ----------


def write_gold_delta_table(df: DataFrame, target_table: str) -> int:
    """Persist a Gold analytical DataFrame as a Delta table using overwrite mode.

    Args:
        df: Gold analytical Spark DataFrame.
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
    logger.info("Wrote Gold table   %-48s | rows: %10,d", target_table, row_count)
    print(f"[GOLD]   {target_table:<48} -> {row_count:>10,d} rows")
    return row_count


def run_gold_pipeline(
    spark: SparkSession,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> Dict[str, int]:
    """Build and write all 9 Gold analytical Delta tables.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.

    Returns:
        Mapping of Gold table names to written row counts.
    """
    logger.info("Starting Gold aggregation pipeline in %s.%s", catalog, schema)
    # Note: Do not call .cache()/.unpersist() so the notebook runs cleanly on Databricks Serverless
    enriched_df = spark.table(get_silver_table("orders_enriched", catalog, schema))

    gold_builders: Dict[str, DataFrame] = {
        "daily_sales": build_gold_daily_sales(enriched_df),
        "monthly_sales": build_gold_monthly_sales(enriched_df),
        "category_performance": build_gold_category_performance(enriched_df),
        "customer_rfm": build_gold_customer_rfm(enriched_df),
        "state_sales": build_gold_state_sales(enriched_df),
        "delivery_performance": build_gold_delivery_performance(enriched_df),
        "payment_mix": build_gold_payment_mix(enriched_df),
        "seller_performance": build_gold_seller_performance(enriched_df),
        "review_vs_delay": build_gold_review_vs_delay(enriched_df),
    }

    summary: Dict[str, int] = {}
    for table_key, df in gold_builders.items():
        target_table = get_gold_table(table_key, catalog=catalog, schema=schema)
        summary[target_table] = write_gold_delta_table(df, target_table)

    print(f"\n{'=' * 66}")
    print(f"GOLD AGGREGATIONS COMPLETE | Tables Written: {len(summary)}")
    print(f"{'=' * 66}\n")
    return summary


# COMMAND ----------

if __name__ == "__main__":
    active_spark = SparkSession.builder.getOrCreate()
    run_gold_pipeline(active_spark)
