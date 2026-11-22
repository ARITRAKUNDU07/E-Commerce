# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Data Quality Validation Gate
# MAGIC
# MAGIC Automated data quality checks executed at the end of the pipeline:
# MAGIC - **Null Primary Keys**: Zero nulls on all Silver and Gold primary keys
# MAGIC - **Uniqueness**: Zero duplicate `order_id` in `silver_orders` and `(order_id, order_item_id)` in `silver_orders_enriched`
# MAGIC - **Financial Reconciliation**: Revenue totals reconcile between Silver (`silver_orders_enriched`) and all Gold revenue tables
# MAGIC - **Monetary Validity**: Zero negative prices, freight values, or item totals
# MAGIC - **Temporal Integrity**: Delivery timestamps never precede purchase timestamps
# MAGIC - **Domain Validity**: Positive row counts and review scores within `[1, 5]`
# MAGIC
# MAGIC Prints a formatted **PASS / FAIL** summary report and raises `DataQualityError` on any critical failure.

# COMMAND ----------

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    CATALOG,
    DQ_REVENUE_TOLERANCE,
    SCHEMA,
    get_gold_table,
    get_silver_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("05_data_quality_checks")

# COMMAND ----------


class DataQualityError(RuntimeError):
    """Raised when one or more critical data quality assertions fail."""


@dataclass(frozen=True)
class QualityCheckResult:
    """Structured outcome for a single data quality assertion."""

    check_name: str
    table_name: str
    severity: str  # "CRITICAL" or "WARNING"
    status: str  # "PASS" or "FAIL"
    metric_value: str
    details: str


def check_no_null_keys(
    df: DataFrame,
    table_name: str,
    key_columns: List[str],
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Assert that none of the specified primary key columns contain NULL values."""
    null_condition = F.lit(False)
    for col_name in key_columns:
        null_condition = null_condition | F.col(col_name).isNull()

    null_count = df.filter(null_condition).count()
    passed = null_count == 0
    return QualityCheckResult(
        check_name=f"No Null PK ({', '.join(key_columns)})",
        table_name=table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=str(null_count),
        details="0 null primary keys" if passed else f"Found {null_count:,d} rows with null PK",
    )


def check_no_duplicate_keys(
    df: DataFrame,
    table_name: str,
    key_columns: List[str],
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Assert that candidate key columns are strictly unique."""
    total_rows = df.count()
    distinct_rows = df.select(*key_columns).distinct().count()
    duplicate_count = total_rows - distinct_rows
    passed = duplicate_count == 0
    return QualityCheckResult(
        check_name=f"Unique Key ({', '.join(key_columns)})",
        table_name=table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=str(duplicate_count),
        details="All keys unique" if passed else f"Found {duplicate_count:,d} duplicate key rows",
    )


def check_revenue_reconciliation(
    silver_revenue: float,
    gold_df: DataFrame,
    gold_table_name: str,
    revenue_col: str = "revenue",
    tolerance: float = DQ_REVENUE_TOLERANCE,
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Assert that total Gold revenue reconciles with Silver revenue within tolerance."""
    gold_row = gold_df.agg(F.coalesce(F.sum(F.col(revenue_col)), F.lit(0.0)).alias("total")).collect()[
        0
    ]
    gold_revenue = float(gold_row["total"])
    diff = abs(silver_revenue - gold_revenue)
    passed = diff <= tolerance
    return QualityCheckResult(
        check_name="Silver vs Gold Revenue Reconciliation",
        table_name=gold_table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=f"diff={diff:.2f}",
        details=(
            f"Silver={silver_revenue:,.2f} | Gold={gold_revenue:,.2f} (tol={tolerance:.2f})"
        ),
    )


def check_no_negative_values(
    df: DataFrame,
    table_name: str,
    numeric_columns: List[str],
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Assert that price/freight/total columns contain zero negative values."""
    neg_condition = F.lit(False)
    for col_name in numeric_columns:
        neg_condition = neg_condition | (F.col(col_name) < 0)

    neg_count = df.filter(neg_condition).count()
    passed = neg_count == 0
    return QualityCheckResult(
        check_name=f"No Negative Values ({', '.join(numeric_columns)})",
        table_name=table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=str(neg_count),
        details="All values >= 0" if passed else f"Found {neg_count:,d} negative rows",
    )


def check_delivery_after_purchase(
    df: DataFrame,
    table_name: str,
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Assert that delivery timestamps are never earlier than purchase timestamps."""
    invalid_count = df.filter(
        F.col("order_delivered_customer_date").isNotNull()
        & F.col("order_purchase_timestamp").isNotNull()
        & (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"))
    ).count()
    passed = invalid_count == 0
    return QualityCheckResult(
        check_name="Delivery Date >= Purchase Date",
        table_name=table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=str(invalid_count),
        details=(
            "All delivery dates valid"
            if passed
            else f"Found {invalid_count:,d} orders delivered before purchase"
        ),
    )


def check_non_empty_table(
    df: DataFrame,
    table_name: str,
    severity: str = "CRITICAL",
) -> QualityCheckResult:
    """Assert that a production table is non-empty."""
    row_count = df.count()
    passed = row_count > 0
    return QualityCheckResult(
        check_name="Non-Empty Table",
        table_name=table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=f"{row_count:,d}",
        details=f"{row_count:,d} rows present" if passed else "Table has 0 rows",
    )


def check_review_score_bounds(
    df: DataFrame,
    table_name: str,
    severity: str = "WARNING",
) -> QualityCheckResult:
    """Assert that non-null review scores lie strictly within [1, 5]."""
    out_of_bounds = df.filter(
        F.col("review_score").isNotNull() & ~F.col("review_score").between(1, 5)
    ).count()
    passed = out_of_bounds == 0
    return QualityCheckResult(
        check_name="Review Score Bounds [1..5]",
        table_name=table_name,
        severity=severity,
        status="PASS" if passed else "FAIL",
        metric_value=str(out_of_bounds),
        details="All review scores in [1, 5]" if passed else f"{out_of_bounds:,d} out-of-range scores",
    )


def format_summary_table(results: List[QualityCheckResult]) -> str:
    """Render a clean ASCII PASS/FAIL summary table for logs and stdout."""
    header = (
        f"{'STATUS':<8} | {'SEVERITY':<8} | {'CHECK':<42} | {'TABLE':<42} | {'METRIC':<14} | DETAILS"
    )
    separator = "-" * len(header)
    lines = [separator, header, separator]
    for r in results:
        lines.append(
            f"{r.status:<8} | {r.severity:<8} | {r.check_name:<42} | "
            f"{r.table_name:<42} | {r.metric_value:<14} | {r.details}"
        )
    lines.append(separator)
    return "\n".join(lines)


# COMMAND ----------


def run_data_quality_checks(
    spark: SparkSession,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> List[QualityCheckResult]:
    """Execute the full Silver & Gold data quality validation suite.

    Prints a PASS/FAIL summary table and raises ``DataQualityError`` if any
    ``CRITICAL`` check fails.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name.
        schema: Schema name.

    Returns:
        List of ``QualityCheckResult`` objects.

    Raises:
        DataQualityError: If one or more critical checks fail.
    """
    logger.info("Running Data Quality validation gate on %s.%s", catalog, schema)
    results: List[QualityCheckResult] = []

    # Load Silver tables
    silver_orders_tbl = get_silver_table("orders", catalog, schema)
    silver_items_tbl = get_silver_table("order_items", catalog, schema)
    silver_customers_tbl = get_silver_table("customers", catalog, schema)
    silver_products_tbl = get_silver_table("products", catalog, schema)
    silver_sellers_tbl = get_silver_table("sellers", catalog, schema)
    silver_reviews_tbl = get_silver_table("order_reviews", catalog, schema)
    silver_enriched_tbl = get_silver_table("orders_enriched", catalog, schema)

    silver_orders_df = spark.table(silver_orders_tbl)
    silver_items_df = spark.table(silver_items_tbl)
    silver_customers_df = spark.table(silver_customers_tbl)
    silver_products_df = spark.table(silver_products_tbl)
    silver_sellers_df = spark.table(silver_sellers_tbl)
    silver_reviews_df = spark.table(silver_reviews_tbl)
    silver_enriched_df = spark.table(silver_enriched_tbl)

    # 1. Non-empty core tables
    results.append(check_non_empty_table(silver_orders_df, silver_orders_tbl))
    results.append(check_non_empty_table(silver_enriched_df, silver_enriched_tbl))

    # 2. Primary key null checks
    results.append(check_no_null_keys(silver_orders_df, silver_orders_tbl, ["order_id"]))
    results.append(check_no_null_keys(silver_customers_df, silver_customers_tbl, ["customer_id"]))
    results.append(check_no_null_keys(silver_products_df, silver_products_tbl, ["product_id"]))
    results.append(check_no_null_keys(silver_sellers_df, silver_sellers_tbl, ["seller_id"]))
    results.append(check_no_null_keys(silver_reviews_df, silver_reviews_tbl, ["review_id"]))
    results.append(
        check_no_null_keys(silver_enriched_df, silver_enriched_tbl, ["order_id", "order_item_id"])
    )

    # 3. Uniqueness checks
    results.append(check_no_duplicate_keys(silver_orders_df, silver_orders_tbl, ["order_id"]))
    results.append(
        check_no_duplicate_keys(
            silver_enriched_df, silver_enriched_tbl, ["order_id", "order_item_id"]
        )
    )

    # 4. Negative price / monetary checks
    results.append(
        check_no_negative_values(
            silver_items_df, silver_items_tbl, ["price", "freight_value", "item_total"]
        )
    )
    results.append(
        check_no_negative_values(
            silver_enriched_df, silver_enriched_tbl, ["price", "freight_value", "item_total"]
        )
    )

    # 5. Delivery dates not before purchase dates
    results.append(check_delivery_after_purchase(silver_orders_df, silver_orders_tbl))
    results.append(check_delivery_after_purchase(silver_enriched_df, silver_enriched_tbl))

    # 6. Review score bounds
    results.append(check_review_score_bounds(silver_enriched_df, silver_enriched_tbl))

    # 7. Silver-to-Gold revenue reconciliation across all Gold revenue tables
    silver_rev_row = silver_enriched_df.agg(
        F.coalesce(F.sum("item_total"), F.lit(0.0)).alias("total")
    ).collect()[0]
    silver_total_revenue = float(silver_rev_row["total"])

    gold_reconcile_targets = [
        ("daily_sales", "revenue"),
        ("monthly_sales", "revenue"),
        ("category_performance", "revenue"),
        ("customer_rfm", "monetary"),
        ("state_sales", "revenue"),
        ("payment_mix", "revenue"),
        ("seller_performance", "revenue"),
    ]
    for gold_key, rev_col in gold_reconcile_targets:
        gold_tbl = get_gold_table(gold_key, catalog, schema)
        gold_df = spark.table(gold_tbl)
        results.append(
            check_revenue_reconciliation(
                silver_revenue=silver_total_revenue,
                gold_df=gold_df,
                gold_table_name=gold_tbl,
                revenue_col=rev_col,
            )
        )

    # Primary key check on gold_customer_rfm
    rfm_tbl = get_gold_table("customer_rfm", catalog, schema)
    results.append(check_no_null_keys(spark.table(rfm_tbl), rfm_tbl, ["customer_unique_id"]))

    # Print formatted PASS/FAIL summary table
    summary_text = format_summary_table(results)
    print("\nDATA QUALITY VALIDATION SUMMARY")
    print(summary_text)

    passed_count = sum(1 for r in results if r.status == "PASS")
    failed_critical = [r for r in results if r.status == "FAIL" and r.severity == "CRITICAL"]
    failed_warning = [r for r in results if r.status == "FAIL" and r.severity == "WARNING"]

    print(
        f"\nTOTAL CHECKS: {len(results)} | PASS: {passed_count} | "
        f"CRITICAL FAIL: {len(failed_critical)} | WARNING FAIL: {len(failed_warning)}\n"
    )

    if failed_critical:
        failure_summary = "; ".join(
            f"[{r.table_name}] {r.check_name}: {r.details}" for r in failed_critical
        )
        logger.error("Critical data quality failures detected: %s", failure_summary)
        raise DataQualityError(
            f"{len(failed_critical)} critical data quality check(s) failed: {failure_summary}"
        )

    logger.info("All critical data quality checks PASSED.")
    return results


# COMMAND ----------

if __name__ == "__main__":
    active_spark = SparkSession.builder.getOrCreate()
    run_data_quality_checks(active_spark)
