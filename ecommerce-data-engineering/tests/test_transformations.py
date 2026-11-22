"""Unit tests for Silver and Gold PySpark transformations using a local SparkSession.

Run with:
    pytest tests/test_transformations.py -v
"""

from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from types import ModuleType

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_notebook_module(module_name: str, relative_path: str) -> ModuleType:
    """Import a numbered Databricks notebook .py file as a standard Python module."""
    file_path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load notebook module from {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


silver_mod = _load_notebook_module("silver_cleaning", "notebooks/03_silver_cleaning.py")
gold_mod = _load_notebook_module("gold_aggregations", "notebooks/04_gold_aggregations.py")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a lightweight local SparkSession shared across all unit tests."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-transformations-pytest")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Silver Layer Tests
# ---------------------------------------------------------------------------


def test_clean_orders_derived_columns_and_split(spark: SparkSession) -> None:
    """Verify delivery_days, delay_days, is_late, date parts, and delivered split."""
    schema = StructType(
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
    )
    rows = [
        # Late delivered order (+3 days late, 13 delivery days)
        (
            "ord_late",
            "cust_1",
            "delivered",
            "2018-05-01 10:00:00",
            "2018-05-01 11:00:00",
            "2018-05-03 10:00:00",
            "2018-05-14 12:00:00",
            "2018-05-11 00:00:00",
        ),
        # Early / on-time delivered order (-2 days delay, 5 delivery days)
        (
            "ord_early",
            "cust_2",
            "DELIVERED ",
            "2018-06-10 08:00:00",
            "2018-06-10 09:00:00",
            "2018-06-11 09:00:00",
            "2018-06-15 16:00:00",
            "2018-06-17 00:00:00",
        ),
        # Delivered order with corrupt delivery timestamp earlier than purchase
        (
            "ord_corrupt_ts",
            "cust_3",
            "delivered",
            "2018-07-10 12:00:00",
            "2018-07-10 13:00:00",
            "2018-07-11 10:00:00",
            "2018-07-08 10:00:00",
            "2018-07-20 00:00:00",
        ),
        # Non-delivered canceled order
        (
            "ord_canceled",
            "cust_4",
            "canceled",
            "2018-08-01 09:00:00",
            "2018-08-01 10:00:00",
            None,
            None,
            "2018-08-15 00:00:00",
        ),
    ]
    raw_df = spark.createDataFrame(rows, schema=schema)

    delivered_df, non_delivered_df = silver_mod.clean_orders(raw_df)
    delivered = {r["order_id"]: r for r in delivered_df.collect()}
    non_delivered = {r["order_id"]: r for r in non_delivered_df.collect()}

    assert set(delivered.keys()) == {"ord_late", "ord_early", "ord_corrupt_ts"}
    assert set(non_delivered.keys()) == {"ord_canceled"}

    # Check late order derived columns
    late = delivered["ord_late"]
    assert late["delivery_days"] == 13
    assert late["delay_days"] == 3
    assert late["is_late"] is True
    assert late["order_year"] == 2018
    assert late["order_month"] == 5
    assert late["year_month"] == "2018-05"

    # Check early order derived columns
    early = delivered["ord_early"]
    assert early["delivery_days"] == 5
    assert early["delay_days"] == -2
    assert early["is_late"] is False
    assert early["year_month"] == "2018-06"

    # Check corrupt delivery date before purchase was safely nullified
    corrupt = delivered["ord_corrupt_ts"]
    assert corrupt["order_delivered_customer_date"] is None
    assert corrupt["delivery_days"] is None
    assert corrupt["delay_days"] is None
    assert corrupt["is_late"] is None


def test_clean_order_items_item_total_and_negative_filter(spark: SparkSession) -> None:
    """Verify item_total = price + freight_value, deduplication, and negative filtering."""
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
        ]
    )
    rows = [
        ("ord_1", 1, "prod_1", "sel_1", "2018-05-05 00:00:00", 100.50, 19.25),
        ("ord_1", 1, "prod_1", "sel_1", "2018-05-05 00:00:00", 100.50, 19.25),  # exact dup
        ("ord_1", 2, "prod_2", "sel_2", "2018-05-05 00:00:00", -10.00, 15.00),  # invalid negative
    ]
    raw_df = spark.createDataFrame(rows, schema=schema)

    result = silver_mod.clean_order_items(raw_df).collect()
    assert len(result) == 1
    assert result[0]["order_id"] == "ord_1"
    assert result[0]["item_total"] == pytest.approx(119.75)


def test_clean_products_unknown_category_and_translation(spark: SparkSession) -> None:
    """Verify null categories become 'unknown', English translation joins, and typos fixed."""
    products_schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_category_name", StringType(), True),
            StructField("product_name_lenght", IntegerType(), True),
            StructField("product_description_lenght", IntegerType(), True),
            StructField("product_photos_qty", IntegerType(), True),
            StructField("product_weight_g", DoubleType(), True),
            StructField("product_length_cm", DoubleType(), True),
            StructField("product_height_cm", DoubleType(), True),
            StructField("product_width_cm", DoubleType(), True),
        ]
    )
    products_df = spark.createDataFrame(
        [
            ("p_translated", "beleza_saude", 40, 200, 2, 500.0, 20.0, 10.0, 15.0),
            ("p_null_cat", None, 30, 150, 1, 300.0, 15.0, 10.0, 10.0),
            ("p_fallback", "pc_gamer", 50, 300, 3, 2500.0, 40.0, 30.0, 20.0),
        ],
        schema=products_schema,
    )
    translation_df = spark.createDataFrame(
        [("beleza_saude", "health_beauty")],
        ["product_category_name", "product_category_name_english"],
    )

    cleaned = {r["product_id"]: r for r in silver_mod.clean_products(products_df, translation_df).collect()}

    assert cleaned["p_translated"]["category_en"] == "health_beauty"
    assert cleaned["p_translated"]["product_name_length"] == 40
    assert cleaned["p_translated"]["product_description_length"] == 200
    assert cleaned["p_null_cat"]["product_category_name"] == "unknown"
    assert cleaned["p_null_cat"]["category_en"] == "unknown"
    assert cleaned["p_fallback"]["category_en"] == "pc_gamer"


def test_deduplication_reviews_customers_geolocation(spark: SparkSession) -> None:
    """Verify deduplication rules for reviews, customers, and geolocation zip averages."""
    # 1. Reviews: deduplicate by review_id keeping latest answer timestamp
    reviews_df = spark.createDataFrame(
        [
            ("rev_1", "ord_1", 3, "t1", "old", "2018-05-10 00:00:00", "2018-05-11 00:00:00"),
            ("rev_1", "ord_1", 5, "t2", "new", "2018-05-10 00:00:00", "2018-05-12 00:00:00"),
        ],
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
    )
    clean_rev = silver_mod.clean_order_reviews(reviews_df).collect()
    assert len(clean_rev) == 1
    assert clean_rev[0]["review_score"] == 5

    # 2. Customers: deduplicate by customer_id & zero-pad zip prefix
    customers_df = spark.createDataFrame(
        [
            ("c_1", "u_1", "1310", " Sao Paulo ", " sp "),
            ("c_1", "u_1", "1310", " Sao Paulo ", " sp "),
        ],
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    )
    clean_cust = silver_mod.clean_customers(customers_df).collect()
    assert len(clean_cust) == 1
    assert clean_cust[0]["customer_zip_code_prefix"] == "01310"
    assert clean_cust[0]["customer_state"] == "SP"

    # 3. Geolocation: aggregate by zip prefix to average lat/lng
    geo_df = spark.createDataFrame(
        [
            ("1310", -23.50, -46.60, "sao paulo", "sp"),
            ("01310", -23.60, -46.70, "sao paulo", "sp"),
        ],
        [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
    )
    clean_geo = silver_mod.clean_geolocation(geo_df).collect()
    assert len(clean_geo) == 1
    assert clean_geo[0]["geolocation_zip_code_prefix"] == "01310"
    assert clean_geo[0]["geolocation_lat"] == pytest.approx(-23.55)
    assert clean_geo[0]["geolocation_lng"] == pytest.approx(-46.65)
    assert clean_geo[0]["coordinate_samples"] == 2


def test_aggregate_order_payments_prevents_fanout(spark: SparkSession) -> None:
    """Verify multi-payment orders collapse to 1 row with correct primary_payment_type."""
    payments_df = spark.createDataFrame(
        [
            ("ord_1", 1, "voucher", 1, 25.00),
            ("ord_1", 2, "credit_card", 6, 175.00),
            ("ord_2", 1, "boleto", 1, 80.00),
        ],
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
    )

    agg_map = {r["order_id"]: r for r in silver_mod.aggregate_order_payments(payments_df).collect()}
    assert len(agg_map) == 2
    assert agg_map["ord_1"]["total_payment_value"] == pytest.approx(200.00)
    assert agg_map["ord_1"]["max_installments"] == 6
    assert agg_map["ord_1"]["payment_count"] == 2
    assert agg_map["ord_1"]["primary_payment_type"] == "credit_card"
    assert agg_map["ord_2"]["primary_payment_type"] == "boleto"


def test_build_orders_enriched_preserves_item_grain(spark: SparkSession) -> None:
    """Ensure joining multi-payment and multi-review dimensions never duplicates items."""
    orders_raw = spark.createDataFrame(
        [
            (
                "ord_1",
                "c_1",
                "delivered",
                "2018-05-01 10:00:00",
                "2018-05-01 10:30:00",
                "2018-05-02 10:00:00",
                "2018-05-06 10:00:00",
                "2018-05-10 00:00:00",
            )
        ],
        [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    items_raw = spark.createDataFrame(
        [
            ("ord_1", 1, "p_1", "s_1", "2018-05-04 00:00:00", 100.0, 10.0),
            ("ord_1", 2, "p_1", "s_1", "2018-05-04 00:00:00", 50.0, 10.0),
        ],
        [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    )
    payments_raw = spark.createDataFrame(
        [
            ("ord_1", 1, "voucher", 1, 20.0),
            ("ord_1", 2, "credit_card", 3, 150.0),
        ],
        [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
    )
    reviews_raw = spark.createDataFrame(
        [
            ("r_1", "ord_1", 4, "", "", "2018-05-07 00:00:00", "2018-05-08 00:00:00"),
            ("r_2", "ord_1", 5, "", "", "2018-05-09 00:00:00", "2018-05-10 00:00:00"),
        ],
        [
            "review_id",
            "order_id",
            "review_score",
            "review_comment_title",
            "review_comment_message",
            "review_creation_date",
            "review_answer_timestamp",
        ],
    )
    customers_raw = spark.createDataFrame(
        [("c_1", "u_1", "01310", "sao paulo", "sp")],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
    )
    products_raw = spark.createDataFrame(
        [("p_1", "beleza_saude", 10, 100, 1, 200.0, 10.0, 10.0, 10.0)],
        [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    )
    trans_raw = spark.createDataFrame(
        [("beleza_saude", "health_beauty")],
        ["product_category_name", "product_category_name_english"],
    )
    sellers_raw = spark.createDataFrame(
        [("s_1", "01310", "sao paulo", "sp")],
        ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"],
    )

    silver_orders, _ = silver_mod.clean_orders(orders_raw)
    enriched = silver_mod.build_orders_enriched(
        silver_orders_df=silver_orders,
        silver_order_items_df=silver_mod.clean_order_items(items_raw),
        silver_order_payments_df=silver_mod.aggregate_order_payments(payments_raw),
        silver_customers_df=silver_mod.clean_customers(customers_raw),
        silver_products_df=silver_mod.clean_products(products_raw, trans_raw),
        silver_sellers_df=silver_mod.clean_sellers(sellers_raw),
        silver_order_reviews_df=silver_mod.clean_order_reviews(reviews_raw),
    )

    rows = enriched.collect()
    assert len(rows) == 2
    assert rows[0]["primary_payment_type"] == "credit_card"
    assert rows[0]["review_score"] == pytest.approx(4.5)
    assert sum(r["item_total"] for r in rows) == pytest.approx(170.0)


# ---------------------------------------------------------------------------
# Gold Layer Tests
# ---------------------------------------------------------------------------


def test_gold_monthly_sales_mom_growth(spark: SparkSession) -> None:
    """Verify lag-based Month-over-Month revenue growth percentage."""
    enriched_df = spark.createDataFrame(
        [
            ("2018-01", "o1", 100.0),
            ("2018-02", "o2", 150.0),
            ("2018-03", "o3", 120.0),
        ],
        ["year_month", "order_id", "item_total"],
    )

    rows = {r["year_month"]: r for r in gold_mod.build_gold_monthly_sales(enriched_df).collect()}
    assert rows["2018-01"]["mom_growth_pct"] is None
    assert rows["2018-02"]["mom_growth_pct"] == pytest.approx(50.0)
    assert rows["2018-03"]["mom_growth_pct"] == pytest.approx(-20.0)


def test_gold_customer_rfm_scoring_and_segments(spark: SparkSession) -> None:
    """Verify RFM metrics, ntile(5) bounds, and segment assignment across 10 customers."""
    schema = StructType(
        [
            StructField("customer_unique_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_date", DateType(), True),
            StructField("item_total", DoubleType(), True),
        ]
    )
    # 10 synthetic customers spanning recent/high-value to lapsed/low-value
    rows = [
        ("u_champ", "o1", date(2018, 8, 29), 500.0),
        ("u_champ", "o2", date(2018, 8, 30), 600.0),
        ("u_champ", "o3", date(2018, 8, 31), 700.0),
        ("u_2", "o4", date(2018, 8, 25), 400.0),
        ("u_2", "o5", date(2018, 8, 28), 350.0),
        ("u_3", "o6", date(2018, 8, 20), 300.0),
        ("u_4", "o7", date(2018, 7, 15), 250.0),
        ("u_5", "o8", date(2018, 6, 15), 200.0),
        ("u_6", "o9", date(2018, 5, 15), 150.0),
        ("u_7", "o10", date(2018, 4, 15), 100.0),
        ("u_8", "o11", date(2018, 3, 15), 80.0),
        ("u_9", "o12", date(2018, 2, 15), 60.0),
        ("u_lost", "o13", date(2018, 1, 1), 20.0),
    ]
    enriched_df = spark.createDataFrame(rows, schema=schema)

    rfm_rows = {
        r["customer_unique_id"]: r for r in gold_mod.build_gold_customer_rfm(enriched_df).collect()
    }
    valid_segments = {"Champions", "Loyal", "Potential Loyalist", "At Risk", "Lost"}

    assert len(rfm_rows) == 10
    for r in rfm_rows.values():
        assert 1 <= r["r_score"] <= 5
        assert 1 <= r["f_score"] <= 5
        assert 1 <= r["m_score"] <= 5
        assert r["rfm_segment"] in valid_segments

    # Most recent, highest frequency & spend customer must be a Champion
    assert rfm_rows["u_champ"]["recency_days"] == 1
    assert rfm_rows["u_champ"]["frequency"] == 3
    assert rfm_rows["u_champ"]["monetary"] == pytest.approx(1800.0)
    assert rfm_rows["u_champ"]["rfm_segment"] == "Champions"

    # Oldest, single-order, lowest-spend customer must be Lost
    assert rfm_rows["u_lost"]["rfm_segment"] == "Lost"


def test_gold_review_vs_delay_buckets(spark: SparkSession) -> None:
    """Verify delay bucket mapping and order-grain deduplication for review scores."""
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("delay_days", IntegerType(), True),
            StructField("review_score", DoubleType(), True),
        ]
    )
    rows = [
        ("o_ontime_1", -3, 5.0),
        ("o_ontime_1", -3, 5.0),  # duplicate item row for same order -> must count once
        ("o_ontime_2", 0, 4.0),
        ("o_1_3", 2, 3.0),
        ("o_4_7", 5, 2.0),
        ("o_8_plus", 12, 1.0),
        ("o_null", None, 4.0),  # ignored because delay_days is null
    ]
    enriched_df = spark.createDataFrame(rows, schema=schema)

    buckets = {
        r["delay_bucket"]: r for r in gold_mod.build_gold_review_vs_delay(enriched_df).collect()
    }
    assert set(buckets.keys()) == {"On Time", "1-3 Days Late", "4-7 Days Late", "8+ Days Late"}
    assert buckets["On Time"]["orders"] == 2
    assert buckets["On Time"]["avg_review"] == pytest.approx(4.5)
    assert buckets["1-3 Days Late"]["avg_review"] == pytest.approx(3.0)
    assert buckets["4-7 Days Late"]["avg_review"] == pytest.approx(2.0)
    assert buckets["8+ Days Late"]["avg_review"] == pytest.approx(1.0)
