"""Centralized configuration for the E-Commerce Customer Behaviour & Sales Intelligence pipeline.

All paths, Unity Catalog namespaces, raw dataset filenames, Delta table names,
and business rule constants live here so notebooks and tests contain zero
hard-coded paths.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict

# ---------------------------------------------------------------------------
# Unity Catalog Namespace & Storage Configuration
# ---------------------------------------------------------------------------
CATALOG: str = os.getenv("ECOMMERCE_CATALOG", "workspace")
SCHEMA: str = os.getenv("ECOMMERCE_SCHEMA", "ecommerce")
VOLUME_NAME: str = os.getenv("ECOMMERCE_VOLUME", "raw")

# Single configurable path for raw Olist CSV files in Databricks Unity Catalog Volume
RAW_DATA_PATH: str = os.getenv(
    "ECOMMERCE_RAW_DATA_PATH",
    f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/",
)

# ---------------------------------------------------------------------------
# Raw Olist CSV Filenames (Kaggle Olist Brazilian E-Commerce Public Dataset)
# ---------------------------------------------------------------------------
RAW_FILES: Dict[str, str] = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

# ---------------------------------------------------------------------------
# Medallion Layer Delta Table Names (unqualified)
# ---------------------------------------------------------------------------
BRONZE_TABLES: Dict[str, str] = {
    "orders": "bronze_orders",
    "order_items": "bronze_order_items",
    "order_payments": "bronze_order_payments",
    "order_reviews": "bronze_order_reviews",
    "customers": "bronze_customers",
    "products": "bronze_products",
    "sellers": "bronze_sellers",
    "geolocation": "bronze_geolocation",
    "category_translation": "bronze_category_translation",
}

SILVER_TABLES: Dict[str, str] = {
    "orders": "silver_orders",
    "orders_non_delivered": "silver_orders_non_delivered",
    "order_items": "silver_order_items",
    "order_payments": "silver_order_payments",
    "order_reviews": "silver_order_reviews",
    "customers": "silver_customers",
    "products": "silver_products",
    "sellers": "silver_sellers",
    "geolocation": "silver_geolocation",
    "orders_enriched": "silver_orders_enriched",
}

GOLD_TABLES: Dict[str, str] = {
    "daily_sales": "gold_daily_sales",
    "monthly_sales": "gold_monthly_sales",
    "category_performance": "gold_category_performance",
    "customer_rfm": "gold_customer_rfm",
    "state_sales": "gold_state_sales",
    "delivery_performance": "gold_delivery_performance",
    "payment_mix": "gold_payment_mix",
    "seller_performance": "gold_seller_performance",
    "review_vs_delay": "gold_review_vs_delay",
}

# ---------------------------------------------------------------------------
# Business Rule & Data Quality Constants
# ---------------------------------------------------------------------------
UNKNOWN_CATEGORY: str = "unknown"
DELIVERED_STATUS: str = "delivered"
RFM_NUM_BINS: int = 5
DQ_REVENUE_TOLERANCE: float = 0.05  # Tolerance in BRL for floating-point rounding across aggregations

# Known Olist Portuguese categories missing from product_category_name_translation.csv
FALLBACK_CATEGORY_TRANSLATIONS: Dict[str, str] = {
    "pc_gamer": "pc_gamer",
    "portateis_cozinha_e_preparadores_de_alimentos": "portable_kitchen_food_preparers",
}


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable snapshot of active pipeline settings."""

    catalog: str = CATALOG
    schema: str = SCHEMA
    volume_name: str = VOLUME_NAME
    raw_data_path: str = RAW_DATA_PATH


def get_raw_file_path(dataset_key: str, base_path: str = RAW_DATA_PATH) -> str:
    """Return the full Unity Catalog Volume path for a given raw dataset key.

    Args:
        dataset_key: Logical dataset key from ``RAW_FILES``.
        base_path: Base directory path containing raw CSV files.

    Returns:
        Normalized POSIX path string to the CSV file.

    Raises:
        KeyError: If ``dataset_key`` is not a recognized Olist dataset.
    """
    if dataset_key not in RAW_FILES:
        raise KeyError(
            f"Unknown dataset_key '{dataset_key}'. Expected one of: {sorted(RAW_FILES.keys())}"
        )
    normalized_base = base_path.rstrip("/")
    return f"{normalized_base}/{RAW_FILES[dataset_key]}"


def get_full_table_name(
    table_name: str,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
) -> str:
    """Return a three-level Unity Catalog table name ``catalog.schema.table``.

    Args:
        table_name: Unqualified Delta table name.
        catalog: Unity Catalog catalog name.
        schema: Unity Catalog schema name.

    Returns:
        Three-part fully qualified table identifier.
    """
    return f"{catalog}.{schema}.{table_name}"


def get_bronze_table(dataset_key: str, catalog: str = CATALOG, schema: str = SCHEMA) -> str:
    """Return the fully qualified Bronze Delta table name for a dataset key."""
    return get_full_table_name(BRONZE_TABLES[dataset_key], catalog=catalog, schema=schema)


def get_silver_table(table_key: str, catalog: str = CATALOG, schema: str = SCHEMA) -> str:
    """Return the fully qualified Silver Delta table name for a table key."""
    return get_full_table_name(SILVER_TABLES[table_key], catalog=catalog, schema=schema)


def get_gold_table(table_key: str, catalog: str = CATALOG, schema: str = SCHEMA) -> str:
    """Return the fully qualified Gold Delta table name for a table key."""
    return get_full_table_name(GOLD_TABLES[table_key], catalog=catalog, schema=schema)
