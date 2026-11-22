# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Environment & Unity Catalog Setup
# MAGIC
# MAGIC Initializes the Unity Catalog namespace (`catalog`, `schema`, and `volume`) and verifies
# MAGIC that all 9 raw Olist Brazilian E-Commerce CSV files are present in the configured volume path.

# COMMAND ----------

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

# Ensure project root is importable in both Databricks Repos and local Python runs
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    PROJECT_ROOT = Path(os.getcwd()).parent

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.config import (  # noqa: E402
    CATALOG,
    RAW_DATA_PATH,
    RAW_FILES,
    SCHEMA,
    VOLUME_NAME,
    get_raw_file_path,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("00_setup")

# COMMAND ----------


def initialize_unity_catalog(
    spark: SparkSession,
    catalog: str = CATALOG,
    schema: str = SCHEMA,
    volume_name: str = VOLUME_NAME,
) -> None:
    """Create Unity Catalog schema and managed volume if they do not exist.

    Note:
        On Databricks Community / Free Edition or restricted workspaces, the
        ``workspace`` catalog already exists and ``CREATE CATALOG`` may require
        metastore admin privileges. We attempt catalog creation safely and fall
        back to using the existing catalog.

    Args:
        spark: Active SparkSession.
        catalog: Target Unity Catalog name.
        schema: Target schema name within the catalog.
        volume_name: Target managed volume name for raw CSV landing files.
    """
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
        logger.info("Verified catalog: %s", catalog)
    except Exception as exc:
        logger.warning(
            "Skipping CREATE CATALOG for '%s' (likely pre-created or restricted): %s",
            catalog,
            exc,
        )

    spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`
        COMMENT 'Medallion Lakehouse schema for Olist E-Commerce Customer Behaviour & Sales Intelligence'
        """
    )
    logger.info("Verified schema: %s.%s", catalog, schema)

    spark.sql(f"USE SCHEMA `{schema}`")
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`
        COMMENT 'Landing volume for raw Olist Brazilian E-Commerce CSV files'
        """
    )
    logger.info("Verified volume: /Volumes/%s/%s/%s/", catalog, schema, volume_name)


def validate_raw_files(raw_data_path: str = RAW_DATA_PATH) -> Tuple[List[str], List[str]]:
    """Check whether all 9 expected Olist CSV files exist in the landing directory.

    Args:
        raw_data_path: Path to the directory containing raw CSV files.

    Returns:
        Tuple of ``(found_files, missing_files)`` lists.
    """
    found: List[str] = []
    missing: List[str] = []

    for dataset_key, filename in RAW_FILES.items():
        full_path = get_raw_file_path(dataset_key, base_path=raw_data_path)
        if os.path.exists(full_path):
            size_mb = os.path.getsize(full_path) / (1024 * 1024)
            logger.info("FOUND   %-22s | %-45s | %6.2f MB", dataset_key, filename, size_mb)
            found.append(full_path)
        else:
            logger.warning("MISSING %-22s | %-45s | expected at %s", dataset_key, filename, full_path)
            missing.append(full_path)

    return found, missing


def run_setup(spark: SparkSession) -> Dict[str, object]:
    """Execute Unity Catalog setup and raw file landing validation.

    Args:
        spark: Active SparkSession.

    Returns:
        Dictionary summarizing environment configuration and raw file status.
    """
    logger.info("Starting environment setup on Spark %s", spark.version)
    initialize_unity_catalog(spark, catalog=CATALOG, schema=SCHEMA, volume_name=VOLUME_NAME)

    found, missing = validate_raw_files(RAW_DATA_PATH)
    print(f"\n{'=' * 72}")
    print("E-COMMERCE LAKEHOUSE SETUP SUMMARY")
    print(f"{'=' * 72}")
    print(f"Catalog       : {CATALOG}")
    print(f"Schema        : {SCHEMA}")
    print(f"Raw Volume    : {RAW_DATA_PATH}")
    print(f"Files Found   : {len(found)} / {len(RAW_FILES)}")

    if missing:
        print("\nMissing raw CSV files (upload these to your Unity Catalog Volume before running 01_bronze_ingestion):")
        for path in missing:
            print(f"  - {path}")
    else:
        print("\nAll 9 Olist CSV files verified and ready for Bronze ingestion.")
    print(f"{'=' * 72}\n")

    return {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "raw_data_path": RAW_DATA_PATH,
        "found_count": len(found),
        "missing_files": missing,
    }


# COMMAND ----------

if __name__ == "__main__":
    active_spark = SparkSession.builder.getOrCreate()
    run_setup(active_spark)
