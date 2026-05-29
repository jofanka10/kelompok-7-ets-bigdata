"""
SILVER LAYER - Clean & Transform Data

Fungsi:
- Baca dari Bronze Delta layer (local)
- 4 transformasi cleaning:
  1. Hapus duplikat (full_name)
  2. Handle null values
  3. Cast tipe data, extract derived columns
  4. Tambah _quality_flag
- Simpan ke Silver Delta layer (local)

Input : ./lakehouse_data/bronze/github_api  (Delta)
        ./lakehouse_data/bronze/github_rss  (Delta)
Output: ./lakehouse_data/silver/github      (Delta)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from _spark_delta_setup import (
    get_spark_session,
    BRONZE_API_PATH, BRONZE_RSS_PATH, SILVER_PATH
)
from pyspark.sql.functions import (
    current_timestamp, col, to_timestamp, hour, date_format,
    when, coalesce, lower, lit
)

# ============================================================================
# SETUP
# ============================================================================
spark = get_spark_session("Silver-GitTrend")

# ============================================================================
# STEP 1: Read from Bronze Layer
# ============================================================================
print("\n" + "="*80)
print("SILVER LAYER: STEP 1 - Read Bronze Data")
print("="*80)

try:
    api_bronze = spark.read.format("delta").load(BRONZE_API_PATH)
    rss_bronze = spark.read.format("delta").load(BRONZE_RSS_PATH)
    print(f"✓ API Bronze: {api_bronze.count()} records")
    print(f"✓ RSS Bronze: {rss_bronze.count()} records")
    bronze_all = api_bronze.unionByName(rss_bronze, allowMissingColumns=True)
    print(f"✓ Combined  : {bronze_all.count()} records")
except Exception as e:
    print(f"❌ Error reading Bronze: {e}")
    print("   Pastikan 01_bronze.py sudah dijalankan terlebih dahulu.")
    spark.stop()
    sys.exit(1)

# ============================================================================
# STEP 2: Cleaning & Transformations
# ============================================================================
print("\n" + "="*80)
print("SILVER LAYER: STEP 2 - Data Cleaning & Transformation")
print("="*80)

before = bronze_all.count()

# T1: Drop Duplicates
silver_df = bronze_all.dropDuplicates(["full_name"])
print(f"✓ T1 Dedup: {before - silver_df.count()} duplikat dihapus")

# T2: Handle NULL
silver_df = silver_df.filter(col("full_name").isNotNull())
silver_df = silver_df.withColumn("stargazers_count",
    coalesce(col("stargazers_count"), lit(0)))
silver_df = silver_df.filter(col("stargazers_count") >= 0)
print("✓ T2 NULL handled: stargazers_count, full_name")

# T3: Cast Types & Extract Columns
silver_df = silver_df \
    .withColumn("stargazers_count", col("stargazers_count").cast("integer")) \
    .withColumn("language", coalesce(col("language"), lit("Unknown"))) \
    .withColumn("language_lower", lower(col("language"))) \
    .withColumn("updated_at_timestamp",
        when(col("updated_at").isNotNull(), to_timestamp(col("updated_at")))
        .otherwise(current_timestamp())) \
    .withColumn("updated_hour", hour(col("updated_at_timestamp"))) \
    .withColumn("updated_date", date_format(col("updated_at_timestamp"), "yyyy-MM-dd"))
print("✓ T3 Types cast, derived columns extracted")

# T4: Processing Metadata
silver_df = silver_df \
    .withColumn("_processed_at", current_timestamp()) \
    .withColumn("_quality_flag",
        when(col("full_name").isNotNull() & col("language").isNotNull(), "OK")
        .otherwise("PARTIAL_DATA"))
print("✓ T4 Metadata: _processed_at, _quality_flag")

silver_df = silver_df.select(
    "full_name", "description", "html_url",
    "stargazers_count", "language", "language_lower",
    "updated_at_timestamp", "updated_hour", "updated_date",
    "_source", "_ingested_at", "_processed_at", "_quality_flag"
)

# ============================================================================
# STEP 3: Save to Silver Delta Layer
# ============================================================================
print("\n" + "="*80)
print("SILVER LAYER: STEP 3 - Save to Delta Lake (local)")
print("="*80)

try:
    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"✓ Silver saved: {SILVER_PATH}")
except Exception as e:
    print(f"❌ Error saving Silver: {e}")
    spark.stop()
    sys.exit(1)

# ============================================================================
# STEP 4: Verification
# ============================================================================
print("\n" + "="*80)
print("SILVER LAYER: STEP 4 - Verification")
print("="*80)

v = spark.read.format("delta").load(SILVER_PATH)
total = v.count()
print(f"✓ Silver verified: {total} records")
print("\nData Quality:")
v.groupBy("_quality_flag").count().show()
print("Records by Source:")
v.groupBy("_source").count().show()
print("Top 5 Languages:")
v.groupBy("language_lower").count().orderBy("count", ascending=False).show(5)

print("\n" + "="*80)
print("✅ SILVER LAYER COMPLETE!")
print("="*80 + "\n")

spark.stop()