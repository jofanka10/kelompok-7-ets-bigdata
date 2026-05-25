"""
SILVER LAYER - Clean & Transform Data

Fungsi:
- Baca data dari Bronze layer (api & rss)
- Lakukan 3+ transformasi cleaning:
  1. Hapus duplikat berdasarkan full_name
  2. Handle null values dan invalid data
  3. Cast tipe data (timestamp, integer)
  4. Extract derived columns (hour, date, language_lower)
- Simpan ke Silver Delta layer di HDFS

Output: Single unified Silver table dengan data yang sudah bersih
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, col, to_timestamp, hour, date_format,
    when, coalesce, lower, lit
)
from delta import configure_spark_with_delta_pip

# ============================================================================
# SETUP: Initialize Spark Session with Delta Lake
# ============================================================================

builder = SparkSession.builder \
    .appName("Silver-GitTrend") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.datanode.hostname", "localhost")

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# STEP 1: Read from Bronze Layer (HDFS)
# ============================================================================

print("\n" + "="*80)
print("SILVER LAYER: STEP 1 - Read Bronze Data")
print("="*80)

bronze_api_path = "hdfs://namenode:8020/lakehouse/bronze/github_api"
bronze_rss_path = "hdfs://namenode:8020/lakehouse/bronze/github_rss"

try:
    api_bronze = spark.read.format("delta").load(bronze_api_path)
    rss_bronze = spark.read.format("delta").load(bronze_rss_path)

    print(f"\n✓ API Bronze: {api_bronze.count()} records")
    print(f"✓ RSS Bronze: {rss_bronze.count()} records")

    # Union both sources (allowMissingColumns handles schema differences)
    bronze_all = api_bronze.unionByName(rss_bronze, allowMissingColumns=True)
    print(f"✓ Combined: {bronze_all.count()} records")

except Exception as e:
    print(f"❌ Error reading Bronze layers: {e}")
    spark.stop()
    exit(1)

# ============================================================================
# STEP 2: Cleaning & Transformations
# ============================================================================

print("\n" + "="*80)
print("SILVER LAYER: STEP 2 - Data Cleaning & Transformation")
print("="*80)

records_before = bronze_all.count()
print(f"\nRecords BEFORE cleaning: {records_before}")

# TRANSFORMATION 1: Drop Duplicates
print("\n--- Transformation 1: Drop Duplicates ---")
silver_df = bronze_all.dropDuplicates(["full_name"])
records_after_dedup = silver_df.count()
duplicates_removed = records_before - records_after_dedup
print(f"✓ Dropped {duplicates_removed} duplicate records")
print(f"  Remaining: {records_after_dedup} records")

# TRANSFORMATION 2: Handle NULL and Invalid Values
print("\n--- Transformation 2: Handle NULL/Invalid Values ---")

silver_df = silver_df.filter(col("full_name").isNotNull())
print(f"✓ Filtered null full_name")

silver_df = silver_df.withColumn("stargazers_count",
    coalesce(col("stargazers_count"), lit(0)))
print(f"✓ Filled null stargazers_count with 0")

records_before_filter = silver_df.count()
silver_df = silver_df.filter(col("stargazers_count") >= 0)
invalid_removed = records_before_filter - silver_df.count()
print(f"✓ Removed {invalid_removed} records with negative stargazers")

# TRANSFORMATION 3: Cast Data Types & Extract Columns
print("\n--- Transformation 3: Cast Data Types & Extract Columns ---")

silver_df = silver_df \
    .withColumn("stargazers_count", col("stargazers_count").cast("integer")) \
    .withColumn("language", coalesce(col("language"), lit("Unknown"))) \
    .withColumn("language_lower", lower(col("language"))) \
    .withColumn("updated_at_timestamp",
        when(col("updated_at").isNotNull(),
            to_timestamp(col("updated_at")))
        .otherwise(current_timestamp())) \
    .withColumn("updated_hour", hour(col("updated_at_timestamp"))) \
    .withColumn("updated_date", date_format(col("updated_at_timestamp"), "yyyy-MM-dd"))

print(f"✓ Cast stargazers_count to integer")
print(f"✓ Standardized language (filled Unknown, lowercase)")
print(f"✓ Parsed timestamp dan extracted hour/date")

# TRANSFORMATION 4: Add Processing Metadata
print("\n--- Transformation 4: Add Processing Metadata ---")

silver_df = silver_df \
    .withColumn("_processed_at", current_timestamp()) \
    .withColumn("_quality_flag",
        when((col("full_name").isNotNull()) & (col("language").isNotNull()), "OK")
        .otherwise("PARTIAL_DATA"))

print(f"✓ Added _processed_at timestamp")
print(f"✓ Added _quality_flag")

# ============================================================================
# STEP 3: Select Final Columns for Silver Layer
# ============================================================================

print("\n" + "="*80)
print("SILVER LAYER: STEP 3 - Select Final Columns")
print("="*80)

silver_df = silver_df.select(
    col("full_name"),
    col("description"),
    col("html_url"),
    col("stargazers_count"),
    col("language"),
    col("language_lower"),
    col("updated_at_timestamp"),
    col("updated_hour"),
    col("updated_date"),
    col("_source"),
    col("_ingested_at"),
    col("_processed_at"),
    col("_quality_flag")
)

print(f"\n✓ Final schema:")
silver_df.printSchema()

# ============================================================================
# STEP 4: Save to Silver Delta Layer (HDFS)
# ============================================================================

print("\n" + "="*80)
print("SILVER LAYER: STEP 4 - Save to Delta Lake")
print("="*80)

silver_path = "hdfs://namenode:8020/lakehouse/silver/github"

try:
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(silver_path)
    print(f"\n✓ Silver layer saved to: {silver_path}")
except Exception as e:
    print(f"❌ Error saving Silver layer: {e}")
    spark.stop()
    exit(1)

# ============================================================================
# STEP 5: Verification & Summary
# ============================================================================

print("\n" + "="*80)
print("SILVER LAYER: STEP 5 - Verification & Summary")
print("="*80)

try:
    silver_verify = spark.read.format("delta").load(silver_path)

    total = silver_verify.count()
    print(f"\n✓ Silver table verified: {total} records")

    print("\nData Quality Summary:")
    silver_verify.groupBy("_quality_flag").count().show()

    print("Records by Source:")
    silver_verify.groupBy("_source").count().show()

    print("Top 5 Languages:")
    silver_verify.groupBy("language_lower").count() \
        .orderBy("count", ascending=False).show(5)

    print("Sample Data:")
    silver_verify.select(
        "full_name", "stargazers_count", "language",
        "_source", "updated_hour", "_quality_flag"
    ).show(5, truncate=False)

except Exception as e:
    print(f"❌ Error verifying Silver layer: {e}")

print("\n" + "="*80)
print("✅ SILVER LAYER COMPLETE!")
print("="*80 + "\n")

spark.stop()