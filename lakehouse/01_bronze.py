"""
BRONZE LAYER - Ingest Raw Data from HDFS to Delta Lake

Fungsi:
- Baca JSON raw dari HDFS (GitHub API & RSS)
- Tambahkan metadata: _ingested_at (timestamp), _source (api/rss)
- Simpan ke Delta Lake format (Bronze layer)

Data Source:
- hdfs://namenode:8020/data/github/api/
- hdfs://namenode:8020/data/github/rss/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip

# ============================================================================
# SETUP: Initialize Spark Session with Delta Lake
# ============================================================================

builder = SparkSession.builder \
    .appName("Bronze-GitTrend") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.datanode.hostname", "localhost") \
    .config("spark.sql.files.maxPartitionBytes", "268435456") \
    .config("spark.task.maxFailures", "4")

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=["io.delta:delta-spark_2.12:3.1.0"]
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# STEP 1: Read API Data from HDFS
# ============================================================================

print("\n" + "="*80)
print("BRONZE LAYER: STEP 1 - Ingest GitHub API Data")
print("="*80)

api_df_with_meta = None
try:
    api_path = "hdfs://namenode:8020/data/github/api/"
    print(f"\nReading from: {api_path}")

    api_df = spark.read \
        .option("multiLine", True) \
        .json(api_path)

    print(f"✓ API Data loaded: {api_df.count()} records")
    print("✓ Schema:")
    api_df.printSchema()

    api_df_with_meta = api_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("api"))

    print("✓ Metadata added: _ingested_at, _source")

except Exception as e:
    print(f"❌ Error reading API data: {e}")

# ============================================================================
# STEP 2: Read RSS Data from HDFS
# ============================================================================

print("\n" + "="*80)
print("BRONZE LAYER: STEP 2 - Ingest RSS Data")
print("="*80)

rss_df_with_meta = None
try:
    rss_path = "hdfs://namenode:8020/data/github/rss/"
    print(f"\nReading from: {rss_path}")

    rss_df = spark.read \
        .option("multiLine", True) \
        .json(rss_path)

    print(f"✓ RSS Data loaded: {rss_df.count()} records")
    print("✓ Schema:")
    rss_df.printSchema()

    rss_df_with_meta = rss_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .withColumn("_source", lit("rss"))

    print("✓ Metadata added: _ingested_at, _source")

except Exception as e:
    print(f"❌ Error reading RSS data: {e}")

# ============================================================================
# STEP 3: Save to Bronze Delta Layer
# ============================================================================

print("\n" + "="*80)
print("BRONZE LAYER: STEP 3 - Save to Delta Lake")
print("="*80)

api_bronze_path = "hdfs://namenode:8020/lakehouse/bronze/github_api"
rss_bronze_path = "hdfs://namenode:8020/lakehouse/bronze/github_rss"

if api_df_with_meta is not None:
    try:
        api_df_with_meta.write \
            .format("delta") \
            .mode("overwrite") \
            .save(api_bronze_path)
        print(f"\n✓ API Bronze layer saved to: {api_bronze_path}")
    except Exception as e:
        print(f"❌ Error saving API bronze: {e}")
        api_df_with_meta = None
else:
    print("⚠️ Skipping API save (no data loaded)")

if rss_df_with_meta is not None:
    try:
        rss_df_with_meta.write \
            .format("delta") \
            .mode("overwrite") \
            .save(rss_bronze_path)
        print(f"✓ RSS Bronze layer saved to: {rss_bronze_path}")
    except Exception as e:
        print(f"❌ Error saving RSS bronze: {e}")
        rss_df_with_meta = None
else:
    print("⚠️ Skipping RSS save (no data loaded)")

# ============================================================================
# STEP 4: Verification
# ============================================================================

print("\n" + "="*80)
print("BRONZE LAYER: STEP 4 - Verification")
print("="*80)

if api_df_with_meta is not None:
    try:
        api_bronze_verify = spark.read.format("delta").load(api_bronze_path)
        print(f"\n✓ API Bronze verified: {api_bronze_verify.count()} records")
        print("  Sample data:")
        api_bronze_verify.select("full_name", "stargazers_count", "_source", "_ingested_at").show(3)
    except Exception as e:
        print(f"❌ Error verifying API bronze: {e}")
else:
    print("\n⚠️ Skipping API Bronze verification (no data loaded)")

if rss_df_with_meta is not None:
    try:
        rss_bronze_verify = spark.read.format("delta").load(rss_bronze_path)
        print(f"\n✓ RSS Bronze verified: {rss_bronze_verify.count()} records")
        print("  Sample data:")
        rss_bronze_verify.select("full_name", "html_url", "_source", "_ingested_at").show(3)
    except Exception as e:
        print(f"❌ Error verifying RSS bronze: {e}")
else:
    print("\n⚠️ Skipping RSS Bronze verification (no data loaded)")

print("\n" + "="*80)
print("✅ BRONZE LAYER COMPLETE!")
print("="*80 + "\n")

spark.stop()