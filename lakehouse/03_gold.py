"""
GOLD LAYER - Business Analysis & Aggregation

Fungsi:
- Baca Silver Delta layer (local)
- Buat 4 tabel Gold:
  1. language_dist  - Distribusi bahasa pemrograman
  2. top_repos      - Top 10 repo berdasarkan bintang
  3. star_velocity  - Deteksi repo viral (Window Function)
  4. emerging_topics- Kata kunci trending di deskripsi
- Demonstrasi Delta Lake Time Travel
- Simpan semua ke Gold Delta layer (local)

Input : ./lakehouse_data/silver/github        (Delta)
Output: ./lakehouse_data/gold/language_dist   (Delta)
        ./lakehouse_data/gold/top_repos       (Delta)
        ./lakehouse_data/gold/star_velocity   (Delta)
        ./lakehouse_data/gold/emerging_topics (Delta)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from _spark_delta_setup import get_spark_session, SILVER_PATH, GOLD_BASE_PATH
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, sum as spark_sum,
    lag, desc, lower, split, explode, regexp_replace,
    coalesce, lit
)

# ============================================================================
# SETUP
# ============================================================================
spark = get_spark_session("Gold-GitTrend")

# ============================================================================
# STEP 1: Read from Silver Layer
# ============================================================================
print("\n" + "="*80)
print("GOLD LAYER: STEP 1 - Read Silver Data")
print("="*80)

try:
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    total = silver_df.count()
    print(f"✓ Silver data loaded: {total} records")
    silver_df.cache()
except Exception as e:
    print(f"❌ Error reading Silver: {e}")
    print("   Pastikan 02_silver.py sudah dijalankan terlebih dahulu.")
    spark.stop()
    sys.exit(1)

# ============================================================================
# GOLD TABLE 1: Language Distribution
# ============================================================================
print("\n" + "="*80)
print("GOLD TABLE 1: Language Distribution")
print("="*80)

gold_lang = silver_df \
    .groupBy("language_lower") \
    .agg(
        count("*").alias("total_repos"),
        avg("stargazers_count").alias("avg_stars"),
        spark_max("stargazers_count").alias("max_stars")
    ).orderBy("total_repos", ascending=False)
gold_lang.show(10, truncate=False)

# ============================================================================
# GOLD TABLE 2: Top 10 Repositories
# ============================================================================
print("\n" + "="*80)
print("GOLD TABLE 2: Top 10 Repositories")
print("="*80)

gold_top = silver_df \
    .select("full_name", "stargazers_count", "language",
            "html_url", "description", "_source") \
    .orderBy(desc("stargazers_count")).limit(10)
gold_top.show(10, truncate=False)

# ============================================================================
# GOLD TABLE 3: Star Velocity (Window Function)
# ============================================================================
print("\n" + "="*80)
print("GOLD TABLE 3: Star Velocity (Window Function)")
print("="*80)

window_spec = Window.partitionBy("full_name").orderBy("updated_at_timestamp")
gold_velocity = silver_df \
    .withColumn("prev_stars", lag("stargazers_count", 1).over(window_spec)) \
    .withColumn("star_gain",
        coalesce(col("stargazers_count") - col("prev_stars"), lit(0))) \
    .filter(col("prev_stars").isNotNull()) \
    .groupBy("full_name", "language_lower") \
    .agg(
        spark_max("stargazers_count").alias("current_stars"),
        spark_sum("star_gain").alias("total_star_gain"),
        avg("star_gain").alias("avg_star_gain_per_update"),
        count("*").alias("observation_count")
    ).orderBy(desc("total_star_gain"))
gold_velocity.show(10, truncate=False)

# ============================================================================
# GOLD TABLE 4: Emerging Topics
# ============================================================================
print("\n" + "="*80)
print("GOLD TABLE 4: Emerging Topics")
print("="*80)

stopwords = [
    "with","that","this","from","have","been","your","will","more","some",
    "very","also","into","than","them","then","they","when","each","made",
    "most","over","such","used","using","based","make","open","data","code",
    "tool","fast","easy","simple","build","support"
]
gold_topics = silver_df \
    .filter(col("description").isNotNull()) \
    .withColumn("word",
        explode(split(lower(regexp_replace(col("description"), "[^a-zA-Z0-9 ]", "")), " "))) \
    .filter(col("word").rlike("^[a-z]{4,}")) \
    .filter(~col("word").isin(stopwords)) \
    .groupBy("word", "_source") \
    .agg(count("*").alias("frequency")) \
    .orderBy(desc("frequency"))
gold_topics.show(20, truncate=False)

# ============================================================================
# STEP 2: Save All Gold Tables
# ============================================================================
print("\n" + "="*80)
print("GOLD LAYER: STEP 2 - Save to Delta Lake (local)")
print("="*80)

tables = {
    "language_dist"  : gold_lang,
    "top_repos"      : gold_top,
    "star_velocity"  : gold_velocity,
    "emerging_topics": gold_topics,
}
for name, df in tables.items():
    path = os.path.join(GOLD_BASE_PATH, name)
    try:
        df.write.format("delta").mode("overwrite").save(path)
        print(f"✓ Saved gold/{name}")
    except Exception as e:
        print(f"❌ Error saving {name}: {e}")

# ============================================================================
# STEP 3: Time Travel Demo
# ============================================================================
print("\n" + "="*80)
print("GOLD LAYER: STEP 3 - Time Travel Demonstration")
print("="*80)

try:
    from delta.tables import DeltaTable

    silver_delta = DeltaTable.forPath(spark, SILVER_PATH)
    print("=== History Tabel Silver ===")
    silver_delta.history().select("version", "timestamp", "operation").show()

    print("\n=== Simulasi Update: language NULL → 'Unknown' ===")
    silver_delta.update(
        condition="language IS NULL",
        set={"language": "'Unknown'"}
    )

    print("\n=== Data SEKARANG (setelah update) ===")
    spark.read.format("delta").load(SILVER_PATH) \
        .groupBy("language").count() \
        .filter(col("language") == "Unknown").show()

    print("\n=== Data VERSI 0 (sebelum update) ===")
    spark.read.format("delta").option("versionAsOf", 0).load(SILVER_PATH) \
        .filter(col("language").isNull() | (col("language") == "Unknown")) \
        .groupBy("language").count().show()

    print("✓ Time Travel berhasil!")
except Exception as e:
    print(f"⚠️  Time Travel demo: {e}")

# ============================================================================
# STEP 4: Final Verification
# ============================================================================
print("\n" + "="*80)
print("GOLD LAYER: STEP 4 - Final Verification")
print("="*80)

for name in tables:
    path = os.path.join(GOLD_BASE_PATH, name)
    try:
        n = spark.read.format("delta").load(path).count()
        print(f"  ✓ gold/{name}: {n} records")
    except Exception as e:
        print(f"  ❌ gold/{name}: {e}")

silver_df.unpersist()

print("\n" + "="*80)
print("✅ GOLD LAYER COMPLETE!")
print("="*80 + "\n")

spark.stop()