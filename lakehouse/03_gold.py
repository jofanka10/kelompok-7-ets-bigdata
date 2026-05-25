"""
GOLD LAYER - Business Analysis & Aggregation
Topik 7: GitTrend - Monitor Repositori Open Source Populer

Fungsi:
- Baca data dari Silver layer (sudah bersih)
- Buat 4 tabel Gold sesuai spesifikasi tugas:
  1. language_dist  (Repro ETS) - Distribusi bahasa pemrograman
  2. top_repos      (Repro ETS) - Top 10 repo berdasarkan bintang
  3. star_velocity  (Enhanced)  - Deteksi repo yang sedang viral (Window Function)
  4. emerging_topics (Enhanced) - Kata kunci baru di deskripsi (cross-source)

Semua tabel disimpan ke HDFS dalam format Delta Lake.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, avg, max as spark_max, sum as spark_sum,
    lag, desc, lower, split, explode, regexp_replace,
    coalesce, lit, abs as spark_abs, when, substring
)
from delta import configure_spark_with_delta_pip

# ============================================================================
# SETUP: Initialize Spark Session with Delta Lake
# ============================================================================

builder = SparkSession.builder \
    .appName("Gold-GitTrend") \
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
# STEP 1: Read from Silver Layer (HDFS)
# ============================================================================

print("\n" + "="*80)
print("GOLD LAYER: STEP 1 - Read Silver Data")
print("="*80)

silver_path = "hdfs://namenode:8020/lakehouse/silver/github"

try:
    silver_df = spark.read.format("delta").load(silver_path)
    total = silver_df.count()
    print(f"\n✓ Silver data loaded: {total} records")
    silver_df.printSchema()
except Exception as e:
    print(f"❌ Error reading Silver layer: {e}")
    spark.stop()
    exit(1)

# Cache silver_df karena dipakai berkali-kali
silver_df.cache()

# ============================================================================
# GOLD TABLE 1: Language Distribution (REPRODUKSI ETS)
# ============================================================================

print("\n" + "="*80)
print("GOLD TABLE 1: Language Distribution (Reproduksi ETS)")
print("="*80)
print("""
Analisis ETS: Bahasa pemrograman apa yang paling banyak digunakan
repositori trending? — groupBy language, count, urutkan descending.

Keunggulan vs ETS: Data sudah bersih (null → 'Unknown', lowercase),
tidak ada duplikat, tipe data sudah benar.
""")

gold_language_dist = silver_df \
    .groupBy("language_lower") \
    .agg(
        count("*").alias("total_repos"),
        avg("stargazers_count").alias("avg_stars"),
        spark_max("stargazers_count").alias("max_stars")
    ) \
    .orderBy("total_repos", ascending=False)

print("✓ Top 10 bahasa pemrograman trending:")
gold_language_dist.show(10, truncate=False)

# ============================================================================
# GOLD TABLE 2: Top 10 Repositories (REPRODUKSI ETS)
# ============================================================================

print("\n" + "="*80)
print("GOLD TABLE 2: Top 10 Repositories (Reproduksi ETS)")
print("="*80)
print("""
Analisis ETS: Tampilkan full_name, language, stargazers_count,
potongan description — orderBy stars descending.

Keunggulan vs ETS: description sudah tersedia, data sudah de-duplikasi.
""")

gold_top_repos = silver_df \
    .select(
        col("full_name"),
        col("stargazers_count"),
        col("language"),
        col("html_url"),
        col("description"),
        col("_source")
    ) \
    .orderBy(desc("stargazers_count")) \
    .limit(10)

print("✓ Top 10 repo berdasarkan bintang:")
gold_top_repos.show(10, truncate=False)

# ============================================================================
# GOLD TABLE 3: Star Velocity — Enhanced (Window Function)
# ============================================================================

print("\n" + "="*80)
print("GOLD TABLE 3: Star Velocity - Trending Detection (ENHANCED)")
print("="*80)
print("""
Enhanced Analysis: Star velocity per repo — perubahan jumlah bintang
antar observasi menggunakan Window Function (lag).

Mengapa tidak bisa di ETS?
- ETS membaca JSON mentah: timestamp belum di-parse, ada duplikat
- Silver sudah bersih: timestamp bertipe TimestampType, no duplikat
- Window Function (lag) butuh data terurut dan bersih

Insight: Repo mana yang sedang viral (star_gain tinggi)?
""")

window_spec = Window.partitionBy("full_name").orderBy("updated_at_timestamp")

star_velocity_df = silver_df \
    .withColumn("prev_stars", lag("stargazers_count", 1).over(window_spec)) \
    .withColumn("star_gain",
        coalesce(col("stargazers_count") - col("prev_stars"), lit(0))) \
    .filter(col("prev_stars").isNotNull())

gold_star_velocity = star_velocity_df \
    .groupBy("full_name", "language_lower") \
    .agg(
        spark_max("stargazers_count").alias("current_stars"),
        spark_sum("star_gain").alias("total_star_gain"),
        avg("star_gain").alias("avg_star_gain_per_update"),
        count("*").alias("observation_count")
    ) \
    .orderBy(desc("total_star_gain"))

print("✓ Repo yang paling banyak mendapat bintang baru:")
gold_star_velocity.show(15, truncate=False)

# ============================================================================
# GOLD TABLE 4: Emerging Topics — Enhanced (Cross-Source + Word Analysis)
# ============================================================================

print("\n" + "="*80)
print("GOLD TABLE 4: Emerging Topics - Kata Kunci Trending (ENHANCED)")
print("="*80)
print("""
Enhanced Analysis: Kata kunci paling sering muncul di deskripsi repo
dari kedua sumber (API + RSS), filter kata pendek < 4 huruf.

Mengapa tidak bisa di ETS?
- ETS membaca JSON mentah dengan schema tidak konsisten
- Silver sudah unified: API + RSS dalam satu tabel bersih
- Bisa langsung split/explode description tanpa preprocessing manual

Insight: Tema/topik apa yang paling banyak diminati developer?
""")

# Ekstrak kata dari deskripsi
gold_emerging_topics = silver_df \
    .filter(col("description").isNotNull()) \
    .withColumn("word",
        explode(
            split(
                lower(regexp_replace(col("description"), "[^a-zA-Z0-9 ]", "")),
                " "
            )
        )
    ) \
    .filter(col("word").rlike("^[a-z]{4,}")) \
    .filter(~col("word").isin(
        "with", "that", "this", "from", "have", "been",
        "your", "will", "more", "some", "very", "also",
        "into", "than", "them", "then", "they", "when",
        "each", "made", "most", "over", "such", "used",
        "using", "based", "make", "open", "data", "code",
        "tool", "fast", "easy", "simple", "build", "support"
    )) \
    .groupBy("word", "_source") \
    .agg(count("*").alias("frequency")) \
    .orderBy(desc("frequency"))

print("✓ Top 20 kata kunci trending di deskripsi repo:")
gold_emerging_topics.show(20, truncate=False)

print("\n✓ Breakdown per sumber (API vs RSS):")
gold_emerging_topics.groupBy("_source") \
    .agg(spark_sum("frequency").alias("total_words")) \
    .show()

# ============================================================================
# STEP 2: Save All Gold Tables to HDFS
# ============================================================================

print("\n" + "="*80)
print("GOLD LAYER: STEP 2 - Save All Tables to Delta Lake (HDFS)")
print("="*80)

gold_base = "hdfs://namenode:8020/lakehouse/gold"

tables_to_save = {
    "language_dist": gold_language_dist,
    "top_repos": gold_top_repos,
    "star_velocity": gold_star_velocity,
    "emerging_topics": gold_emerging_topics
}

for table_name, table_df in tables_to_save.items():
    table_path = f"{gold_base}/{table_name}"
    try:
        table_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(table_path)
        print(f"✓ Saved gold/{table_name} → {table_path}")
    except Exception as e:
        print(f"❌ Error saving {table_name}: {e}")

# ============================================================================
# STEP 3: Time Travel Demo 
# ============================================================================

print("\n" + "="*80)
print("GOLD LAYER: STEP 3 - Time Travel Demonstration")
print("="*80)
print("""
Delta Lake Time Travel: kemampuan query data di versi sebelumnya.
Ini tidak bisa dilakukan dengan HDFS/JSON biasa di ETS.
""")

from delta.tables import DeltaTable

silver_delta = DeltaTable.forPath(spark, silver_path)

print("=== History Tabel Silver ===")
silver_delta.history().select("version", "timestamp", "operation").show()

# Lakukan update kecil untuk demonstrasi versioning
print("\n=== Simulasi Update: Isi language null → 'Unknown' ===")
silver_delta.update(
    condition="language IS NULL",
    set={"language": "'Unknown'"}
)

print("\n=== Data SEKARANG (setelah update) ===")
spark.read.format("delta").load(silver_path) \
    .groupBy("language").count() \
    .filter(col("language") == "Unknown").show()

print("\n=== Data VERSI 0 (sebelum update) ===")
spark.read.format("delta").option("versionAsOf", 0).load(silver_path) \
    .groupBy("language").count() \
    .filter(col("language").isNull() | (col("language") == "Unknown")).show()

print("\n✓ Time Travel berhasil! Bisa akses data versi lama kapan saja.")

# ============================================================================
# STEP 4: Verification
# ============================================================================

print("\n" + "="*80)
print("GOLD LAYER: STEP 4 - Final Verification")
print("="*80)

print("\n📊 GOLD LAYER SUMMARY:")
for table_name in tables_to_save.keys():
    table_path = f"{gold_base}/{table_name}"
    try:
        verified = spark.read.format("delta").load(table_path)
        print(f"  ✓ gold/{table_name}: {verified.count()} records")
    except Exception as e:
        print(f"  ❌ gold/{table_name}: {e}")

silver_df.unpersist()

print("\n" + "="*80)
print("✅ GOLD LAYER COMPLETE!")
print("="*80 + "\n")

spark.stop()