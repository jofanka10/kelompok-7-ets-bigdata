"""
BRONZE LAYER - Ingest Raw Data from HDFS to Lakehouse

Fungsi:
- Baca JSON raw dari HDFS (GitHub API & RSS) via Python hdfs client
- Tambahkan metadata: _ingested_at (timestamp), _source (api/rss)
- Simpan sebagai JSON via Python (kompatibel Java 23)
  → Ganti ke Delta format otomatis jika pakai Java 17 (lihat STORAGE_FORMAT)

Output: ./lakehouse_data/bronze/github_api.json  (atau Delta jika Java 17)
        ./lakehouse_data/bronze/github_rss.json
"""

import sys
import os
import json
from datetime import datetime
sys.path.insert(0, os.path.dirname(__file__))

from _spark_delta_setup import (
    get_spark_session, make_hdfs_client, read_hdfs_dir,
    HDFS_RAW_API, HDFS_RAW_RSS, LOCAL_LAKEHOUSE,
    BRONZE_API_PATH, BRONZE_RSS_PATH, STORAGE_FORMAT
)
from pyspark.sql.functions import current_timestamp, lit

BRONZE_DIR = os.path.join(LOCAL_LAKEHOUSE, "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)

def save(data, name):
    if STORAGE_FORMAT == "delta":
        return None  # handled by caller with spark.write
    path = os.path.join(BRONZE_DIR, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"✓ Saved {name}.json ({len(data)} records)")
    return path

# ============================================================================
spark = get_spark_session("Bronze-GitTrend")
hdfs  = make_hdfs_client()
ts    = datetime.now().isoformat()

# ============================================================================
print("\n" + "="*80)
print("BRONZE LAYER: STEP 1 - Ingest GitHub API Data")
print("="*80)

api_df = None
try:
    api_df = read_hdfs_dir(hdfs, spark, HDFS_RAW_API)
    if api_df is None:
        print("⚠️  Belum ada data API di HDFS — skip.")
    else:
        api_df = api_df.withColumn("_ingested_at", current_timestamp()) \
                       .withColumn("_source", lit("api"))
        print(f"✓ API Data: {api_df.count()} records, metadata added")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "="*80)
print("BRONZE LAYER: STEP 2 - Ingest RSS Data")
print("="*80)

rss_df = None
try:
    rss_df = read_hdfs_dir(hdfs, spark, HDFS_RAW_RSS)
    if rss_df is None:
        print("⚠️  Belum ada data RSS di HDFS — skip.")
    else:
        rss_df = rss_df.withColumn("_ingested_at", current_timestamp()) \
                       .withColumn("_source", lit("rss"))
        print(f"✓ RSS Data: {rss_df.count()} records, metadata added")
except Exception as e:
    print(f"❌ Error: {e}")

print("\n" + "="*80)
print(f"BRONZE LAYER: STEP 3 - Save ({STORAGE_FORMAT.upper()} format)")
print("="*80)

if STORAGE_FORMAT == "delta":
    if api_df:
        try:
            api_df.write.format("delta").mode("overwrite").save(BRONZE_API_PATH)
            print(f"✓ API Bronze (Delta): {BRONZE_API_PATH}")
        except Exception as e:
            print(f"❌ {e}"); api_df = None
    if rss_df:
        try:
            rss_df.write.format("delta").mode("overwrite").save(BRONZE_RSS_PATH)
            print(f"✓ RSS Bronze (Delta): {BRONZE_RSS_PATH}")
        except Exception as e:
            print(f"❌ {e}"); rss_df = None
else:
    if api_df:
        rows = [dict(r.asDict(), _ingested_at=ts) for r in api_df.collect()]
        save(rows, "github_api")
    if rss_df:
        rows = [dict(r.asDict(), _ingested_at=ts) for r in rss_df.collect()]
        save(rows, "github_rss")

print("\n" + "="*80)
print("BRONZE LAYER: STEP 4 - Verification")
print("="*80)
if STORAGE_FORMAT == "delta":
    if api_df: print(f"✓ API: {spark.read.format('delta').load(BRONZE_API_PATH).count()} records")
    if rss_df: print(f"✓ RSS: {spark.read.format('delta').load(BRONZE_RSS_PATH).count()} records")
else:
    for name in ["github_api", "github_rss"]:
        p = os.path.join(BRONZE_DIR, f"{name}.json")
        if os.path.exists(p):
            print(f"✓ {name}.json: {len(json.load(open(p)))} records")

print("\n" + "="*80)
print("✅ BRONZE LAYER COMPLETE!")
print("="*80 + "\n")
spark.stop()