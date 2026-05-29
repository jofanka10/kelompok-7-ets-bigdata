"""
Helper bersama untuk lakehouse scripts (Bronze/Silver/Gold).
Kompatibel dengan Java 17, data disimpan di HDFS via Spark (hdfs://localhost:8020).

Arsitektur:
  1. Baca raw data dari HDFS via Python hdfs client (HTTP/WebHDFS)
  2. Proses dengan Spark (in-memory transformasi)
  3. Tulis Delta tables ke HDFS via Spark (hdfs://localhost:8020/lakehouse/)
     → Bisa karena Java 17 (UserGroupInformation berjalan normal)
  4. DataNode hostname rewrite: Docker pakai hostname internal (datanode:9866)
     → Spark config dfs.client.use.datanode.hostname=true + dfs.datanode.hostname=localhost
"""

import os
import glob
import json
from urllib.parse import urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from hdfs import InsecureClient
from pyspark.sql import SparkSession

# ============================================================
# PATH KONFIGURASI
# ============================================================
HDFS_URL       = "http://localhost:9870"   # WebHDFS (Python client)
HDFS_RPC       = "hdfs://localhost:8020"   # HDFS RPC (Spark read/write)
HDFS_RAW_API   = "/data/github/api/"
HDFS_RAW_RSS   = "/data/github/rss/"

# Lakehouse paths — semua di HDFS
BRONZE_API_PATH = f"{HDFS_RPC}/lakehouse/bronze/github_api"
BRONZE_RSS_PATH = f"{HDFS_RPC}/lakehouse/bronze/github_rss"
SILVER_PATH     = f"{HDFS_RPC}/lakehouse/silver/github"
GOLD_BASE_PATH  = f"{HDFS_RPC}/lakehouse/gold"

# Tetap ada LOCAL_LAKEHOUSE untuk fallback mode JSON (Java 18+)
PROJECT_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL_LAKEHOUSE = os.path.join(PROJECT_ROOT, "lakehouse_data")

# Auto-detect Java version → pilih format storage yang kompatibel
# Java ≤ 17 : "delta"  (Delta Lake penuh, Time Travel, ACID)
# Java 18+  : "json"   (fallback, Hadoop FileSystem tidak kompatibel)
def _detect_storage_format():
    import subprocess
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        output = result.stderr or result.stdout
        # Contoh: 'java version "17.0.x"' atau 'openjdk version "17.x"'
        import re
        m = re.search(r'"(\d+)', output)
        if m:
            major = int(m.group(1))
            if major <= 17:
                print(f"[SETUP] Java {major} terdeteksi → format: delta ✅")
                return "delta"
            else:
                print(f"[SETUP] Java {major} terdeteksi → format: json (Delta butuh Java ≤ 17)")
                return "json"
    except Exception:
        pass
    print("[SETUP] Tidak bisa deteksi Java → default: json")
    return "json"

STORAGE_FORMAT = _detect_storage_format()

# ============================================================
# HDFS CLIENT (HTTP-based, bypass Hadoop Java FileSystem)
# ============================================================
class _DockerHostRewriteAdapter(HTTPAdapter):
    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname not in ('localhost', '127.0.0.1', None):
            port = parsed.port
            new_netloc = f'localhost:{port}' if port else 'localhost'
            request.url = urlunparse(parsed._replace(netloc=new_netloc))
        return super().send(request, **kwargs)

def make_hdfs_client():
    session = requests.Session()
    session.mount('http://', _DockerHostRewriteAdapter())
    return InsecureClient(HDFS_URL, user='root', session=session)

def read_hdfs_dir(hdfs_client, spark, hdfs_dir):
    """
    Baca semua file JSON dari direktori HDFS via Python client (HTTP).
    Bypass Hadoop Java FileSystem → aman di Java 23.
    Return Spark DataFrame, atau None jika kosong.
    """
    try:
        files = hdfs_client.list(hdfs_dir, status=False)
    except Exception as e:
        raise RuntimeError(f"Tidak bisa list {hdfs_dir}: {e}")

    all_records = []
    for fname in sorted(files):
        if not fname.endswith('.json'):
            continue
        fpath = f"{hdfs_dir}{fname}"
        try:
            with hdfs_client.read(fpath, encoding='utf-8') as reader:
                parsed = json.loads(reader.read())
            if isinstance(parsed, list):
                all_records.extend(parsed)
            elif isinstance(parsed, dict):
                all_records.append(parsed)
        except Exception as e:
            print(f"  [WARNING] Gagal baca {fpath}: {e}")

    if not all_records:
        return None
    return spark.createDataFrame(all_records)

# ============================================================
# SPARK SESSION dengan Delta (Java 23 compatible)
# ============================================================
def _load_delta_jars_to_classpath():
    """
    Cari Delta JARs dari Ivy cache dan inject ke SPARK_CLASSPATH.
    Ini bypass DependencyUtils.resolveGlobPaths yang crash di Java 23.
    """
    ivy_cache = os.path.expanduser("~/.ivy2.5.2/jars")
    patterns = [
        "io.delta_delta-spark_4.1_2.13-*.jar",
        "io.delta_delta-storage-*.jar",
        "io.delta_delta-kernel-api-*.jar",
        "io.delta_delta-kernel-defaults-*.jar",
    ]
    jars = []
    for p in patterns:
        jars.extend(glob.glob(os.path.join(ivy_cache, p)))

    if not jars:
        print("[WARNING] Delta JARs tidak ditemukan di Ivy cache.")
        print("          Jalankan 'python dashboard/app.py' sekali dulu untuk download JAR-nya.")
        return False

    existing_cp = os.environ.get("SPARK_CLASSPATH", "")
    new_cp = ":".join(jars)
    os.environ["SPARK_CLASSPATH"] = f"{existing_cp}:{new_cp}" if existing_cp else new_cp
    print(f"[SETUP] {len(jars)} Delta JAR dimuat via SPARK_CLASSPATH")
    return True

def get_spark_session(app_name):
    """
    Buat SparkSession dengan Delta Lake + HDFS support.

    - Java 17 : tulis Delta ke HDFS (hdfs://localhost:8020/lakehouse/)
    - Java 18+: fallback ke JSON lokal (Hadoop tidak kompatibel)

    DataNode hostname rewrite:
    Docker DataNode menggunakan hostname internal 'datanode'.
    Config dfs.client.use.datanode.hostname=true membuat client
    minta hostname dari DataNode, lalu kita override ke localhost.
    """
    _load_delta_jars_to_classpath()

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.defaultFS", HDFS_RPC) \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.hadoop.dfs.datanode.hostname", "localhost") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    return spark
