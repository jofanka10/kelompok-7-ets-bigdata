import json
import time
import signal
import sys
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc, substring, split, lower
from hdfs import InsecureClient
import requests
from requests.adapters import HTTPAdapter

# ============================================================
# KONFIGURASI
# ============================================================
# Spark baca raw data via WebHDFS — pakai localhost (akses dari luar Docker)
HDFS_RAW_API = "webhdfs://localhost:9870/data/github/api/"
HDFS_RAW_RSS = "webhdfs://localhost:9870/data/github/rss/"
HDFS_CLEAN_DIR = "/data/clean/"

HDFS_URL = "http://localhost:9870"
ANALYSIS_INTERVAL = 60  # Detik antar siklus analisis

# ============================================================
# GRACEFUL SHUTDOWN (Ctrl+C)
# ============================================================
running = True

def signal_handler(sig, frame):
    global running
    print("\n[CTRL+C] Sinyal diterima. Menghentikan Spark Analyzer...")
    running = False

signal.signal(signal.SIGINT, signal_handler)

# ============================================================
# HDFS CLIENT dengan DataNode hostname rewrite
# WebHDFS me-redirect tulis ke DataNode pakai hostname internal
# Docker (mis. 'datanode:9866') yang tidak bisa diakses dari luar.
# Adapter ini otomatis rewrite hostname tersebut ke 'localhost'.
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

hdfs_client = make_hdfs_client()

def init_hdfs_dirs():
    """Buat direktori /data/clean di HDFS jika belum ada."""
    try:
        status = hdfs_client.status(HDFS_CLEAN_DIR, strict=False)
        if status is None:
            hdfs_client.makedirs(HDFS_CLEAN_DIR, permission=777)
            print(f"[HDFS] Direktori {HDFS_CLEAN_DIR} berhasil dibuat.")
        else:
            print(f"[HDFS] Direktori {HDFS_CLEAN_DIR} sudah ada.")
    except Exception as e:
        print(f"[WARNING] Tidak bisa init direktori HDFS: {e}")

def write_clean_to_hdfs(file_name, data):
    """Menulis data JSON ke HDFS /data/clean/, overwrite jika sudah ada."""
    hdfs_path = f"{HDFS_CLEAN_DIR}{file_name}"
    try:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(json_str)
        print(f"  [HDFS] Berhasil menulis: {hdfs_path}")
    except Exception as e:
        print(f"  [ERROR] Gagal menulis {hdfs_path}: {e}")

# ============================================================
# FUNGSI ANALISIS UTAMA
# ============================================================
def run_analysis(spark):
    """Menjalankan 1 siklus analisis lengkap dan tulis hasilnya ke HDFS."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*60}")
    print(f"[{timestamp}] MEMULAI SIKLUS ANALISIS SPARK")
    print(f"{'='*60}")

    # ----------------------------------------------------------
    # 1. BACA DATA MENTAH DARI HDFS
    # ----------------------------------------------------------
    try:
        df_api = spark.read.option("multiline", "true").json(HDFS_RAW_API) \
            .dropDuplicates(["full_name"])
        total_repos = df_api.count()
        print(f"  Berhasil membaca {total_repos} repositori unik dari HDFS")
    except Exception as e:
        print(f"  [ERROR] Gagal membaca data API: {e}")
        return

    # ----------------------------------------------------------
    # 2. ANALISIS 1: Distribusi Bahasa Pemrograman
    # ----------------------------------------------------------
    print("\n  [ANALISIS 1] Distribusi Bahasa Pemrograman...")
    df_lang = df_api.filter(col("language").isNotNull()) \
        .groupBy("language").count() \
        .orderBy(desc("count"))
    
    lang_data = [{"language": row["language"], "count": row["count"]} 
                 for row in df_lang.collect()]
    
    df_lang.show(10)
    write_clean_to_hdfs("language_dist.json", lang_data)

    # ----------------------------------------------------------
    # 3. ANALISIS 2: Top 10 Repositori Berdasarkan Stars
    # ----------------------------------------------------------
    print("\n  [ANALISIS 2] Top 10 Repositori...")
    df_top = df_api.orderBy(desc("stargazers_count")) \
        .select(
            "full_name",
            "language",
            "stargazers_count",
            "html_url",
            substring(col("description"), 1, 80).alias("short_description")
        ).limit(10)
    
    top_data = [row.asDict() for row in df_top.collect()]
    
    df_top.show(10, truncate=False)
    write_clean_to_hdfs("top_repos.json", top_data)

    # ----------------------------------------------------------
    # 4. ANALISIS 3: Word Frequency dari Deskripsi
    # ----------------------------------------------------------
    print("\n  [ANALISIS 3] Word Frequency...")
    df_words = df_api.filter(col("description").isNotNull()) \
        .select(explode(split(lower(col("description")), " ")).alias("word")) \
        .filter("length(word) > 3") \
        .groupBy("word").count() \
        .orderBy(desc("count")).limit(15)
    
    word_data = [{"word": row["word"], "count": row["count"]} 
                 for row in df_words.collect()]
    
    df_words.show(15)
    write_clean_to_hdfs("word_freq.json", word_data)

    # ----------------------------------------------------------
    # 5. BACA & SIMPAN DATA RSS TERBARU
    # ----------------------------------------------------------
    print("\n  [RSS] Memproses berita terbaru...")
    try:
        df_rss = spark.read.option("multiline", "true").json(HDFS_RAW_RSS) \
            .orderBy(desc("timestamp")).limit(10)
        
        rss_data = [row.asDict() for row in df_rss.collect()]
        write_clean_to_hdfs("rss_latest.json", rss_data)
        print(f"  Berhasil memproses {len(rss_data)} berita RSS")
    except Exception as e:
        print(f"  [WARNING] Gagal memproses RSS (mungkin belum ada data): {e}")
        rss_data = []

    # ----------------------------------------------------------
    # 6. TULIS SUMMARY / METADATA
    # ----------------------------------------------------------
    highest_stars = top_data[0]["stargazers_count"] if top_data else 0
    top_language = lang_data[0]["language"] if lang_data else "N/A"

    summary = {
        "last_updated": timestamp,
        "total_repos": total_repos,
        "highest_stars": highest_stars,
        "top_language": top_language,
        "total_languages": len(lang_data),
        "total_rss_articles": len(rss_data)
    }
    write_clean_to_hdfs("summary.json", summary)

    print(f"\n  Siklus selesai! Semua data clean telah ditulis ke HDFS {HDFS_CLEAN_DIR}")
    print(f"  Total Repos: {total_repos} | Top Language: {top_language} | Highest Stars: {highest_stars:,}")

# ============================================================
# MAIN: LOOP TERUS-MENERUS
# ============================================================
def main():
    print("=" * 60)
    print("  SPARK ANALYZER — Mode Continuous")
    print("  Tekan Ctrl+C untuk menghentikan")
    print("=" * 60)

    # spark = SparkSession.builder \
    #     .appName("ETS_BigData_Jofanka") \
    #     .getOrCreate()

    jvm_opts = (
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED"
    )

    spark = SparkSession.builder \
        .appName("ETS_BigData_Jofanka") \
        .config("spark.driver.extraJavaOptions", jvm_opts) \
        .config("spark.executor.extraJavaOptions", jvm_opts) \
        .getOrCreate()


    print("[SPARK] Session berhasil dibuat!")
    init_hdfs_dirs()

    try:
        while running:
            run_analysis(spark)

            if not running:
                break

            print(f"\n  Menunggu {ANALYSIS_INTERVAL} detik untuk siklus berikutnya...")
            # Sleep yang bisa di-interrupt
            for _ in range(ANALYSIS_INTERVAL):
                if not running:
                    break
                time.sleep(1)
    finally:
        print("\n[SHUTDOWN] Menghentikan Spark Session...")
        spark.stop()
        print("[SHUTDOWN] Spark Analyzer berhasil dimatikan. Sampai jumpa!")
        sys.exit(0)

if __name__ == "__main__":
    main()