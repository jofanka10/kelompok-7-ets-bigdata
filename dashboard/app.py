import json
from flask import Flask, render_template, jsonify
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


# KONFIGURASI

app = Flask(__name__)

# HDFS configuration
HDFS_NAMENODE = "hdfs://namenode:8020"
GOLD_BASE_PATH = f"{HDFS_NAMENODE}/lakehouse/gold"  # Match 03_gold.py save path

# Initialize Spark Session (single instance, reused)
def get_spark_session():
    """Get or create Spark session for reading Gold tables."""
    builder = SparkSession.builder \
        .appName("Dashboard-GitTrend") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .config("spark.hadoop.dfs.datanode.hostname", "localhost")
    
    spark = configure_spark_with_delta_pip(builder, extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# FUNGSI UTILITAS: BACA DARI GOLD DELTA TABLES

spark = get_spark_session()

def read_gold_table(table_name):
    """Membaca Delta table dari Gold layer. Return empty list/dict jika gagal."""
    try:
        table_path = f"{GOLD_BASE_PATH}/{table_name}"
        print(f"  [INFO] Reading Gold table: {table_name}...")
        df = spark.read.format("delta").load(table_path)
        
        # Convert to list of dicts using Pandas (no Python invocation needed)
        data = df.toPandas().to_dict('records')
        
        # Transform field names untuk dashboard compatibility
        if table_name == "language_dist":
            data = [{"language": d.get("language_lower", "Unknown"), "count": int(d.get("total_repos", 0))} for d in data]
        elif table_name == "emerging_topics":
            data = [{"word": d.get("word", ""), "frequency": int(d.get("frequency", 0))} for d in data]
        
        print(f"    ✓ Loaded {len(data)} rows from {table_name}")
        return data
    except Exception as e:
        print(f"  [WARNING] Gagal membaca Gold table {table_name}: {e}")
        return []


# ROUTES

@app.route('/')
def index():
    """Halaman utama dashboard — baca dari Gold layer."""
    print("\n[DASHBOARD] Loading data from Gold layer...")
    
    # Baca semua Gold tables
    language_dist = read_gold_table("language_dist")
    top_repos = read_gold_table("top_repos")
    star_velocity = read_gold_table("star_velocity")
    emerging_topics = read_gold_table("emerging_topics")
    
    # Build summary dari data
    summary = {
        "last_updated": "Sekarang",
        "total_repos": len(top_repos) + len(emerging_topics),
        "highest_stars": max([r.get('stargazers_count', 0) for r in top_repos], default=0),
        "top_language": language_dist[0].get('language', 'N/A') if language_dist else 'N/A',
        "total_languages": len(language_dist),
        "trending_count": len(star_velocity)
    }
    
    # Debug logging
    print(f"\n[DASHBOARD] Data loaded from Gold layer:")
    print(f"  - Language dist: {len(language_dist)} items")
    print(f"  - Top repos: {len(top_repos)} items")
    print(f"  - Star velocity: {len(star_velocity)} items")
    print(f"  - Emerging topics: {len(emerging_topics)} items")

    lang_dist_json = json.dumps(language_dist, ensure_ascii=False, default=str)
    word_freq_json = json.dumps(emerging_topics, ensure_ascii=False, default=str)
    top_repos_json = json.dumps(top_repos, ensure_ascii=False, default=str)

    return render_template('index.html',
        summary=summary,
        top_repos=top_repos,
        lang_dist=language_dist,
        star_velocity=star_velocity,
        emerging_topics=emerging_topics,
        lang_dist_json=lang_dist_json,
        word_freq_json=word_freq_json,
        top_repos_json=top_repos_json,
        rss_latest=[]  # No RSS in Gold layer yet
    )

@app.route('/api/data')
def api_data():
    """API endpoint — kembalikan semua Gold data sebagai JSON."""
    print("\n[API] Serving Gold data...")
    
    language_dist = read_gold_table("language_dist")
    top_repos = read_gold_table("top_repos")
    star_velocity = read_gold_table("star_velocity")
    emerging_topics = read_gold_table("emerging_topics")
    
    return jsonify({
        "language_dist": language_dist,
        "top_repos": top_repos,
        "star_velocity": star_velocity,
        "emerging_topics": emerging_topics
    })


# MAIN

if __name__ == '__main__':
    print()
    print("=" * 55)
    print("   GitHub Trending Dashboard — Kelompok 7")
    print("=" * 55)
    print(f"   Sumber data : Gold Layer @ {HDFS_NAMENODE}")
    print(f"   Dashboard   : http://localhost:5000")
    print(f"   API data    : http://localhost:5000/api/data")
    print("=" * 55)
    print("   Tekan Ctrl+C untuk menghentikan server")
    print("=" * 55)
    print()
    app.run(host='0.0.0.0', port=5000, debug=False)