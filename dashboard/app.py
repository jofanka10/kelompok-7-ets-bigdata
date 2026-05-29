"""
GitHub Trending Dashboard — Kelompok 7

Sumber data (prioritas):
  1. Gold Layer (Delta Lake via Spark) — hasil Bronze → Silver → Gold pipeline
     Path: hdfs://localhost:8020/lakehouse/gold/
  2. Fallback ke /data/clean/ (hasil spark_analyzer.py via WebHDFS)

Dengan ini dashboard membuktikan bahwa pipeline Medallion Lakehouse bekerja end-to-end.
"""

import json
import sys
import os
from datetime import datetime
from urllib.parse import urlparse, urlunparse

from flask import Flask, render_template, jsonify
import requests
from requests.adapters import HTTPAdapter
from hdfs import InsecureClient

# ============================================================
# KONFIGURASI
# ============================================================
app = Flask(__name__)

HDFS_URL   = "http://localhost:9870"
HDFS_CLEAN = "/data/clean/"

# Path Gold layer di HDFS (RPC untuk Spark)
HDFS_RPC       = "hdfs://localhost:8020"
GOLD_BASE_PATH = f"{HDFS_RPC}/lakehouse/gold"

# ============================================================
# HDFS CLIENT (HTTP-based, tanpa Hadoop Java)
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

def read_clean(file_name):
    """Baca file JSON dari /data/clean/ di HDFS. Return [] jika tidak ada."""
    hdfs_path = f"{HDFS_CLEAN}{file_name}"
    try:
        with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
            return json.loads(reader.read())
    except Exception as e:
        print(f"  [WARNING] Tidak bisa baca {hdfs_path}: {e}")
        return []

# ============================================================
# GOLD LAYER READER (Spark + Delta Lake)
# ============================================================
def _make_spark():
    """Buat SparkSession ringan untuk baca Gold layer Delta."""
    try:
        # Tambahkan path lakehouse ke sys.path untuk import _spark_delta_setup
        lakehouse_dir = os.path.join(os.path.dirname(__file__), '..', 'lakehouse')
        lakehouse_dir = os.path.abspath(lakehouse_dir)
        if lakehouse_dir not in sys.path:
            sys.path.insert(0, lakehouse_dir)

        from _spark_delta_setup import get_spark_session
        return get_spark_session("Dashboard-Gold-Reader")
    except Exception as e:
        print(f"  [WARNING] Tidak bisa buat Spark session: {e}")
        return None

def read_gold_table(spark, table_name):
    """Baca satu tabel Gold dari Delta Lake di HDFS. Return list of dict."""
    path = f"{GOLD_BASE_PATH}/{table_name}"
    try:
        df = spark.read.format("delta").load(path)
        rows = [row.asDict() for row in df.collect()]
        print(f"  [GOLD] OK {table_name}: {len(rows)} records (dari Delta Lake)")
        return rows
    except Exception as e:
        print(f"  [GOLD] FAIL {table_name}: {e}")
        return None

def load_from_gold():
    """
    Coba baca semua data dari Gold Layer.
    Return (data_dict, source_label) atau (None, None) jika gagal.
    """
    print("  [GOLD] Mencoba membaca dari Gold Layer (Delta Lake)...")
    spark = _make_spark()
    if spark is None:
        return None, None

    try:
        lang_rows      = read_gold_table(spark, "language_dist")
        top_rows       = read_gold_table(spark, "top_repos")
        velocity_rows  = read_gold_table(spark, "star_velocity")
        topics_rows    = read_gold_table(spark, "emerging_topics")

        # Jika semua tabel kosong/gagal, kembalikan None
        if not any([lang_rows, top_rows, topics_rows]):
            print("  [GOLD] Semua tabel Gold kosong - fallback ke /data/clean/")
            spark.stop()
            return None, None

        # ── Normalisasi language_dist ──────────────────────────────────
        # Gold: {language_lower, total_repos, avg_stars, max_stars}
        # Template butuh: {language, count}
        language_dist = []
        if lang_rows:
            for r in lang_rows:
                language_dist.append({
                    "language": (r.get("language_lower") or r.get("language") or "Unknown").title(),
                    "count":    int(r.get("total_repos", 0)),
                    "avg_stars": round(float(r.get("avg_stars", 0) or 0), 1),
                    "max_stars": int(r.get("max_stars", 0) or 0),
                })

        # ── Normalisasi top_repos ──────────────────────────────────────
        # Gold: {full_name, stargazers_count, language, html_url, description, _source}
        # Template butuh: {full_name, stargazers_count, language, html_url, short_description}
        top_repos = []
        if top_rows:
            for r in top_rows:
                desc = r.get("description") or ""
                top_repos.append({
                    "full_name":        r.get("full_name", ""),
                    "stargazers_count": int(r.get("stargazers_count", 0) or 0),
                    "language":         r.get("language") or "",
                    "html_url":         r.get("html_url", "#"),
                    "short_description": desc[:80] if desc else "—",
                    "_source":          r.get("_source", ""),
                })

        # ── Normalisasi emerging_topics ────────────────────────────────
        # Gold: {word, _source, frequency}
        # Template butuh: {word, frequency}
        emerging_topics = []
        seen_words = set()
        if topics_rows:
            for r in sorted(topics_rows, key=lambda x: x.get("frequency", 0), reverse=True):
                word = r.get("word", "")
                if word and word not in seen_words:
                    seen_words.add(word)
                    emerging_topics.append({
                        "word":      word,
                        "frequency": int(r.get("frequency", 0)),
                    })
                if len(emerging_topics) >= 15:
                    break

        # ── Normalisasi star_velocity ──────────────────────────────────
        star_velocity = []
        if velocity_rows:
            for r in velocity_rows[:10]:
                star_velocity.append({
                    "full_name":    r.get("full_name", ""),
                    "language":     (r.get("language_lower") or "Unknown").title(),
                    "current_stars": int(r.get("current_stars", 0) or 0),
                    "total_gain":   int(r.get("total_star_gain", 0) or 0),
                })

        # ── Summary ────────────────────────────────────────────────────
        summary = {
            "last_updated":       datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "total_repos":        sum(r["count"] for r in language_dist),
            "highest_stars":      max((r["stargazers_count"] for r in top_repos), default=0),
            "top_language":       language_dist[0]["language"] if language_dist else "N/A",
            "total_languages":    len(language_dist),
            "total_rss_articles": 0,
            "data_source":        "Gold Layer (Delta Lake)",
        }

        spark.stop()
        return {
            "language_dist":  language_dist,
            "top_repos":      top_repos,
            "emerging_topics": emerging_topics,
            "star_velocity":  star_velocity,
            "summary":        summary,
        }, "gold"

    except Exception as e:
        print(f"  [GOLD] Error saat baca Gold Layer: {e}")
        try:
            spark.stop()
        except Exception:
            pass
        return None, None

def load_from_clean():
    """Fallback: baca dari /data/clean/ (output spark_analyzer.py)."""
    print("  [FALLBACK] Membaca dari /data/clean/ (spark_analyzer output)...")

    language_dist_raw = read_clean("language_dist.json")
    top_repos_raw     = read_clean("top_repos.json")
    word_freq_raw     = read_clean("word_freq.json")
    rss_latest        = read_clean("rss_latest.json")
    summary_hdfs      = read_clean("summary.json")

    emerging_topics = [
        {"word": d.get("word", ""), "frequency": d.get("count", 0)}
        for d in word_freq_raw
    ]

    # Pastikan top_repos punya field short_description
    top_repos = []
    for r in top_repos_raw:
        desc = r.get("description") or r.get("short_description") or ""
        r["short_description"] = desc[:80] if desc else "—"
        top_repos.append(r)

    summary = summary_hdfs if summary_hdfs else {
        "last_updated":    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "total_repos":     len(top_repos),
        "highest_stars":   max((r.get("stargazers_count", 0) for r in top_repos), default=0),
        "top_language":    language_dist_raw[0].get("language", "N/A") if language_dist_raw else "N/A",
        "total_languages": len(language_dist_raw),
        "total_rss_articles": len(rss_latest),
    }
    summary["data_source"] = "Spark Analyzer (/data/clean/)"

    return {
        "language_dist":   language_dist_raw,
        "top_repos":       top_repos,
        "emerging_topics": emerging_topics,
        "star_velocity":   [],
        "rss_latest":      rss_latest,
        "summary":         summary,
    }, "clean"

# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index():
    """Halaman utama — coba Gold Layer dulu, fallback ke /data/clean/."""
    print("\n[DASHBOARD] Loading data...")

    data, source = load_from_gold()
    if data is None:
        data, source = load_from_clean()

    language_dist   = data.get("language_dist", [])
    top_repos       = data.get("top_repos", [])
    emerging_topics = data.get("emerging_topics", [])
    star_velocity   = data.get("star_velocity", [])
    rss_latest      = data.get("rss_latest", [])
    summary         = data.get("summary", {})

    # lang_dist_json untuk Chart.js langChart
    # Template menggunakan d.language dan d.count
    lang_dist_json = json.dumps(language_dist, ensure_ascii=False, default=str)
    # word_freq_json untuk Chart.js wordChart  
    # Template menggunakan d.word dan d.frequency
    word_freq_json = json.dumps(emerging_topics, ensure_ascii=False, default=str)
    top_repos_json = json.dumps(top_repos, ensure_ascii=False, default=str)

    print(f"  [DASHBOARD] Sumber data  : {summary.get('data_source', source)}")
    print(f"  [DASHBOARD] language_dist: {len(language_dist)} items")
    print(f"  [DASHBOARD] top_repos    : {len(top_repos)} items")
    print(f"  [DASHBOARD] topics       : {len(emerging_topics)} items")
    print(f"  [DASHBOARD] star_velocity: {len(star_velocity)} items")

    return render_template('index.html',
        summary         = summary,
        top_repos       = top_repos,
        lang_dist       = language_dist,
        star_velocity   = star_velocity,
        emerging_topics = emerging_topics,
        lang_dist_json  = lang_dist_json,
        word_freq_json  = word_freq_json,
        top_repos_json  = top_repos_json,
        rss_latest      = rss_latest,
        data_source     = source,
    )


@app.route('/api/data')
def api_data():
    """API endpoint — kembalikan semua data sebagai JSON (Gold dulu, fallback clean)."""
    data, source = load_from_gold()
    if data is None:
        data, source = load_from_clean()

    return jsonify({
        "source":          source,
        "language_dist":   data.get("language_dist", []),
        "top_repos":       data.get("top_repos", []),
        "emerging_topics": data.get("emerging_topics", []),
        "star_velocity":   data.get("star_velocity", []),
        "rss_latest":      data.get("rss_latest", []),
        "summary":         data.get("summary", {}),
    })


@app.route('/api/source')
def api_source():
    """Cek sumber data aktif: 'gold' atau 'clean'."""
    spark = _make_spark()
    if spark is None:
        return jsonify({"source": "clean", "gold_available": False})
    try:
        from _spark_delta_setup import GOLD_BASE_PATH as _gold_path
        df = spark.read.format("delta").load(f"{_gold_path}/top_repos")
        count = df.count()
        spark.stop()
        return jsonify({"source": "gold", "gold_available": True, "top_repos_count": count})
    except Exception as e:
        try:
            spark.stop()
        except Exception:
            pass
        return jsonify({"source": "clean", "gold_available": False, "reason": str(e)})


# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print()
    print("=" * 60)
    print("   GitHub Trending Dashboard — Kelompok 7")
    print("=" * 60)
    print("   Sumber data  : Gold Layer (Delta Lake) → fallback /data/clean/")
    print("   Dashboard    : http://localhost:5000")
    print("   API data     : http://localhost:5000/api/data")
    print("   Cek sumber   : http://localhost:5000/api/source")
    print("=" * 60)
    print("   Tekan Ctrl+C untuk menghentikan server")
    print("=" * 60)
    print()
    app.run(host='0.0.0.0', port=5000, debug=False)
