import json
from flask import Flask, render_template, jsonify
from hdfs import InsecureClient

# ============================================================
# KONFIGURASI
# ============================================================
app = Flask(__name__)

# Pakai localhost karena dashboard dijalankan dari luar Docker
HDFS_URL = "http://localhost:9870"
HDFS_CLEAN_DIR = "/data/clean/"

# ============================================================
# FUNGSI UTILITAS: BACA JSON DARI HDFS
# ============================================================
def get_hdfs_client():
    """Buat koneksi HDFS baru per request agar tidak crash saat startup."""
    return InsecureClient(HDFS_URL, user='root')

def read_clean_from_hdfs(file_name):
    """Membaca file JSON clean dari HDFS. Return None jika gagal."""
    hdfs_path = f"{HDFS_CLEAN_DIR}{file_name}"
    try:
        client = get_hdfs_client()
        with client.read(hdfs_path, encoding='utf-8') as reader:
            return json.load(reader)
    except Exception as e:
        print(f"  [WARNING] Gagal membaca {hdfs_path}: {e}")
        return None

# ============================================================
# ROUTES
# ============================================================
@app.route('/')
def index():
    """Halaman utama dashboard — baca semua data clean dari HDFS."""
    summary = read_clean_from_hdfs("summary.json") or {
        "last_updated": "Belum ada data — Jalankan spark_analyzer.py terlebih dahulu",
        "total_repos": 0,
        "highest_stars": 0,
        "top_language": "N/A",
        "total_languages": 0,
        "total_rss_articles": 0
    }

    top_repos = read_clean_from_hdfs("top_repos.json") or []
    lang_dist  = read_clean_from_hdfs("language_dist.json") or []
    word_freq  = read_clean_from_hdfs("word_freq.json") or []
    rss_latest = read_clean_from_hdfs("rss_latest.json") or []

    return render_template('index.html',
        summary=summary,
        top_repos=top_repos,
        lang_dist=lang_dist,
        word_freq=word_freq,
        rss_latest=rss_latest,
        lang_dist_json=json.dumps(lang_dist),
        word_freq_json=json.dumps(word_freq)
    )

@app.route('/api/data')
def api_data():
    """API endpoint — kembalikan semua data clean sebagai JSON."""
    return jsonify({
        "summary":    read_clean_from_hdfs("summary.json") or {},
        "top_repos":  read_clean_from_hdfs("top_repos.json") or [],
        "lang_dist":  read_clean_from_hdfs("language_dist.json") or [],
        "word_freq":  read_clean_from_hdfs("word_freq.json") or [],
        "rss_latest": read_clean_from_hdfs("rss_latest.json") or []
    })

# ============================================================
# MAIN
# ============================================================
if __name__ == '__main__':
    print()
    print("=" * 55)
    print("   GitHub Trending Dashboard — Kelompok 7")
    print("=" * 55)
    print(f"   Sumber data : HDFS ({HDFS_URL})")
    print(f"   Dashboard   : http://localhost:5000")
    print(f"   API data    : http://localhost:5000/api/data")
    print("=" * 55)
    print("   Tekan Ctrl+C untuk menghentikan server")
    print("=" * 55)
    print()
    app.run(host='0.0.0.0', port=5000, debug=False)