# 00_setup.md — Panduan Menjalankan Data Lakehouse Pipeline
**Topik 7: GitTrend | Kelompok 7**

---

## Prasyarat

Sebelum menjalankan pipeline Lakehouse, pastikan semua komponen ETS sudah berjalan:

| Komponen | Status yang Dibutuhkan |
|----------|----------------------|
| Docker Desktop | Running |
| `hadoop-namenode` | Up |
| `hadoop-datanode` | Up (port 9864 & 9866 ter-expose) |
| `kafka-broker` | Up |
| Producer API & RSS | Sudah pernah jalan & ada data di HDFS |
| Python venv | Aktif |

---

## Struktur Folder

```
kelompok-7-ets-bigdata/
├── producer_api.py
├── producer_rss.py
├── consumer_to_hdfs.py
├── dashboard/
├── spark/
└── lakehouse/
    ├── 00_setup.md         ← Panduan ini
    ├── 01_bronze.py        ← Ingest HDFS → Bronze Delta
    ├── 02_silver.py        ← Cleaning → Silver Delta
    ├── 03_gold.py          ← Agregasi → Gold Delta
    └── README_lakehouse.md ← Dokumentasi & refleksi
```

---

## Konfigurasi Docker Compose (Penting!)

Pastikan `docker-compose.yml` sudah memiliki port `9866` di service `datanode`:

```yaml
datanode:
  image: apache/hadoop:3
  ports:
    - "9864:9864"
    - "9866:9866"   # ← Wajib ada untuk koneksi Spark dari Windows
```

Dan `hadoop.env` sudah berisi konfigurasi hostname DataNode:

```dotenv
HDFS-SITE.XML_dfs.datanode.hostname=localhost
HDFS-SITE.XML_dfs.client.use.datanode.hostname=true
```

Jika belum, tambahkan lalu jalankan:

```powershell
docker-compose up -d --force-recreate namenode datanode
```

---

## Instalasi Dependensi Python

Aktifkan virtual environment terlebih dahulu:

```powershell
cd "D:/Semester 4/Big Data/kelompok-7-ets-bigdata"
.\venv\Scripts\Activate.ps1
```

Install library yang dibutuhkan:

```powershell
pip install pyspark delta-spark
```

> Delta Spark 3.1.0 kompatibel dengan PySpark 3.5.x. Versi ini otomatis di-download saat script pertama kali dijalankan via `configure_spark_with_delta_pip`.

---

## Persiapan HDFS

### 1. Pastikan data ETS sudah ada

```powershell
docker exec -it hadoop-namenode hdfs dfs -ls /data/github/api/
docker exec -it hadoop-namenode hdfs dfs -ls /data/github/rss/
```

Jika folder kosong atau tidak ada, jalankan producer + consumer selama ±10 menit:

```powershell
# Terminal 1
python producer_api.py

# Terminal 2
python producer_rss.py

# Terminal 3
python consumer_to_hdfs.py
```

### 2. Buat folder Lakehouse di HDFS

```powershell
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /lakehouse/bronze
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /lakehouse/silver
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /lakehouse/gold
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /lakehouse/bronze
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /lakehouse/silver
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /lakehouse/gold
```

### 3. Verifikasi struktur HDFS

```powershell
docker exec -it hadoop-namenode hdfs dfs -ls -R /lakehouse/
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/github/
```

---

## Menjalankan Pipeline Lakehouse

Jalankan script secara **berurutan** dari folder root project:

### Step 1 — Bronze Layer

```powershell
python lakehouse/01_bronze.py
```

**Yang dihasilkan:**
- `hdfs://namenode:8020/lakehouse/bronze/github_api` — data API dalam format Delta
- `hdfs://namenode:8020/lakehouse/bronze/github_rss` — data RSS dalam format Delta
- Kolom tambahan: `_ingested_at`, `_source`

**Verifikasi:**
```powershell
docker exec -it hadoop-namenode hdfs dfs -ls /lakehouse/bronze/
```

---

### Step 2 — Silver Layer

```powershell
python lakehouse/02_silver.py
```

**Yang dihasilkan:**
- `hdfs://namenode:8020/lakehouse/silver/github` — data bersih hasil union API + RSS

**Transformasi yang dilakukan:**
1. `dropDuplicates(["full_name"])` — hapus repo yang sama dari kedua sumber
2. Filter `full_name IS NOT NULL` — buang record tidak valid
3. `fillna(0, "stargazers_count")` — isi nilai null dengan 0
4. Cast `stargazers_count` ke integer
5. `fillna("Unknown", "language")` — standarisasi bahasa null
6. Ekstrak `updated_hour` dan `updated_date` dari timestamp
7. Tambah `_quality_flag` dan `_processed_at`

---

### Step 3 — Gold Layer

```powershell
python lakehouse/03_gold.py
```

**Yang dihasilkan (4 tabel Gold):**

| Tabel | Path HDFS | Keterangan |
|-------|-----------|------------|
| `language_dist` | `/lakehouse/gold/language_dist` | Distribusi bahasa pemrograman (Repro ETS) |
| `top_repos` | `/lakehouse/gold/top_repos` | Top 10 repo by bintang (Repro ETS) |
| `star_velocity` | `/lakehouse/gold/star_velocity` | Deteksi repo viral via Window Function (Enhanced) |
| `emerging_topics` | `/lakehouse/gold/emerging_topics` | Kata kunci trending di deskripsi (Enhanced) |

Script ini juga menjalankan **demonstrasi Time Travel** secara otomatis di Step 3.

---

## Verifikasi Akhir

Cek semua tabel Gold sudah tersimpan:

```powershell
docker exec -it hadoop-namenode hdfs dfs -ls -R /lakehouse/gold/
```

Cek jumlah record tiap tabel (jalankan di Python):

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder.appName("Verify") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
    .config("spark.hadoop.dfs.datanode.hostname", "localhost")

spark = configure_spark_with_delta_pip(builder,
    extra_packages=["io.delta:delta-spark_2.12:3.1.0"]).getOrCreate()

for tabel in ["language_dist", "top_repos", "star_velocity", "emerging_topics"]:
    df = spark.read.format("delta").load(f"hdfs://namenode:8020/lakehouse/gold/{tabel}")
    print(f"gold/{tabel}: {df.count()} records")
```

---

## Troubleshooting

### BlockMissingException / Dead nodes
DataNode tidak bisa diakses dari Windows. Pastikan:
1. Port `9866:9866` sudah ada di `docker-compose.yml`
2. `hadoop.env` sudah punya `dfs.datanode.hostname=localhost`
3. Hosts file Windows: `127.0.0.1 namenode` (jalankan CMD sebagai Administrator)

```powershell
# Tambah namenode ke hosts (CMD as Administrator)
echo 127.0.0.1 namenode >> C:\Windows\System32\drivers\etc\hosts
```

### PATH_NOT_FOUND
Folder HDFS belum dibuat. Jalankan perintah `mkdir` di bagian Persiapan HDFS di atas.

### Permission denied
```powershell
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /lakehouse/
```

### NativeIO / winutils error saat save lokal
Jangan gunakan path lokal Windows untuk menyimpan Delta. Selalu gunakan path HDFS (`hdfs://namenode:8020/...`).

---

## Catatan

- Pipeline ini bersifat **additive** — kode ETS lama tidak dimodifikasi
- Delta Lake disimpan di **HDFS** (bukan lokal) karena kendala winutils di Windows
- Star velocity menunjukkan 0 records jika data dikumpulkan dalam waktu singkat (< 1 jam) — ini limitasi data, bukan bug
- Time Travel demo ada di `03_gold.py` Step 3