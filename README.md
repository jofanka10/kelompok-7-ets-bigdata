# **ETS Big Data - GitHub Trending Analytics Pipeline**
Proyek ini mengimplementasikan pipeline data lengkap untuk menganalisis repositori GitHub yang sedang tren. Arsitektur sistem mencakup pengambilan data melalui GitHub API, pengiriman pesan via Kafka, penyimpanan terdistribusi di HDFS, pemrosesan data secara kontinu menggunakan Apache Spark, dan visualisasi interaktif dengan Flask dan Chart.js.

---

## **Persiapan Lingkungan (Setup)**
### **1. Kloning Repository & Virtual Environment**
```bash
# Clone repository
git clone https://github.com/jofanka10/kelompok-7-ets-bigdata.git
cd kelompok-7-ets-bigdata

# Buat virtual environment
python -m venv venv

# Aktifkan venv
# Mac/Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### **2. Konfigurasi Jaringan (Hosts File)**
Agar sistem dapat mengenali node Hadoop di dalam Docker, tambahkan baris berikut pada file hosts sistem operasi Anda:

File Path:

Windows: `C:\Windows\System32\drivers\etc\hosts` (Buka dengan Notepad as Administrator)

Mac/Linux: `/etc/hosts` (Gunakan `sudo nano /etc/hosts`)

Tambahkan baris ini:

```
127.0.0.1 namenode datanode
```

---

## **Langkah-Langkah Menjalankan Sistem**

### **Langkah 1: Menyalakan Infrastruktur Docker**
Pastikan Docker Desktop sudah aktif, lalu jalankan perintah berikut untuk menyalakan Kafka dan Hadoop cluster:

```
docker-compose up -d
```

### **Langkah 2: Mengatur Izin Akses HDFS**
Berikan izin akses penuh ke direktori penyimpanan di HDFS agar skrip Python dapat menulis data:

```
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /data/github
```

### **Langkah 3: Konfigurasi GitHub Token**
Buka file `ingestion/producer_api.py` dan masukkan Personal Access Token Anda pada variabel berikut (JANGAN DITEKAN PUSH KE GITHUB JIKA BERISI TOKEN ASLI):

```python
GITHUB_TOKEN = "isi_token_anda_di_sini"
```

---

## **Menjalankan Pipeline Data**
Buka 5 terminal berbeda dan pastikan setiap terminal sudah masuk ke dalam venv:

### **Terminal 1: Ingestion (Producer API)**
Menarik data dari GitHub API dan mengirimkannya ke Kafka.

```
python ingestion/producer_api.py
```

### **Terminal 2: Ingestion (Producer RSS)**
Menarik data dari RSS Feed TechCrunch melalui URL `https://techcrunch.com/feed/`.

```
python ingestion/producer_rss.py
```

### **Terminal 3: Storage (Consumer)**
Membaca data dari Kafka dan menyimpannya secara permanen ke HDFS.

```
python storage/consumer_to_hdfs.py
```

### **Terminal 4: Processing (Spark Analyzer)**
Membaca data mentah dari HDFS, melakukan analisis distribusi bahasa, frekuensi kata, dan repositori populer secara kontinu menggunakan PySpark, lalu menyimpannya ke HDFS.

```
python processing/spark_analyzer.py
```

### **Terminal 5: Visualization (Dashboard)**
Menjalankan dashboard web ringan berbasis Flask yang menampilkan grafik Chart.js dengan performa tinggi.

```
python dashboard/app.py
```
Setelah aplikasi Flask berjalan, buka di browser kamu: **http://localhost:5000**

---

## **Data Lakehouse Pipeline (Delta Lake)**

Sebagai upgrade dari pipeline ETS, proyek ini menambahkan lapisan **Data Lakehouse** berbasis **Medallion Architecture** menggunakan Delta Lake. Pipeline ini membaca data dari HDFS yang sudah dikumpulkan oleh consumer, lalu memprosesnya melalui tiga layer: Bronze → Silver → Gold.

### **Arsitektur Medallion**

```
HDFS /data/github/api/*.json  ──→  [BRONZE]  ──→  [SILVER]  ──→  [GOLD]
HDFS /data/github/rss/*.json  ──→  Delta Lake      Cleaned        Aggregated
```

| Layer | Path HDFS | Keterangan |
|-------|-----------|------------|
| Bronze | `/lakehouse/bronze/github_api` | Raw data dari HDFS + metadata `_ingested_at`, `_source` |
| Bronze | `/lakehouse/bronze/github_rss` | Raw data RSS dari HDFS |
| Silver | `/lakehouse/silver/github` | Data bersih, de-duplikasi, union API + RSS |
| Gold | `/lakehouse/gold/language_dist` | Distribusi bahasa pemrograman (Repro ETS) |
| Gold | `/lakehouse/gold/top_repos` | Top 10 repo by bintang (Repro ETS) |
| Gold | `/lakehouse/gold/star_velocity` | Deteksi repo viral via Window Function (Enhanced) |
| Gold | `/lakehouse/gold/emerging_topics` | Kata kunci trending di deskripsi (Enhanced) |

### **Konfigurasi Tambahan Docker (Wajib)**

Pastikan `docker-compose.yml` sudah memiliki port `9866` di service `datanode`:

```yaml
datanode:
  ports:
    - "9864:9864"
    - "9866:9866"   # Wajib untuk koneksi Spark dari Windows
```

Dan `hadoop.env` sudah berisi:

```dotenv
HDFS-SITE.XML_dfs.datanode.hostname=localhost
HDFS-SITE.XML_dfs.client.use.datanode.hostname=true
```

Jika baru ditambahkan, recreate container:

```
docker-compose up -d --force-recreate namenode datanode
```

### **Persiapan HDFS untuk Lakehouse**

Buat folder lakehouse di HDFS:

```
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /lakehouse/bronze
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /lakehouse/silver
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /lakehouse/gold
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /lakehouse/bronze
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /lakehouse/silver
docker exec -it hadoop-namenode hdfs dfs -chmod 777 /lakehouse/gold
```

### **Menjalankan Pipeline Lakehouse**

Jalankan script secara **berurutan** setelah consumer sudah mengumpulkan data di HDFS:

```bash
# Step 1 - Bronze: Ingest JSON dari HDFS ke Delta Lake
python lakehouse/01_bronze.py

# Step 2 - Silver: Cleaning & transformasi
python lakehouse/02_silver.py

# Step 3 - Gold: Agregasi & analisis bisnis
python lakehouse/03_gold.py
```

### **Verifikasi Hasil**

Cek struktur folder HDFS setelah pipeline selesai:

```
docker exec -it hadoop-namenode hdfs dfs -ls /lakehouse/bronze/
docker exec -it hadoop-namenode hdfs dfs -ls /lakehouse/silver/
docker exec -it hadoop-namenode hdfs dfs -ls /lakehouse/gold/
```

Atau verifikasi jumlah record via Python:

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

for name, path in [
    ("bronze/api",       "hdfs://namenode:8020/lakehouse/bronze/github_api"),
    ("bronze/rss",       "hdfs://namenode:8020/lakehouse/bronze/github_rss"),
    ("silver",           "hdfs://namenode:8020/lakehouse/silver/github"),
    ("gold/language",    "hdfs://namenode:8020/lakehouse/gold/language_dist"),
    ("gold/top_repos",   "hdfs://namenode:8020/lakehouse/gold/top_repos"),
    ("gold/velocity",    "hdfs://namenode:8020/lakehouse/gold/star_velocity"),
    ("gold/topics",      "hdfs://namenode:8020/lakehouse/gold/emerging_topics"),
]:
    df = spark.read.format("delta").load(path)
    print(f"{name}: {df.count()} records")
```

### **Struktur Folder Lakehouse**

```
lakehouse/
├── 00_setup.md              # Panduan lengkap setup & troubleshooting
├── 01_bronze.py             # Ingest HDFS JSON → Delta Lake Bronze
├── 02_silver.py             # Cleaning & transformasi → Silver
├── 03_gold.py               # Agregasi & analisis bisnis → Gold
└── README_lakehouse.md      # Dokumentasi arsitektur & refleksi
```

---

## **Anggota Kelompok 7**
- Khumaidi Kharis Az-zacky (5027241049)
- Prabaswara Febrian Winandika (5027241069)
- Zahra Khaalishah (5027241070)
- I Gede Bagus Saka Sinatrya (5027241088)
- Jofanka Al-Kautsar Pangestu Abady (5027241107)

---

## **Teknologi yang Digunakan**

| Kategori | Teknologi |
|----------|-----------|
| Language | Python |
| Streaming | Apache Kafka |
| Storage | Hadoop HDFS |
| Processing | Apache Spark (PySpark) |
| Lakehouse | Delta Lake 3.1.0 |
| Dashboard | Flask & Chart.js |
