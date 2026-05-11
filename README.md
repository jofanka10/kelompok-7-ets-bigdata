# **ETS Big Data - GitHub Trending Analytics Pipeline**
Proyek ini mengimplementasikan pipeline data lengkap untuk menganalisis repositori GitHub yang sedang tren. Arsitektur sistem mencakup pengambilan data melalui GitHub API, pengiriman pesan via Kafka, penyimpanan terdistribusi di HDFS, pemrosesan data secara kontinu menggunakan Apache Spark, dan visualisasi interaktif dengan Flask dan Chart.js.

## **Persiapan Lingkungan (Setup)**
### **1. Kloning Repository & Virtual Environment**
```Bash
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

Mac/Linux: `/etc/hosts` (Gunakan sudo nano /etc/hosts)

Tambahkan baris ini:

```
127.0.0.1 namenode datanode
```
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

```Python
GITHUB_TOKEN = "isi_token_anda_di_sini"
```

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

## **Anggota Kelompok 7**
- Khumaidi Kharis Az-zacky (5027241049)

- Prabaswara Febrian Winandika (5027241069)

- Zahra Khaalishah (5027241070)

- I Gede Bagus Saka Sinatrya (5027241088)

- Jofanka Al-Kautsar Pangestu Abady (5027241107)

## **Teknologi yang Digunakan:**

- Language: Python
- Streaming: Apache Kafka
- Storage: Hadoop HDFS
- Processing: Apache Spark (PySpark)
- Dashboard: Flask & Chart.js
