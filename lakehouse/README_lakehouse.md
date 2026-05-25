# README_lakehouse.md
# Upgrade Pipeline ke Data Lakehouse — Topik 7: GitTrend
**Kelompok 7 | Big Data dan Data Lakehouse**

---

## Daftar Isi

1. [Arsitektur: Sebelum vs Sesudah](#1-arsitektur-sebelum-vs-sesudah)
2. [Justifikasi Transformasi Silver](#2-justifikasi-transformasi-silver)
3. [Perbandingan Gold vs Analisis ETS](#3-perbandingan-gold-vs-analisis-ets)
4. [Demonstrasi Time Travel](#4-demonstrasi-time-travel)
5. [Refleksi: Keuntungan Delta Lake](#5-refleksi-keuntungan-delta-lake)

---

## 1. Arsitektur: Sebelum vs Sesudah

### Sebelum (ETS) — Pipeline HDFS + JSON

```
[GitHub API]  ──→  Kafka (github-api)  ──→  Consumer  ──→  HDFS /data/github/api/*.json
[TechCrunch]  ──→  Kafka (github-rss)  ──→  Consumer  ──→  HDFS /data/github/rss/*.json
                                                                        │
                                                                        ▼
                                                              Spark analysis.py
                                                         (baca JSON mentah dari HDFS)
                                                                        │
                                                              ┌─────────────────────┐
                                                              │  3 Analisis Spark:  │
                                                              │  1. Dist. bahasa    │
                                                              │  2. Top 10 repos    │
                                                              │  3. Freq. kata      │
                                                              └─────────────────────┘
                                                                        │
                                                              spark_results.json
                                                                        │
                                                              Flask Dashboard
```

**Kelemahan ETS:**
- Tidak ada schema enforcement — tipe data tidak konsisten antar file JSON
- Tidak ada versioning — jika data berubah, tidak bisa kembali ke versi lama
- Duplikat data — repo yang sama bisa masuk berkali-kali ke HDFS
- Tidak ada ACID — jika proses gagal di tengah jalan, data bisa korup
- Analisis terbatas — Window Function tidak bisa dipakai karena timestamp belum di-parse

---

### Sesudah (Tugas Ini) — Medallion Architecture + Delta Lake

```
[GitHub API]  ──→  Kafka (github-api)  ──→  Consumer  ──→  HDFS /data/github/api/*.json
[TechCrunch]  ──→  Kafka (github-rss)  ──→  Consumer  ──→  HDFS /data/github/rss/*.json
                                                                        │
                                                                        ▼
                                              ┌─────────────────────────────────────────┐
                                              │           MEDALLION ARCHITECTURE        │
                                              │                                         │
                                              │  [BRONZE] /lakehouse/bronze/            │
                                              │   • Data raw dari HDFS                  │
                                              │   • Tambah _ingested_at, _source        │
                                              │   • Format Delta Lake (ACID)            │
                                              │              │                          │
                                              │              ▼                          │
                                              │  [SILVER] /lakehouse/silver/            │
                                              │   • dropDuplicates by full_name         │
                                              │   • Filter null, cast tipe data         │
                                              │   • Ekstrak jam, tanggal               │
                                              │   • Union API + RSS dalam satu tabel   │
                                              │              │                          │
                                              │              ▼                          │
                                              │  [GOLD] /lakehouse/gold/                │
                                              │   • language_dist  (Repro ETS)          │
                                              │   • top_repos      (Repro ETS)          │
                                              │   • star_velocity  (Enhanced)           │
                                              │   • emerging_topics (Enhanced)          │
                                              └─────────────────────────────────────────┘
                                                                        │
                                                              Flask Dashboard
                                                           (baca dari Gold Delta)
```

**Keunggulan arsitektur baru:**
- Schema konsisten di setiap layer
- ACID transactions — tidak ada data korup jika proses gagal
- Time Travel — bisa query data versi mana pun
- Duplikat sudah dihapus di Silver — analisis lebih akurat
- Analisis lintas sumber (API + RSS) di Gold

---

## 2. Justifikasi Transformasi Silver

Script `02_silver.py` melakukan 4 transformasi cleaning. Berikut justifikasi masing-masing:

---

### Transformasi 1 — `dropDuplicates(["full_name"])`

**Mengapa dilakukan?**
Producer API melakukan polling setiap 30 menit. Dalam satu sesi pengumpulan data, repositori yang sama bisa masuk ke HDFS berkali-kali (di file JSON yang berbeda). Selain itu, ada kemungkinan repositori yang sama muncul di sumber API dan RSS sekaligus.

**Dampak:**
- Sebelum: ~1000+ record (gabungan API + RSS dengan duplikat)
- Sesudah: berkurang signifikan, hanya repo unik yang tersisa
- Agregasi di Gold menjadi akurat — tidak ada penggandaan hitungan bintang

---

### Transformasi 2 — Filter `full_name IS NOT NULL` + `fillna(0, stargazers_count)`

**Mengapa dilakukan?**
`full_name` adalah identifier utama repositori GitHub. Record tanpa `full_name` tidak memiliki identitas dan tidak berguna untuk analisis apapun. Sementara `stargazers_count` null bisa menyebabkan error di window function dan agregasi numerik.

**Dampak:**
- Record dengan `full_name` null dibuang sepenuhnya
- Record dengan `stargazers_count` null diisi 0 (aman untuk agregasi)
- Filter tambahan: `stargazers_count >= 0` membuang data tidak valid

---

### Transformasi 3 — Cast Tipe Data & Ekstrak Kolom Waktu

**Mengapa dilakukan?**
JSON dari HDFS menyimpan semua nilai sebagai string secara default. `stargazers_count` yang harusnya integer bisa terbaca sebagai string, menyebabkan sorting dan aggregation numerik tidak benar. Timestamp perlu di-parse agar bisa digunakan untuk Window Function.

**Transformasi yang dilakukan:**
```python
.withColumn("stargazers_count", col("stargazers_count").cast("integer"))
.withColumn("updated_at_timestamp", to_timestamp(col("updated_at")))
.withColumn("updated_hour", hour(col("updated_at_timestamp")))
.withColumn("updated_date", date_format(col("updated_at_timestamp"), "yyyy-MM-dd"))
```

**Dampak:**
- `stargazers_count` bertipe integer → sorting numerik benar
- `updated_at_timestamp` bertipe TimestampType → Window Function `lag()` bisa dipakai
- `updated_hour` dan `updated_date` → analisis temporal lebih mudah

---

### Transformasi 4 — Standarisasi `language` + Metadata Kualitas

**Mengapa dilakukan?**
Banyak repositori GitHub tidak mencantumkan bahasa pemrograman (null). Jika dibiarkan null, analisis distribusi bahasa akan tidak lengkap. `language_lower` dibuat untuk menghindari duplikasi kategori karena perbedaan huruf kapital (misal "Python" vs "python").

**Transformasi yang dilakukan:**
```python
.withColumn("language", coalesce(col("language"), lit("Unknown")))
.withColumn("language_lower", lower(col("language")))
.withColumn("_quality_flag",
    when((col("full_name").isNotNull()) & (col("language").isNotNull()), "OK")
    .otherwise("PARTIAL_DATA"))
```

**Dampak:**
- Tidak ada null di kolom language → pie chart distribusi bahasa lengkap
- `_quality_flag` memungkinkan monitoring kualitas data secara ongoing

---

## 3. Perbandingan Gold vs Analisis ETS

### Tabel 1: `gold/language_dist` vs Analisis ETS #1

| Aspek | ETS (Spark + JSON Mentah) | Gold (Delta Lake) |
|-------|--------------------------|-------------------|
| Sumber data | JSON mentah dari HDFS | Silver yang sudah bersih |
| Duplikat | Ada — repo sama dihitung berkali-kali | Tidak ada — `dropDuplicates` di Silver |
| Null language | Muncul sebagai `null` di hasil | Diganti `Unknown`, tidak hilang dari analisis |
| Case sensitivity | `Python` dan `python` dianggap beda | Sudah lowercase semua |
| Akurasi | Kurang akurat karena duplikat | Lebih akurat |

**Keunggulan Gold:** Hasil distribusi bahasa lebih representatif karena tidak ada penggandaan dan null sudah ditangani.

---

### Tabel 2: `gold/top_repos` vs Analisis ETS #2

| Aspek | ETS (Spark + JSON Mentah) | Gold (Delta Lake) |
|-------|--------------------------|-------------------|
| Sumber data | JSON mentah | Silver unified (API + RSS) |
| Kolom tersedia | Terbatas pada JSON fields | + `description`, `html_url`, `_source` |
| Duplikat di top 10 | Mungkin ada repo sama | Tidak ada, sudah de-duplikasi |
| Reproducibility | Berbeda tiap kali dijalankan | Konsisten, bisa query versi historis |

**Keunggulan Gold:** Top 10 yang ditampilkan sudah dipastikan unik dan memiliki kolom lebih lengkap.

---

### Tabel 3: `gold/star_velocity` — Enhanced (Tidak Ada di ETS)

**Mengapa tidak bisa dibuat di ETS?**

Di ETS, Spark membaca JSON mentah dari HDFS:
- `updated_at` masih bertipe String → Window Function `lag()` tidak bisa dipakai untuk urutan waktu yang benar
- Ada duplikat → `lag()` akan menghitung perubahan bintang ke record duplikat, bukan observasi baru
- Tidak ada `previous_stars` → tidak bisa hitung `star_gain`

**Apa yang dilakukan di Gold:**
```python
window_spec = Window.partitionBy("full_name").orderBy("updated_at_timestamp")

gold_star_velocity = silver_df \
    .withColumn("prev_stars", lag("stargazers_count", 1).over(window_spec)) \
    .withColumn("star_gain", coalesce(col("stargazers_count") - col("prev_stars"), lit(0)))
```

**Insight yang dihasilkan:** Repo mana yang sedang viral — bukan hanya yang punya bintang terbanyak, tapi yang paling cepat bertambah bintangnya dalam periode observasi.

> **Catatan:** Jika data dikumpulkan dalam waktu singkat (< 1 jam), sebagian besar repo hanya punya 1 observasi sehingga `star_velocity` menunjukkan 0 records. Ini adalah limitasi data, bukan bug. Dalam production dengan data 24 jam+, tabel ini akan terisi.

---

### Tabel 4: `gold/emerging_topics` — Enhanced (Tidak Ada di ETS)

**Mengapa tidak bisa dibuat di ETS?**

Di ETS, analisis kata hanya dilakukan pada satu sumber (API saja). Schema antara API dan RSS tidak konsisten sehingga tidak bisa di-union langsung.

**Apa yang dilakukan di Gold:**
```python
gold_emerging_topics = silver_df \
    .filter(col("description").isNotNull()) \
    .withColumn("word", explode(split(lower(regexp_replace(col("description"), "[^a-zA-Z0-9 ]", "")), " "))) \
    .filter(col("word").rlike("^[a-z]{4,}")) \
    .groupBy("word", "_source") \
    .agg(count("*").alias("frequency"))
```

**Keunggulan vs ETS:**
- Analisis kata dari **dua sumber** (API + RSS) sekaligus dalam satu query
- Filter noise word yang lebih komprehensif
- Breakdown per sumber (`_source`) menunjukkan kata apa yang dominan di mana

---

## 4. Demonstrasi Time Travel

Time Travel Delta Lake dijalankan otomatis di `03_gold.py` Step 3. Berikut output yang dihasilkan:

### History Tabel Silver

```
+-------+-------------------+---------+
|version|          timestamp|operation|
+-------+-------------------+---------+
|      1|2026-05-24 23:05:xx|   UPDATE|
|      0|2026-05-24 23:03:xx|    WRITE|
+-------+-------------------+---------+
```

### Operasi Update

```python
# Update: isi semua language NULL menjadi 'Unknown'
silver_delta.update(
    condition="language IS NULL",
    set={"language": "'Unknown'"}
)
```

### Perbandingan Versi

```
=== Data SEKARANG (setelah update) ===
Tidak ada record dengan language = NULL

=== Data VERSI 0 (sebelum update) ===
Ada N record dengan language = NULL
```

**Apa artinya?**
Delta Lake menyimpan semua perubahan data beserta historynya. Kapanpun dibutuhkan, kita bisa kembali ke versi data sebelum update — sesuatu yang **tidak mungkin dilakukan** dengan file JSON di HDFS biasa. Jika update yang dilakukan ternyata salah, data bisa di-restore tanpa perlu menjalankan ulang seluruh pipeline.

---

## 5. Refleksi: Keuntungan Nyata Delta Lake

### Apa yang berubah secara teknis?

| Fitur | HDFS/JSON (ETS) | Delta Lake (Tugas Ini) |
|-------|-----------------|----------------------|
| Format penyimpanan | JSON teks biasa | Parquet + transaction log |
| ACID Transactions | ❌ Tidak ada | ✅ Ada |
| Schema enforcement | ❌ Tidak ada | ✅ Ada |
| Time Travel | ❌ Tidak bisa | ✅ Bisa query versi mana pun |
| Hapus duplikat | ❌ Manual di setiap analisis | ✅ Sekali di Silver, berlaku seterusnya |
| Window Function | ❌ Terbatas (timestamp string) | ✅ Penuh (timestamp sudah parsed) |
| Analisis lintas sumber | ❌ Sulit (schema beda) | ✅ Mudah (Silver sudah unified) |
| Reproducibility | ❌ Hasil beda tiap run | ✅ Konsisten, bisa audit |

### Apa keuntungan nyata yang dirasakan?

**1. Analisis lebih akurat**
Di ETS, distribusi bahasa pemrograman bisa tidak akurat karena repo yang sama dihitung berkali-kali. Di Gold, karena Silver sudah di-deduplikasi, hasilnya mencerminkan kondisi sebenarnya.

**2. Analisis yang sebelumnya tidak mungkin**
Star velocity (deteksi repo viral) membutuhkan Window Function dengan timestamp yang sudah di-parse. Ini tidak bisa dilakukan di ETS karena timestamp masih string. Dengan Silver yang sudah bersih, analisis ini langsung bisa dijalankan.

**3. Data recovery yang mudah**
Dalam scenario nyata, jika ada bug di pipeline yang menyebabkan data salah tersimpan, Delta Lake memungkinkan rollback ke versi sebelumnya. Dengan JSON di HDFS, tidak ada cara untuk melakukan ini — file yang sudah ditimpa tidak bisa dikembalikan.

**4. Pipeline yang lebih maintainable**
Medallion Architecture memisahkan tanggung jawab dengan jelas:
- Bronze: simpan data apa adanya, jangan ubah apapun
- Silver: cleaning sekali, berlaku untuk semua analisis
- Gold: analisis bisnis spesifik

Jika ada perubahan logic cleaning, cukup update Silver — semua tabel Gold otomatis menggunakan data yang sudah bersih tanpa harus menulis ulang logika cleaning di setiap script analisis.

---

*Kelompok 7 — Big Data dan Data Lakehouse*