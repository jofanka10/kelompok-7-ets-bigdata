from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, desc

def main():
    print("🚀 Menghidupkan Apache Spark...")
    
    spark = SparkSession.builder \
        .appName("ETS_BigData_Kelompok7") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    print("✅ Spark Session Berhasil Dibuat!")

    path_hdfs = "webhdfs://namenode:9870/data/github/api/"
    
    try:
        # Membaca data dengan opsi multiline
        df_github = spark.read.option("multiline", "true").json(path_hdfs)
        df_github = df_github.dropDuplicates(["full_name"])
        
        # --- MULAI PROSES ANALISIS DATA (KOMPONEN 3) ---
        print("\n" + "="*50)
        print("📊 HASIL ANALISIS BIG DATA DENGAN APACHE SPARK")
        print("="*50)

        # Analisis 1: Menghitung Distribusi Bahasa Pemrograman
        print("\n📈 1. Top 10 Bahasa Pemrograman Paling Trending:")
        # Filter yang bahasanya tidak null, kelompokkan, hitung, lalu urutkan dari yang terbanyak
        df_lang = df_github.filter(col("language").isNotNull()) \
            .groupBy("language").count() \
            .orderBy(desc("count"))
        df_lang.show(10)

        # Analisis 2: Top 5 Repositori dengan Bintang Terbanyak
        print("\n🌟 2. Top 5 Repositori Paling Populer (Berdasarkan Stars):")
        df_top_repos = df_github.orderBy(desc("stargazers_count")) \
            .select("full_name", "language", "stargazers_count")
        df_top_repos.show(5, truncate=False)

        # Analisis 3: Top 10 Topik/Tags Terpopuler
        print("\n🏷️ 3. Top 10 Topik/Tags Paling Banyak Digunakan:")
        # Memecah array 'topics' menjadi baris terpisah dengan fungsi 'explode', lalu dihitung
        df_topics = df_github.select(explode(col("topics")).alias("topic")) \
            .groupBy("topic").count() \
            .orderBy(desc("count"))
        df_topics.show(10)

    except Exception as e:
        print(f"❌ Gagal memproses data: {e}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
