import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession

st.set_page_config(page_title="GitHub Trending Dashboard", layout="wide")

st.markdown("""
    <style>
    .main-title {
        font-size: 2.5rem;
        font-weight: 800;
        color: #4A90E2;
        margin-bottom: 0px;
    }
    .sub-title {
        font-size: 1.1rem;
        color: #A0AAB2;
        margin-bottom: 30px;
    }
    .credit-card {
        text-align: center; 
        border: 1px solid #2e3440; 
        border-radius: 10px; 
        padding: 20px; 
        background-color: #1e222a;
        margin-top: 50px;
    }
    .credit-title {
        color: #4A90E2; 
        margin-bottom: 15px;
        font-weight: bold;
    }
    .credit-names {
        font-size: 15px; 
        margin: 0; 
        color: #eceff4;
        line-height: 1.8;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-title">GitHub Trending Analytics</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-title">Real-time Data Pipeline: API - Kafka - HDFS - Apache Spark</p>', unsafe_allow_html=True)

@st.cache_resource
def init_spark():
    return SparkSession.builder \
        .appName("Streamlit_Spark_Dashboard") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

spark = init_spark()
path_hdfs = "webhdfs://namenode:9870/data/github/api/"

try:
    with st.spinner("Menyedot data dari HDFS dengan Apache Spark..."):
        df_spark = spark.read.option("multiline", "true").json(path_hdfs)
        df_spark = df_spark.dropDuplicates(["full_name"])
        df_pandas = df_spark.select("full_name", "language", "stargazers_count").toPandas()

    st.success("Sinkronisasi berhasil! Menampilkan data HDFS terkini.")

    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Distribusi Bahasa Pemrograman")
        lang_count = df_pandas['language'].value_counts()
        st.bar_chart(lang_count, use_container_width=True)
        
    with col2:
        st.subheader("Top 5 Repositori Populer")
        top_repos = df_pandas.sort_values(by='stargazers_count', ascending=False).head(5)
        st.dataframe(
            top_repos, 
            use_container_width=True,
            hide_index=True
        )

    st.markdown("---")

    st.markdown("### Quick Insights")
    m1, m2, m3 = st.columns(3)
    
    m1.metric("Total Trending Repositories", f"{len(df_pandas)} Repo")
    
    if not df_pandas['language'].empty:
        top_lang = df_pandas['language'].mode()[0]
    else:
        top_lang = "N/A"
    m2.metric("Most Popular Language", top_lang)
    
    highest_stars = df_pandas['stargazers_count'].max()
    m3.metric("Highest Stars in Dataset", f"{highest_stars:,}")

except Exception as e:
    st.error(f"Gagal membaca data dari HDFS. Error detail: {e}")

st.markdown(f"""
    <div class="credit-card">
        <h3 class="credit-title">Tim Pengembang - Kelompok 7</h3>
        <p class="credit-names">
            Khumaidi Kharis Az-zacky (5027241049)<br>
            Prabaswara Febrian Winandika (5027241069)<br>
            Zahra Khaalishah (5027241070)<br>
            I Gede Bagus Saka Sinatrya (5027241088)<br>
            Jofanka Al-Kautsar Pangestu Abady (5027241107)
        </p>
    </div>
""", unsafe_allow_html=True)
