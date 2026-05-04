import json
import time
import feedparser
import hashlib
import requests
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',
    enable_idempotence=True,
    api_version=(3, 9, 0)
)

TOPIC_NAME = 'github-rss'
RSS_URL = 'https://techcrunch.com/feed/' 

def generate_hash_key(url):
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def fetch_rss_feed(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return feedparser.parse(response.content)
    except Exception as e:
        print(f"[ERROR] Gagal mengambil RSS: {e}")
        return None

def main():
    print("Memulai jalurnya... Menghubungkan RSS Producer ke Kafka...")
    seen_urls = set()
    
    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Mengambil berita terbaru dari RSS Feed...")
        
        feed = fetch_rss_feed(RSS_URL)
        new_articles_count = 0
        
        if feed and feed.entries:
            for entry in feed.entries:
                link = entry.link
                
                if link not in seen_urls:
                    data = {
                        "title": entry.title,
                        "link": link,
                        "summary": entry.get("summary", ""),
                        "published": entry.get("published", ""),
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    key = generate_hash_key(link)
                    producer.send(TOPIC_NAME, key=key, value=data)
                    seen_urls.add(link)
                    new_articles_count += 1
                    
            producer.flush()
            
        if new_articles_count > 0:
            print(f"Berhasil mengirim {new_articles_count} berita baru ke topik '{TOPIC_NAME}'!")
        else:
            print("Belum ada berita baru (atau gagal parsing). Menunggu iterasi selanjutnya...")

        waktu_tunggu = 60
        print(f"Menunggu {waktu_tunggu/60} menit untuk polling berikutnya...")
        time.sleep(waktu_tunggu)

if __name__ == "__main__":
    main()
