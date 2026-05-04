import json
import time
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',
    enable_idempotence=True,
    api_version=(3, 9, 0) 
)

TOPIC_NAME = 'github-api'

GITHUB_TOKEN = ""

def get_dynamic_date():
    from datetime import datetime, timedelta
    past_date = datetime.now() - timedelta(days=7)
    return past_date.strftime('%Y-%m-%d')

def fetch_github_trending():
    url = "https://api.github.com/search/repositories"
    date_str = get_dynamic_date()
    
    params = {
        'q': f'created:>{date_str} stars:>0', 
        'sort': 'stars',
        'order': 'desc',
        'per_page': 30
    }
    
    headers = {'Accept': 'application/vnd.github.v3+json'}
    if GITHUB_TOKEN:
        headers['Authorization'] = f'token {GITHUB_TOKEN}'

    try:
        response = requests.get(url, headers=headers, params=params)
        print(f"[DEBUG] Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            print(f"[SUKSES] Berhasil menarik {len(items)} repositori!")
            return items
            
        elif response.status_code == 401:
            print("[ERROR] Status 401: Token GitHub kamu salah, kadaluarsa, atau ada spasi nyempil!")
            return []
            
        elif response.status_code == 403:
            print("[ERROR] Status 403: Memang terkena Rate Limit sungguhan. Tunggu 1 menit!")
            return []
            
        else:
            print(f"[ERROR] GitHub menjawab dengan aneh: {response.text}")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"[FATAL ERROR] Gagal menghubungi GitHub: {e}")
        return []

def main():
    print("Memulai jalurnya... Menghubungkan Producer ke Kafka...")
    
    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Mengambil data trending dari GitHub...")
        repos = fetch_github_trending()
        
        if repos:
            print(f"Ditemukan {len(repos)} repositori. Memproses dan mengirim ke topik '{TOPIC_NAME}'...")
            
            for repo in repos:
                data = {
                    "full_name": repo.get("full_name"),
                    "description": repo.get("description"),
                    "language": repo.get("language"),
                    "stargazers_count": repo.get("stargazers_count"),
                    "topics": repo.get("topics", []),
                    "html_url": repo.get("html_url"),
                    "timestamp": datetime.now().isoformat()
                }
                
                key = data["full_name"]
                
                producer.send(TOPIC_NAME, key=key, value=data)
            
            producer.flush()
            print("Data berhasil dikirim ke Kafka!")
        else:
            print("Tidak ada data yang diambil (mungkin terkena rate limit GitHub).")

        waktu_tunggu = 60 
        print(f"Menunggu {waktu_tunggu/60} menit untuk polling berikutnya...")
        time.sleep(waktu_tunggu)

if __name__ == "__main__":
    main()
