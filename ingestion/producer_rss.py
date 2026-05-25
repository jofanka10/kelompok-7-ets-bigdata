import json
import time
import hashlib
import requests
from datetime import datetime
from kafka import KafkaProducer
from bs4 import BeautifulSoup

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8') if v else None,
    api_version=(3, 9, 0),
    retries=5
)

TOPIC_NAME = 'github-rss'

def generate_hash_key(repo_name):
    return hashlib.md5(repo_name.encode('utf-8')).hexdigest()

def fetch_github_trending():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    try:
        # Scrape GitHub trending page
        url = 'https://github.com/trending?spoken_language_code=en&since=daily'
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        repos = []
        
        # Find all repo articles
        for article in soup.find_all('article', class_='Box-row'):
            try:
                # Get repo name and URL
                h2 = article.find('h2', class_='h3')
                if not h2:
                    continue
                
                repo_link = h2.find('a')
                if not repo_link:
                    continue
                
                full_name = repo_link.get('href', '').strip('/')
                repo_url = f"https://github.com{repo_link.get('href', '')}"
                
                # Get description
                p = article.find('p', class_='col-9')
                description = p.text.strip() if p else ""
                
                # Get language
                lang_span = article.find('span', attrs={'itemprop': 'programmingLanguage'})
                language = lang_span.text.strip() if lang_span else "Unknown"
                
                # Get stars
                stars_text = ""
                for span in article.find_all('span'):
                    if 'stars' in span.text:
                        stars_text = span.text.strip()
                        break
                
                # Parse star count
                try:
                    stars_count = int(stars_text.split()[0].replace(',', ''))
                except:
                    stars_count = 0
                
                repo_data = {
                    "full_name": full_name,
                    "html_url": repo_url,
                    "description": description,
                    "language": language,
                    "stargazers_count": stars_count,
                    "updated_at": datetime.now().isoformat(),
                    "source": "github-trending",
                    "timestamp": datetime.now().isoformat()
                }
                
                repos.append(repo_data)
                
            except Exception as e:
                print(f"  ⚠️ Error parsing repo: {e}")
                continue
        
        return repos
        
    except Exception as e:
        print(f"[ERROR] Gagal scrape GitHub Trending: {e}")
        return []

def main():
    print("Memulai RSS Producer untuk GitHub Trending Repositories...")
    print(f"Source: https://github.com/trending")
    
    iteration = 0
    
    while True:
        iteration += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Iterasi {iteration}: Mengambil GitHub Trending...")
        
        repos = fetch_github_trending()
        new_repos_count = 0
        
        if repos:
            for repo in repos:
                try:
                    key = generate_hash_key(repo['full_name'])
                    producer.send(TOPIC_NAME, key=key, value=repo)
                    new_repos_count += 1
                    print(f"  ✓ Sent: {repo['full_name']} ({repo['stargazers_count']} ⭐)")
                    
                except Exception as e:
                    print(f"  ❌ Error sending {repo['full_name']}: {e}")
            
            producer.flush()
            
            if new_repos_count > 0:
                print(f"\n✅ Berhasil mengirim {new_repos_count} GitHub trending repos ke topik '{TOPIC_NAME}'!")
            else:
                print(f"\n⚠️ Tidak ada repos yang dikirim")
        else:
            print("❌ Gagal fetch GitHub trending repos")

        waktu_tunggu = 60  # 60 detik
        print(f"Menunggu {waktu_tunggu} detik untuk polling berikutnya...")
        time.sleep(waktu_tunggu)

if __name__ == "__main__":
    main()
