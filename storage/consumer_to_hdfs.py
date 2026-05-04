import json
import time
import os
import threading
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient

HDFS_API_DIR = "/data/github/api/"
HDFS_RSS_DIR = "/data/github/rss/"
LOCAL_API_LIVE = "dashboard/data/live_api.json"
LOCAL_RSS_LIVE = "dashboard/data/live_rss.json"

# Menggunakan 'namenode' (karena Windows sudah diajari lewat file hosts)
hdfs_client = InsecureClient('http://namenode:9870', user='root')

def init_local_files():
    os.makedirs("dashboard/data", exist_ok=True)
    for path in [LOCAL_API_LIVE, LOCAL_RSS_LIVE]:
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump([], f)

def process_topic(topic_name, hdfs_dir, live_file_path):
    print(f"[{topic_name}] Consumer siap mendengarkan pesan...")
    
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'hdfs_writer_{topic_name}_opsiB', 
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    buffer = []
    last_save_time = time.time()
    flush_interval = 60 

    for message in consumer:
        data = message.value
        buffer.append(data)
        
        # SPEED LAYER
        try:
            with open(live_file_path, 'r') as f:
                live_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            live_data = []
            
        live_data.insert(0, data)
        live_data = live_data[:30]
        
        with open(live_file_path, 'w') as f:
            json.dump(live_data, f, indent=4)
            
        # BATCH LAYER (Opsi B murni)
        current_time = time.time()
        
        if (current_time - last_save_time >= flush_interval) or (len(buffer) >= 10):
            if len(buffer) > 0:
                timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
                file_name = f"{timestamp_str}.json"
                hdfs_path = f"{hdfs_dir}{file_name}"
                
                print(f"[{topic_name}] Mengemas {len(buffer)} pesan ke {file_name}...")
                
                try:
                    # Tulis langsung ke HDFS pakai Python!
                    json_data = json.dumps(buffer, indent=4)
                    with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
                        writer.write(json_data)
                        
                    print(f"[{topic_name}] Berhasil disimpan di HDFS: {hdfs_path}")
                except Exception as e:
                    print(f"[{topic_name}] Gagal menyimpan ke HDFS: {e}")
                    
                buffer = []
                last_save_time = time.time()

def main():
    print("Menjalankan HDFS Consumer (Opsi B - Murni Python!)...")
    init_local_files()
    
    t1 = threading.Thread(target=process_topic, args=('github-api', HDFS_API_DIR, LOCAL_API_LIVE))
    t2 = threading.Thread(target=process_topic, args=('github-rss', HDFS_RSS_DIR, LOCAL_RSS_LIVE))
    
    t1.start()
    t2.start()
    
    t1.join()
    t2.join()

if __name__ == "__main__":
    main()
