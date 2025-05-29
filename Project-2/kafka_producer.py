# proyek_big_data_kafka_spark/kafka_producer.py
import pandas as pd
from kafka import KafkaProducer
import json
import time
import random

KAFKA_TOPIC = 'smart_home_stream'
# Jika producer.py dijalankan di host Anda dan Kafka di Docker mem-publish port 9092:
KAFKA_BROKERS = ['localhost:9092'] 

# Inisialisasi Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer connected successfully!")
except Exception as e:
    print(f"Error connecting to Kafka Producer: {e}")
    print("Pastikan Kafka broker berjalan dan dapat diakses di KAFKA_BROKERS.")
    exit()

# Pastikan file dataset ada di path yang benar relatif terhadap skrip ini
DATASET_PATH = 'dataset/smart_home_device_usage_data.csv' 

def send_data_from_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"Reading data from {file_path} ({len(df)} rows)...")
    except FileNotFoundError:
        print(f"Error: Dataset file not found at {file_path}")
        print(f"Pastikan file '{os.path.basename(file_path)}' ada di direktori yang sama dengan skrip ini, atau ubah DATASET_PATH.")
        return

    for index, row in df.iterrows():
        message = row.to_dict()
        # UserID mungkin tidak selalu relevan untuk "event" baru, tapi kita sertakan saja
        # message['event_timestamp'] = time.time() # Opsional: tambahkan timestamp event
        
        try:
            producer.send(KAFKA_TOPIC, value=message)
            print(f"Sent message ({index+1}/{len(df)}): {message['UserID']}") # Print UserID untuk tracking
        except Exception as e:
            print(f"Error sending message for UserID {message.get('UserID', 'N/A')}: {e}")
            # Anda bisa menambahkan logika retry atau menghentikan jika ada error fatal
            
        sleep_time = random.uniform(0.01, 0.1) # Jeda kecil antara 10ms dan 100ms
        time.sleep(sleep_time)
        
    try:
        producer.flush() # Pastikan semua pesan terkirim
        print("All data sent and producer flushed.")
    except Exception as e:
        print(f"Error flushing producer: {e}")

if __name__ == "__main__":
    send_data_from_csv(DATASET_PATH)
    if producer:
        producer.close()
        print("Producer closed.")