import pandas as pd
from kafka import KafkaProducer
import json
import time
import random
import os

KAFKA_TOPIC = 'urban_traffic_stream'
KAFKA_BROKERS = ['localhost:9092'] 

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8') # Mengirim pesan sebagai JSON
    )
    print(f"Kafka Producer connected successfully to brokers: {KAFKA_BROKERS} for topic: {KAFKA_TOPIC}")
except Exception as e:
    print(f"Error connecting to Kafka Producer: {e}")
    print("Pastikan Kafka broker berjalan dan dapat diakses di KAFKA_BROKERS.")
    exit()

DATASET_PATH = os.path.join('dataset', 'futuristic_city_traffic.csv') 

def send_data_from_csv(file_path):
    if not os.path.exists(file_path):
        print(f"Error: Dataset file not found at {file_path}")
        print(f"Pastikan file '{os.path.basename(file_path)}' ada di direktori '{os.path.dirname(file_path)}'.")
        return

    try:
        df = pd.read_csv(file_path)
        print(f"Reading data from {file_path} ({len(df)} rows)...")
    except Exception as e_read:
        print(f"Error reading CSV file {file_path}: {e_read}")
        return

    # Ganti nama kolom jika mengandung spasi atau karakter yang tidak ideal untuk field JSON/Spark
    # Dalam kasus ini, nama kolom dari Kaggle tampaknya sudah baik.
    # df.columns = df.columns.str.replace(' ', '_').str.replace('[^A-Za-z0-9_]+', '', regex=True)
    # print(f"New column names: {df.columns.tolist()}")


    for index, row in df.iterrows():
        message = row.to_dict()
        
        try:
            producer.send(KAFKA_TOPIC, value=message)
            # Mengurangi verbosity log agar tidak terlalu penuh saat mengirim banyak data
            if (index + 1) % 10000 == 0 or index == 0 or index == len(df) - 1 : # Print setiap 10000 pesan, dan pesan pertama/terakhir
                 print(f"Sent message ({index+1}/{len(df)}): Data from row {index+1}")
        except Exception as e:
            print(f"Error sending message for row {index+1}: {e}")
            
        # Jeda acak yang lebih kecil untuk dataset besar, atau bahkan bisa dihilangkan jika laju pengiriman tidak masalah
        sleep_time = random.uniform(0.0001, 0.001) # 0.1ms - 1ms
        time.sleep(sleep_time)
        
    try:
        producer.flush() 
        print("All data sent and producer flushed.")
    except Exception as e:
        print(f"Error flushing producer: {e}")

if __name__ == "__main__":
    print(f"Starting Kafka producer for dataset: {DATASET_PATH}")
    send_data_from_csv(DATASET_PATH)
    if producer:
        producer.close()
        print("Producer closed.")