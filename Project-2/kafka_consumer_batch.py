# proyek_big_data_kafka_spark/kafka_consumer_batch.py
from kafka import KafkaConsumer
import json
import pandas as pd
import os
import time # Untuk memberi nama file batch unik jika dijalankan berulang

KAFKA_TOPIC = 'smart_home_stream'
KAFKA_BROKERS = ['localhost:9092'] # Sama seperti producer
BATCH_SIZE = 1000
OUTPUT_DIR = 'batched_data'       # Akan dibuat di dalam proyek_big_data_kafka_spark/
FILE_PREFIX = f'smart_home_batch_{int(time.time())}_' # Tambahkan timestamp untuk keunikan run

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Created directory: {OUTPUT_DIR}")

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest', 
        consumer_timeout_ms=30000, # Stop setelah 30 detik jika tidak ada pesan baru
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='smart-home-batcher-group' # group_id penting untuk tracking offset
    )
    print("Kafka Consumer connected successfully!")
except Exception as e:
    print(f"Error connecting to Kafka Consumer: {e}")
    print("Pastikan Kafka broker berjalan dan dapat diakses di KAFKA_BROKERS, dan topik ada.")
    exit()

def consume_and_batch():
    messages_buffer = []
    batch_file_count = 1
    processed_message_count = 0
    first_message_data = None

    print(f"Listening for messages on topic '{KAFKA_TOPIC}'...")
    try:
        for message in consumer: # Loop ini akan berhenti setelah consumer_timeout_ms
            processed_message_count += 1
            msg_data = message.value
            print(f"Received message ({processed_message_count}): UserID {msg_data.get('UserID', 'N/A')}")

            if first_message_data is None and isinstance(msg_data, dict):
                first_message_data = msg_data # Simpan struktur pesan pertama

            if isinstance(msg_data, dict):
                 messages_buffer.append(msg_data)

            if len(messages_buffer) >= BATCH_SIZE:
                if first_message_data: # Pastikan kita punya struktur kolom
                    df_batch = pd.DataFrame(messages_buffer, columns=first_message_data.keys())
                    file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{batch_file_count}.csv")
                    df_batch.to_csv(file_path, index=False)
                    print(f"Saved batch {batch_file_count} to {file_path} ({len(messages_buffer)} records)")
                    messages_buffer = []
                    batch_file_count += 1
                else:
                    print("Warning: Could not determine column structure for saving batch.")
        
        # Simpan sisa pesan jika ada
        if messages_buffer and first_message_data:
            df_batch = pd.DataFrame(messages_buffer, columns=first_message_data.keys())
            file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{batch_file_count}.csv")
            df_batch.to_csv(file_path, index=False)
            print(f"Saved final batch {batch_file_count} to {file_path} ({len(messages_buffer)} records)")
        elif messages_buffer:
             print(f"Warning: Could not save final batch of {len(messages_buffer)} messages, column structure unknown.")


    except Exception as e:
        print(f"An error occurred during consumption: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consumer closed.")
        print(f"Total messages processed by consumer: {processed_message_count}")

if __name__ == "__main__":
    consume_and_batch()