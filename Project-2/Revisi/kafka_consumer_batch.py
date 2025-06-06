from kafka import KafkaConsumer
import json
import pandas as pd
import os
import time

KAFKA_TOPIC = 'urban_traffic_stream'
KAFKA_BROKERS = ['localhost:9092']
BATCH_SIZE = 100000
OUTPUT_DIR = 'batched_data_traffic'
FILE_PREFIX = f'urban_traffic_batch_{int(time.time())}_' 

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Created directory: {OUTPUT_DIR}")

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest', 
        consumer_timeout_ms=60000, # Timeout lebih lama (60 detik) karena data besar
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id='urban-traffic-batcher-group'
    )
    print(f"Kafka Consumer connected successfully to topic: {KAFKA_TOPIC}")
except Exception as e:
    print(f"Error connecting to Kafka Consumer: {e}")
    print("Pastikan Kafka broker berjalan dan dapat diakses, dan topik ada.")
    exit()

def consume_and_batch():
    messages_buffer = []
    batch_file_count = 1
    processed_message_count = 0
    column_names = None

    print(f"Listening for messages on topic '{KAFKA_TOPIC}' (Batch size: {BATCH_SIZE})...")
    try:
        for message in consumer: 
            processed_message_count += 1
            msg_data = message.value
            
            if (processed_message_count % 1000 == 0) or processed_message_count == 1: # Log setiap 1000 pesan
                print(f"Received message ({processed_message_count}): City {msg_data.get('City', 'N/A')}, Hour {msg_data.get('Hour Of Day', 'N/A')}")

            if column_names is None and isinstance(msg_data, dict):
                # Ambil nama kolom dari pesan pertama, ini penting untuk konsistensi header CSV
                column_names = list(msg_data.keys())
                print(f"Detected column names from first message: {column_names}")

            if isinstance(msg_data, dict):
                 messages_buffer.append(msg_data)

            if len(messages_buffer) >= BATCH_SIZE:
                if column_names: 
                    df_batch = pd.DataFrame(messages_buffer, columns=column_names) # Gunakan urutan kolom dari pesan pertama
                    file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{batch_file_count}.csv")
                    df_batch.to_csv(file_path, index=False, header=True) # Pastikan header ditulis
                    print(f"Saved batch {batch_file_count} to {file_path} ({len(messages_buffer)} records)")
                    messages_buffer = []
                    batch_file_count += 1
                else:
                    # Seharusnya tidak terjadi jika producer mengirim dictionary yang konsisten
                    print("Warning: Column names not yet determined. Skipping save for this batch interim.")
        
        if messages_buffer:
            if column_names:
                df_batch = pd.DataFrame(messages_buffer, columns=column_names)
                file_path = os.path.join(OUTPUT_DIR, f"{FILE_PREFIX}{batch_file_count}.csv")
                df_batch.to_csv(file_path, index=False, header=True)
                print(f"Saved final batch {batch_file_count} to {file_path} ({len(messages_buffer)} records)")
            else:
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