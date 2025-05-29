# Proyek Big Data: Simulasi Streaming Data Smart Home dengan Kafka, Spark, dan API

## Overview Proyek

Proyek ini bertujuan untuk mensimulasikan arsitektur pemrosesan Big Data secara real-time menggunakan Apache Kafka untuk streaming data, Apache Spark untuk pemrosesan batch dan pelatihan model Machine Learning, serta Flask API untuk menyajikan hasil prediksi model. Data yang digunakan adalah dataset penggunaan perangkat smart home, di mana model-model yang dibangun bertujuan untuk memprediksi efisiensi perangkat, potensi kerusakan, dan melakukan segmentasi perangkat berdasarkan karakteristiknya.

**Arsitektur Sistem:**

1.  **Kafka Producer:** Membaca dataset penggunaan perangkat smart home (`smart_home_device_usage_data.csv`) baris per baris dan mengirimkannya sebagai stream pesan ke Kafka Server.
2.  **Kafka Server:** Bertindak sebagai message broker yang menampung stream data.
3.  **Kafka Consumer:** Mengkonsumsi data dari Kafka Server dan menyimpan data tersebut dalam bentuk file-file batch CSV.
4.  **Apache Spark:** Membaca file-file batch CSV tersebut untuk melakukan:
    *   Preprocessing data.
    *   Melatih tiga jenis model Machine Learning secara akumulatif (setiap model dilatih ulang dengan data yang lebih banyak seiring masuknya batch baru):
        1.  **Model Klasifikasi Efisiensi Perangkat:** Memprediksi apakah perangkat efisien atau tidak.
        2.  **Model Regresi Potensi Kerusakan:** Memprediksi jumlah potensi insiden kerusakan.
        3.  **Model Clustering Perangkat:** Mengelompokkan perangkat ke dalam segmen-segmen berdasarkan karakteristiknya.
    *   Menyimpan model-model yang telah dilatih.
5.  **Flask API:** Memuat model-model Machine Learning terbaru yang telah dilatih oleh Spark dan menyediakan endpoint RESTful untuk:
    *   Menerima input data perangkat dari pengguna.
    *   Mengembalikan hasil prediksi dari ketiga model.
6.  **Streamlit UI:** Antarmuka pengguna berbasis web sederhana untuk berinteraksi dengan Flask API secara visual.

**Teknologi yang Digunakan:**
*   Apache Kafka: Untuk message queuing dan data streaming.
*   Apache Spark (PySpark): Untuk pemrosesan data batch dan pelatihan model Machine Learning.
*   Flask: Untuk membangun REST API.
*   Streamlit: Untuk membangun antarmuka pengguna (UI) interaktif.
*   Python: Bahasa pemrograman utama.
*   Docker & Docker Compose: Untuk manajemen environment Kafka dan Zookeeper.
*   Mamba/Conda: Untuk manajemen environment Python dan dependensinya.

## Struktur Direktori Proyek
```
Project-2/
├── dataset/
│   └── smart_home_device_usage_data.csv    # Dataset utama
├── kafka_producer.py                       # Skrip Kafka Producer
├── kafka_consumer_batch.py                 # Skrip Kafka Consumer (membuat batch)
├── spark_training.py                       # Skrip Spark untuk training model
├── analyze_clusters.py                     # Skrip untuk analisis hasil clustering (opsional)
├── app_api.py                              # Skrip Flask API
├── app_ui.py                               # Skrip Streamlit UI
├── batched_data/                           # Direktori output Kafka Consumer
├── spark_models/                           # Hasil output Spark (berisi model yang telah dilatih)
└── docker-compose.yml                      # File konfigurasi Docker Compose untuk Kafka & Zookeeper
```

## Langkah-Langkah Penggunaan

### 1. Pra-Requisite

*   **Docker dan Docker Compose:**
*   **Mamba atau Conda:**
*   **Java JDK:**
*   **Apache Spark:**

### 2. Penyiapan Environment Python

1.  Clone repositori
2.  Buat environment Conda/Mamba baru (misalnya, `python_env`):
    ```bash
    mamba create --name python_env python=3.9 # atau versi Python lain yang kompatibel
    mamba activate python_env
    ```
3.  Instal dependensi Python yang dibutuhkan:
    ```bash
    mamba install pandas kafka-python pyspark flask flask-cors scikit-learn joblib requests streamlit -c conda-forge
    ```

### 3. Menjalankan Kafka dengan Docker

1.  Pindah ke direktori proyek Anda yang berisi file `docker-compose.yml`.
2.  Jalankan Kafka dan Zookeeper menggunakan Docker Compose:
    ```bash
    docker-compose up -d
    ```
3.  Verifikasi bahwa kontainer berjalan:
    ```bash
    docker ps
    ```
    Anda akan melihat kontainer untuk Kafka dan Zookeeper.
4.  **(Opsional) Buat topik Kafka secara manual jika belum ada (producer biasanya akan membuatnya secara otomatis jika dikonfigurasi):**
    Ganti `<NAMA_KONTAINER_KAFKA>` dengan nama kontainer Kafka Anda dari output `docker ps`.
    ```bash
    docker exec -it <NAMA_KONTAINER_KAFKA> kafka-topics --create --topic smart_home_stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
    ```
    Verifikasi topik:
    ```bash
    docker exec -it <NAMA_KONTAINER_KAFKA> kafka-topics --list --bootstrap-server localhost:9092
    ```
    Anda seharusnya melihat `smart_home_stream`.

### 4. Menjalankan Alur Data dan Pelatihan Model

Jalankan skrip-skrip berikut secara berurutan, masing-masing di terminal terpisah di dalam direktori proyek dan pastikan environment Mamba `python_env` sudah aktif (`mamba activate python_env`).

1.  **Jalankan Kafka Producer:**
    ```bash
    python kafka_producer.py
    ```
    Producer akan mulai membaca `smart_home_device_usage_data.csv` dan mengirimkan data ke Kafka. Biarkan ini berjalan sampai selesai.

2.  **Jalankan Kafka Consumer (untuk membuat batch):**
    Setelah producer mulai mengirim data (atau telah selesai):
    ```bash
    python kafka_consumer_batch.py
    ```
    Consumer akan membaca pesan dari Kafka dan membuat file-file CSV batch di direktori `batched_data/`. Consumer akan berhenti setelah tidak ada pesan baru selama periode timeout tertentu.

3.  **Jalankan Skrip Pelatihan Spark:**
    Setelah file-file batch dibuat:
    ```bash
    spark-submit spark_training.py
    ```
    Skrip ini akan membaca file batch, melatih tiga jenis model (Klasifikasi Efisiensi, Regresi Malfungsi, Clustering Perangkat) secara akumulatif, dan menyimpan model-model yang telah dilatih di direktori `spark_models/`. Proses ini mungkin memakan waktu.


### 5. (Opsional) Analisis Hasil Clustering

Untuk memahami karakteristik setiap cluster yang dihasilkan oleh model KMeans dan membantu membuat deskripsi yang lebih baik untuk UI:
```bash
spark-submit analyze_clusters.py
```

> Perhatikan output di terminal. Gunakan informasi ini untuk memperbarui dictionary CLUSTER_DESCRIPTIONS di app_ui.py.

### 6. Menjalankan Flask API

Setelah model-model dilatih dan disimpan:

```bash   
python app_api.py
```

Server API akan berjalan (default di http://localhost:5000). API akan secara otomatis memuat set model terbaru dari direktori spark_models/.

### 7. Menggunakan API
Kita dapat berinteraksi dengan API menggunakan tools seperti Postman, curl, atau melalui UI Streamlit.

**Endpoint API:**
- Prediksi Efisiensi Perangkat:
    *   URL: `http://localhost:5000/predict_efficiency`
    *   Metode: `POST`
    *   Respons (JSON): Prediksi kelas efisiensi dan probabilitasnya.

- Prediksi Potensi Kerusakan:
    *   URL: `http://localhost:5000/predict_malfunction`
    *   Metode: `POST`
    *   Respons (JSON): Prediksi jumlah kerusakan.

- Identifikasi Segmen Perangkat:
    *   URL: `http://localhost:5000/get_device_segment`
    *   Metode: `POST`
    *   Respons (JSON): ID cluster perangkat.

### 8. Menjalankan UI (Streamlit)

Jika ingin melihat interface user:
1. Pastikan server API Flask (`app_api.py`) sedang berjalan.
2. Jalankan aplikasi Streamlit:
```bash
streamlit run app_ui.py
```
Aplikasi akan terbuka di browser Anda (seharusnya di `http://localhost:8501`).