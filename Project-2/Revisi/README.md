# Proyek 2 - Big Data: Analisis Kepadatan Lalu Lintas Perkotaan dengan Kafka & Spark

## 🚗 Overview Proyek

Proyek ini bertujuan untuk membangun dan mensimulasikan sebuah sistem Big Data end-to-end untuk analisis lalu lintas di lingkungan perkotaan. Sistem ini menggunakan Apache Kafka untuk menangani streaming data lalu lintas, Apache Spark untuk pemrosesan data secara batch dan pelatihan model Machine Learning, serta Flask API yang diekspos melalui interface pengguna Streamlit untuk interaksi dan visualisasi prediksi.

Dataset yang digunakan ["Urban Traffic Density in Cities"](https://www.kaggle.com/datasets/tanishqdublish/urban-traffic-density-in-cities) dari Kaggle, yang berisi lebih dari 1.2 juta catatan snapshot lalu lintas dari enam kota fiksi, mencakup berbagai faktor seperti tipe kendaraan, kondisi cuaca, kondisi ekonomi, waktu, kecepatan, hingga kepadatan lalu lintas.

**Alur Kerja Sistem:**

1.  **Kafka Producer (`kafka_producer.py`):** Membaca dataset `futuristic_city_traffic.csv` secara sekuensial dan mengirimkan setiap baris data sebagai pesan JSON ke topik Kafka (`urban_traffic_stream`), mensimulasikan aliran data real-time.
2.  **Apache Kafka (via Docker):** Bertindak sebagai message broker terpusat, menerima dan menyimpan stream data dari producer.
3.  **Kafka Consumer (`kafka_consumer_batch.py`):** Mengkonsumsi data dari topik Kafka dan menyimpan data tersebut dalam bentuk file-file batch CSV (misalnya, setiap 100.000 record) di direktori `batched_data_traffic/`.
4.  **Apache Spark (`spark_training.py`):**
    *   Membaca semua file batch CSV yang dihasilkan.
    *   Membagi keseluruhan dataset menjadi tiga bagian yang berbeda.
    *   Melatih tiga model Machine Learning yang berbeda, masing-masing pada satu bagian data:
        1.  **Model Prediksi Kepadatan Lalu Lintas (Regresi):** Dilatih pada bagian pertama data untuk memprediksi nilai `Traffic Density`. Menggunakan `RandomForestRegressor`.
        2.  **Model Prediksi Jam Sibuk (Klasifikasi Biner):** Dilatih pada bagian kedua data untuk memprediksi `Is Peak Hour` (Ya/Tidak). Menggunakan `RandomForestClassifier`.
        3.  **Model Prediksi Konsumsi Energi (Regresi):** Dilatih pada bagian ketiga data untuk memprediksi `Energy Consumption`. Menggunakan `RandomForestRegressor`.
    *   Menyimpan ketiga model yang telah dilatih ke direktori `spark_models_traffic/`.
5.  **Flask API (`app_api.py`):**
    *   Memuat ketiga model Machine Learning yang telah dilatih dari `spark_models_traffic/`.
    *   Menyediakan tiga endpoint RESTful (`/predict_traffic_density`, `/predict_is_peak_hour`, `/predict_energy_consumption`) untuk menerima data input dan mengembalikan prediksi dari masing-masing model.
6.  **Streamlit UI (`app_ui.py`):**
    *   Menyediakan antarmuka pengguna berbasis web yang interaktif.
    *   Memungkinkan pengguna untuk memasukkan parameter lalu lintas.
    *   Mengirimkan input ke Flask API dan menampilkan hasil prediksi secara visual dan mudah dipahami.

**Teknologi yang Digunakan:**

*   Apache Kafka & Zookeeper (dijalankan via Docker)
*   Apache Spark (PySpark)
*   Flask (untuk REST API)
*   Streamlit (untuk User Interface)
*   Python 3.9+
*   Pandas
*   Mamba/Conda (untuk manajemen environment)
*   Docker & Docker Compose

## 🏗️ Struktur Proyek
```
Project-2/
├── dataset/
│ └── futuristic_city_traffic.csv # Dataset utama
├── batched_data_traffic/ # Dibuat oleh kafka_consumer_batch.py
│ └── urban_traffic_batch_*.csv
├── spark_models_traffic/ # Dibuat oleh spark_training.py
│ └── traffic_density_model/
│ └── peak_hour_model/
│ └── energy_consumption_model/
├── kafka_producer.py
├── kafka_consumer_batch.py
├── spark_training.py
├── app_api.py
├── app_ui.py
└── docker-compose.yml
```


## 🚀 Langkah-Langkah Penggunaan

### 1. Prerequisite

*   **Docker dan Docker Compose:** Terinstal dan berjalan.
*   **Mamba atau Conda:** Terinstal.
*   **Java JDK:** Versi 11 atau 17 direkomendasikan (Spark membutuhkannya).
    *   Pastikan `JAVA_HOME` terkonfigurasi dengan benar.
*   **Apache Spark:** Terinstal secara sistem atau Anda akan menggunakan instalasi PySpark dari environment Conda.
    *   Pastikan `SPARK_HOME` terkonfigurasi dan `$SPARK_HOME/bin` ada di `PATH`.

### 2. Siapkan Environment Python

1.  **Clone Repositori (jika ada) atau Unduh Kode Proyek.**
2.  **Buat Direktori Dataset:**
    Di dalam direktori root proyek, buat folder `dataset` dan letakkan file `futuristic_city_traffic.csv` di dalamnya.
3.  **Buat Environment Conda/Mamba:**
    ```bash
    mamba create --name urban_traffic_env python=3.9 # Ganti nama env jika perlu
    mamba activate urban_traffic_env
    ```
4.  **Instal Dependensi Python:**
    ```bash
    mamba install pandas kafka-python pyspark flask flask-cors scikit-learn joblib requests streamlit -c conda-forge
    ```

### 3. Menjalankan Kafka dengan Docker

1.  Navigasi ke direktori root proyek Anda (yang berisi `docker-compose.yml`).
2.  Mulai layanan Kafka dan Zookeeper:
    ```bash
    docker-compose up -d
    ```
3.  Verifikasi kontainer berjalan: `docker ps` (Anda akan melihat `zookeeper_urban_traffic` dan `kafka_urban_traffic`).
4.  **Buat Topik Kafka `urban_traffic_stream`:**
    Ganti `kafka_urban_traffic` dengan nama kontainer Kafka Anda jika berbeda.
    ```bash
    docker exec -it kafka_urban_traffic kafka-topics --create --topic urban_traffic_stream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
    ```
    Verifikasi topik:
    ```bash
    docker exec -it kafka_urban_traffic kafka-topics --list --bootstrap-server localhost:9092
    ```

### 4. Menjalankan Alur Pemrosesan Data dan Pelatihan Model

Lakukan langkah-langkah berikut secara berurutan. Setiap skrip Python sebaiknya dijalankan di terminal terpisah dari dalam direktori root proyek, dengan environment Mamba (`urban_traffic_env`) yang sudah diaktifkan.

1.  **Jalankan Kafka Producer:**
    ```bash
    python kafka_producer.py
    ```
    Producer akan membaca dataset dan mengirimkannya ke topik Kafka. Proses ini akan memakan waktu karena ukuran dataset yang besar. Perhatikan output log untuk melihat kemajuannya.

2.  **Jalankan Kafka Consumer (untuk membuat batch):**
    Setelah producer mulai mengirim data (atau setelah selesai):
    ```bash
    python kafka_consumer_batch.py
    ```
    Consumer akan membuat file-file batch CSV (masing-masing berisi 100.000 baris) di direktori `batched_data_traffic/`. Skrip ini akan berhenti setelah timeout jika tidak ada pesan baru.

3.  **Jalankan Skrip Pelatihan Spark:**
    Setelah semua file batch data selesai dibuat:
    ```bash
    spark-submit spark_training.py
    ```
    Skrip ini akan membaca semua file batch, membagi data menjadi tiga bagian, melatih tiga model machine learning, dan menyimpannya di `spark_models_traffic/`. Ini juga akan memakan waktu.

### 5. Menjalankan Flask API

Setelah model-model dilatih dan disimpan:
```bash
python app_api.py
```

Server API akan dimulai dan berjalan di http://localhost:5000 (atau http://0.0.0.0:5000). API akan otomatis memuat model-model yang telah dilatih.

### 6. Menggunakan Aplikasi via Streamlit UI

1. Pastikan server API Flask (app_api.py) sedang berjalan.
2. Jalankan aplikasi Streamlit:
```bash
streamlit run app_ui.py
```
Antarmuka pengguna akan terbuka di browser Anda (biasanya http://localhost:8501). Anda dapat memasukkan parameter untuk mendapatkan prediksi dari ketiga model.

### 7. Menghentikan Layanan
Untuk menghentikan UI Streamlit atau API Flask: Tekan `Ctrl+C` di terminal masing-masing.

Untuk menghentikan Kafka dan Zookeeper:
```bash
docker-compose down
```
Jika Anda ingin menghapus volume data Kafka juga (data topik akan hilang):
```bash
docker-compose down -v
```

### Detail Endpoint API

Semua endpoint menerima request `POST` dengan body JSON.

**1. `/predict_traffic_density`**
- Memprediksi kepadatan lalu lintas (nilai numerik kontinu).
- Contoh Payload:
```bash
{
    "City": "SolarisVille",
    "Vehicle Type": "Drone",
    "Weather": "Snowy",
    "Economic Condition": "Stable",
    "Day Of Week": "Sunday",
    "Hour Of Day": 20,
    "Speed": 29.42,
    "Is Peak Hour": 0,
    "Random Event Occurred": 0,
    "Energy Consumption": 14.71
}
```

**2. `/predict_is_peak_hour`**
- Memprediksi apakah kondisi saat ini adalah jam sibuk (0 atau 1).
- Contoh Payload:
```bash
{
    "City": "Ecoopolis",
    "Vehicle Type": "Car",
    "Weather": "Clear",
    "Economic Condition": "Booming",
    "Day Of Week": "Monday",
    "Hour Of Day": 8,
    "Speed": 45.0,
    "Random Event Occurred": 0,
    "Energy Consumption": 30.5,
    "Traffic Density": 0.75
}
```

**3. `/predict_energy_consumption`**
- Memprediksi konsumsi energi kendaraan (nilai numerik kontinu).
- Contoh Payload:
```bash
{
    "City": "TechHaven",
    "Vehicle Type": "Autonomous Vehicle",
    "Weather": "Rainy",
    "Economic Condition": "Recession",
    "Day Of Week": "Tuesday",
    "Hour Of Day": 15,
    "Speed": 80.0,
    "Is Peak Hour": 1,
    "Random Event Occurred": 0,
    "Traffic Density": 0.6
}
```
 > Sekian, Semoga Bermanfaat:)