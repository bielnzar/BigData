# analyze_clusters.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
import os
import glob

# --- Konfigurasi ---
MODEL_BASE_DIR = 'spark_models/'
# GANTI DENGAN NOMOR SET MODEL CLUSTERING TERAKHIR YANG INGIN DIANALISIS
# Cara otomatis menentukan LATEST_SET_NUM:
latest_set_num_found = None
if os.path.exists(MODEL_BASE_DIR):
    model_sets_nums = []
    for item in os.listdir(MODEL_BASE_DIR):
        if item.startswith("clustering_model_set_") and os.path.isdir(os.path.join(MODEL_BASE_DIR, item)):
            try:
                set_num = int(item.split("_")[-1])
                model_sets_nums.append(set_num)
            except ValueError:
                continue
    if model_sets_nums:
        latest_set_num_found = max(model_sets_nums)

LATEST_SET_NUM = latest_set_num_found # Gunakan yang ditemukan, atau set manual jika gagal
# LATEST_SET_NUM = 6 # Atau override manual jika perlu

BATCH_DATA_DIR_ANALYSIS = 'batched_data/'
BATCH_FILE_PATTERN_ANALYSIS = os.path.join(BATCH_DATA_DIR_ANALYSIS, 'smart_home_batch_*.csv')

# Kolom asli yang digunakan sebagai fitur saat training model clustering
# Pastikan ini SAMA PERSIS dengan yang Anda gunakan di pipeline clustering di spark_training.py
# Terutama nama kolom setelah StringIndexer/OneHotEncoder jika Anda menganalisis fitur transformed,
# TAPI untuk interpretasi pengguna, kita ingin fitur asli.
ORIGINAL_FEATURE_COLS_FOR_INTERPRETATION = [
    "DeviceType", 
    "UsageHoursPerDay", 
    "EnergyConsumption", 
    "UserPreferences", 
    "MalfunctionIncidents", 
    "DeviceAgeMonths"
]

def analyze_clusters():
    if LATEST_SET_NUM is None:
        print("Tidak dapat menentukan LATEST_SET_NUM. Periksa direktori model atau set manual.")
        return

    print(f"Memulai analisis untuk model clustering set nomor: {LATEST_SET_NUM}")

    spark_analyzer = SparkSession.builder.appName(f"ClusterAnalysis_Set{LATEST_SET_NUM}").getOrCreate()
    
    # --- Atur Level Log Spark ---
    spark_analyzer.sparkContext.setLogLevel("WARN") # Pilihan: "INFO", "WARN", "ERROR"
    print("Level log Spark diatur ke WARN.")

    model_cluster_path = os.path.join(MODEL_BASE_DIR, f"clustering_model_set_{LATEST_SET_NUM}")
    if not os.path.exists(model_cluster_path):
        print(f"Model clustering tidak ditemukan di: {model_cluster_path}")
        spark_analyzer.stop()
        return

    print(f"Memuat model clustering dari: {model_cluster_path}")
    try:
        loaded_clustering_model = PipelineModel.load(model_cluster_path)
        print("Model clustering berhasil dimuat.")
    except Exception as e:
        print(f"Error saat memuat model clustering: {e}")
        spark_analyzer.stop()
        return
    
    # --- Menggabungkan Semua File Batch untuk Analisis ---
    all_batch_files = sorted(glob.glob(BATCH_FILE_PATTERN_ANALYSIS))
    
    if not all_batch_files:
        print(f"Tidak ada file batch ditemukan dengan pola {BATCH_FILE_PATTERN_ANALYSIS} di {BATCH_DATA_DIR_ANALYSIS}.")
        spark_analyzer.stop()
        return

    print(f"\nDitemukan file batch untuk analisis: {len(all_batch_files)} file.")
    # print(all_batch_files) # Uncomment untuk melihat daftar file

    df_for_analysis = None
    for i, file_path in enumerate(all_batch_files):
        print(f"  Membaca file batch {i+1}/{len(all_batch_files)}: {os.path.basename(file_path)}")
        try:
            batch_df = spark_analyzer.read.csv(file_path, header=True, inferSchema=True)
            if 'UserID' in batch_df.columns: # UserID tidak relevan untuk analisis cluster general
                batch_df = batch_df.drop('UserID')
            
            if df_for_analysis is None:
                df_for_analysis = batch_df
            else:
                df_for_analysis = df_for_analysis.unionByName(batch_df)
        except Exception as e_read:
            print(f"    Error membaca file {file_path}: {e_read}")
            continue
            
    if df_for_analysis is None or df_for_analysis.count() == 0:
        print("Tidak ada data yang valid untuk dianalisis setelah mencoba membaca semua batch.")
        spark_analyzer.stop()
        return

    print(f"\nTotal data gabungan untuk analisis cluster: {df_for_analysis.count()} baris.")
    print("Skema data gabungan:")
    df_for_analysis.printSchema()

    # --- Mendapatkan Prediksi Cluster ---
    print("\nMelakukan transformasi data untuk mendapatkan prediksi cluster...")
    try:
        clustered_data = loaded_clustering_model.transform(df_for_analysis)
        print("Transformasi selesai.")
        print("\nContoh Data Hasil Clustering (5 baris, semua kolom):")
        clustered_data.show(5, truncate=False)
    except Exception as e_transform:
        print(f"Error saat melakukan transformasi dengan model clustering: {e_transform}")
        print("Pastikan skema data input sesuai dengan yang diharapkan model.")
        df_for_analysis.show(5, truncate=False) # Tampilkan data input jika error
        spark_analyzer.stop()
        return

    # --- Analisis Pusat Cluster (Jika KMeans) ---
    # Tahap terakhir dari pipeline harusnya model KMeans
    try:
        kmeans_model_in_pipeline = loaded_clustering_model.stages[-1]
        if hasattr(kmeans_model_in_pipeline, 'clusterCenters'):
            print("\nPusat Cluster (dalam skala fitur setelah scaling oleh pipeline):")
            centers = kmeans_model_in_pipeline.clusterCenters()
            for i, center_vec in enumerate(centers):
                print(f"  Pusat Cluster {i}: {center_vec}")
        else:
            print("\nTidak dapat mengambil pusat cluster (model mungkin bukan KMeans atau struktur pipeline berbeda).")
    except Exception as e_centers:
        print(f"Error saat mencoba mengambil pusat cluster: {e_centers}")

    # --- Analisis Karakteristik per Cluster (Menggunakan Fitur Asli) ---
    print("\nAnalisis Karakteristik per Cluster (menggunakan fitur asli sebelum scaling):")
    
    if "prediction" not in clustered_data.columns:
        print("CRITICAL ERROR: Kolom 'prediction' (ID cluster) tidak ditemukan di clustered_data.")
        print("Ini biasanya output default dari model KMeans. Periksa output transformasi model Anda.")
        clustered_data.printSchema()
        spark_analyzer.stop()
        return
    
    # Cast kolom prediksi ke integer jika belum (beberapa versi Spark bisa mengembalikan double)
    if dict(clustered_data.dtypes)['prediction'] != 'int':
       print("Warning: Tipe data kolom 'prediction' bukan integer, mencoba cast.")
       clustered_data = clustered_data.withColumn("prediction", col("prediction").cast("integer"))

    try:
        num_clusters = kmeans_model_in_pipeline.getK()
        print(f"\nJumlah K (cluster) pada model yang dimuat: {num_clusters}")
    except Exception as e_getk:
        print(f"Error mendapatkan K dari model: {e_getk}. Mencoba dari data prediksi...")
        # Jika getK() gagal, coba hitung dari data prediksi (kurang ideal)
        distinct_clusters = clustered_data.select("prediction").distinct().collect()
        num_clusters = len(distinct_clusters)
        print(f"Jumlah cluster unik ditemukan di data prediksi: {num_clusters}")


    for cluster_id_val in range(num_clusters):
        print(f"\n--- Karakteristik untuk Cluster ID: {cluster_id_val} ---")
        cluster_subset_df = clustered_data.filter(col("prediction") == cluster_id_val)
        
        count_in_cluster = cluster_subset_df.count()
        print(f"  Jumlah perangkat di cluster ini: {count_in_cluster}")
        
        if count_in_cluster == 0:
            print("  Tidak ada perangkat di cluster ini untuk dianalisis.")
            continue
            
        for feature_col in ORIGINAL_FEATURE_COLS_FOR_INTERPRETATION:
            if feature_col not in cluster_subset_df.columns:
                print(f"  Peringatan: Kolom fitur asli '{feature_col}' tidak ditemukan di DataFrame clustered_data.")
                continue

            if feature_col == "DeviceType": 
                print(f"  Distribusi {feature_col}:")
                try:
                    dist_df = cluster_subset_df.groupBy(feature_col).count().orderBy(col("count").desc())
                    dist_df.show(truncate=False)
                except Exception as e_dist:
                    print(f"    Error menghitung distribusi untuk {feature_col}: {e_dist}")
            elif feature_col == "UserPreferences":
                print(f"  Distribusi {feature_col} (0=Rendah, 1=Tinggi):")
                try:
                    dist_df = cluster_subset_df.groupBy(feature_col).count().orderBy(col("count").desc())
                    dist_df.show(truncate=False)
                except Exception as e_dist:
                    print(f"    Error menghitung distribusi untuk {feature_col}: {e_dist}")
            else: # Kolom numerik
                print(f"  Statistik untuk {feature_col}:")
                try:
                    # Menggunakan backtick untuk nama kolom jika ada spasi atau karakter khusus (tidak dalam kasus ini tapi praktik baik)
                    stats = cluster_subset_df.selectExpr(
                        f"round(mean(`{feature_col}`), 2) as mean_val",
                        f"round(stddev_pop(`{feature_col}`), 2) as std_val",
                        f"min(`{feature_col}`) as min_val",
                        f"max(`{feature_col}`) as max_val",
                        f"percentile_approx(`{feature_col}`, 0.25) as p25_val", # Tambah persentil
                        f"percentile_approx(`{feature_col}`, 0.50) as median_val",
                        f"percentile_approx(`{feature_col}`, 0.75) as p75_val"
                    ).first()
                    
                    if stats and stats['mean_val'] is not None:
                        print(f"    Rata-rata: {stats['mean_val']}")
                        print(f"    Median    : {stats['median_val']}")
                        print(f"    StdDev    : {stats['std_val']}")
                        print(f"    Min       : {stats['min_val']}")
                        print(f"    Max       : {stats['max_val']}")
                        print(f"    P25       : {stats['p25_val']}")
                        print(f"    P75       : {stats['p75_val']}")
                    else:
                        print(f"    Statistik tidak dapat dihitung (mungkin semua nilai null atau error).")
                except Exception as e_stats:
                    print(f"    Error menghitung statistik untuk {feature_col}: {e_stats}")
    
    print("\n--- Analisis cluster selesai. ---")
    print("Gunakan output di atas untuk membuat deskripsi yang bermakna untuk CLUSTER_DESCRIPTIONS di app_ui.py.")
    
    spark_analyzer.stop()
    print("SparkSession untuk analisis dihentikan.")

if __name__ == "__main__":
    analyze_clusters()