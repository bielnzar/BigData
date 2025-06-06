from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator
import os
import glob

BATCH_DATA_DIR = 'batched_data_traffic/' 
MODEL_OUTPUT_DIR = 'spark_models_traffic/'
BATCH_FILE_PATTERN = os.path.join(BATCH_DATA_DIR, 'urban_traffic_batch_*.csv')

if not os.path.exists(MODEL_OUTPUT_DIR):
    os.makedirs(MODEL_OUTPUT_DIR)
    print(f"Created directory: {MODEL_OUTPUT_DIR}")

def train_traffic_models():
    spark = SparkSession.builder.appName("UrbanTrafficModelTraining").getOrCreate()
    spark.sparkContext.setLogLevel("WARN") # Mengurangi log INFO

    all_batch_files = sorted(glob.glob(BATCH_FILE_PATTERN))
    if not all_batch_files:
        print(f"Tidak ada file batch ditemukan dengan pola '{BATCH_FILE_PATTERN}' di '{BATCH_DATA_DIR}'.")
        print("Pastikan kafka_consumer_batch.py sudah berjalan dan menghasilkan file.")
        spark.stop()
        return

    print(f"Membaca {len(all_batch_files)} file batch dari {BATCH_DATA_DIR}...")
    
    df_full = spark.read.option("header", "true").option("inferSchema", "true").csv(all_batch_files)
    
    total_rows = df_full.count()
    if total_rows < 100:
        print(f"Total data hanya {total_rows} baris. Terlalu sedikit untuk melanjutkan training. Hentikan.")
        spark.stop()
        return
        
    print(f"Total baris data yang digabungkan: {total_rows}")
    print("Skema data gabungan:")
    df_full.printSchema()

    # Hapus duplikat jika ada (opsional, tergantung kualitas dataset)
    # df_full = df_full.dropDuplicates()
    # print(f"Total baris setelah dropDuplicates: {df_full.count()}")

    # Pembagian data menjadi 3 bagian dengan proporsi yang mendekati sama
    # (df_part1, df_part2, df_part3) = df_full.randomSplit([1.0/3, 1.0/3, 1.0/3], seed=42)
    # Untuk memastikan setiap model dilatih pada data yang cukup besar, dan untuk simplicity
    # kita bisa juga melatih setiap model pada keseluruhan data yang telah di-split untuk train/test.
    # Namun, sesuai permintaan Anda untuk membagi dataset menjadi 3 bagian untuk 3 model:
    # Ini akan melatih setiap model pada subset data yang berbeda.
    
    # Menggunakan bobot yang sedikit berbeda untuk memastikan semua data terpakai
    splits = df_full.randomSplit([0.33, 0.33, 0.34], seed=12345)
    df_part1 = splits[0].cache()
    df_part2 = splits[1].cache()
    df_part3 = splits[2].cache()

    print(f"Data Part 1 (untuk Model Traffic Density): {df_part1.count()} baris")
    print(f"Data Part 2 (untuk Model Is Peak Hour): {df_part2.count()} baris")
    print(f"Data Part 3 (untuk Model Energy Consumption): {df_part3.count()} baris")

    # --- Tahapan Preprocessing ---
    categorical_cols = ["City", "Vehicle Type", "Weather", "Economic Condition", "Day Of Week"]
    
    indexers = [StringIndexer(inputCol=col_name, outputCol=col_name + "_Index", handleInvalid="keep") 
                for col_name in categorical_cols]
    
    encoder_input_cols = [indexer.getOutputCol() for indexer in indexers]
    encoder_output_cols = [col_name + "_Vec" for col_name in categorical_cols]
    encoder = OneHotEncoder(inputCols=encoder_input_cols, outputCols=encoder_output_cols, handleInvalid="keep") # handleInvalid='keep'

    # --- Model 1: Prediksi Traffic Density (Regresi) ---
    # Dilatih pada df_part1
    print("\n--- Training Model 1: Prediksi Traffic Density (dilatih pada bagian 1 data) ---")
    target_col_density = "Traffic Density"
    numerical_features_density = ["Hour Of Day", "Speed", "Is Peak Hour", "Random Event Occurred", "Energy Consumption"]
    
    assembler_inputs_density = encoder_output_cols + numerical_features_density
    # Pastikan tidak ada duplikasi kolom atau kolom target masuk ke assembler_inputs
    assembler_density = VectorAssembler(inputCols=assembler_inputs_density, outputCol="features_unscaled_density", handleInvalid="skip")
    scaler_density = StandardScaler(inputCol="features_unscaled_density", outputCol="features_density")
    
    rf_density = RandomForestRegressor(featuresCol="features_density", labelCol=target_col_density, seed=42)
    pipeline_density_stages = indexers + [encoder, assembler_density, scaler_density, rf_density]
    pipeline_density = Pipeline(stages=pipeline_density_stages)
    
    model_density = pipeline_density.fit(df_part1)
    model_density.write().overwrite().save(os.path.join(MODEL_OUTPUT_DIR, "traffic_density_model"))
    print(f"  Model Traffic Density disimpan ke {os.path.join(MODEL_OUTPUT_DIR, 'traffic_density_model')}")

    # --- Model 2: Prediksi Is Peak Hour (Klasifikasi Biner) ---
    # Dilatih pada df_part2
    print("\n--- Training Model 2: Prediksi Is Peak Hour (dilatih pada bagian 2 data) ---")
    target_col_peak = "Is Peak Hour" # Sudah integer 0 atau 1
    numerical_features_peak = ["Hour Of Day", "Speed", "Random Event Occurred", "Energy Consumption", "Traffic Density"]

    assembler_inputs_peak = encoder_output_cols + numerical_features_peak
    assembler_peak = VectorAssembler(inputCols=assembler_inputs_peak, outputCol="features_unscaled_peak", handleInvalid="skip")
    scaler_peak = StandardScaler(inputCol="features_unscaled_peak", outputCol="features_peak")

    rf_peak = RandomForestClassifier(featuresCol="features_peak", labelCol=target_col_peak, seed=42)
    pipeline_peak_stages = indexers + [encoder, assembler_peak, scaler_peak, rf_peak]
    pipeline_peak = Pipeline(stages=pipeline_peak_stages)

    model_peak = pipeline_peak.fit(df_part2)
    model_peak.write().overwrite().save(os.path.join(MODEL_OUTPUT_DIR, "peak_hour_model"))
    print(f"  Model Is Peak Hour disimpan ke {os.path.join(MODEL_OUTPUT_DIR, 'peak_hour_model')}")

    # --- Model 3: Prediksi Energy Consumption (Regresi) ---
    # Dilatih pada df_part3
    print("\n--- Training Model 3: Prediksi Energy Consumption (dilatih pada bagian 3 data) ---")
    target_col_energy = "Energy Consumption"
    # Fitur: Semua kecuali target_col_energy
    numerical_features_energy = ["Hour Of Day", "Speed", "Is Peak Hour", "Random Event Occurred", "Traffic Density"]
    
    assembler_inputs_energy = encoder_output_cols + numerical_features_energy
    assembler_energy = VectorAssembler(inputCols=assembler_inputs_energy, outputCol="features_unscaled_energy", handleInvalid="skip")
    scaler_energy = StandardScaler(inputCol="features_unscaled_energy", outputCol="features_energy")

    # Menggunakan RandomForestRegressor untuk model energi
    rf_energy = RandomForestRegressor(featuresCol="features_energy", labelCol=target_col_energy, seed=42) # <--- SUDAH BENAR
    pipeline_energy_stages = indexers + [encoder, assembler_energy, scaler_energy, rf_energy]
    pipeline_energy = Pipeline(stages=pipeline_energy_stages)
    
    model_energy = pipeline_energy.fit(df_part3)
    model_energy.write().overwrite().save(os.path.join(MODEL_OUTPUT_DIR, "energy_consumption_model"))
    print(f"  Model Energy Consumption disimpan ke {os.path.join(MODEL_OUTPUT_DIR, 'energy_consumption_model')}")

    df_part1.unpersist()
    df_part2.unpersist()
    df_part3.unpersist()
    
    spark.stop()
    print("\nPelatihan semua model selesai.")

if __name__ == "__main__":
    train_traffic_models()