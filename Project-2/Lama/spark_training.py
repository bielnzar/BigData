from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator, ClusteringEvaluator
import os
import glob

BATCH_DATA_DIR = 'batched_data/'
MODEL_OUTPUT_DIR = 'spark_models/'
BATCH_FILE_PATTERN = os.path.join(BATCH_DATA_DIR, 'smart_home_batch_*.csv')

if not os.path.exists(MODEL_OUTPUT_DIR):
    os.makedirs(MODEL_OUTPUT_DIR)
    print(f"Created directory: {MODEL_OUTPUT_DIR}")

def train_models():
    spark = SparkSession.builder \
    .appName("SmartHomeModelTraining") \
    .config("spark.sql.legacy.setCommandReplaced", "true") \
    .getOrCreate()
    
    batch_files = sorted(glob.glob(BATCH_FILE_PATTERN))
    
    if not batch_files:
        print(f"No batch files found matching pattern {BATCH_FILE_PATTERN} in {BATCH_DATA_DIR}. Exiting.")
        spark.stop()
        return

    print(f"Found batch files: {batch_files}")

    accumulated_df = None
    
    # Kolom yang akan digunakan untuk fitur dan label
    label_col_eff = "SmartHomeEfficiency"
    label_col_malf = "MalfunctionIncidents"
    
    categorical_col_feature = "DeviceType" # Fitur kategorikal utama
    
    # Fitur numerik (UserPreferences juga numerik 0/1)
    numerical_cols_all = ["UsageHoursPerDay", "EnergyConsumption", "UserPreferences", "DeviceAgeMonths"]
    
    # Fitur untuk Model Efisiensi & Malfungsi akan menyertakan 'MalfunctionIncidents' sebagai fitur untuk efisiensi
    numerical_cols_for_eff = numerical_cols_all + ["MalfunctionIncidents"] # MalfunctionIncidents adalah fitur untuk model efisiensi
    numerical_cols_for_malf = numerical_cols_all[:] # Salin list, karena MalfunctionIncidents adalah target di sini
    numerical_cols_for_cluster = numerical_cols_all + ["MalfunctionIncidents"] # untuk clustering

    # Preprocessing Umum
    # StringIndexer dan OneHotEncoder untuk DeviceType
    string_indexer = StringIndexer(inputCol=categorical_col_feature, outputCol=categorical_col_feature + "_Index", handleInvalid="keep") # 'keep' agar tidak error jika ada kategori baru di test
    encoder = OneHotEncoder(inputCols=[string_indexer.getOutputCol()], outputCols=[categorical_col_feature + "_Vec"])

    # Model Training Loop (Skema B - Akumulatif)
    for i, batch_file_path in enumerate(batch_files):
        print(f"\n--- Processing Batch {i+1}: {batch_file_path} ---")
        
        df_current_batch = spark.read.csv(batch_file_path, header=True, inferSchema=True)

        # Hapus UserID
        if 'UserID' in df_current_batch.columns:
            df_current_batch = df_current_batch.drop('UserID')
        
        # Pastikan tipe data label benar
        if label_col_eff in df_current_batch.columns:
            df_current_batch = df_current_batch.withColumn(label_col_eff, col(label_col_eff).cast("integer"))
        if label_col_malf in df_current_batch.columns:
             df_current_batch = df_current_batch.withColumn(label_col_malf, col(label_col_malf).cast("double")) # Regresi suka double


        if accumulated_df is None:
            accumulated_df = df_current_batch
        else:
            accumulated_df = accumulated_df.unionByName(df_current_batch)
        
        accumulated_df.cache()
        current_total_records = accumulated_df.count()
        print(f"Total accumulated records for training Model Set {i+1}: {current_total_records}")

        if current_total_records < 50:
            print(f"Skipping model training for set {i+1}, not enough data ({current_total_records} records).")
            accumulated_df.unpersist()
            continue

        # 1. Model Klasifikasi Efisiensi
        print(f"\nTraining Efficiency Model (Set {i+1})...")
        assembler_inputs_eff = [encoder.getOutputCols()[0]] + numerical_cols_for_eff
        vector_assembler_eff = VectorAssembler(inputCols=assembler_inputs_eff, outputCol="features_unscaled_eff")
        scaler_eff = StandardScaler(inputCol="features_unscaled_eff", outputCol="features_eff")
        lr_model_eff = LogisticRegression(featuresCol="features_eff", labelCol=label_col_eff)
        pipeline_eff = Pipeline(stages=[string_indexer, encoder, vector_assembler_eff, scaler_eff, lr_model_eff])
        
        (train_eff, test_eff) = accumulated_df.randomSplit([0.8, 0.2], seed=100 + i)
        model_eff = pipeline_eff.fit(train_eff)
        predictions_eff = model_eff.transform(test_eff)
        evaluator_eff_acc = MulticlassClassificationEvaluator(labelCol=label_col_eff, metricName="accuracy")
        accuracy = evaluator_eff_acc.evaluate(predictions_eff)
        print(f"  Efficiency Model (Set {i+1}) Accuracy: {accuracy:.4f}")
        model_eff.write().overwrite().save(os.path.join(MODEL_OUTPUT_DIR, f"efficiency_model_set_{i+1}"))
        print(f"  Saved Efficiency Model (Set {i+1})")

        # 2. Model Regresi Malfungsi
        print(f"\nTraining Malfunction Model (Set {i+1})...")
        assembler_inputs_malf = [encoder.getOutputCols()[0]] + numerical_cols_for_malf
        vector_assembler_malf = VectorAssembler(inputCols=assembler_inputs_malf, outputCol="features_unscaled_malf")
        scaler_malf = StandardScaler(inputCol="features_unscaled_malf", outputCol="features_malf")
        lr_model_malf = LinearRegression(featuresCol="features_malf", labelCol=label_col_malf)
        pipeline_malf = Pipeline(stages=[string_indexer, encoder, vector_assembler_malf, scaler_malf, lr_model_malf])

        (train_malf, test_malf) = accumulated_df.randomSplit([0.8, 0.2], seed=200 + i)
        model_malf = pipeline_malf.fit(train_malf)
        predictions_malf = model_malf.transform(test_malf)
        evaluator_malf_rmse = RegressionEvaluator(labelCol=label_col_malf, metricName="rmse")
        rmse = evaluator_malf_rmse.evaluate(predictions_malf)
        print(f"  Malfunction Model (Set {i+1}) RMSE: {rmse:.4f}")
        model_malf.write().overwrite().save(os.path.join(MODEL_OUTPUT_DIR, f"malfunction_model_set_{i+1}"))
        print(f"  Saved Malfunction Model (Set {i+1})")

        # 3. Model Clustering Perangkat
        print(f"\nTraining Device Clustering Model (Set {i+1})...")
        assembler_inputs_cluster = [encoder.getOutputCols()[0]] + numerical_cols_for_cluster
        vector_assembler_cluster = VectorAssembler(inputCols=assembler_inputs_cluster, outputCol="features_unscaled_cluster")
        scaler_cluster = StandardScaler(inputCol="features_unscaled_cluster", outputCol="features_cluster")
        kmeans = KMeans(featuresCol="features_cluster", k=4, seed=300 + i)
        pipeline_cluster = Pipeline(stages=[string_indexer, encoder, vector_assembler_cluster, scaler_cluster, kmeans])
        
        # Clustering biasanya dilatih pada semua data yang relevan untuk segmentasi
        model_cluster = pipeline_cluster.fit(accumulated_df) 
        
        # Evaluasi Silhouette score
        predictions_cluster_eval = model_cluster.transform(accumulated_df)
        evaluator_cluster = ClusteringEvaluator(featuresCol="features_cluster", predictionCol="prediction")
        silhouette = evaluator_cluster.evaluate(predictions_cluster_eval)
        print(f"  Clustering Model (Set {i+1}) Silhouette Score: {silhouette:.4f}")
        model_cluster.write().overwrite().save(os.path.join(MODEL_OUTPUT_DIR, f"clustering_model_set_{i+1}"))
        print(f"  Saved Clustering Model (Set {i+1})")
        
        accumulated_df.unpersist()

    spark.stop()
    print("Spark session stopped. Model training complete.")

if __name__ == "__main__":
    train_models()