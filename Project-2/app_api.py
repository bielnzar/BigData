import pandas as pd
import os
import glob
from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

app = Flask(__name__)
CORS(app)

# Inisialisasi SparkSession global untuk API
try:
    spark = SparkSession.builder.appName("SmartHomeAPI").getOrCreate()
    print("SparkSession for API initialized.")
except Exception as e:
    print(f"Error initializing SparkSession for API: {e}")
    spark = None

MODEL_BASE_DIR = 'spark_models/'
LATEST_SET_NUM = None

model_efficiency = None
model_malfunction = None
model_clustering = None

def load_latest_models():
    global LATEST_SET_NUM, model_efficiency, model_malfunction, model_clustering
    
    if not os.path.exists(MODEL_BASE_DIR):
        print(f"Model directory {MODEL_BASE_DIR} not found. Cannot load models.")
        return

    model_sets = []
    for item in os.listdir(MODEL_BASE_DIR):
        if item.startswith("efficiency_model_set_") and os.path.isdir(os.path.join(MODEL_BASE_DIR, item)):
            try:
                set_num = int(item.split("_")[-1])
                model_sets.append(set_num)
            except ValueError:
                continue
    
    if not model_sets:
        print("No trained model sets found in {MODEL_BASE_DIR}")
        return

    LATEST_SET_NUM = max(model_sets)
    print(f"Attempting to load models from set {LATEST_SET_NUM}...")

    model_eff_path = os.path.join(MODEL_BASE_DIR, f"efficiency_model_set_{LATEST_SET_NUM}")
    model_malf_path = os.path.join(MODEL_BASE_DIR, f"malfunction_model_set_{LATEST_SET_NUM}")
    model_cluster_path = os.path.join(MODEL_BASE_DIR, f"clustering_model_set_{LATEST_SET_NUM}")

    try:
        if os.path.exists(model_eff_path):
            model_efficiency = PipelineModel.load(model_eff_path)
            print(f"Efficiency model (Set {LATEST_SET_NUM}) loaded successfully.")
        else:
            print(f"Efficiency model path not found: {model_eff_path}")

        if os.path.exists(model_malf_path):
            model_malfunction = PipelineModel.load(model_malf_path)
            print(f"Malfunction model (Set {LATEST_SET_NUM}) loaded successfully.")
        else:
            print(f"Malfunction model path not found: {model_malf_path}")
            
        if os.path.exists(model_cluster_path):
            model_clustering = PipelineModel.load(model_cluster_path)
            print(f"Clustering model (Set {LATEST_SET_NUM}) loaded successfully.")
        else:
            print(f"Clustering model path not found: {model_cluster_path}")
    except Exception as e:
        print(f"Error loading one or more Spark models for set {LATEST_SET_NUM}: {e}")

if spark:
    load_latest_models()

def create_spark_dataframe_from_request(json_data):
    """
    Membuat Spark DataFrame dari data JSON input tunggal.
    Pipeline yang dilatih akan menangani semua transformasi fitur.
    Schema di sini harus mencerminkan kolom mentah yang diharapkan pipeline.
    """
    if not spark:
        raise ConnectionError("SparkSession not available in API.")

    schema = StructType([
        StructField("DeviceType", StringType(), True),
        StructField("UsageHoursPerDay", DoubleType(), True),
        StructField("EnergyConsumption", DoubleType(), True),
        StructField("UserPreferences", IntegerType(), True),
        StructField("MalfunctionIncidents", IntegerType(), True),
        StructField("DeviceAgeMonths", IntegerType(), True)
    ])
    
    # Buat list of one dictionary untuk createDataFrame
    data_list = [{
        "DeviceType": json_data.get("DeviceType"),
        "UsageHoursPerDay": float(json_data.get("UsageHoursPerDay", 0.0)),
        "EnergyConsumption": float(json_data.get("EnergyConsumption", 0.0)),
        "UserPreferences": int(json_data.get("UserPreferences", 0)),
        "MalfunctionIncidents": int(json_data.get("MalfunctionIncidents", 0)), # Model efisiensi perlu ini sebagai fitur
        "DeviceAgeMonths": int(json_data.get("DeviceAgeMonths", 0))
    }]
    
    return spark.createDataFrame(data_list, schema=schema)


@app.route('/predict_efficiency', methods=['POST'])
def api_predict_efficiency():
    if not model_efficiency:
        return jsonify({"error": "Efficiency model is not loaded or not found."}), 503
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400

        input_df = create_spark_dataframe_from_request(data)
        
        prediction_df = model_efficiency.transform(input_df)
        
        # Ambil hasil prediksi (kolom 'prediction' default)
        result = prediction_df.select("prediction").first()[0]
        
        # Ambil probabilitas (kolom 'probability' default)
        probability_value = None
        if "probability" in prediction_df.columns:
            prob_vector = prediction_df.select("probability").first()[0]
            probability_value = float(prob_vector[int(result)])

        return jsonify({
            "predicted_efficiency_class": int(result), # 0 atau 1
            "label": "Efisien" if int(result) == 1 else "Tidak Efisien",
            "probability_of_predicted_class": probability_value
        })
    except Exception as e:
        print(f"Error in /predict_efficiency: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/predict_malfunction', methods=['POST'])
def api_predict_malfunction():
    if not model_malfunction:
        return jsonify({"error": "Malfunction model is not loaded or not found."}), 503
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400
        
        # Untuk model malfungsi, 'MalfunctionIncidents' adalah target, jadi tidak boleh ada di input fitur
        input_df = create_spark_dataframe_from_request(data) # Pipeline akan memilih fitur yang benar

        prediction_df = model_malfunction.transform(input_df)
        result = prediction_df.select("prediction").first()[0]
        return jsonify({"predicted_malfunctions": round(float(result), 2)})
    except Exception as e:
        print(f"Error in /predict_malfunction: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/get_device_segment', methods=['POST'])
def api_get_device_segment():
    if not model_clustering:
        return jsonify({"error": "Clustering model is not loaded or not found."}), 503
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400
            
        input_df = create_spark_dataframe_from_request(data)
        
        prediction_df = model_clustering.transform(input_df)
        # Kolom output KMeans
        result = prediction_df.select("prediction").first()[0] 
        return jsonify({"device_segment_id": int(result)})
    except Exception as e:
        print(f"Error in /get_device_segment: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({
        "status": "API is running", 
        "message": "Welcome to Smart Home Analytics API!",
        "loaded_model_set": LATEST_SET_NUM # Menampilkan set model yang dimuat
        }), 200

if __name__ == '__main__':
    if not spark:
        print("Flask API could not start due to SparkSession initialization failure.")
    else:
        print("Starting Flask API server on port 5000...")
        app.run(host='0.0.0.0', port=5000, debug=True)
        # spark.stop() 