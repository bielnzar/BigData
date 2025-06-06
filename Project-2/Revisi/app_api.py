from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

app = Flask(__name__)
CORS(app)

MODEL_BASE_DIR = 'spark_models_traffic/'

model_traffic_density = None
model_peak_hour = None
model_energy_consumption = None

try:
    spark = SparkSession.builder.appName("UrbanTrafficAPI").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Hanya tampilkan error dari Spark di log API
    print("SparkSession for API initialized.")

    # Memuat model-model
    path_density_model = os.path.join(MODEL_BASE_DIR, "traffic_density_model")
    path_peak_model = os.path.join(MODEL_BASE_DIR, "peak_hour_model")
    path_energy_model = os.path.join(MODEL_BASE_DIR, "energy_consumption_model")

    if os.path.exists(path_density_model):
        model_traffic_density = PipelineModel.load(path_density_model)
        print("Traffic Density model loaded successfully.")
    else:
        print(f"WARNING: Traffic Density model not found at {path_density_model}")

    if os.path.exists(path_peak_model):
        model_peak_hour = PipelineModel.load(path_peak_model)
        print("Is Peak Hour model loaded successfully.")
    else:
        print(f"WARNING: Is Peak Hour model not found at {path_peak_model}")

    if os.path.exists(path_energy_model):
        model_energy_consumption = PipelineModel.load(path_energy_model)
        print("Energy Consumption model loaded successfully.")
    else:
        print(f"WARNING: Energy Consumption model not found at {path_energy_model}")

except Exception as e:
    print(f"Error during SparkSession initialization or model loading: {e}")
    spark = None # Tandai Spark tidak tersedia jika ada error

def create_spark_dataframe_for_traffic_model(json_data):
    """
    Membuat Spark DataFrame dari input JSON untuk model traffic.
    Schema harus cocok dengan kolom yang diharapkan oleh pipeline (sebelum transformasi).
    """
    if not spark:
        raise ConnectionError("SparkSession not available in API.")

    # Semua kolom fitur yang mungkin dibutuhkan oleh salah satu dari 3 model
    # Pipeline akan memilih fitur yang relevan berdasarkan konfigurasinya.
    schema = StructType([
        StructField("City", StringType(), True),
        StructField("Vehicle Type", StringType(), True), # Perhatikan spasi di nama kolom
        StructField("Weather", StringType(), True),
        StructField("Economic Condition", StringType(), True), # Spasi
        StructField("Day Of Week", StringType(), True), # Spasi
        StructField("Hour Of Day", IntegerType(), True), # Spasi
        StructField("Speed", DoubleType(), True),
        StructField("Is Peak Hour", IntegerType(), True), # Fitur untuk beberapa, target untuk satu
        StructField("Random Event Occurred", IntegerType(), True), # Spasi
        StructField("Energy Consumption", DoubleType(), True), # Fitur untuk beberapa, target untuk satu
        StructField("Traffic Density", DoubleType(), True) # Fitur untuk beberapa, target untuk satu
    ])
    
    # Buat list of one dictionary untuk createDataFrame
    # Pastikan nama field di json_data persis sama dengan nama kolom di schema ini (termasuk spasi)
    data_list = [{
        "City": json_data.get("City"),
        "Vehicle Type": json_data.get("Vehicle Type"),
        "Weather": json_data.get("Weather"),
        "Economic Condition": json_data.get("Economic Condition"),
        "Day Of Week": json_data.get("Day Of Week"),
        "Hour Of Day": int(json_data.get("Hour Of Day", 0)),
        "Speed": float(json_data.get("Speed", 0.0)),
        "Is Peak Hour": int(json_data.get("Is Peak Hour", 0)), # Sebagai fitur untuk model lain
        "Random Event Occurred": int(json_data.get("Random Event Occurred", 0)),
        "Energy Consumption": float(json_data.get("Energy Consumption", 0.0)), # Sebagai fitur untuk model lain
        "Traffic Density": float(json_data.get("Traffic Density", 0.0)) # Sebagai fitur untuk model lain
    }]
    
    return spark.createDataFrame(data_list, schema=schema)

@app.route('/', methods=['GET'])
def health_check():
    model_status = {
        "traffic_density_model_loaded": model_traffic_density is not None,
        "peak_hour_model_loaded": model_peak_hour is not None,
        "energy_consumption_model_loaded": model_energy_consumption is not None
    }
    return jsonify({
        "status": "API is running", 
        "message": "Welcome to Urban Traffic Analytics API!",
        "models_status": model_status
        }), 200

@app.route('/predict_traffic_density', methods=['POST'])
def api_predict_traffic_density():
    if not model_traffic_density:
        return jsonify({"error": "Traffic Density model is not loaded."}), 503
    try:
        data = request.get_json()
        if not data: return jsonify({"error": "Request body must be JSON"}), 400

        input_df = create_spark_dataframe_for_traffic_model(data)
        prediction_df = model_traffic_density.transform(input_df)
        result = prediction_df.select("prediction").first()[0]
        return jsonify({"predicted_traffic_density": round(float(result), 4)})
    except Exception as e:
        print(f"Error in /predict_traffic_density: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/predict_is_peak_hour', methods=['POST'])
def api_predict_is_peak_hour():
    if not model_peak_hour:
        return jsonify({"error": "Is Peak Hour model is not loaded."}), 503
    try:
        data = request.get_json()
        if not data: return jsonify({"error": "Request body must be JSON"}), 400

        input_df = create_spark_dataframe_for_traffic_model(data)
        prediction_df = model_peak_hour.transform(input_df)
        result = prediction_df.select("prediction").first()[0]
        
        probability_value = None
        if "probability" in prediction_df.columns:
            prob_vector = prediction_df.select("probability").first()[0]
            probability_value = float(prob_vector[int(result)])

        return jsonify({
            "predicted_is_peak_hour": int(result), # 0 atau 1
            "label": "Peak Hour" if int(result) == 1 else "Not Peak Hour",
            "probability_of_predicted_class": round(probability_value, 4) if probability_value is not None else None
        })
    except Exception as e:
        print(f"Error in /predict_is_peak_hour: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/predict_energy_consumption', methods=['POST'])
def api_predict_energy_consumption():
    if not model_energy_consumption:
        return jsonify({"error": "Energy Consumption model is not loaded."}), 503
    try:
        data = request.get_json()
        if not data: return jsonify({"error": "Request body must be JSON"}), 400
            
        input_df = create_spark_dataframe_for_traffic_model(data)
        prediction_df = model_energy_consumption.transform(input_df)
        result = prediction_df.select("prediction").first()[0]
        return jsonify({"predicted_energy_consumption": round(float(result), 4)})
    except Exception as e:
        print(f"Error in /predict_energy_consumption: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    if not spark:
        print("Flask API could not start due to SparkSession initialization failure.")
    else:
        print(f"Models loaded: Density={model_traffic_density is not None}, Peak={model_peak_hour is not None}, Energy={model_energy_consumption is not None}")
        print("Starting Flask API server on http://0.0.0.0:5000 ...")
        app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False) 