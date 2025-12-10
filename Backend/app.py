from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
import tensorflow as tf
import numpy as np
import pandas as pd
import pickle
import os
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend requests

# --- Configuration ---
LSTM_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'lstm_energy_model.h5')
RF_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'rf_energy_model')
GBT_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'gbt_energy_model')
SCALER_PATH = os.path.join(os.getcwd(), 'models', 'scaler.pkl')
SEQUENCE_LENGTH = 24

# --- Initialize Spark Session ---
spark = SparkSession.builder \
    .appName("EnergyForecastingAPI") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Suppress Spark logs
spark.sparkContext.setLogLevel("ERROR")

# --- Load Models and Scaler ---
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("Power_Consumption", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("current", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

lstm_model = None
rf_model = None
gbt_model = None
scaler = None

try:
    print("Loading models and scaler...")
    lstm_model = tf.keras.models.load_model(LSTM_MODEL_PATH)
    print("✓ LSTM model loaded successfully.")
    
    with open(SCALER_PATH, 'rb') as f:
        scaler = pickle.load(f)
    print("✓ Scaler loaded successfully.")
    
    rf_model = PipelineModel.load(RF_MODEL_PATH)
    print("✓ Random Forest model loaded successfully.")
    
    gbt_model = PipelineModel.load(GBT_MODEL_PATH)
    print("✓ GBT model loaded successfully.")
    
    print("✓ All models and scaler loaded successfully!")
except FileNotFoundError as fe:
    print(f"✗ Model file not found: {fe}")
    print("Note: Models will be loaded when first requested. Continuing with API startup...")
except Exception as e:
    print(f"✗ Error loading models: {e}")
    print("Note: Continuing with API startup. Models will fail at prediction time if not available.")

# --- Preprocessing and Feature Engineering Functions ---

def preprocess_data_spark(spark_df_input):
    """Clean and prepare data for modeling"""
    temp_mean = spark_df_input.select(mean('temperature')).collect()[0][0]
    humidity_mean = spark_df_input.select(mean('humidity')).collect()[0][0]
    
    df_filled = spark_df_input.na.fill({
        'Power_Consumption': 0.0,
        'temperature': temp_mean if temp_mean is not None else 20.0,
        'humidity': humidity_mean if humidity_mean is not None else 50.0
    })

    df_sorted = df_filled.orderBy('timestamp')
    return df_sorted

def create_features_spark(spark_df_input):
    """Create comprehensive feature set"""
    df = spark_df_input

    # Temporal features
    df = df \
        .withColumn("hour", hour("timestamp")) \
        .withColumn("day_of_week", dayofweek("timestamp")) \
        .withColumn("day_of_month", dayofmonth("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("quarter", quarter("timestamp")) \
        .withColumn("year", year("timestamp")) \
        .withColumn("week_of_year", weekofyear("timestamp"))

    # Binary features
    df = df \
        .withColumn("is_weekend",
                   when(col("day_of_week").isin([1, 7]), 1).otherwise(0)) \
        .withColumn("is_business_hours",
                   when((col("hour") >= 9) & (col("hour") <= 17), 1).otherwise(0)) \
        .withColumn("is_peak_hours",
                   when((col("hour") >= 17) & (col("hour") <= 21), 1).otherwise(0))

    window_spec = Window.orderBy("timestamp")

    # Lag features
    for lag_val in [1, 2, 3, 24, 48, 168]:
        df = df.withColumn(
            f"energy_lag_{lag_val}",
            lag("Power_Consumption", lag_val).over(window_spec)
        )

    # Rolling window features (24h)
    window_24h = Window \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(-24*3600, 0)

    df = df \
        .withColumn("energy_mean_24h", avg("Power_Consumption").over(window_24h)) \
        .withColumn("energy_std_24h", stddev("Power_Consumption").over(window_24h)) \
        .withColumn("energy_min_24h", min("Power_Consumption").over(window_24h)) \
        .withColumn("energy_max_24h", max("Power_Consumption").over(window_24h))

    # Rolling window features (7d)
    window_7d = Window \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(-7*24*3600, 0)

    df = df \
        .withColumn("energy_mean_7d", avg("Power_Consumption").over(window_7d)) \
        .withColumn("energy_std_7d", stddev("Power_Consumption").over(window_7d))

    # Weather interaction features
    df = df \
        .withColumn("temp_hour_interaction", col("temperature") * col("hour")) \
        .withColumn("humidity_temp_interaction", col("humidity") * col("temperature"))

    df = df.na.drop(subset=["energy_lag_1", "energy_lag_24"])
    df = df.withColumnRenamed("Power_Consumption", "energy_kwh")

    return df

def prepare_lstm_data_for_inference(spark_df_for_inference, scaler_obj, sequence_length=24):
    """Prepares data for LSTM inference"""
    if spark_df_for_inference.count() < sequence_length:
        raise ValueError(f"Input data must contain at least {sequence_length} records for LSTM inference.")

    pdf = spark_df_for_inference.orderBy('timestamp').toPandas()

    feature_cols = [
        'energy_kwh', 'temperature', 'humidity',
        'hour', 'day_of_week', 'is_weekend', 'is_peak_hours',
        'energy_lag_1', 'energy_lag_24', 'energy_mean_24h'
    ]

    missing_cols = [col for col in feature_cols if col not in pdf.columns]
    if missing_cols:
        raise ValueError(f"Missing feature columns in input data: {missing_cols}")

    scaled_data = scaler_obj.transform(pdf[feature_cols])
    X_inference = scaled_data[-sequence_length:].reshape(1, sequence_length, len(feature_cols))

    return X_inference

# --- HybridEnergyPredictor Class ---

class HybridEnergyPredictorAPI:
    def __init__(self, lstm_model, rf_model, gbt_model, scaler):
        self.lstm_model = lstm_model
        self.rf_model = rf_model
        self.gbt_model = gbt_model
        self.scaler = scaler

    def predict_lstm(self, sequence_data):
        """Make LSTM predictions"""
        if self.lstm_model is None:
            raise ValueError("LSTM model not loaded")
        return self.lstm_model.predict(sequence_data, verbose=0)

    def predict_ensemble(self, spark_df):
        """Make ensemble predictions"""
        if self.rf_model is None or self.gbt_model is None:
            raise ValueError("Ensemble models not loaded")
        rf_pred = self.rf_model.transform(spark_df)
        gbt_pred = self.gbt_model.transform(spark_df)
        return rf_pred, gbt_pred

    def hybrid_predict(self, spark_df_input_for_features, sequence_length=24):
        """Combines LSTM and ensemble predictions"""
        featured_spark_df = create_features_spark(
            preprocess_data_spark(spark_df_input_for_features)
        )

        if featured_spark_df.count() < sequence_length:
            raise ValueError(
                f"Not enough data after feature creation. Required: {sequence_length}, Got: {featured_spark_df.count()}"
            )

        # Get latest row for ensemble models
        latest_row = featured_spark_df.orderBy(col('timestamp').desc()).limit(1).collect()[0]
        ensemble_input_spark_df = spark.createDataFrame([latest_row], schema=featured_spark_df.schema)

        # Prepare LSTM input (last `sequence_length` records)
        lstm_input_pdf = featured_spark_df.orderBy('timestamp').toPandas().tail(sequence_length)
        lstm_input_spark_df = spark.createDataFrame(lstm_input_pdf, schema=featured_spark_df.schema)
        X_lstm = prepare_lstm_data_for_inference(lstm_input_spark_df, self.scaler, sequence_length)

        # Get LSTM prediction
        lstm_pred_scaled = self.predict_lstm(X_lstm)[0][0]

        # Inverse transform LSTM prediction
        dummy_array = np.zeros((1, self.scaler.n_features_in_))
        dummy_array[0, 0] = lstm_pred_scaled
        lstm_pred_original = self.scaler.inverse_transform(dummy_array)[0, 0]

        # Get ensemble predictions
        rf_pred_df = self.rf_model.transform(ensemble_input_spark_df)
        gbt_pred_df = self.gbt_model.transform(ensemble_input_spark_df)
        
        rf_value = rf_pred_df.select('prediction').collect()[0][0]
        gbt_value = gbt_pred_df.select('prediction').collect()[0][0]

        # Weighted ensemble
        weights = {'lstm': 0.5, 'rf': 0.25, 'gbt': 0.25}
        final_pred = (
            weights['lstm'] * lstm_pred_original +
            weights['rf'] * rf_value +
            weights['gbt'] * gbt_value
        )

        return final_pred

# Instantiate predictor
hybrid_predictor_api = HybridEnergyPredictorAPI(lstm_model, rf_model, gbt_model, scaler)

@app.route('/predict', methods=['POST'])
def predict():
    """Hybrid energy consumption prediction endpoint"""
    try:
        data = request.get_json()
        
        if data is None:
            return jsonify({"error": "Request body must be valid JSON"}), 400

        # Frontend sends array of data point objects
        # [{ timestamp, Power_Consumption, voltage, current, temperature, humidity }, ...]
        if isinstance(data, list):
            if len(data) < SEQUENCE_LENGTH:
                return jsonify({"error": f"Input must have at least {SEQUENCE_LENGTH} data points. Received {len(data)}."}), 400
            data_to_process = data
        
        # Fallback: accept dict with 'history' array (for backwards compatibility)
        elif isinstance(data, dict) and 'history' in data:
            history = data['history']
            if not isinstance(history, list) or len(history) < SEQUENCE_LENGTH:
                return jsonify({"error": f"history array must have at least {SEQUENCE_LENGTH} values"}), 400
            
            # Create synthetic data points
            from datetime import datetime, timedelta
            now = datetime.now()
            synthetic_data = []
            for i, power in enumerate(history):
                synthetic_data.append({
                    'timestamp': (now - timedelta(hours=len(history)-i-1)).isoformat(),
                    'Power_Consumption': float(power),
                    'voltage': float(data.get('voltage', 230.0)),
                    'current': float(data.get('current', 10.0)),
                    'temperature': float(data.get('weather', {}).get('temp', 20.0)),
                    'humidity': float(data.get('weather', {}).get('humidity', 50.0))
                })
            data_to_process = synthetic_data
        else:
            return jsonify({"error": "Request must be array of data points or dict with 'history' key"}), 400

        print(f"Processing {len(data_to_process)} data points...")

        # Convert to Spark DataFrame
        pdf = pd.DataFrame(data_to_process)
        
        # Ensure timestamp is datetime
        pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
        
        # Ensure numeric types
        pdf['Power_Consumption'] = pd.to_numeric(pdf['Power_Consumption'], errors='coerce')
        pdf['voltage'] = pd.to_numeric(pdf['voltage'], errors='coerce')
        pdf['current'] = pd.to_numeric(pdf['current'], errors='coerce')
        pdf['temperature'] = pd.to_numeric(pdf['temperature'], errors='coerce')
        pdf['humidity'] = pd.to_numeric(pdf['humidity'], errors='coerce')
        
        # Check for NaN values
        if pdf.isnull().any().any():
            return jsonify({"error": "Invalid numeric values in input data"}), 400
        
        spark_df = spark.createDataFrame(pdf, schema=schema)

        # Make prediction
        prediction = hybrid_predictor_api.hybrid_predict(spark_df, SEQUENCE_LENGTH)
        
        print(f"Prediction made: {prediction}")
        return jsonify({"prediction": float(prediction)}), 200

    except ValueError as ve:
        print(f"ValueError: {ve}")
        return jsonify({"error": f"Invalid input: {str(ve)}"}), 400
    except Exception as e:
        print(f"Prediction error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Prediction failed: {str(e)}"}), 500

@app.route('/')
def health_check():
    return jsonify({
        "status": "ok", 
        "message": "Energy Forecasting API is running!",
        "models_loaded": {
            "lstm": lstm_model is not None,
            "rf": rf_model is not None,
            "gbt": gbt_model is not None,
            "scaler": scaler is not None
        }
    }), 200

if __name__ == '__main__':
    print("Starting Energy Forecasting API on http://localhost:5000")
    app.run(host='127.0.0.1', port=5050, debug=True)