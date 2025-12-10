from flask import Flask, request, jsonify
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

app = Flask(__name__)

# --- Configuration ---
# These paths assume the models/ directory is relative to where app.py is run
LSTM_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'lstm_energy_model.h5')
RF_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'rf_energy_model')
GBT_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'gbt_energy_model')
SCALER_PATH = os.path.join(os.getcwd(), 'models', 'scaler.pkl')
SEQUENCE_LENGTH = 24 # Matches the sequence_length used in prepare_lstm_data during training

# --- Initialize Spark Session (for PySpark models) ---
# This is a local Spark session. For production, consider a Spark cluster.
spark = SparkSession.builder \
    .appName("EnergyForecastingAPI") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# --- Load Models and Scaler ---
# Define schema for smart meter CSV data (needs to match what was used in training)
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
    with open(SCALER_PATH, 'rb') as f:
        scaler = pickle.load(f)
    rf_model = PipelineModel.load(RF_MODEL_PATH)
    gbt_model = PipelineModel.load(GBT_MODEL_PATH)
    print("Models and scaler loaded successfully.")
except Exception as e:  # Fixed: was "excep" instead of "except Exception as e"
    print(f"Error loading models: {e}")
    # In a production setup, you might want to raise the exception or exit gracefully
    exit(1)

# --- Re-implement Preprocessing and Feature Engineering Functions ---
# These functions were designed for Spark DataFrames during training.
# They are re-implemented here to be used with the Spark DataFrame created from API input.

def preprocess_data_spark(spark_df_input):
    """
    Clean and prepare data for modeling (Spark DataFrame version).
    For inference, if a small batch or single record is passed, mean/quantile aggregations
    are computed over that batch. For robust production, pre-calculated training means/bounds
    might be used instead.
    """
    # Calculate means safely
    temp_mean = spark_df_input.select(mean('temperature')).collect()[0][0]
    humidity_mean = spark_df_input.select(mean('humidity')).collect()[0][0]
    
    df_filled = spark_df_input.na.fill({
        'Power_Consumption': 0.0,
        'temperature': temp_mean if temp_mean is not None else 0.0,
        'humidity': humidity_mean if humidity_mean is not None else 0.0
    })

    # Sort by timestamp (important for lag/window features)
    df_sorted = df_filled.orderBy('timestamp')

    # Outlier filtering (IQR) is omitted for inference on small datasets
    # as it might remove the very data point being predicted or not be representative.
    # In a production system, this would typically use bounds learned from training data.
    return df_sorted

def create_features_spark(spark_df_input):
    """
    Create comprehensive feature set for energy forecasting (Spark DataFrame version).
    Assumes `spark_df_input` contains enough historical points for lags and rolling windows.
    """
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

    # Define window for lag features
    window_spec = Window.orderBy("timestamp")

    # Lag features
    for lag_val in [1, 2, 3, 24, 48, 168]:
        df = df.withColumn(
            f"energy_lag_{lag_val}",
            lag("Power_Consumption", lag_val).over(window_spec)
        )

    # Rolling window features
    window_24h = Window \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(-24*3600, 0) # 24 hours back

    df = df \
        .withColumn("energy_mean_24h", avg("Power_Consumption").over(window_24h)) \
        .withColumn("energy_std_24h", stddev("Power_Consumption").over(window_24h)) \
        .withColumn("energy_min_24h", min("Power_Consumption").over(window_24h)) \
        .withColumn("energy_max_24h", max("Power_Consumption").over(window_24h))

    window_7d = Window \
        .orderBy(col("timestamp").cast("long")) \
        .rangeBetween(-7*24*3600, 0) # 7 days back

    df = df \
        .withColumn("energy_mean_7d", avg("Power_Consumption").over(window_7d)) \
        .withColumn("energy_std_7d", stddev("Power_Consumption").over(window_7d))

    # Weather interaction features
    df = df \
        .withColumn("temp_hour_interaction", col("temperature") * col("hour")) \
        .withColumn("humidity_temp_interaction", col("humidity") * col("temperature"))

    # Remove rows with null lag features. For inference, ensure enough history is provided.
    df = df.na.drop(subset=["energy_lag_1", "energy_lag_24"])

    # Rename 'Power_Consumption' to 'energy_kwh' for consistency with modeling steps
    df = df.withColumnRenamed("Power_Consumption", "energy_kwh")

    return df

def prepare_lstm_data_for_inference(spark_df_for_inference, scaler_obj, sequence_length=24):
    """
    Prepares a Spark DataFrame (expected to contain SEQUENCE_LENGTH records)
    for LSTM inference. It returns the X input needed for lstm_model.predict().
    """
    if spark_df_for_inference.count() < sequence_length:
        raise ValueError(f"Input data must contain at least {sequence_length} records for LSTM inference.")

    pdf = spark_df_for_inference.toPandas()

    feature_cols = [
        'energy_kwh', 'temperature', 'humidity',
        'hour', 'day_of_week', 'is_weekend', 'is_peak_hours',
        'energy_lag_1', 'energy_lag_24', 'energy_mean_24h'
    ]

    missing_cols = [col for col in feature_cols if col not in pdf.columns]
    if missing_cols:
        raise ValueError(f"Missing feature columns in input data: {missing_cols}")

    scaled_data = scaler_obj.transform(pdf[feature_cols])

    # For inference, take the last sequence_length points as input for the next step.
    X_inference = scaled_data[-sequence_length:].reshape(1, sequence_length, -1)

    return X_inference


# Define the HybridEnergyPredictor class again, adapted for API use
class HybridEnergyPredictorAPI:
    def __init__(self, lstm_model, rf_model, gbt_model, scaler):
        self.lstm_model = lstm_model
        self.rf_model = rf_model
        self.gbt_model = gbt_model
        self.scaler = scaler

    def predict_lstm(self, sequence_data):
        """Make LSTM predictions"""
        return self.lstm_model.predict(sequence_data, verbose=0)

    def predict_ensemble(self, spark_df):
        """Make ensemble predictions"""
        rf_pred = self.rf_model.transform(spark_df)
        gbt_pred = self.gbt_model.transform(spark_df)
        return rf_pred, gbt_pred

    def hybrid_predict(self, spark_df_input_for_features, sequence_length=24):
        """
        Combines LSTM and ensemble predictions for a single future point.
        `spark_df_input_for_features` should contain at least `SEQUENCE_LENGTH` records,
        representing the historical window to predict the next point.
        """
        # 1. Preprocess and create features for the Spark DataFrame input
        featured_spark_df = create_features_spark(
            preprocess_data_spark(spark_df_input_for_features)
        )

        # Ensure we have enough data after feature creation and drops
        if featured_spark_df.count() < sequence_length:
            raise ValueError(
                "Not enough data to create all features for prediction after preprocessing. "
                f"Required: {sequence_length} valid feature rows, Got: {featured_spark_df.count()}"
            )

        # Get the latest row from the featured Spark DataFrame for ensemble models
        # Fixed: Simplified to get the last row properly
        ensemble_input_rows = featured_spark_df.orderBy('timestamp', ascending=False).limit(1).collect()
        ensemble_input_spark_df = spark.createDataFrame(ensemble_input_rows, schema=featured_spark_df.schema)

        # Prepare data for LSTM prediction (requires the last `sequence_length` historical points)
        lstm_input_pdf = featured_spark_df.orderBy('timestamp').toPandas().tail(sequence_length)
        lstm_input_spark_df_for_scaler = spark.createDataFrame(lstm_input_pdf, schema=featured_spark_df.schema)
        X_lstm_inference = prepare_lstm_data_for_inference(
            lstm_input_spark_df_for_scaler, self.scaler, sequence_length
        )

        lstm_predictions_scaled = self.predict_lstm(X_lstm_inference)

        # Inverse transform LSTM prediction to original scale
        # The scaler is for all feature_cols. The 0th col is energy_kwh. We need to create a dummy array.
        dummy_array = np.zeros((1, self.scaler.n_features_in_))
        dummy_array[0, 0] = lstm_predictions_scaled[0] # Put the scaled prediction in the energy_kwh slot
        lstm_prediction_original_scale = self.scaler.inverse_transform(dummy_array)[:, 0][0]

        # Get ensemble predictions
        rf_pred, gbt_pred = self.predict_ensemble(ensemble_input_spark_df)
        rf_value = rf_pred.select('prediction').collect()[0]['prediction']
        gbt_value = gbt_pred.select('prediction').collect()[0]['prediction']

        # Weighted ensemble (weights from notebook)
        weights = {
            'lstm': 0.5,
            'rf': 0.25,
            'gbt': 0.25
        }

        final_prediction = (
            weights['lstm'] * lstm_prediction_original_scale +
            weights['rf'] * rf_value +
            weights['gbt'] * gbt_value
        )

        return final_prediction

# Instantiate the predictor
hybrid_predictor_api = HybridEnergyPredictorAPI(lstm_model, rf_model, gbt_model, scaler)

@app.route('/predict', methods=['POST'])
def predict():
    """
    API endpoint for hybrid energy consumption prediction.
    Expects a JSON array of `SEQUENCE_LENGTH` data points (historical readings).
    Each data point should have 'timestamp', 'Power_Consumption', 'voltage', 'current', 'temperature', 'humidity'.
    """
    data = request.get_json(force=True)

    if not isinstance(data, list) or len(data) < SEQUENCE_LENGTH:
        return jsonify({"error": f"Input must be a JSON array of at least {SEQUENCE_LENGTH} historical data points."}), 400

    # Convert incoming JSON list of dicts to Pandas DataFrame, then Spark DataFrame
    try:
        pdf_input = pd.DataFrame(data)
        pdf_input['timestamp'] = pd.to_datetime(pdf_input['timestamp']) # Convert timestamp strings
        pdf_input = pdf_input[[col.name for col in schema.fields]] # Ensure column order matches schema

        spark_df_input = spark.createDataFrame(pdf_input, schema=schema)
    except Exception as e:
        return jsonify({"error": f"Invalid input data format or missing columns: {e}"}), 400

    try:
        prediction = hybrid_predictor_api.hybrid_predict(spark_df_input, SEQUENCE_LENGTH)
        return jsonify({"predicted_energy_kwh": float(prediction)}), 200  # Fixed: Convert to float for JSON serialization
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": f"Prediction failed: {e}"}), 500

@app.route('/')
def health_check():
    return jsonify({"status": "ok", "message": "Energy Forecasting API is running!"}), 200

if __name__ == '__main__':
    # For local development, uncomment below. For Colab, you might use ngrok.
    app.run(host='0.0.0.0', port=8000, debug=True)
    print("Flask app is ready. Use 'flask run' or a WSGI server like Gunicorn to run it.")