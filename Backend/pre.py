import os
import pickle
import tensorflow as tf
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

# Initialize Spark Session (if not already running in a Spark environment)
spark = SparkSession.builder \
    .appName("EnergyForecastingApp") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Define model paths (assuming they are in the 'models' directory relative to app.py)
LSTM_MODEL_PATH = 'models/lstm_energy_model.h5'
SCALER_PATH = 'models/scaler.pkl'
RF_MODEL_PATH = 'models/rf_energy_model'
GBT_MODEL_PATH = 'models/gbt_energy_model'

print("Loading models and scaler...")

try:
    # Load LSTM model
    lstm_model = tf.keras.models.load_model(LSTM_MODEL_PATH)
    print(f"Successfully loaded LSTM model from {LSTM_MODEL_PATH}")

    # Load scaler
    with open(SCALER_PATH, 'rb') as f:
        scaler = pickle.load(f)
    print(f"Successfully loaded scaler from {SCALER_PATH}")

    # Load Random Forest model
    rf_model = PipelineModel.load(RF_MODEL_PATH)
    print(f"Successfully loaded Random Forest model from {RF_MODEL_PATH}")

    # Load Gradient Boosted Trees model
    gbt_model = PipelineModel.load(GBT_MODEL_PATH)
    print(f"Successfully loaded GBT model from {GBT_MODEL_PATH}")

except Exception as e:
    print(f"Error loading models: {e}")
    # In a real application, you might want to handle this more gracefully, e.g., exit or use dummy models

print("All models and scaler loaded.")
