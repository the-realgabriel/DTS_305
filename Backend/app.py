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
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

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
except Exception as e:
    print(f"✗ Error loading models: {e}")

# --- Preprocessing and Feature Engineering Functions ---

def preprocess_data_pandas(df):
    """Clean and prepare data using pandas"""
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Fill missing values
    df['temperature'].fillna(df['temperature'].mean() or 20.0, inplace=True)
    df['humidity'].fillna(df['humidity'].mean() or 50.0, inplace=True)
    df['Power_Consumption'].fillna(0.0, inplace=True)
    
    return df

def create_features_pandas(df):
    """Create comprehensive feature set using pandas"""
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Temporal features
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['day_of_month'] = df['timestamp'].dt.day
    df['month'] = df['timestamp'].dt.month
    df['quarter'] = df['timestamp'].dt.quarter
    df['year'] = df['timestamp'].dt.year
    df['week_of_year'] = df['timestamp'].dt.isocalendar().week
    
    # Binary features
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_business_hours'] = ((df['hour'] >= 9) & (df['hour'] <= 17)).astype(int)
    df['is_peak_hours'] = ((df['hour'] >= 17) & (df['hour'] <= 21)).astype(int)
    
    # Lag features
    for lag_val in [1, 2, 3, 24, 48, 168]:
        df[f'energy_lag_{lag_val}'] = df['Power_Consumption'].shift(lag_val)
    
    # Rolling window features (24h)
    df['energy_mean_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).mean()
    df['energy_std_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).std()
    df['energy_min_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).min()
    df['energy_max_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).max()
    
    # Rolling window features (7d = 168 hours)
    df['energy_mean_7d'] = df['Power_Consumption'].rolling(window=168, min_periods=1).mean()
    df['energy_std_7d'] = df['Power_Consumption'].rolling(window=168, min_periods=1).std()
    
    # Weather interaction features
    df['temp_hour_interaction'] = df['temperature'] * df['hour']
    df['humidity_temp_interaction'] = df['humidity'] * df['temperature']
    
    # Rename column for consistency
    df = df.rename(columns={'Power_Consumption': 'energy_kwh'})
    
    # Drop rows with NaN lag features
    df = df.dropna(subset=['energy_lag_1', 'energy_lag_24']).reset_index(drop=True)
    
    return df

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

    def hybrid_predict(self, df_input, sequence_length=24):
        """Combines LSTM and ensemble predictions"""
        try:
            # Preprocess and create features
            df_preprocessed = preprocess_data_pandas(df_input)
            df_featured = create_features_pandas(df_preprocessed)
            
            if len(df_featured) < sequence_length:
                raise ValueError(
                    f"Not enough data after feature creation. Required: {sequence_length}, Got: {len(df_featured)}"
                )
            
            # Prepare LSTM input
            feature_cols = [
                'energy_kwh', 'temperature', 'humidity',
                'hour', 'day_of_week', 'is_weekend', 'is_peak_hours',
                'energy_lag_1', 'energy_lag_24', 'energy_mean_24h'
            ]
            
            missing_cols = [col for col in feature_cols if col not in df_featured.columns]
            if missing_cols:
                raise ValueError(f"Missing feature columns: {missing_cols}")
            
            # Get last sequence_length rows
            df_lstm_input = df_featured[feature_cols].tail(sequence_length)
            
            # Scale features
            scaled_data = self.scaler.transform(df_lstm_input.values)
            X_lstm = scaled_data.reshape(1, sequence_length, len(feature_cols))
            
            # Get LSTM prediction
            lstm_pred_scaled = self.predict_lstm(X_lstm)[0][0]
            
            # Inverse transform LSTM prediction
            dummy_array = np.zeros((1, self.scaler.n_features_in_))
            dummy_array[0, 0] = lstm_pred_scaled
            lstm_pred_original = self.scaler.inverse_transform(dummy_array)[0, 0]
            
            # Get latest row for ensemble models
            latest_row = df_featured.iloc[-1:].copy()
            latest_spark_df = spark.createDataFrame(latest_row)
            
            # Get ensemble predictions
            rf_pred_df = self.rf_model.transform(latest_spark_df)
            gbt_pred_df = self.gbt_model.transform(latest_spark_df)
            
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
            
        except Exception as e:
            print(f"Hybrid prediction error: {e}")
            raise

# Instantiate predictor
hybrid_predictor_api = HybridEnergyPredictorAPI(lstm_model, rf_model, gbt_model, scaler)

@app.route('/predict', methods=['POST'])
def predict():
    """Hybrid energy consumption prediction endpoint (accepts padding for short inputs)"""
    try:
        data = request.get_json()
        
        if data is None:
            return jsonify({"error": "Request body must be valid JSON"}), 400

        # Frontend sends array of data point objects
        if isinstance(data, list):
            received_len = len(data)
            app.logger.info(f"Received {received_len} data points")
            if received_len == 0:
                return jsonify({"error": "Request array is empty"}), 400

            # If fewer than SEQUENCE_LENGTH, pad by repeating the oldest entries (synthetic)
            if received_len < SEQUENCE_LENGTH:
                pad_count = SEQUENCE_LENGTH - received_len
                app.logger.warning(f"Received only {received_len} points; padding {pad_count} synthetic points to reach {SEQUENCE_LENGTH}")
                # assume incoming points are ordered oldest->newest or timestamps present
                # create padding using the first available point with older timestamps
                first_point = data[0]
                try:
                    first_ts = datetime.fromisoformat(first_point['timestamp'])
                except Exception:
                    first_ts = datetime.utcnow()
                padding = []
                for i in range(pad_count, 0, -1):
                    padding.append({
                        'timestamp': (first_ts - timedelta(hours=i)).isoformat(),
                        'Power_Consumption': float(first_point.get('Power_Consumption', 0.0)),
                        'voltage': float(first_point.get('voltage', 230.0)),
                        'current': float(first_point.get('current', 10.0)),
                        'temperature': float(first_point.get('temperature', 20.0)),
                        'humidity': float(first_point.get('humidity', 50.0))
                    })
                data_to_process = padding + data
            else:
                data_to_process = data
        else:
            return jsonify({"error": "Request must be array of data points"}), 400

        app.logger.info(f"Processing {len(data_to_process)} data points (after padding if applied). Sample: {data_to_process[:3]}")
        # Convert to pandas DataFrame
        df = pd.DataFrame(data_to_process)
        
        # Ensure correct data types
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['Power_Consumption'] = pd.to_numeric(df['Power_Consumption'], errors='coerce')
        df['voltage'] = pd.to_numeric(df['voltage'], errors='coerce')
        df['current'] = pd.to_numeric(df['current'], errors='coerce')
        df['temperature'] = pd.to_numeric(df['temperature'], errors='coerce')
        df['humidity'] = pd.to_numeric(df['humidity'], errors='coerce')
        
        # Check for NaN values
        if df.isnull().any().any():
            app.logger.error("DataFrame has NaN values:\n%s", df.isnull().sum().to_dict())
            return jsonify({"error": "Invalid numeric values in input data"}), 400
        
        # Make prediction
        prediction = hybrid_predictor_api.hybrid_predict(df, SEQUENCE_LENGTH)
        
        app.logger.info(f"✓ Prediction made: {prediction}")
        return jsonify({"prediction": float(prediction)}), 200

    except ValueError as ve:
        app.logger.error("ValueError: %s", ve)
        return jsonify({"error": f"Invalid input: {str(ve)}"}), 400
    except Exception as e:
        app.logger.error("Prediction error: %s: %s", type(e).__name__, e, exc_info=True)
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