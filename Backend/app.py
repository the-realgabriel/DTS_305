from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import tensorflow as tf
import numpy as np
import pandas as pd
import pickle
import os
from datetime import datetime, timedelta
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CORS Configuration
CORS(app, resources={r"/*": {"origins": [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5050",
        "http://127.0.0.1:5050"
    ]}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization", "Accept", "Origin"])

# --- Configuration & Model Paths ---
LSTM_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'lstm_energy_model.h5')
RF_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'rf_energy_model')
GBT_MODEL_PATH = os.path.join(os.getcwd(), 'models', 'gbt_energy_model')
SCALER_PATH = os.path.join(os.getcwd(), 'models', 'scaler.pkl')
SEQUENCE_LENGTH = 24

REQUIRED_FIELDS = ['timestamp', 'Power_Consumption', 'voltage', 'current', 'temperature', 'humidity']

# CRITICAL FIX CONFIGURATION: Define the exact feature order used when training the scaler (scaler.fit())
# This list MUST be in the same order as the input features used for LSTM training.
SCALER_FEATURE_ORDER = [
    'energy_kwh', 'temperature', 'humidity',
    'hour', 'day_of_week', 'is_weekend', 'is_peak_hours',
    'energy_lag_1', 'energy_lag_24', 'energy_mean_24h'
]

# CRITICAL FIX CONFIGURATION: Define the index of the target variable ('energy_kwh') in the list above.
# Since 'energy_kwh' is the first element (index 0), this is 0.
TARGET_FEATURE_INDEX = 0 

# --- Initialize Spark Session ---
try:
    spark = SparkSession.builder \
        .appName("EnergyForecastingAPI") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("✓ Spark session initialized successfully")
except Exception as e:
    logger.error(f"✗ Failed to initialize Spark: {e}")
    spark = None

# --- Load Models and Scaler ---
lstm_model = None
rf_model = None
gbt_model = None
scaler = None

def load_models():
    """Load all models and scaler with proper error handling"""
    global lstm_model, rf_model, gbt_model, scaler
    try:
        logger.info("Loading models and scaler...")
        
        if os.path.exists(LSTM_MODEL_PATH):
            lstm_model = tf.keras.models.load_model(LSTM_MODEL_PATH)
            logger.info("✓ LSTM model loaded successfully")
        else:
            logger.warning(f"✗ LSTM model not found at {LSTM_MODEL_PATH}")
        
        if os.path.exists(SCALER_PATH):
            with open(SCALER_PATH, 'rb') as f:
                scaler = pickle.load(f)
            logger.info(f"✓ Scaler loaded successfully (features: {scaler.n_features_in_})")
        else:
            logger.warning(f"✗ Scaler not found at {SCALER_PATH}")
        
        if os.path.exists(RF_MODEL_PATH):
            rf_model = PipelineModel.load(RF_MODEL_PATH)
            logger.info("✓ Random Forest model loaded successfully")
        else:
            logger.warning(f"✗ RF model not found at {RF_MODEL_PATH}")
        
        if os.path.exists(GBT_MODEL_PATH):
            gbt_model = PipelineModel.load(GBT_MODEL_PATH)
            logger.info("✓ GBT model loaded successfully")
        else:
            logger.warning(f"✗ GBT model not found at {GBT_MODEL_PATH}")
        
        if all([lstm_model, rf_model, gbt_model, scaler]):
            logger.info("✓ All models and scaler loaded successfully!")
            return True
        else:
            logger.warning("⚠ Some models failed to load")
            return False
            
    except Exception as e:
        logger.error(f"✗ Error loading models: {e}", exc_info=True)
        return False

# Load models on startup
load_models()

# --- Preprocessing and Feature Engineering Functions (Unchanged) ---

def preprocess_data_pandas(df):
    """Clean and prepare data using pandas"""
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Fill missing values with safe defaults
    df['temperature'] = df['temperature'].fillna(df['temperature'].mean() if df['temperature'].notna().any() else 20.0)
    df['humidity'] = df['humidity'].fillna(df['humidity'].mean() if df['humidity'].notna().any() else 50.0)
    df['voltage'] = df['voltage'].fillna(df['voltage'].mean() if df['voltage'].notna().any() else 230.0)
    df['current'] = df['current'].fillna(df['current'].mean() if df['current'].notna().any() else 10.0)
    df['Power_Consumption'] = df['Power_Consumption'].fillna(0.0)
    
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
    df['week_of_year'] = df['timestamp'].dt.isocalendar().week.astype(int)
    
    # Binary features
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_business_hours'] = ((df['hour'] >= 9) & (df['hour'] <= 17)).astype(int)
    df['is_peak_hours'] = ((df['hour'] >= 17) & (df['hour'] <= 21)).astype(int)
    
    # Lag features
    for lag_val in [1, 2, 3, 24, 48, 168]:
        df[f'energy_lag_{lag_val}'] = df['Power_Consumption'].shift(lag_val)
    
    # Rolling window features
    df['energy_mean_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).mean()
    df['energy_std_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).std().fillna(0)
    df['energy_min_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).min()
    df['energy_max_24h'] = df['Power_Consumption'].rolling(window=24, min_periods=1).max()
    df['energy_mean_7d'] = df['Power_Consumption'].rolling(window=168, min_periods=1).mean()
    df['energy_std_7d'] = df['Power_Consumption'].rolling(window=168, min_periods=1).std().fillna(0)
    
    # Weather interaction features
    df['temp_hour_interaction'] = df['temperature'] * df['hour']
    df['humidity_temp_interaction'] = df['humidity'] * df['temperature']
    
    # Rename column for consistency
    df = df.rename(columns={'Power_Consumption': 'energy_kwh'})
    
    lag_cols = [col for col in df.columns if 'lag' in col or 'mean' in col or 'std' in col or 'min' in col or 'max' in col]
    df[lag_cols] = df[lag_cols].ffill().bfill().fillna(0)
    
    return df

# --- HybridEnergyPredictor Class (FIXED) ---

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
        # Use predict on the tf model
        return self.lstm_model.predict(sequence_data, verbose=0)

    def hybrid_predict(self, df_input, sequence_length=24):
        """Combines LSTM and ensemble predictions"""
        try:
            # 1. Feature Engineering
            df_preprocessed = preprocess_data_pandas(df_input)
            df_featured = create_features_pandas(df_preprocessed)
            
            if len(df_featured) < sequence_length:
                raise ValueError(
                    f"Not enough data after feature creation. Required: {sequence_length}, Got: {len(df_featured)}"
                )
            
            # 2. LSTM Path: Use the configured feature order
            missing_cols = [col for col in SCALER_FEATURE_ORDER if col not in df_featured.columns]
            if missing_cols:
                raise ValueError(f"Missing feature columns required by scaler: {missing_cols}")
            
            # Select columns in the correct order for the scaler
            df_lstm_input = df_featured[SCALER_FEATURE_ORDER].tail(sequence_length)
            
            # Scale features
            scaled_data = self.scaler.transform(df_lstm_input.values)
            X_lstm = scaled_data.reshape(1, sequence_length, len(SCALER_FEATURE_ORDER))
            
            # Get LSTM prediction
            lstm_pred_scaled = self.predict_lstm(X_lstm)[0][0]
            
            # Inverse transform LSTM prediction (FIXED)
            # Create a dummy array, setting the prediction at the TARGET_FEATURE_INDEX
            dummy_array = np.zeros((1, self.scaler.n_features_in_))
            
            # Optional: If using StandardScaler, fill with mean to center other features
            if hasattr(self.scaler, 'mean_'):
                dummy_array[0] = self.scaler.mean_
            
            # Place the scaled prediction in the correct column slot
            dummy_array[0, TARGET_FEATURE_INDEX] = lstm_pred_scaled
            
            # Inverse transform and extract the target value
            lstm_pred_original = self.scaler.inverse_transform(dummy_array)[0, TARGET_FEATURE_INDEX]
            
            # Ensure lstm prediction is non-negative
            lstm_pred_original = np.maximum(0, lstm_pred_original)
            
            # 3. Spark Ensemble Path (FIXED)
            latest_row = df_featured.iloc[-1:].copy()
            
            # Convert to dictionary and sanitize types for Spark
            latest_record = latest_row.to_dict(orient='records')[0]
            clean_record = {}
            
            for k, v in latest_record.items():
                if pd.isna(v): clean_record[k] = 0.0
                elif isinstance(v, pd.Timestamp): clean_record[k] = v.isoformat()
                elif isinstance(v, (bool, np.bool_)): clean_record[k] = int(v)
                elif isinstance(v, (int, np.integer)): clean_record[k] = int(v)
                elif isinstance(v, (float, np.floating)): clean_record[k] = float(v)
                else: clean_record[k] = str(v)

            # Create Spark DataFrame from the sanitized dictionary
            latest_spark_df = spark.createDataFrame([clean_record])
            
            # Get ensemble predictions
            rf_pred_df = self.rf_model.transform(latest_spark_df)
            gbt_pred_df = self.gbt_model.transform(latest_spark_df)
            
            rf_value = float(rf_pred_df.select('prediction').collect()[0][0])
            gbt_value = float(gbt_pred_df.select('prediction').collect()[0][0])
            
            # Ensure non-negative predictions
            rf_value = np.maximum(0, rf_value)
            gbt_value = np.maximum(0, gbt_value)
            
            # 4. Weighted ensemble
            weights = {'lstm': 0.5, 'rf': 0.25, 'gbt': 0.25}
            final_pred = (
                weights['lstm'] * lstm_pred_original +
                weights['rf'] * rf_value +
                weights['gbt'] * gbt_value
            )
            
            logger.info(f"Predictions - LSTM: {lstm_pred_original:.2f}, RF: {rf_value:.2f}, GBT: {gbt_value:.2f}, Final: {final_pred:.2f}")
            
            return np.maximum(0, final_pred)
            
        except Exception as e:
            logger.error(f"Hybrid prediction error: {e}", exc_info=True)
            raise

# Instantiate predictor
hybrid_predictor_api = HybridEnergyPredictorAPI(lstm_model, rf_model, gbt_model, scaler)

@app.route('/predict', methods=['POST'])
def predict():
    """Hybrid energy consumption prediction endpoint"""
    try:
        # Check if models are loaded
        if not all([lstm_model, rf_model, gbt_model, scaler]):
            return jsonify({"error": "Models not fully loaded."}), 503
        
        data = request.get_json()
        
        if not isinstance(data, list) or len(data) == 0:
            return jsonify({"error": "Request must be a non-empty list"}), 400
        
        received_len = len(data)
        
        # Padding logic
        if received_len < SEQUENCE_LENGTH:
            # ... (padding logic remains the same) ...
            pad_count = SEQUENCE_LENGTH - received_len
            first_point = data[0]
            try:
                first_ts = datetime.fromisoformat(first_point['timestamp'].replace('Z', '+00:00'))
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

        # Convert to pandas DataFrame
        df = pd.DataFrame(data_to_process)
        
        # Ensure correct data types
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        for col in ['Power_Consumption', 'voltage', 'current', 'temperature', 'humidity']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        
        # Make prediction
        prediction = hybrid_predictor_api.hybrid_predict(df, SEQUENCE_LENGTH)
        
        logger.info(f"✓ Prediction made: {prediction:.2f} kWh")
        return jsonify({
            "prediction": float(prediction),
            "data_points_used": len(data_to_process),
            "data_points_received": received_len,
            "timestamp": datetime.utcnow().isoformat()
        }), 200

    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        return jsonify({"error": f"Invalid input: {str(ve)}"}), 400
    except Exception as e:
        logger.error(f"Prediction error: {type(e).__name__}: {e}", exc_info=True)
        return jsonify({"error": f"Prediction failed: {str(e)}"}), 500

@app.route('/health', methods=['GET'])
@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    # ... (health check logic remains the same) ...
    models_loaded = {
        "lstm": lstm_model is not None,
        "rf": rf_model is not None,
        "gbt": gbt_model is not None,
        "scaler": scaler is not None
    }
    
    all_loaded = all(models_loaded.values())
    
    return jsonify({
        "status": "healthy" if all_loaded else "degraded",
        "message": "Energy Forecasting API is running!",
        "models_loaded": models_loaded,
        "sequence_length": SEQUENCE_LENGTH,
        "spark_active": spark is not None
    }), 200 if all_loaded else 503

@app.route('/reload-models', methods=['POST'])
def reload_models():
    """Endpoint to reload models without restarting server"""
    success = load_models()
    return jsonify({
        "success": success,
        "models_loaded": {
            "lstm": lstm_model is not None,
            "rf": rf_model is not None,
            "gbt": gbt_model is not None,
            "scaler": scaler is not None
        }
    }), 200 if success else 500

if __name__ == '__main__':
    logger.info("Starting Energy Forecasting API on http://localhost:5050")
    app.run(host='127.0.0.1', port=5050, debug=True)