from flask import Flask, request, jsonify
import joblib
import pandas as pd
import numpy as np
from utils import build_features

app = Flask(__name__)

# Load PKL model
model = joblib.load("models/forecast_model.pkl")

@app.route("/predict", methods=["POST"])
def predict():
    data = request.json.get("history")  # array of past values

    if not data or len(data) < 3:
        return jsonify({"error": "Need at least 3 past values"}), 400

    df = pd.DataFrame({"value": data})

    # Build engineered features (lags, moving averages, etc.)
    df_features = build_features(df)
    last_row = df_features.iloc[-1:].copy()

    # Predict using the PKL model
    pred = float(model.predict(last_row)[0])

    return jsonify({
        "prediction": pred
    })

if __name__ == "__main__":
    app.run(debug=True)
