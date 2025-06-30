import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
import joblib
from flask import request, jsonify, Blueprint

predict_blueprint = Blueprint('predict', __name__)

# Configuration
import os

# Base path relatif au script
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
print(BASE_PATH)

CSV_PATH = os.path.join(BASE_PATH, "owid-covid-data.csv")
MODEL_PATH = os.path.join(BASE_PATH, "modele_lstm_owid.pth")
ENCODER_PATH = os.path.join(BASE_PATH, "label_encoder.pkl")

LOOKBACK = 50

# Chargement des données
df = pd.read_csv(CSV_PATH, usecols=["location", "date", "new_cases", "new_tests", "people_vaccinated"])
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["new_cases"])
df = df[df["new_cases"] > 0]
df = df[df["date"].between("2020-01-01", "2022-12-31")]
df = df.fillna(0)

# Chargement de l'encodeur
country_encoder = joblib.load(ENCODER_PATH)
df = df[df["location"].isin(country_encoder.classes_)]
df["country_id"] = country_encoder.transform(df["location"])
num_countries = len(country_encoder.classes_)

# Modèle LSTM
class LSTMCovid(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size=input_size, hidden_size=128, num_layers=2, batch_first=True)
        self.fc = nn.Linear(128, 1)

    def forward(self, x):
        _, (hn, _) = self.lstm(x)
        return self.fc(hn[-1])

model = LSTMCovid(input_size=3 + num_countries)
model.load_state_dict(torch.load(MODEL_PATH))
model.eval()

# Endpoint de prédiction
@predict_blueprint.route('/predict_cases', methods=['GET'])
def predict_cases():
    country = request.args.get('country')
    start_date = request.args.get('start_date')
    days = request.args.get('days', default=30, type=int)

    if not country or not start_date:
        return jsonify({"error": "Les paramètres 'country' et 'start_date' sont requis."}), 400

    df_country = df[df["location"] == country].copy().reset_index(drop=True)
    if len(df_country) < LOOKBACK:
        return jsonify({"error": f"Trop peu de données pour {country}."}), 400

    features = df_country[["new_cases", "new_tests", "people_vaccinated"]].values
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(features)

    try:
        country_id = country_encoder.transform([country])[0]
    except:
        return jsonify({"error": f"Pays inconnu : {country}."}), 400

    country_onehot = np.eye(num_countries)[country_id]

    start_idx_series = df_country[df_country["date"] >= pd.to_datetime(start_date)].index
    if len(start_idx_series) == 0 or start_idx_series.min() < LOOKBACK:
        return jsonify({"error": f"Date invalide ou insuffisante pour {country}."}), 400

    start_idx = start_idx_series.min()
    input_seq = scaled[start_idx - LOOKBACK:start_idx].copy()

    if input_seq.shape[0] != LOOKBACK:
        return jsonify({"error": "Séquence invalide pour la prédiction."}), 400

    input_seq = np.hstack([input_seq, np.repeat(country_onehot.reshape(1, -1), LOOKBACK, axis=0)])

    predicted_scaled = []
    for _ in range(days):
        x = torch.tensor(input_seq[-LOOKBACK:], dtype=torch.float32).view(1, LOOKBACK, -1)
        with torch.no_grad():
            pred = model(x).item()
        predicted_scaled.append([pred])
        new_row = np.concatenate([[[pred, 0, 0]], country_onehot.reshape(1, -1)], axis=1)
        input_seq = np.vstack([input_seq, new_row])

    predicted = scaler.inverse_transform(np.hstack([predicted_scaled, np.zeros((days, 2))]))[:, 0]
    last_known_date = df_country.iloc[start_idx - 1]["date"]
    prediction_dates = pd.date_range(start=last_known_date + pd.Timedelta(days=1), periods=days)

    result = [
        {"date": date.strftime("%Y-%m-%d"), "predicted_cases": int(pred)} 
        for date, pred in zip(prediction_dates, predicted)
    ]

    return jsonify({
        "country": country,
        "start_date": start_date,
        "days": days,
        "predictions": result
    }), 200
