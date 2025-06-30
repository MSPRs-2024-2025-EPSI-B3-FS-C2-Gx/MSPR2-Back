import pandas as pd
import numpy as np
import torch
import torch.nn as nn
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
import joblib
import os
from sklearn.metrics import mean_absolute_error
from flask import request, jsonify
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error


# Configuration
CSV_PATH = "C:/Users/merou/OneDrive/Bureau/ESPI_MSPR/MSPR501/pandemie_app/spark-exemple-covid/owid-covid-data.csv"
MODEL_PATH = "C:/Users/merou/OneDrive/Bureau/ESPI_MSPR/MSPR501/pandemie_app/spark-exemple-covid/modele_lstm_owid.pth"
ENCODER_PATH = "C:/Users/merou/OneDrive/Bureau/ESPI_MSPR/MSPR501/pandemie_app/spark-exemple-covid/label_encoder.pkl"

LOOKBACK = 50
DAYS_TO_PREDICT = 30
TESTS = [
    ("France", "2021-03-10"),
    ("Italy", "2021-02-15"),
    ("Germany", "2021-03-10"),
    ("Spain", "2021-03-10"),
    ("United Kingdom", "2021-01-20")
]

# Chargement des données
print("Chargement des données...")
df = pd.read_csv(CSV_PATH, usecols=["location", "date", "new_cases", "new_tests", "people_vaccinated"])
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["new_cases"])
df = df[df["new_cases"] > 0]
df = df[df["date"].between("2020-01-01", "2022-12-31")]
df = df.fillna(0)

# Chargement de l'encodeur utilisé à l'entraînement
from sklearn.preprocessing import LabelEncoder
country_encoder = joblib.load(ENCODER_PATH)
df = df[df["location"].isin(country_encoder.classes_)]

df["country_id"] = country_encoder.transform(df["location"])
num_countries = len(country_encoder.classes_)
print(f"🔁 Nombre de pays encodés : {num_countries}")

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
print(f"📦 Tentative de chargement du modèle depuis {MODEL_PATH}...")
model.load_state_dict(torch.load(MODEL_PATH))
print("✅ Modèle chargé avec succès.")
model.eval()

# Prédictions
for country, start_predict_date in TESTS:
    df_country = df[df["location"] == country].copy().reset_index(drop=True)
    if len(df_country) < LOOKBACK:
        print(f"⛔ Trop peu de données pour {country}")
        continue

    features = df_country[["new_cases", "new_tests", "people_vaccinated"]].values
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(features)

    country_id = country_encoder.transform([country])[0]
    country_onehot = np.eye(num_countries)[country_id]

    start_idx_series = df_country[df_country["date"] >= pd.to_datetime(start_predict_date)].index
    if len(start_idx_series) == 0 or start_idx_series.min() < LOOKBACK:
        print(f"⚠️ Date invalide pour {country}, sautée.")
        continue

    start_idx = start_idx_series.min()
    input_seq = scaled[start_idx - LOOKBACK:start_idx].copy()

    if input_seq.shape[0] != LOOKBACK:
        print(f"❌ Séquence invalide pour {country}")
        continue

    input_seq = np.hstack([input_seq, np.repeat(country_onehot.reshape(1, -1), LOOKBACK, axis=0)])

    predicted_scaled = []
    for _ in range(DAYS_TO_PREDICT):
        x = torch.tensor(input_seq[-LOOKBACK:], dtype=torch.float32).view(1, LOOKBACK, -1)
        with torch.no_grad():
            pred = model(x).item()
        predicted_scaled.append([pred])
        new_row = np.concatenate([[[pred, 0, 0]], country_onehot.reshape(1, -1)], axis=1)
        input_seq = np.vstack([input_seq, new_row])

    predicted = scaler.inverse_transform(np.hstack([predicted_scaled, np.zeros((DAYS_TO_PREDICT, 2))]))[:, 0]
    last_known_date = df_country.iloc[start_idx - 1]["date"]
    prediction_dates = pd.date_range(start=last_known_date + pd.Timedelta(days=1), periods=DAYS_TO_PREDICT)

    # 🧮 Calcul de l'erreur MAE
    truth = df_country.iloc[start_idx:start_idx + DAYS_TO_PREDICT]["new_cases"].values
    mae = mean_absolute_error(truth[:len(predicted)], predicted)
    print(f"📉 MAE pour {country} : {mae:,.0f} cas")

    plt.figure(figsize=(12, 5))
    plt.plot(df_country["date"], df_country["new_cases"], label="Données réelles", color="blue")
    plt.plot(prediction_dates, predicted, label="Prédictions", color="orange")
    plt.axvline(x=pd.to_datetime(start_predict_date), color="gray", linestyle="--", label="Début prédiction")
    plt.title(f"{country} - Prédiction à partir de {start_predict_date}")
    plt.xlabel("Date")
    plt.ylabel("Cas journaliers")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.xticks(rotation=45)
    plt.show()


