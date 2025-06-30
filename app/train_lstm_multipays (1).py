import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from torch.utils.data import DataLoader, TensorDataset
import joblib
import os
import matplotlib.pyplot as plt

# Config
BASE_PATH = "C:/Users/merou/OneDrive/Bureau/ESPI_MSPR/MSPR501/pandemie_app/spark-exemple-covid/"
CSV_PATH = os.path.join(BASE_PATH, "owid-covid-data.csv")
ENCODER_PATH = os.path.join(BASE_PATH, "label_encoder.pkl")
MODEL_BASENAME = "modele_lstm_v"
LOOKBACK = 50
EPOCHS = 100
BATCH_SIZE = 64
THRESHOLD_CASES = 10000
SEQUENCES_PER_COUNTRY = 1000

# Déterminer le numéro de version suivant
existing_models = [f for f in os.listdir(BASE_PATH) if f.startswith(MODEL_BASENAME) and f.endswith(".pth")]
version_numbers = [int(f.replace(MODEL_BASENAME, "").replace(".pth", "")) for f in existing_models if f.replace(MODEL_BASENAME, "").replace(".pth", "").isdigit()]
next_version = max(version_numbers + [0]) + 1
MODEL_SAVE_PATH = os.path.join(BASE_PATH, f"{MODEL_BASENAME}{next_version}.pth")

# Chargement des données
print("📥 Chargement des données OWID...")
df = pd.read_csv(CSV_PATH, usecols=["location", "date", "new_cases", "new_tests", "people_vaccinated"])
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["new_cases"])
df = df[df["new_cases"] > 0]
df = df[df["date"].between("2020-01-01", "2022-12-31")]
df = df.fillna(0)

totals = df.groupby("location")["new_cases"].sum()
top_countries = totals[totals > THRESHOLD_CASES].index.tolist()
df = df[df["location"].isin(top_countries)]

from sklearn.preprocessing import LabelEncoder
country_encoder = LabelEncoder()
df["country_id"] = country_encoder.fit_transform(df["location"])
num_countries = len(country_encoder.classes_)
joblib.dump(country_encoder, ENCODER_PATH)
print(f"✅ LabelEncoder sauvegardé sous {ENCODER_PATH}")

# Séquences
print(f"🧠 Préparation de {SEQUENCES_PER_COUNTRY} séquences max par pays...")
all_seq, all_target = [], []

for country in top_countries:
    df_country = df[df["location"] == country].copy()
    values = df_country[["new_cases", "new_tests", "people_vaccinated"]].values
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(values)

    country_id = country_encoder.transform([country])[0]
    country_onehot = np.eye(num_countries)[country_id]

    country_seqs = 0
    for i in range(LOOKBACK, len(scaled)):
        seq = scaled[i - LOOKBACK:i]
        seq_with_country = np.hstack([seq, np.repeat(country_onehot.reshape(1, -1), LOOKBACK, axis=0)])
        all_seq.append(seq_with_country)
        all_target.append(scaled[i, 0])
        country_seqs += 1
        if country_seqs >= SEQUENCES_PER_COUNTRY:
            break

X = np.array(all_seq)
y = np.array(all_target)

X_tensor = torch.tensor(X, dtype=torch.float32)
y_tensor = torch.tensor(y, dtype=torch.float32).view(-1, 1)

dataset = TensorDataset(X_tensor, y_tensor)
dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

# Modèle amélioré
class LSTMCovid(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size=input_size, hidden_size=128, num_layers=2, batch_first=True, dropout=0.2)
        self.fc1 = nn.Linear(128, 64)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(64, 1)
        self.relu_out = nn.ReLU()

    def forward(self, x):
        _, (hn, _) = self.lstm(x)
        out = self.relu(self.fc1(hn[-1]))
        out = self.fc2(out)
        return self.relu_out(out)

model = LSTMCovid(input_size=X.shape[2])
loss_fn = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Entraînement
print(f"🚀 Entraînement du modèle version {next_version}...")
losses = []
for epoch in range(EPOCHS):
    model.train()
    epoch_loss = 0.0
    for batch_x, batch_y in dataloader:
        optimizer.zero_grad()
        output = model(batch_x)
        loss = loss_fn(output, batch_y)
        loss.backward()
        optimizer.step()
        epoch_loss += loss.item() * batch_x.size(0)
    
    avg_loss = epoch_loss / len(dataloader.dataset)
    losses.append(avg_loss)
    if (epoch + 1) % 10 == 0 or epoch == 0:
        print(f"Epoch {epoch + 1}/{EPOCHS} - Loss: {avg_loss:.6f}")

# Sauvegarde versionnée
torch.save(model.state_dict(), MODEL_SAVE_PATH)
print(f"✅ Nouveau modèle sauvegardé sous {MODEL_SAVE_PATH}")

# Courbe de perte
plt.figure(figsize=(10, 4))
plt.plot(range(1, EPOCHS + 1), losses, label="Loss")
plt.title(f"Courbe de perte - Version {next_version}")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.grid(True)
plt.tight_layout()
plt.show()
