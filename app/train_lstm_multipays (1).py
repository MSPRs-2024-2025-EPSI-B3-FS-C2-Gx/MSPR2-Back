import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
from torch.utils.data import DataLoader, TensorDataset
import matplotlib.pyplot as plt
import os
import joblib
# Configuration
CSV_PATH = "C:/Users/merouanmeneu/Desktop/MSPR501/spark-exemple-covid/owid-covid-data.csv"
LOOKBACK = 50
EPOCHS = 100
BATCH_SIZE = 64
THRESHOLD_CASES = 10000
SEQUENCES_PER_COUNTRY = 1000
MODEL_SAVE_PATH = "C:/Users/merouanmeneu/Desktop/MSPR501/spark-exemple-covid/modele_lstm_owid.pth"


# Chargement du fichier OWID
print("📥 Chargement du fichier OWID...")
df = pd.read_csv(CSV_PATH, usecols=["location", "date", "new_cases", "new_tests", "people_vaccinated"])
df["date"] = pd.to_datetime(df["date"])
df = df.dropna(subset=["new_cases"])
df = df[df["new_cases"] > 0]
df = df[df["date"].between("2020-01-01", "2022-12-31")]
df = df.fillna(0)

# Filtrer les pays significativement touchés
totals = df.groupby("location")["new_cases"].sum()
top_countries = totals[totals > THRESHOLD_CASES].index.tolist()
df = df[df["location"].isin(top_countries)]

# Encodage pays
country_encoder = LabelEncoder()
df["country_id"] = country_encoder.fit_transform(df["location"])
num_countries = len(country_encoder.classes_)

# Sauvegarde du LabelEncoder utilisé pour l'entraînement
label_path = "C:/Users/merouanmeneu/Desktop/MSPR501/spark-exemple-covid/label_encoder.pkl"
joblib.dump(country_encoder, label_path)
print(f"✅ LabelEncoder sauvegardé sous {label_path}")


# Préparation des séquences équilibrées
print(f"🧠 Préparation de {SEQUENCES_PER_COUNTRY} séquences max par pays pour {len(top_countries)} pays...")
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
        # Ajouter les embeddings pays à chaque pas de temps
        seq_with_country = np.hstack([seq, np.repeat(country_onehot.reshape(1, -1), LOOKBACK, axis=0)])
        all_seq.append(seq_with_country)
        all_target.append(scaled[i, 0])  # Prédire "new_cases"
        country_seqs += 1
        if country_seqs >= SEQUENCES_PER_COUNTRY:
            break

X = np.array(all_seq)
y = np.array(all_target)

X_tensor = torch.tensor(X, dtype=torch.float32)
y_tensor = torch.tensor(y, dtype=torch.float32).view(-1, 1)

# DataLoader
dataset = TensorDataset(X_tensor, y_tensor)
dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

# Définition du modèle
class LSTMCovid(nn.Module):
    def __init__(self, input_size):
        super().__init__()
        self.lstm = nn.LSTM(input_size=input_size, hidden_size=128, num_layers=2, batch_first=True)
        self.fc = nn.Linear(128, 1)

    def forward(self, x):
        _, (hn, _) = self.lstm(x)
        return self.fc(hn[-1])

model = LSTMCovid(input_size=X.shape[2])
loss_fn = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Entraînement
print("🚀 Entraînement...")
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
        print(f"Epoch {epoch+1}/{EPOCHS} - Loss: {avg_loss:.4f}")

# Sauvegarde
torch.save(model.state_dict(), MODEL_SAVE_PATH)
print(f"✅ Modèle sauvegardé sous {MODEL_SAVE_PATH}")

# Courbe de perte
plt.figure(figsize=(10, 4))
plt.plot(range(1, EPOCHS + 1), losses, label="Loss")
plt.title("Courbe de perte durant l'entraînement")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.grid(True)
plt.tight_layout()
plt.show()

