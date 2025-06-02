from flask import Blueprint, jsonify
from sqlalchemy import create_engine
from sklearn.metrics import r2_score, mean_squared_error
import pandas as pd
import joblib

# Charger le modèle lors du démarrage du blueprint
try:
    model = joblib.load("models/ml_covid_random_forest.joblib")
    print("✅ Modèle Random Forest chargé avec succès")
except Exception as e:
    print(f"❌ Erreur lors du chargement du modèle : {e}")
    model = None

# Connexion à PostgreSQL
engine = create_engine('postgresql+psycopg2://username:password@localhost:5432/mydatabase')

# Créer le blueprint
metrics_blueprint = Blueprint('metrics', __name__)

@metrics_blueprint.route('/metrics', methods=['GET'])
def get_metrics():
    try:
        query = """
        SELECT new_cases, new_vaccinations, total_cases, total_vaccinations, reproduction_rate, positive_rate, new_deaths
        FROM daily_vaccine_statistics;
        """
        data = pd.read_sql(query, engine)

        # Préparer les données
        X = data[['new_cases', 'new_vaccinations', 'total_cases', 'total_vaccinations', 
                  'reproduction_rate', 'positive_rate']]
        y_true = data['new_deaths']
        y_pred = model.predict(X)

        # Calcul des métriques
        r2 = r2_score(y_true, y_pred)
        rmse = mean_squared_error(y_true, y_pred, squared=False)

        return jsonify({'R2': round(r2, 4), 'RMSE': round(rmse, 2)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
