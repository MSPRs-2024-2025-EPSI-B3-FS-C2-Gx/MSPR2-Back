from flask import Blueprint, request, jsonify
import numpy as np
import joblib

# Charger le modèle lors du démarrage du blueprint
try:
    model = joblib.load("models/ml_covid_random_forest.joblib")
    print("✅ Modèle Random Forest chargé avec succès")
except Exception as e:
    print(f"❌ Erreur lors du chargement du modèle : {e}")
    model = None

# Créer le blueprint
predict_blueprint = Blueprint('predict', __name__)

@predict_blueprint.route('/predict', methods=['POST'])
def predict():
    try:
        # Récupérer les données JSON depuis la requête
        data = request.get_json()
        features = np.array([[
            data['cases'], data['vaccinated_percent'], data['gdp'], data['gdp_per_capita'],
            data['health_exp'], data['life_exp'], data['beds'], data['density'],
            data['neonatal_mortality']
        ]])
        # Prédiction avec le modèle
        prediction = model.predict(features)
        return jsonify({"predicted_deaths": int(prediction[0])}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500