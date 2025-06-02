from flask import Blueprint, jsonify, request
from sqlalchemy import text
import pandas as pd
from database.db import get_db_connection

graph_blueprint = Blueprint('graph', __name__)
engine = get_db_connection()

# Mapper les métriques aux colonnes de la base
METRIC_MAP = {
    "cases": "new_reported_shots",         # Vérifier le nom exact
    "deaths": "new_reported_deaths",       # Vérifier le nom exact
    "vaccinated": "new_reported_vaccinated" # Vérifier le nom exact
}

# Fonction générique pour récupérer les données d'un pays
def fetch_country_data(country, metric):
    try:
        # Vérifier que le paramètre pays est bien défini
        if not country:
            return jsonify({"error": "Le paramètre 'country' est manquant"}), 400
        
        # Vérifier que la métrique est valide
        if metric not in METRIC_MAP:
            return jsonify({"error": f"Métrique '{metric}' inconnue"}), 400
        
        metric_column = METRIC_MAP[metric]
        
        query = f"""
        SELECT day_of_report AS date, {metric_column} AS value
        FROM daily_vaccine_statistics
        WHERE country_short_code = :country
        ORDER BY day_of_report ASC;
        """
        data = pd.read_sql(text(query), engine, params={"country": country})
        
        # Vérifier si des résultats sont retournés
        if data.empty:
            return jsonify({"error": f"Aucune donnée trouvée pour le pays '{country}'"}), 404
        
        result = data.to_dict(orient='records')
        return jsonify({"data": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint pour récupérer les données par métrique
@graph_blueprint.route('/graph/country/<metric>', methods=['GET'])
def graph_country(metric):
    country = request.args.get("country")
    return fetch_country_data(country, metric)
