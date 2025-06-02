from flask import Blueprint, jsonify, request
from sqlalchemy import text
import pandas as pd
from database.db import get_db_connection

worldmap_blueprint = Blueprint('worldmap', __name__)
engine = get_db_connection()

# Fonction pour récupérer les données par pays
def fetch_worldmap_data(metric):
    try:
        query = f"""
        SELECT country_short_code AS country, SUM({metric}) AS value
        FROM daily_vaccine_statistics
        GROUP BY country_short_code;
        """
        data = pd.read_sql(text(query), engine)
        result = data.to_dict(orient='records')
        return jsonify({"data": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Endpoint pour les cas par pays
@worldmap_blueprint.route('/worldmap/cases', methods=['GET'])
def worldmap_cases():
    return fetch_worldmap_data("cases")

# Endpoint pour les décès par pays
@worldmap_blueprint.route('/worldmap/deaths', methods=['GET'])
def worldmap_deaths():
    return fetch_worldmap_data("deaths")

# Endpoint pour les vaccinés par pays
@worldmap_blueprint.route('/worldmap/vaccinated', methods=['GET'])
def worldmap_vaccinated():
    return fetch_worldmap_data("vaccinated_percent")
