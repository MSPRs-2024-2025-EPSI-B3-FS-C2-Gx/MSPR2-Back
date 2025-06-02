from flask import Blueprint, jsonify
from sqlalchemy import text
import pandas as pd
from database.db import get_db_connection

data_blueprint = Blueprint('data', __name__)
engine = get_db_connection()

def fetch_data(query):
    try:
        data = pd.read_sql(text(query), engine)
        result = data.to_dict(orient='records')
        return jsonify({"data": result}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Génération d'un endpoint pour chaque table
@data_blueprint.route('/predicted_weekly_statistics', methods=['GET'])
def get_predicted_weekly_statistics():
    query = "SELECT * FROM predicted_weekly_statistics;"
    return fetch_data(query)

@data_blueprint.route('/who_region', methods=['GET'])
def get_who_region():
    query = "SELECT * FROM who_region;"
    return fetch_data(query)

@data_blueprint.route('/country', methods=['GET'])
def get_country():
    query = "SELECT * FROM country;"
    return fetch_data(query)

@data_blueprint.route('/daily_vaccine_statistics', methods=['GET'])
def get_daily_vaccine_statistics():
    query = "SELECT * FROM daily_vaccine_statistics;"
    return fetch_data(query)

@data_blueprint.route('/vaccine', methods=['GET'])
def get_vaccine():
    query = "SELECT * FROM vaccine;"
    return fetch_data(query)

@data_blueprint.route('/disease', methods=['GET'])
def get_disease():
    query = "SELECT * FROM disease;"
    return fetch_data(query)

@data_blueprint.route('/weekly_statistics', methods=['GET'])
def get_weekly_statistics():
    query = "SELECT * FROM weekly_statistics;"
    return fetch_data(query)
