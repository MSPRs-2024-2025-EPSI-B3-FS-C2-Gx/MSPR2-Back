from flask import Blueprint, jsonify, request
from sqlalchemy import text
import pandas as pd
from database.db import get_db_connection
import requests
import math
import datetime
from decimal import Decimal
import numpy as np

data_blueprint = Blueprint('data', __name__)
engine = get_db_connection()

def clean_records(records):
    clean_data = []
    for row in records:
        clean_row = {}
        for key, value in row.items():
            if isinstance(value, (np.integer, np.int64, np.int32)):
                clean_row[key] = int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                clean_row[key] = float(value)
            elif isinstance(value, (datetime.date, datetime.datetime)):
                clean_row[key] = value.isoformat()
            elif isinstance(value, (np.ndarray, list)):
                # Tu peux décider quoi faire : convertir en list, str ou ignorer
                clean_row[key] = value.tolist() if isinstance(value, np.ndarray) else value
            elif pd.isnull(value):
                clean_row[key] = None
            else:
                clean_row[key] = value
        clean_data.append(clean_row)
    return clean_data

def clean_value(value):
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (np.integer, np.int64, np.int32)):
        return int(value)
    if isinstance(value, (np.floating, np.float64, np.float32)):
        return float(value)
    if pd.isna(value):
        return None
    return value

def fetch_data(query, params=None):
    try:
        data = pd.read_sql(text(query), engine, params=params)
        records = data.to_dict(orient='records')
        clean_records = []
        for row in records:
            clean_row = {key: clean_value(value) for key, value in row.items()}
            clean_records.append(clean_row)
        return clean_records
    except Exception as e:
        return {"error": str(e)} 

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

@data_blueprint.route('/total_cases', methods=['GET'])
def get_total_cases():
    query = "SELECT SUM(week_new_reported_cases) AS total_weekly_cases FROM weekly_statistics;"
    return fetch_data(query)

@data_blueprint.route('/total_vaccines', methods=['GET'])
def get_total_vaccines():
    query = "SELECT SUM(new_reported_shots) AS total_reported_shots FROM daily_vaccine_statistics;"
    return fetch_data(query)

@data_blueprint.route('/total_deaths', methods=['GET'])
def get_total_deaths():
    query = "SELECT SUM(week_new_reported_deaths) AS total_weekly_deaths FROM weekly_statistics;"
    return fetch_data(query)

@data_blueprint.route('/weekly_statistics_total', methods=['GET'])
def get_weekly_statistics_total():
    page = request.args.get('page', default=1, type=int)
    limit = request.args.get('limit', default=100, type=int)
    offset = (page - 1) * limit

    count_query = "SELECT COUNT(*) FROM weekly_statistics;"
    try:
        total_rows_raw = pd.read_sql(text(count_query), engine).iloc[0, 0]
        total_rows = int(total_rows_raw)  # Forcer conversion en int natif
    except Exception as e:
        return jsonify({"error": f"Erreur lors du comptage des lignes : {str(e)}"}), 500

    total_pages = int(math.ceil(total_rows / limit))  # Assurer int natif

    query = f"""
    WITH weekly_vaccinations AS (
        SELECT 
            country_short_code,
            EXTRACT(YEAR FROM day_of_report) AS year,
            EXTRACT(WEEK FROM day_of_report) AS week,
            SUM(new_reported_shots) AS total_weekly_vaccinations
        FROM daily_vaccine_statistics
        GROUP BY country_short_code, year, week
    )
    SELECT
        ws.date_of_report AS date,
        c.country_name AS country,
        ws.week_new_reported_cases AS confirmed_cases,
        ws.week_new_reported_deaths AS deaths,
        wv.total_weekly_vaccinations AS vaccinations
    FROM weekly_statistics ws
    LEFT JOIN country c 
        ON ws.country_short_code = c.country_short_code
    LEFT JOIN weekly_vaccinations wv
        ON ws.country_short_code = wv.country_short_code
        AND EXTRACT(YEAR FROM ws.date_of_report) = wv.year
        AND EXTRACT(WEEK FROM ws.date_of_report) = wv.week
    ORDER BY c.country_name, ws.date_of_report
    LIMIT :limit OFFSET :offset;
    """

    records = fetch_data(query, {"limit": limit, "offset": offset})

    if isinstance(records, dict) and "error" in records:
        return jsonify(records), 500

    # Nettoyer records : forcer types natifs
    clean_data = []
    for row in records:
        clean_row = {}
        for key, value in row.items():
            if isinstance(value, (np.integer, np.int64, np.int32)):
                clean_row[key] = int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                clean_row[key] = float(value)
            elif isinstance(value, (datetime.date, datetime.datetime)):
                clean_row[key] = value.isoformat()
            elif pd.isna(value):
                clean_row[key] = None
            else:
                clean_row[key] = value
        clean_data.append(clean_row)

    return jsonify({
        "page": page,
        "limit": limit,
        "total_pages": total_pages,
        "total_rows": total_rows,
        "data": clean_data
    }), 200

@data_blueprint.route('/weekly_statistics_by_country', methods=['GET'])
def get_weekly_statistics_by_country():
    country_code = request.args.get('country_code')
    
    if not country_code:
        return jsonify({"error": "Le paramètre 'country_code' est requis"}), 400
    
    query = """
    WITH weekly_vaccinations AS (
        SELECT 
            country_short_code,
            EXTRACT(YEAR FROM day_of_report) AS year,
            EXTRACT(WEEK FROM day_of_report) AS week,
            SUM(new_reported_shots) AS total_weekly_vaccinations
        FROM daily_vaccine_statistics
        GROUP BY country_short_code, year, week
    )
    SELECT
        ws.date_of_report AS date,
        c.country_name AS country,
        ws.week_new_reported_cases AS confirmed_cases,
        ws.week_new_reported_deaths AS deaths,
        wv.total_weekly_vaccinations AS vaccinations
    FROM weekly_statistics ws
    LEFT JOIN country c 
        ON ws.country_short_code = c.country_short_code
    LEFT JOIN weekly_vaccinations wv
        ON ws.country_short_code = wv.country_short_code
        AND EXTRACT(YEAR FROM ws.date_of_report) = wv.year
        AND EXTRACT(WEEK FROM ws.date_of_report) = wv.week
    WHERE ws.country_short_code = :country_code
    ORDER BY ws.date_of_report;
    """
    
    return fetch_data(query, {"country_code": country_code})

@data_blueprint.route('/covid_cases_evolution', methods=['GET'])
def covid_cases_evolution():
    query = """
    SELECT
        ws.date_of_report AS date,
        SUM(ws.week_new_reported_cases) AS total_cases
    FROM weekly_statistics ws
             JOIN disease d ON ws.disease_id = d.id
    WHERE d.name = 'COVID-19'
    GROUP BY ws.date_of_report
    ORDER BY ws.date_of_report;
    """
    try:
        data = pd.read_sql(text(query), engine)
        records = data.to_dict(orient='records')
        clean_data = clean_records(records)
        return jsonify({"data": clean_data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@data_blueprint.route('/vaccinations_evolution', methods=['GET'])
def vaccinations_evolution():
    query = """
    SELECT
        day_of_report AS date,
        SUM(new_reported_shots) AS total_daily_vaccinations
    FROM daily_vaccine_statistics
    GROUP BY day_of_report
    ORDER BY day_of_report;
    """
    try:
        data = pd.read_sql(text(query), engine)
        records = data.to_dict(orient='records')
        
        # Nettoyer les données
        clean_data = []
        for row in records:
            clean_row = {}
            for key, value in row.items():
                if isinstance(value, (np.integer, np.int64, np.int32)):
                    clean_row[key] = int(value)
                elif isinstance(value, (np.floating, np.float64, np.float32)):
                    clean_row[key] = float(value)
                elif isinstance(value, (datetime.date, datetime.datetime)):
                    clean_row[key] = value.isoformat()
                elif pd.isnull(value):
                    clean_row[key] = None
                else:
                    clean_row[key] = value
            clean_data.append(clean_row)
        
        return jsonify({"data": clean_data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@data_blueprint.route('/top5_summary', methods=['GET'])
def top5_summary():
    query_deaths = """
    SELECT
        c.country_name,
        SUM(ws.week_new_reported_deaths) AS total_deaths
    FROM
        weekly_statistics ws
        JOIN country c ON ws.country_short_code = c.country_short_code
    GROUP BY
        c.country_name
    ORDER BY
        total_deaths DESC
    LIMIT 5;
    """

    query_cases = """
    SELECT
        c.country_name,
        SUM(ws.week_new_reported_cases) AS total_cases
    FROM
        weekly_statistics ws
        JOIN country c ON ws.country_short_code = c.country_short_code
    GROUP BY
        c.country_name
    ORDER BY
        total_cases DESC
    LIMIT 5;
    """

    try:
        deaths_data = pd.read_sql(text(query_deaths), engine).to_dict(orient='records')
        cases_data = pd.read_sql(text(query_cases), engine).to_dict(orient='records')

        # Nettoyer les données
        def clean(records):
            clean_list = []
            for row in records:
                clean_row = {}
                for key, value in row.items():
                    if isinstance(value, (np.integer, np.int64, np.int32)):
                        clean_row[key] = int(value)
                    elif isinstance(value, (np.floating, np.float64, np.float32)):
                        clean_row[key] = float(value)
                    elif pd.isnull(value):
                        clean_row[key] = None
                    else:
                        clean_row[key] = value
                clean_list.append(clean_row)
            return clean_list

        return jsonify({
            "top5_deaths": clean(deaths_data),
            "top5_cases": clean(cases_data)
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    

@data_blueprint.route('/country_covid_rates', methods=['GET'])
def country_covid_rates():
    query = """
    WITH total_stats AS (
        SELECT
            ws.country_short_code,
            SUM(ws.week_new_reported_cases) AS total_cases,
            SUM(ws.week_new_reported_deaths) AS total_deaths
        FROM weekly_statistics ws
        JOIN disease d ON ws.disease_id = d.id
        WHERE d.name = 'COVID-19'
        GROUP BY ws.country_short_code
    ),
    latest_population AS (
        SELECT DISTINCT ON (p.country_code)
            p.country_code,
            p.population
        FROM population p
        ORDER BY p.country_code, p.year DESC
    )
    SELECT
        ts.country_short_code AS country_code,
        c.country_name,
        ts.total_cases,
        ts.total_deaths,
        lp.population,
        ROUND((ts.total_cases::DECIMAL / lp.population) * 100, 2) AS case_rate_percent,
        ROUND((ts.total_deaths::DECIMAL / lp.population) * 100, 4) AS death_rate_percent
    FROM total_stats ts
    JOIN country c ON ts.country_short_code = c.country_short_code
    JOIN latest_population lp ON ts.country_short_code = lp.country_code
    WHERE lp.population > 0
    ORDER BY case_rate_percent DESC;
    """
    try:
        data = pd.read_sql(text(query), engine)
        records = data.to_dict(orient='records')

        # Nettoyage pour JSON
        clean_data = []
        for row in records:
            clean_row = {}
            for key, value in row.items():
                if isinstance(value, (np.integer, np.int64, np.int32)):
                    clean_row[key] = int(value)
                elif isinstance(value, (np.floating, np.float64, np.float32)):
                    clean_row[key] = float(value)
                elif pd.isnull(value):
                    clean_row[key] = None
                else:
                    clean_row[key] = value
            clean_data.append(clean_row)

        return jsonify({"data": clean_data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
