from flask import Blueprint, jsonify
from sqlalchemy import inspect
import pandas as pd
from database.db import get_db_connection

# Créer le blueprint
tables_blueprint = Blueprint('tables', __name__)

# Récupérer la connexion à la base de données
engine = get_db_connection()

# Fonction pour détecter et corriger l'encodage des chaînes
def decode_safe(value):
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            try:
                return value.decode('latin1')
            except UnicodeDecodeError:
                return value.decode('windows-1252', errors='replace')
    return value

# Endpoint pour récupérer le contenu d'une table
@tables_blueprint.route('/table/<name>', methods=['GET'])
def get_table(name):
    try:
        # Charger la table depuis PostgreSQL
        query = f"SELECT * FROM {name} LIMIT 100;"
        data = pd.read_sql(query, engine)

        # Vérifier et corriger l'encodage des chaînes
        for col in data.select_dtypes(include=['object']).columns:
            data[col] = data[col].apply(lambda x: decode_safe(x) if isinstance(x, str) else x)

        # Convertir en JSON
        result = data.to_dict(orient='records')
        return jsonify({name: result}), 200
    except Exception as e:
        return jsonify({"error": f"Impossible de récupérer la table '{name}': {str(e)}"}), 500
