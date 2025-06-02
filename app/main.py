from flask import Flask, jsonify
from routes.predict import predict_blueprint
from routes.metrics import metrics_blueprint
from routes.tables import tables_blueprint
from routes.data import data_blueprint
from routes.worldmap import worldmap_blueprint
from routes.graph import graph_blueprint

from database.db import get_db_connection

app = Flask(__name__)

# Vérification de l'état du backend
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "running"}), 200

@app.route('/api/encoding', methods=['GET'])
def check_encoding():
    try:
        conn = get_db_connection()
        if conn:
            with conn.connect() as connection:
                # Vérifier l'encodage serveur et client
                server_encoding = connection.execute("SHOW server_encoding;").fetchone()[0]
                client_encoding = connection.execute("SHOW client_encoding;").fetchone()[0]
                return jsonify({"server_encoding": server_encoding, "client_encoding": client_encoding}), 200
        return jsonify({"error": "Connexion à la base de données impossible"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Enregistrement des blueprints
app.register_blueprint(predict_blueprint, url_prefix="/api")
app.register_blueprint(metrics_blueprint, url_prefix="/api")
app.register_blueprint(tables_blueprint, url_prefix="/api")
app.register_blueprint(data_blueprint, url_prefix="/api")
app.register_blueprint(worldmap_blueprint, url_prefix="/api")
app.register_blueprint(graph_blueprint, url_prefix="/api")

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)