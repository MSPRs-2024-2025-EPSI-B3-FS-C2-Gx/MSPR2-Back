from flask import Flask, request, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
import os
from datetime import datetime
from flasgger import Swagger

app = Flask(__name__)
swagger = Swagger(app)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ma_base")
DB_USER = os.getenv("DB_USER", "mon_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mon_password")

app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)


# 1) Modèle CountryStatistics
class CountryStatistics(db.Model):
    __tablename__ = 'country_statistics'
    
    country = db.Column("Country", db.String(255), primary_key=True)
    total_cases = db.Column("total_cases", db.Integer)
    total_vaccinated = db.Column("total_vaccinated", db.Integer)

    def to_dict(self):
        return {
            'country': self.country,
            'total_cases': self.total_cases,
            'total_vaccinated': self.total_vaccinated
        }


# 2) Modèle RegionYearlySummary
class RegionYearlySummary(db.Model):
    __tablename__ = 'region_yearly_summary'
    
    who_region = db.Column("WHO_region", db.String(50), primary_key=True)
    year = db.Column("Year", db.Integer, primary_key=True)
    total_cases = db.Column("total_cases", db.Integer)
    total_deaths = db.Column("total_deaths", db.Integer)
    
    def to_dict(self):
        return {
            'who_region': self.who_region,
            'year': self.year,
            'total_cases': self.total_cases,
            'total_deaths': self.total_deaths
        }


# CRUD pour CountryStatistics

@app.route('/country_statistics', methods=['GET'])
def get_all_country_statistics():
    """
    Récupère la liste de tous les enregistrements de la table country_statistics.
    ---
    tags:
      - Country Statistics
    responses:
      200:
        description: Liste de tous les enregistrements
    """
    records = CountryStatistics.query.all()
    return jsonify([r.to_dict() for r in records]), 200

@app.route('/country_statistics/<string:country>', methods=['GET'])
def get_country_statistics(country):
    """
    Récupère un enregistrement par le champ 'country'.
    ---
    tags:
      - Country Statistics
    parameters:
      - name: country
        in: path
        description: Nom du pays
        required: true
        schema:
          type: string
    responses:
      200:
        description: Enregistrement trouvé
      404:
        description: Enregistrement non trouvé
    """
    record = CountryStatistics.query.get_or_404(country)
    return jsonify(record.to_dict()), 200

@app.route('/country_statistics', methods=['POST'])
def create_country_statistics():
    """
    Crée un nouvel enregistrement dans la table country_statistics.
    ---
    tags:
      - Country Statistics
    parameters:
      - name: body
        in: body
        required: true
        description: Données du nouveau pays
        schema:
          type: object
          required:
            - country
          properties:
            country:
              type: string
              example: "France"
            total_cases:
              type: integer
              example: 12000
            total_vaccinated:
              type: integer
              example: 3000
    responses:
      201:
        description: Enregistrement créé
      400:
        description: Erreur dans les données fournies
    """
    data = request.get_json()
    if not data or 'country' not in data:
        return jsonify({"error": "Le champ 'country' est obligatoire"}), 400

    new_record = CountryStatistics(
        country=data['country'],
        total_cases=data.get('total_cases', 0),
        total_vaccinated=data.get('total_vaccinated', 0)
    )
    db.session.add(new_record)
    db.session.commit()
    return jsonify({"message": "Enregistrement créé avec succès"}), 201

@app.route('/country_statistics/<string:country>', methods=['PUT'])
def update_country_statistics(country):
    """
    Met à jour un enregistrement existant dans la table country_statistics.
    ---
    tags:
      - Country Statistics
    parameters:
      - name: country
        in: path
        description: Nom du pays à mettre à jour
        required: true
        schema:
          type: string
      - name: body
        in: body
        required: true
        description: Champs à mettre à jour
        schema:
          type: object
          properties:
            total_cases:
              type: integer
            total_vaccinated:
              type: integer
    responses:
      200:
        description: Enregistrement mis à jour
      404:
        description: Enregistrement non trouvé
    """
    record = CountryStatistics.query.get_or_404(country)
    data = request.get_json()
    if not data:
        return jsonify({"error": "Aucune donnée fournie pour la mise à jour"}), 400

    if 'total_cases' in data:
        record.total_cases = data['total_cases']
    if 'total_vaccinated' in data:
        record.total_vaccinated = data['total_vaccinated']

    db.session.commit()
    return jsonify({"message": "Enregistrement mis à jour avec succès"}), 200

@app.route('/country_statistics/<string:country>', methods=['DELETE'])
def delete_country_statistics(country):
    """
    Supprime un enregistrement de la table country_statistics.
    ---
    tags:
      - Country Statistics
    parameters:
      - name: country
        in: path
        description: Nom du pays à supprimer
        required: true
        schema:
          type: string
    responses:
      200:
        description: Enregistrement supprimé
      404:
        description: Enregistrement non trouvé
    """
    record = CountryStatistics.query.get_or_404(country)
    db.session.delete(record)
    db.session.commit()
    return jsonify({"message": "Enregistrement supprimé avec succès"}), 200


# CRUD pour RegionYearlySummary 

@app.route('/region_yearly_summary', methods=['GET'])
def get_all_region_yearly_summaries():
    """
    Récupère la liste de tous les enregistrements de la table region_yearly_summary.
    ---
    tags:
      - Region Yearly Summary
    responses:
      200:
        description: Liste de tous les enregistrements
    """
    records = RegionYearlySummary.query.all()
    return jsonify([r.to_dict() for r in records]), 200

@app.route('/region_yearly_summary/<string:who_region>/<int:year>', methods=['GET'])
def get_region_yearly_summary(who_region, year):
    """
    Récupère un enregistrement par la clé composite (who_region et year).
    ---
    tags:
      - Region Yearly Summary
    parameters:
      - name: who_region
        in: path
        description: Région (WHO_region)
        required: true
        schema:
          type: string
      - name: year
        in: path
        description: Année
        required: true
        schema:
          type: integer
    responses:
      200:
        description: Enregistrement trouvé
      404:
        description: Enregistrement non trouvé
    """
    record = RegionYearlySummary.query.get_or_404((who_region, year))
    return jsonify(record.to_dict()), 200

@app.route('/region_yearly_summary', methods=['POST'])
def create_region_yearly_summary():
    """
    Crée un nouvel enregistrement dans la table region_yearly_summary.
    ---
    tags:
      - Region Yearly Summary
    parameters:
      - name: body
        in: body
        required: true
        description: Données pour le résumé régional
        schema:
          type: object
          required:
            - who_region
            - year
          properties:
            who_region:
              type: string
              example: "Europe"
            year:
              type: integer
              example: 2021
            total_cases:
              type: integer
              example: 200000
            total_deaths:
              type: integer
              example: 5000
    responses:
      201:
        description: Enregistrement créé
      400:
        description: Erreur dans les données fournies
    """
    data = request.get_json()
    if not data or 'who_region' not in data or 'year' not in data:
        return jsonify({"error": "Les champs 'who_region' et 'year' sont obligatoires"}), 400

    new_record = RegionYearlySummary(
        who_region=data['who_region'],
        year=data['year'],
        total_cases=data.get('total_cases', 0),
        total_deaths=data.get('total_deaths', 0)
    )
    db.session.add(new_record)
    db.session.commit()
    return jsonify({"message": "Enregistrement créé avec succès"}), 201

@app.route('/region_yearly_summary/<string:who_region>/<int:year>', methods=['PUT'])
def update_region_yearly_summary(who_region, year):
    """
    Met à jour un enregistrement existant dans la table region_yearly_summary.
    ---
    tags:
      - Region Yearly Summary
    parameters:
      - name: who_region
        in: path
        description: Région (WHO_region)
        required: true
        schema:
          type: string
      - name: year
        in: path
        description: Année
        required: true
        schema:
          type: integer
      - name: body
        in: body
        required: true
        description: Champs à mettre à jour
        schema:
          type: object
          properties:
            total_cases:
              type: integer
            total_deaths:
              type: integer
    responses:
      200:
        description: Enregistrement mis à jour
      404:
        description: Enregistrement non trouvé
    """
    record = RegionYearlySummary.query.get_or_404((who_region, year))
    data = request.get_json()
    if not data:
        return jsonify({"error": "Aucune donnée fournie pour la mise à jour"}), 400

    if 'total_cases' in data:
        record.total_cases = data['total_cases']
    if 'total_deaths' in data:
        record.total_deaths = data['total_deaths']

    db.session.commit()
    return jsonify({"message": "Enregistrement mis à jour avec succès"}), 200

@app.route('/region_yearly_summary/<string:who_region>/<int:year>', methods=['DELETE'])
def delete_region_yearly_summary(who_region, year):
    """
    Supprime un enregistrement de la table region_yearly_summary.
    ---
    tags:
      - Region Yearly Summary
    parameters:
      - name: who_region
        in: path
        description: Région (WHO_region)
        required: true
        schema:
          type: string
      - name: year
        in: path
        description: Année
        required: true
        schema:
          type: integer
    responses:
      200:
        description: Enregistrement supprimé
      404:
        description: Enregistrement non trouvé
    """
    record = RegionYearlySummary.query.get_or_404((who_region, year))
    db.session.delete(record)
    db.session.commit()
    return jsonify({"message": "Enregistrement supprimé avec succès"}), 200


if __name__ == '__main__':
    app.run(debug=True)
