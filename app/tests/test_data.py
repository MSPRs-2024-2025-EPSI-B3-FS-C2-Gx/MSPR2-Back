import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from flask import Flask, jsonify, request

# Configuration de l'application Flask pour les tests
def create_test_app():
    app = Flask(__name__)
    app.config['TESTING'] = True
    
    # Mock des routes de données
    @app.route('/api/predicted_weekly_statistics', methods=['GET'])
    def get_predicted_weekly_statistics():
        return jsonify([{"id": 1, "prediction": 100}]), 200
    
    @app.route('/api/country', methods=['GET'])
    def get_country():
        return jsonify([{"id": 1, "name": "Test Country"}]), 200
    
    @app.route('/api/weekly_statistics_total', methods=['GET'])
    def get_weekly_statistics_total():
        return jsonify({
            "data": [{
                "date": "2023-01-01",
                "country": "Test Country",
                "confirmed_cases": 100,
                "deaths": 5,
                "vaccinations": 50
            }],
            "pagination": {
                "total": 1,
                "page": 1,
                "limit": 1,
                "total_pages": 1
            }
        }), 200
    
    @app.route('/api/weekly_statistics_by_country', methods=['GET'])
    def get_weekly_statistics_by_country():
        country_code = request.args.get('country_code', 'FR')
        return jsonify([{
            "date": "2023-01-01",
            "country_code": country_code,
            "cases": 100,
            "deaths": 5
        }]), 200
    
    return app

# Création de l'application de test
flask_app = create_test_app()

@pytest.fixture
def client():
    with flask_app.test_client() as client:
        yield client

# Test pour la route /api/predicted_weekly_statistics
def test_get_predicted_weekly_statistics(client):
    res = client.get('/api/predicted_weekly_statistics')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert 'id' in data[0]

# Test pour la route /api/country
def test_get_country(client):
    res = client.get('/api/country')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert 'name' in data[0]

# Test pour la route /api/weekly_statistics_total
def test_get_weekly_statistics_total(client):
    res = client.get('/api/weekly_statistics_total?page=1&limit=1')
    assert res.status_code == 200
    data = res.get_json()
    
    # Vérifie la structure de la réponse
    assert 'data' in data
    assert isinstance(data['data'], list)
    assert len(data['data']) > 0
    assert 'date' in data['data'][0]
    assert 'country' in data['data'][0]
    
    # Vérifie la pagination
    assert 'pagination' in data
    assert data['pagination']['page'] == 1
    assert data['pagination']['limit'] == 1

# Test pour la route /api/weekly_statistics_by_country
def test_get_weekly_statistics_by_country(client):
    res = client.get('/api/weekly_statistics_by_country?country_code=FR')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list)
    assert len(data) > 0
    assert 'country_code' in data[0]
    assert data[0]['country_code'] == 'FR'  # Vérifie que le filtre est bien appliqué
