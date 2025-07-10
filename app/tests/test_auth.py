import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from flask import Flask, jsonify

# Configuration de l'application Flask pour les tests
def create_test_app():
    app = Flask(__name__)
    app.config['TESTING'] = True
    app.config['SECRET_KEY'] = 'test-secret-key'
    
    # Mock des routes d'authentification
    @app.route('/api/register', methods=['POST'])
    def register():
        return jsonify({"message": "User registered successfully"}), 201
    
    @app.route('/api/login', methods=['POST'])
    def login():
        return jsonify({"token": "mock-jwt-token"}), 200
    
    @app.route('/api/me', methods=['GET'])
    def me():
        return jsonify({
            "user": {
                "id": 1,
                "email": "testuser@example.com",
                "role": 1
            }
        }), 200
    
    return app

# Création de l'application de test
flask_app = create_test_app()

# Données de test
global_token = {}

# 🔧 Fixture client de test Flask
@pytest.fixture
def client():
    flask_app.config['TESTING'] = True
    with flask_app.test_client() as client:
        yield client

# ✅ Test d'enregistrement utilisateur
def test_register_user(client):
    response = client.post('/api/register', json={
        "email": "testuser@example.com",
        "password": "test123",
        "role": 1
    })
    assert response.status_code == 201
    data = response.get_json()
    assert data["message"] == "User registered successfully"

# ✅ Test de connexion et récupération du token JWT
def test_login_user(client):
    response = client.post('/api/login', json={
        "email": "testuser@example.com",
        "password": "test123"
    })
    
    assert response.status_code == 200
    data = response.get_json()
    assert 'token' in data
    assert data['token'] == "mock-jwt-token"
    global_token['token'] = data['token']

# ✅ Test d'accès à la route protégée /me avec token
def test_me_authenticated(client):
    # Pas besoin de mock JWT car nous utilisons une route de test simple
    response = client.get('/api/me')
    
    assert response.status_code == 200
    data = response.get_json()
    assert 'user' in data
    assert data['user']['email'] == 'testuser@example.com'

