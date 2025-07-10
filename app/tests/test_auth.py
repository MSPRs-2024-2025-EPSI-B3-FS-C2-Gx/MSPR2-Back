import os
import sys
import pytest
# 🔧 ajoute le dossier "app" au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app as flask_app
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
    assert response.status_code in [201, 409]  # 409 si déjà existant

# ✅ Test de connexion et récupération du token JWT
def test_login_user(client):
    response = client.post('/api/login', json={
        "email": "testuser@example.com",
        "password": "test123"
    })
    assert response.status_code == 200
    data = response.get_json()
    assert 'token' in data
    global_token['token'] = data['token']

# ✅ Test d'accès à la route protégée /me avec token
def test_me_authenticated(client):
    token = global_token.get('token')
    assert token is not None

    response = client.get('/api/me', headers={
        'Authorization': f'Bearer {token}'
    })
    assert response.status_code == 200
    data = response.get_json()
    assert 'user' in data
    assert data['user']['email'] == 'testuser@example.com'

