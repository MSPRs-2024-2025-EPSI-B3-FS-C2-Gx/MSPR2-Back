import os
import sys
import pytest
# 🔧 ajoute le dossier "app" au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app as flask_app

@pytest.fixture
def client():
    flask_app.config['TESTING'] = True
    with flask_app.test_client() as client:
        yield client

# ✅ Test basique : /api/predicted_weekly_statistics
def test_get_predicted_weekly_statistics(client):
    res = client.get('/api/predicted_weekly_statistics')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list) or 'error' in data

# ✅ Test : /api/country
def test_get_country(client):
    res = client.get('/api/country')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list) or 'error' in data

# ✅ Test paginé : /api/weekly_statistics_total
def test_get_weekly_statistics_total(client):
    res = client.get('/api/weekly_statistics_total?page=1&limit=2')
    assert res.status_code == 200
    data = res.get_json()
    assert 'data' in data
    assert isinstance(data['data'], list)

# ✅ Test filtré : /api/weekly_statistics_by_country
def test_get_weekly_statistics_by_country(client):
    res = client.get('/api/weekly_statistics_by_country?country_code=FR')
    assert res.status_code == 200
    data = res.get_json()
    assert isinstance(data, list) or 'error' in data
