from flask import Flask, request, jsonify, abort
from flask_sqlalchemy import SQLAlchemy
import os
from datetime import datetime

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "ma_base")
DB_USER = os.getenv("DB_USER", "mon_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "mon_password")

app.config['SQLALCHEMY_DATABASE_URI'] = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class CovidData(db.Model):
    __tablename__ = 'covid19_data'
    id = db.Column(db.Integer, primary_key=True)
    country = db.Column(db.String(100), nullable=False)
    date = db.Column(db.DateTime, nullable=False)
    cumulative_total_cases = db.Column(db.Float)
    daily_new_cases = db.Column(db.Float)
    daily_active_cases = db.Column(db.Float)
    cumulative_total_deaths = db.Column(db.Float)
    daily_new_deaths = db.Column(db.Float)
    daily_mortality_rate = db.Column(db.Float)
    daily_recovery_rate = db.Column(db.Float)
    continent = db.Column(db.String(50))
    total_confirmed = db.Column(db.Integer)
    total_deaths = db.Column(db.Float)
    total_recovered = db.Column(db.Float)
    summary_active_cases = db.Column(db.Float)
    serious_or_critical = db.Column(db.Float)
    total_cases_per_1m_population = db.Column(db.Float)
    total_deaths_per_1m_population = db.Column(db.Float)
    total_tests = db.Column(db.Float)
    total_tests_per_1m_population = db.Column(db.Float)
    population = db.Column(db.BigInteger)
    summary_mortality_rate = db.Column(db.Float)
    summary_recovery_rate = db.Column(db.Float)

    def to_dict(self, fields=None):
        data = {
            'id': self.id,
            'country': self.country,
            'date': self.date.isoformat(),
            'cumulative_total_cases': self.cumulative_total_cases,
            'daily_new_cases': self.daily_new_cases,
            'daily_active_cases': self.daily_active_cases,
            'cumulative_total_deaths': self.cumulative_total_deaths,
            'daily_new_deaths': self.daily_new_deaths,
            'daily_mortality_rate': self.daily_mortality_rate,
            'daily_recovery_rate': self.daily_recovery_rate,
            'continent': self.continent,
            'total_confirmed': self.total_confirmed,
            'total_deaths': self.total_deaths,
            'total_recovered': self.total_recovered,
            'summary_active_cases': self.summary_active_cases,
            'serious_or_critical': self.serious_or_critical,
            'total_cases_per_1m_population': self.total_cases_per_1m_population,
            'total_deaths_per_1m_population': self.total_deaths_per_1m_population,
            'total_tests': self.total_tests,
            'total_tests_per_1m_population': self.total_tests_per_1m_population,
            'population': self.population,
            'summary_mortality_rate': self.summary_mortality_rate,
            'summary_recovery_rate': self.summary_recovery_rate,
        }
        if fields:
            return {k: v for k, v in data.items() if k in fields or k == 'id'}
        return data


@app.route('/coviddata', methods=['GET'])
def get_covid_data():
    fields_param = request.args.get('fields')
    fields = fields_param.split(',') if fields_param else None

    records = CovidData.query.all()
    data = [record.to_dict(fields) for record in records]
    return jsonify(data), 200

@app.route('/coviddata/<int:id>', methods=['GET'])
def get_covid_data_item(id):
    record = CovidData.query.get_or_404(id)
    return jsonify(record.to_dict()), 200

@app.route('/coviddata', methods=['POST'])
def add_covid_data():
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Aucune donnée fournie'}), 400

    if 'country' not in data or 'date' not in data:
        return jsonify({'error': "Les champs 'country' et 'date' sont obligatoires."}), 400

    try:
        date_value = datetime.fromisoformat(data.get('date'))
    except Exception:
        return jsonify({'error': 'Format de date invalide. Attendu un format ISO.'}), 400

    new_record = CovidData(
        country=data.get('country'),
        date=date_value,
        cumulative_total_cases=data.get('cumulative_total_cases'),
        daily_new_cases=data.get('daily_new_cases'),
        daily_active_cases=data.get('daily_active_cases'),
        cumulative_total_deaths=data.get('cumulative_total_deaths'),
        daily_new_deaths=data.get('daily_new_deaths'),
        daily_mortality_rate=data.get('daily_mortality_rate'),
        daily_recovery_rate=data.get('daily_recovery_rate'),
        continent=data.get('continent'),
        total_confirmed=data.get('total_confirmed'),
        total_deaths=data.get('total_deaths'),
        total_recovered=data.get('total_recovered'),
        summary_active_cases=data.get('summary_active_cases'),
        serious_or_critical=data.get('serious_or_critical'),
        total_cases_per_1m_population=data.get('total_cases_per_1m_population'),
        total_deaths_per_1m_population=data.get('total_deaths_per_1m_population'),
        total_tests=data.get('total_tests'),
        total_tests_per_1m_population=data.get('total_tests_per_1m_population'),
        population=data.get('population'),
        summary_mortality_rate=data.get('summary_mortality_rate'),
        summary_recovery_rate=data.get('summary_recovery_rate')
    )
    db.session.add(new_record)
    db.session.commit()
    return jsonify({'message': 'Donnée ajoutée avec succès', 'id': new_record.id}), 201

@app.route('/coviddata/<int:id>', methods=['PUT'])
def update_covid_data(id):
    record = CovidData.query.get_or_404(id)
    data = request.get_json()

    if not data:
        return jsonify({'error': 'Aucune donnée fournie pour la mise à jour.'}), 400

    for key, value in data.items():
        if key in CovidData.__table__.columns.keys() and key != 'id':
            if key == 'date':
                try:
                    setattr(record, key, datetime.fromisoformat(value))
                except Exception:
                    return jsonify({'error': 'Format de date invalide pour le champ date.'}), 400
            else:
                setattr(record, key, value)

    db.session.commit()
    return jsonify({'message': 'Donnée mise à jour avec succès'}), 200

@app.route('/coviddata/<int:id>', methods=['DELETE'])
def delete_covid_data(id):
    record = CovidData.query.get_or_404(id)
    db.session.delete(record)
    db.session.commit()
    return jsonify({'message': 'Donnée supprimée avec succès'}), 200

if __name__ == '__main__':
    app.run(debug=True)