from flask import Blueprint, request, jsonify, current_app
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import text
from database.db import get_db_connection
import datetime
import jwt
from functools import wraps

users_blueprint = Blueprint('users', __name__)

# 🔐 Décorateur pour protéger les routes avec un token
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header.split(" ")[1]

        if not token:
            return jsonify({'error': 'Token manquant'}), 401

        try:
            decoded = jwt.decode(token, current_app.config['SECRET_KEY'], algorithms=["HS256"])
            request.user = decoded
        except jwt.ExpiredSignatureError:
            return jsonify({'error': 'Token expiré'}), 401
        except jwt.InvalidTokenError:
            return jsonify({'error': 'Token invalide'}), 401

        return f(*args, **kwargs)
    return decorated

# ✅ Inscription d'un utilisateur
@users_blueprint.route('/register', methods=['POST'])
def register_user():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    role = data.get('role')

    if not email or not password or role is None:
        return jsonify({'error': 'Champs manquants'}), 400

    hashed_pw = generate_password_hash(password)

    try:
        conn = get_db_connection()
        with conn.begin() as connection:
            result = connection.execute(
                text("SELECT * FROM users WHERE email = :email"),
                {"email": email}
            ).fetchone()

            if result:
                return jsonify({'error': 'Utilisateur déjà existant'}), 409

            connection.execute(
                text("INSERT INTO users (email, password, role) VALUES (:email, :password, :role)"),
                {"email": email, "password": hashed_pw, "role": role}
            )

        return jsonify({'message': 'Utilisateur enregistré'}), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ✅ Connexion avec génération de token JWT
@users_blueprint.route('/login', methods=['POST'])
def login_user():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    if not email or not password:
        return jsonify({'error': 'Champs manquants'}), 400

    try:
        conn = get_db_connection()
        with conn.connect() as connection:
            user = connection.execute(
                text("SELECT id_user, email, password, role FROM users WHERE email = :email"),
                {"email": email}
            ).fetchone()

            if user and check_password_hash(user[2], password):
                payload = {
                    'id_user': user[0],
                    'email': user[1],
                    'role': user[3],
                    'exp': datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=2)
                }
                token = jwt.encode(payload, current_app.config['SECRET_KEY'], algorithm='HS256')
                if isinstance(token, bytes):
                    token = token.decode('utf-8')

                return jsonify({
                    'message': 'Connexion réussie',
                    'token': token,
                    'user': {
                        'id_user': user[0],
                        'email': user[1],
                        'role': user[3]
                    }
                }), 200

            return jsonify({'error': 'Identifiants invalides'}), 401

    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ✅ Route protégée pour obtenir les infos du token
@users_blueprint.route('/me', methods=['GET'])
@token_required
def get_current_user():
    return jsonify({
        'user': {
            'id_user': request.user['id_user'],
            'email': request.user['email'],
            'role': request.user['role']
        }
    }), 200

@users_blueprint.route('/delete', methods=['DELETE'])
@token_required
def delete_user():
    user_id = request.user['id_user']
    try:
        conn = get_db_connection()
        with conn.begin() as connection:
            connection.execute(
                text("DELETE FROM users WHERE id_user = :id_user"),
                {"id_user": user_id}
            )
        return jsonify({'message': 'Compte supprimé avec succès'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500