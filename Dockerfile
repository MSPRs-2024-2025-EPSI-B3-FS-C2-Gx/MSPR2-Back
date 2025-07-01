# Utilise une image officielle Python
FROM python:3.12-slim

# Variables d'environnement pour Flask
ENV FLASK_APP=app/main.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Crée le dossier pour l'application
WORKDIR /app

# Copie les fichiers dans le conteneur
COPY . /app

# Installe les dépendances
RUN pip install --upgrade pip && \
    pip install -r app/requirements.txt

# Expose le port utilisé par Flask
EXPOSE 5000

# Commande pour démarrer l’application
CMD ["python", "app/main.py"]
