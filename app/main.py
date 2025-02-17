from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from dotenv import load_dotenv
import os

"""
Script ETL pour le nettoyage, la transformation et l'enrichissement des données COVID-19.
Les étapes réalisées dans ce script sont les suivantes :
1. Chargement des fichiers de données journalières et résumées.
2. Nettoyage et transformation des données, incluant le calcul des taux de mortalité et de récupération.
3. Fusion des deux ensembles de données et insertion du résultat dans une base de données PostgreSQL.
"""

# Chargement des variables d'environnement depuis le fichier .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Une ou plusieurs variables d'environnement sont manquantes dans le fichier .env.")

# Initialisation de la session Spark avec le driver PostgreSQL
spark = SparkSession.builder \
    .appName("ETL_COVID19_Postgres") \
    .config("spark.jars", "../postgresql-42.7.5.jar") \
    .getOrCreate()

# Définition des chemins vers les fichiers CSV contenant les données journalières et résumées
daily_data_path = "data/worldometer_coronavirus_daily_data.csv"
summary_data_path = "data/worldometer_coronavirus_summary_data.csv"

# Chargement des fichiers CSV
daily_data = spark.read.csv(daily_data_path, header=True, inferSchema=True)
summary_data = spark.read.csv(summary_data_path, header=True, inferSchema=True)

# Nettoyage des données en supprimant les doublons et en remplissant les valeurs manquantes par 0
daily_data_cleaned = daily_data.dropDuplicates().na.fill(0)
summary_data_cleaned = summary_data.dropDuplicates().na.fill(0)

# Renommage des colonnes pour éviter les conflits lors de la fusion des jeux de données
daily_data_cleaned = daily_data_cleaned.withColumnRenamed("active_cases", "daily_active_cases") \
                                       .withColumnRenamed("mortality_rate", "daily_mortality_rate") \
                                       .withColumnRenamed("recovery_rate", "daily_recovery_rate")

summary_data_cleaned = summary_data_cleaned.withColumnRenamed("active_cases", "summary_active_cases") \
                                           .withColumnRenamed("mortality_rate", "summary_mortality_rate") \
                                           .withColumnRenamed("recovery_rate", "summary_recovery_rate")

# Calcul des taux de mortalité et de récupération pour les données journalières
daily_data_cleaned = daily_data_cleaned.withColumn(
    "daily_mortality_rate",
    when(col("cumulative_total_cases") > 0,
         (col("cumulative_total_deaths") / col("cumulative_total_cases")) * 100).otherwise(0)
).withColumn(
    "daily_recovery_rate",
    when(col("cumulative_total_cases") > 0,
         (col("daily_active_cases") / col("cumulative_total_cases")) * 100).otherwise(0)
)

# Calcul des taux de mortalité et de récupération pour les données résumées
summary_data_cleaned = summary_data_cleaned.withColumn(
    "summary_mortality_rate",
    when(col("total_confirmed") > 0,
         (col("total_deaths") / col("total_confirmed")) * 100).otherwise(0)
).withColumn(
    "summary_recovery_rate",
    when(col("total_confirmed") > 0,
         (col("total_recovered") / col("total_confirmed")) * 100).otherwise(0)
)

# Fusion des données journalières et résumées en utilisant la colonne "country" comme clé
enriched_data = daily_data_cleaned.join(summary_data_cleaned, on="country", how="inner")

# Configuration de la connexion JDBC pour PostgreSQL
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Insertion des données enrichies dans la table "covid19_data" de PostgreSQL
enriched_data.write \
    .jdbc(url=postgres_url, table="covid19_data", mode="overwrite", properties=postgres_properties)

print("Données insérées avec succès dans la base PostgreSQL.")

# Fermeture de la session Spark pour libérer les ressources
spark.stop()
