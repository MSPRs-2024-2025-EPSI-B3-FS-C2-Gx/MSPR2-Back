from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, monotonically_increasing_id
from dotenv import load_dotenv
import os

"""
Script ETL pour nettoyer, transformer et enrichir les données COVID-19.
Étapes :
1. Charger les fichiers de données journalières et résumées.
2. Nettoyer et transformer les données (taux de mortalité et de récupération).
3. Joindre les datasets et insérer les données dans PostgreSQL.
"""

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("❌ Une ou plusieurs variables d'environnement sont manquantes dans le fichier .env.")

# Initialiser la session Spark avec le driver PostgreSQL
spark = SparkSession.builder \
    .appName("ETL_COVID19_Postgres") \
    .config("spark.jars", "../postgresql-42.7.5.jar") \
    .getOrCreate()

# Chemins des fichiers CSV
daily_data_path = "data/worldometer_coronavirus_daily_data.csv"
summary_data_path = "data/worldometer_coronavirus_summary_data.csv"

# Étape 1: Charger les fichiers CSV
daily_data = spark.read.csv(daily_data_path, header=True, inferSchema=True)
summary_data = spark.read.csv(summary_data_path, header=True, inferSchema=True)

# Étape 2: Nettoyage des doublons et gestion des valeurs manquantes
daily_data_cleaned = daily_data.dropDuplicates().na.fill(0)
summary_data_cleaned = summary_data.dropDuplicates().na.fill(0)

# Renommer les colonnes conflictuelles
daily_data_cleaned = daily_data_cleaned.withColumnRenamed("active_cases", "daily_active_cases") \
                                       .withColumnRenamed("mortality_rate", "daily_mortality_rate") \
                                       .withColumnRenamed("recovery_rate", "daily_recovery_rate")

summary_data_cleaned = summary_data_cleaned.withColumnRenamed("active_cases", "summary_active_cases") \
                                           .withColumnRenamed("mortality_rate", "summary_mortality_rate") \
                                           .withColumnRenamed("recovery_rate", "summary_recovery_rate")

# Calcul des taux de mortalité et de récupération
daily_data_cleaned = daily_data_cleaned.withColumn(
    "daily_mortality_rate",
    when(col("cumulative_total_cases") > 0, (col("cumulative_total_deaths") / col("cumulative_total_cases")) * 100).otherwise(0)
).withColumn(
    "daily_recovery_rate",
    when(col("cumulative_total_cases") > 0, (col("daily_active_cases") / col("cumulative_total_cases")) * 100).otherwise(0)
)

summary_data_cleaned = summary_data_cleaned.withColumn(
    "summary_mortality_rate",
    when(col("total_confirmed") > 0, (col("total_deaths") / col("total_confirmed")) * 100).otherwise(0)
).withColumn(
    "summary_recovery_rate",
    when(col("total_confirmed") > 0, (col("total_recovered") / col("total_confirmed")) * 100).otherwise(0)
)

# Étape 3: Joindre les données journalières et résumées
enriched_data = daily_data_cleaned.join(summary_data_cleaned, on="country", how="inner")

# Ajout de la colonne id auto-générée
enriched_data = enriched_data.withColumn("id", monotonically_increasing_id())

# Réorganiser les colonnes pour placer "id" en première position
cols = enriched_data.columns
ordered_cols = ["id"] + [c for c in cols if c != "id"]
enriched_data = enriched_data.select(*ordered_cols)

# Étape 4: Connexion PostgreSQL via JDBC
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Étape 5: Insertion des données dans PostgreSQL
enriched_data.write \
    .jdbc(url=postgres_url, table="covid19_data", mode="overwrite", properties=postgres_properties)

print("✅ Données insérées avec succès dans la base PostgreSQL !")

# Étape 6: Arrêter la session Spark
spark.stop()