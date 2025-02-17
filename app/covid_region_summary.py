from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max as spark_max, sum as spark_sum, when
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv
import os

# Chargement des variables d'environnement nécessaires pour la connexion à la base de données
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Une ou plusieurs variables d'environnement sont manquantes dans le fichier .env.")

# Initialisation de la session Spark pour le traitement des données
spark = SparkSession.builder \
    .appName("ETL_COVID19_Postgres") \
    .config("spark.jars", "../postgresql-42.7.5.jar") \
    .getOrCreate()

# Extraction des données relatives à la Covid-19 depuis des fichiers CSV
vaccination_data = spark.read.option("header", "true").csv("data/data_covid/vaccination-data.csv")
covid_global_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# Conversion de la colonne 'Date_reported' en type date pour assurer la cohérence des types
covid_global_data = covid_global_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))
vaccine_start_date = "2021-01-01"

# Création d'une colonne indiquant si les données datent d'avant ou d'après le début de la vaccination
covid_global_data = covid_global_data.withColumn(
    "vaccine_period",
    when(col("Date_reported") < vaccine_start_date, "Before Vaccine").otherwise("After Vaccine")
)

# Remplacement des valeurs manquantes par des valeurs par défaut afin d'éviter des erreurs lors des agrégations
covid_global_data = covid_global_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0,
    "WHO_region": "OTHER"
})

# Conversion des colonnes de chiffres en type numérique (Double) pour permettre des calculs corrects
covid_global_data = covid_global_data.withColumn("New_cases", col("New_cases").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("New_deaths", col("New_deaths").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType()))

# Agrégation des données par région et par période (avant/après vaccination)
covid_region_summary = covid_global_data.groupBy("WHO_region", "vaccine_period") \
    .agg(
        spark_max("Cumulative_cases").alias("total_cumulative_cases"),  # Utilisation de spark_max pour obtenir le cumul maximal de cas
        spark_sum("New_cases").alias("total_new_cases"),
        spark_sum("New_deaths").alias("total_new_deaths")
    )

# Configuration de la connexion à la base de données PostgreSQL
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Insertion des données agrégées dans la table 'covid_region_summary' de PostgreSQL en mode écrasement
covid_region_summary.write \
    .jdbc(url=postgres_url, table="covid_region_summary", mode="overwrite", properties=postgres_properties)

print("Données régionales insérées avec succès dans PostgreSQL.")

# Arrêt de la session Spark pour libérer les ressources
spark.stop()
