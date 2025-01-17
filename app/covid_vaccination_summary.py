# Import des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, sum as spark_sum, row_number
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

# 🔒 Chargement des variables d'environnement
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("❌ Une ou plusieurs variables d'environnement sont manquantes dans le fichier .env.")

# 🚀 Initialisation de la session Spark avec le driver PostgreSQL
spark = SparkSession.builder \
    .appName("ETL_COVID19_Postgres") \
    .config("spark.jars", "../postgresql-42.7.5.jar") \
    .getOrCreate()

# 1️⃣ EXTRACTION : Chargement des fichiers CSV
vaccination_data = spark.read.option("header", "true").csv("data/data_covid/vaccination-data.csv")
covid_global_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# 2️⃣ TRANSFORMATION : Nettoyage et normalisation des données
covid_global_data = covid_global_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))

# Suppression des doublons et gestion des valeurs manquantes
vaccination_data = vaccination_data.dropDuplicates().fillna({
    "PERSONS_VACCINATED_1PLUS_DOSE": 0,
    "PERSONS_LAST_DOSE": 0
})

covid_global_data = covid_global_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0
})

# Conversion des colonnes en types numériques
vaccination_data = vaccination_data.withColumn("PERSONS_VACCINATED_1PLUS_DOSE", col("PERSONS_VACCINATED_1PLUS_DOSE").cast(DoubleType()))
vaccination_data = vaccination_data.withColumn("PERSONS_LAST_DOSE", col("PERSONS_LAST_DOSE").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("New_cases", col("New_cases").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("New_deaths", col("New_deaths").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType()))

# 3️⃣ AGRÉGATION

# Fenêtre pour sélectionner la dernière valeur des cas cumulés par pays
window_spec = Window.partitionBy("Country").orderBy(col("Date_reported").desc())

# Récupération de la dernière entrée pour les cas cumulés
latest_covid_data = covid_global_data.withColumn("row_num", row_number().over(window_spec)) \
                                     .filter(col("row_num") == 1) \
                                     .drop("row_num")

# Agrégation des données de vaccination
vaccination_summary = vaccination_data.groupBy("COUNTRY") \
    .agg(avg("PERSONS_VACCINATED_1PLUS_DOSE").alias("avg_people_vaccinated"),
         avg("PERSONS_LAST_DOSE").alias("avg_people_fully_vaccinated"))

# ➡️ Agrégation des nouveaux cas et des décès sur l'ensemble des données
covid_summary = covid_global_data.groupBy("Country") \
    .agg(
        avg("New_cases").alias("avg_new_cases"),
        avg("New_deaths").alias("avg_new_deaths")
    )

# ➡️ Agrégation des cas cumulés à partir des dernières données
covid_cumulative_summary = latest_covid_data.groupBy("Country") \
    .agg(spark_sum("Cumulative_cases").alias("total_cumulative_cases"))

# ➡️ Fusion des données COVID (nouveaux cas et cumulés)
covid_combined_summary = covid_summary.join(
    covid_cumulative_summary,
    covid_summary.Country == covid_cumulative_summary.Country,
    "inner"
).select(
    covid_summary.Country,
    "avg_new_cases",
    "avg_new_deaths",
    "total_cumulative_cases"
)

# 4️⃣ JOINTURE : Analyse croisée des données
combined_data = vaccination_summary.join(
    covid_combined_summary,
    vaccination_summary.COUNTRY == covid_combined_summary.Country,
    "inner"
).select(
    vaccination_summary.COUNTRY.alias("Country"),
    "avg_people_vaccinated",
    "avg_people_fully_vaccinated",
    "avg_new_cases",
    "avg_new_deaths",
    "total_cumulative_cases"
)

# 5️⃣ CONNEXION PostgreSQL via JDBC
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# 6️⃣ INSERTION DES DONNÉES DANS POSTGRESQL
combined_data.write \
    .jdbc(url=postgres_url, table="covid_vaccination_summary", mode="overwrite", properties=postgres_properties)

print("✅ Données insérées avec succès dans la base PostgreSQL !")

# 7️⃣ Arrêt de la session Spark
spark.stop()