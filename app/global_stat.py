# Importation des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, avg, sum as spark_sum, row_number
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

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

# Extraction des données à partir des fichiers CSV
vaccination_data = spark.read.option("header", "true").csv("data/data_covid/vaccination-data.csv")
covid_global_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# Conversion de la colonne 'Date_reported' en type date
covid_global_data = covid_global_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))

# Suppression des doublons et gestion des valeurs manquantes pour les données de vaccination
vaccination_data = vaccination_data.dropDuplicates().fillna({
    "PERSONS_VACCINATED_1PLUS_DOSE": 0,
    "PERSONS_LAST_DOSE": 0
})

# Remplacement des valeurs manquantes dans les données COVID-19 globales
covid_global_data = covid_global_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0
})

# Conversion des colonnes en types numériques (Double) pour permettre les calculs
vaccination_data = vaccination_data.withColumn("PERSONS_VACCINATED_1PLUS_DOSE", col("PERSONS_VACCINATED_1PLUS_DOSE").cast(DoubleType()))
vaccination_data = vaccination_data.withColumn("PERSONS_LAST_DOSE", col("PERSONS_LAST_DOSE").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("New_cases", col("New_cases").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("New_deaths", col("New_deaths").cast(DoubleType()))
covid_global_data = covid_global_data.withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType()))

# Agrégation des données COVID-19
# Définition d'une fenêtre pour sélectionner la dernière entrée (la plus récente) par pays en fonction de la date
window_spec = Window.partitionBy("Country").orderBy(col("Date_reported").desc())

# Sélection de la dernière entrée pour chaque pays concernant les cas cumulés
latest_covid_data = covid_global_data.withColumn("row_num", row_number().over(window_spec)) \
                                      .filter(col("row_num") == 1) \
                                      .drop("row_num")

# Calcul de la moyenne (moyenne des valeurs cumulatives) pour les personnes vaccinées
# Renommage en total_cumulative_people_vaccinated et total_cumulative_people_fully_vaccinated pour refléter le contenu
vaccination_summary = vaccination_data.groupBy("COUNTRY") \
    .agg(
        avg("PERSONS_VACCINATED_1PLUS_DOSE").alias("total_cumulative_people_vaccinated"),
        avg("PERSONS_LAST_DOSE").alias("total_cumulative_people_fully_vaccinated")
    )

# Calcul des moyennes des nouveaux cas et des nouveaux décès par pays
covid_summary = covid_global_data.groupBy("Country") \
    .agg(
        avg("New_cases").alias("avg_new_cases"),
        avg("New_deaths").alias("avg_new_deaths")
    )

# Calcul du total des cas cumulés par pays en utilisant les dernières données disponibles
covid_cumulative_summary = latest_covid_data.groupBy("Country") \
    .agg(spark_sum("Cumulative_cases").alias("total_cumulative_cases"))

# 🔥 Récupération du total mondial des cas cumulés
global_cumulative_cases = covid_cumulative_summary.agg(
    spark_sum("total_cumulative_cases").alias("global_total_cumulative_cases")
)

# Configuration de la connexion JDBC pour PostgreSQL
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ✅ Insertion des données dans "global_total_cumulative_cases"
global_cumulative_cases.write \
    .jdbc(url=postgres_url, table="global_total_cumulative_cases", mode="overwrite", properties=postgres_properties)

print("📊 Total des cas cumulés inséré dans global_total_cumulative_cases avec succès !")

# Fusion des agrégats COVID-19 : nouveaux cas, nouveaux décès et cas cumulés
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

# Jointure entre les données de vaccination et les données COVID-19 fusionnées
# Ici, nous conservons également la colonne total_cumulative_cases dans global_statistics
combined_data = vaccination_summary.join(
    covid_combined_summary,
    vaccination_summary.COUNTRY == covid_combined_summary.Country,
    "inner"
).select(
    vaccination_summary.COUNTRY.alias("Country"),
    "total_cumulative_people_vaccinated",
    "total_cumulative_people_fully_vaccinated",
    "avg_new_cases",
    "avg_new_deaths",
    "total_cumulative_cases"
)

# ✅ Insertion des données fusionnées dans la table "global_statistics"
combined_data.write \
    .jdbc(url=postgres_url, table="global_statistics", mode="overwrite", properties=postgres_properties)

print("✅ Données insérées avec succès dans la table global_statistics.")

# Fermeture de la session Spark pour libérer les ressources
spark.stop()
