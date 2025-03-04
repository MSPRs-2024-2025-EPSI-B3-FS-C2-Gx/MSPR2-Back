# Importation des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, concat, lit, to_timestamp, sum as spark_sum, avg, when
from pyspark.sql.types import DoubleType
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
    .appName("ETL_COVID19_Grafana_NoNulls") \
    .config("spark.jars", "../postgresql-42.7.5.jar") \
    .getOrCreate()

# Extraction des données à partir du fichier CSV
covid_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# Transformation des données
# Conversion de la colonne "Date_reported" en type date
covid_data = covid_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))

# Remplacement des valeurs NULL par des valeurs par défaut pour assurer la cohérence lors des agrégations
covid_data = covid_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0,
    "Cumulative_deaths": 0,
    "WHO_region": "UNKNOWN"
})

# Conversion des colonnes en types numériques (Double) pour permettre des calculs corrects
covid_data = covid_data.withColumn("New_cases", col("New_cases").cast(DoubleType()))
covid_data = covid_data.withColumn("New_deaths", col("New_deaths").cast(DoubleType()))
covid_data = covid_data.withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType()))
covid_data = covid_data.withColumn("Cumulative_deaths", col("Cumulative_deaths").cast(DoubleType()))

# Agrégation globale des données par année
covid_global_yearly = covid_data.withColumn("Year", year(col("Date_reported"))) \
    .groupBy("Year") \
    .agg(
        spark_sum("New_cases").alias("total_new_cases"),
        spark_sum("New_deaths").alias("total_new_deaths"),
        spark_sum("Cumulative_cases").alias("total_cumulative_cases"),
        spark_sum("Cumulative_deaths").alias("total_cumulative_deaths")
    )

# Conversion de l'année en timestamp (1er janvier de l'année correspondante) pour les agrégats globaux
covid_global_yearly = covid_global_yearly.withColumn(
    "Year_ts",
    to_timestamp(concat(col("Year").cast("string"), lit("-01-01")), "yyyy-MM-dd")
)

# Calcul du taux de létalité global (CFR) par année
covid_global_yearly = covid_global_yearly.withColumn(
    "CFR",
    when(col("total_cumulative_cases") > 0,
         (col("total_cumulative_deaths") / col("total_cumulative_cases")) * 100).otherwise(0)
)

# Agrégation des données par région de l'OMS et par année
covid_region_yearly = covid_data.withColumn("Year", year(col("Date_reported"))) \
    .groupBy("WHO_region", "Year") \
    .agg(
        spark_sum("New_cases").alias("total_new_cases"),
        spark_sum("New_deaths").alias("total_new_deaths")
    )

# Conversion de l'année en timestamp (1er janvier) pour les agrégats régionaux
covid_region_yearly = covid_region_yearly.withColumn(
    "Year_ts",
    to_timestamp(concat(col("Year").cast("string"), lit("-01-01")), "yyyy-MM-dd")
)

# Configuration de la connexion JDBC pour PostgreSQL
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Insertion des agrégats dans les tables PostgreSQL correspondantes
covid_global_yearly.write.jdbc(url=postgres_url, table="covid_global_yearly_summary", mode="overwrite", properties=postgres_properties)
covid_region_yearly.write.jdbc(url=postgres_url, table="covid_region_yearly_summary", mode="overwrite", properties=postgres_properties)

print("Données insérées avec succès dans PostgreSQL sans valeurs NULL.")

# Fermeture de la session Spark pour libérer les ressources
spark.stop()