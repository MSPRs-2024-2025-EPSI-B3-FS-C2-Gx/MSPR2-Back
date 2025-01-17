
# Import des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, sum as spark_sum, avg, when
from pyspark.sql.types import DoubleType
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

# 🚀 Initialisation de la session Spark
spark = SparkSession.builder     .appName("ETL_COVID19_Grafana_NoNulls")     .config("spark.jars", "../postgresql-42.7.5.jar")     .getOrCreate()

# 1️⃣ EXTRACTION
covid_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# 2️⃣ TRANSFORMATION
# ➡️ Conversion des colonnes nécessaires
covid_data = covid_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))

# ➡️ Remplir les valeurs NULL avec 0 pour éviter les erreurs dans les agrégations
covid_data = covid_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0,
    "Cumulative_deaths": 0,
    "WHO_region": "UNKNOWN"
})

# ➡️ Conversion des colonnes en types numériques
covid_data = covid_data.withColumn("New_cases", col("New_cases").cast(DoubleType()))
covid_data = covid_data.withColumn("New_deaths", col("New_deaths").cast(DoubleType()))
covid_data = covid_data.withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType()))
covid_data = covid_data.withColumn("Cumulative_deaths", col("Cumulative_deaths").cast(DoubleType()))

# ➡️ Agrégation globale par année
covid_global_yearly = covid_data.withColumn("Year", year(col("Date_reported")))     .groupBy("Year")     .agg(
        spark_sum("New_cases").alias("total_new_cases"),
        spark_sum("New_deaths").alias("total_new_deaths"),
        spark_sum("Cumulative_cases").alias("total_cumulative_cases"),
        spark_sum("Cumulative_deaths").alias("total_cumulative_deaths")
    )

# ➡️ Calcul du CFR global
covid_global_yearly = covid_global_yearly.withColumn(
    "CFR",
    when(col("total_cumulative_cases") > 0, (col("total_cumulative_deaths") / col("total_cumulative_cases")) * 100).otherwise(0)
)

# ➡️ Agrégation par région OMS et année
covid_region_yearly = covid_data.withColumn("Year", year(col("Date_reported")))     .groupBy("WHO_region", "Year")     .agg(
        spark_sum("New_cases").alias("total_new_cases"),
        spark_sum("New_deaths").alias("total_new_deaths")
    )

# 3️⃣ CHARGEMENT dans PostgreSQL sans valeurs NULL
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Insertion des données corrigées dans de nouvelles tables
covid_global_yearly.write.jdbc(url=postgres_url, table="covid_global_yearly_summary", mode="overwrite", properties=postgres_properties)
covid_region_yearly.write.jdbc(url=postgres_url, table="covid_region_yearly_summary", mode="overwrite", properties=postgres_properties)

print("✅ Données insérées sans valeurs NULL dans PostgreSQL !")

# 4️⃣ Fermeture de la session Spark
spark.stop()
