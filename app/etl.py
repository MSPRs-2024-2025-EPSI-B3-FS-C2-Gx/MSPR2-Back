# Importation des bibliothèques nécessaires
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, concat, lit, to_timestamp, max as spark_max, sum as spark_sum, row_number, when
from pyspark.sql.types import DoubleType, IntegerType
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

##########################################
# ETL region_yearly_summary (ETL1)
##########################################

# Extraction des données à partir du fichier CSV
covid_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# Transformation des données
# Conversion de la colonne "Date_reported" en type date
covid_data = covid_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))

# Remplacement des valeurs NULL par des valeurs par défaut
covid_data = covid_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0,
    "Cumulative_deaths": 0,
    "WHO_region": "UNKNOWN"
})

# Conversion des colonnes numériques en type Double
covid_data = covid_data.withColumn("New_cases", col("New_cases").cast(DoubleType())) \
                       .withColumn("New_deaths", col("New_deaths").cast(DoubleType())) \
                       .withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType())) \
                       .withColumn("Cumulative_deaths", col("Cumulative_deaths").cast(DoubleType()))

# Ajout d'une colonne "Year"
covid_data = covid_data.withColumn("Year", year(col("Date_reported")))

# Pour chaque pays et chaque année, récupérer la dernière valeur de Cumulative_cases
window_spec_country_year = Window.partitionBy("Country", "Year").orderBy(col("Date_reported").desc())
latest_per_country_year = covid_data.withColumn("rn", row_number().over(window_spec_country_year)) \
                                      .filter(col("rn") == 1) \
                                      .drop("rn")

# Pour region_yearly_summary, nous pouvons aussi utiliser latest_per_country_year pour obtenir, par région et année,
# la somme des dernières valeurs de Cumulative_cases et Cumulative_deaths par pays.
region_yearly_summary = latest_per_country_year.groupBy("WHO_region", "Year") \
    .agg(
        spark_sum("Cumulative_cases").alias("total_cases"),
        spark_sum("Cumulative_deaths").alias("total_deaths")
    )
region_yearly_summary = region_yearly_summary.withColumn(
    "Year_ts",
    to_timestamp(concat(col("Year").cast("string"), lit("-01-01")), "yyyy-MM-dd")
)

##########################################
# ETL pour country_statistics (ETL2)
##########################################

# Extraction des données vaccination et réutilisation de covid_data
vaccination_data = spark.read.option("header", "true").csv("data/data_covid/vaccination-data.csv")
vaccination_data = vaccination_data.dropDuplicates().fillna({"PERSONS_VACCINATED_1PLUS_DOSE": 0})
vaccination_data = vaccination_data.withColumn("PERSONS_VACCINATED_1PLUS_DOSE", col("PERSONS_VACCINATED_1PLUS_DOSE").cast(DoubleType()))

# Pour country_statistics, récupérer la dernière valeur de Cumulative_cases par pays
window_spec_country = Window.partitionBy("Country").orderBy(col("Date_reported").desc())
latest_country_stats = covid_data.withColumn("rn", row_number().over(window_spec_country)) \
                                 .filter(col("rn") == 1) \
                                 .drop("rn")

# Sélection des colonnes nécessaires pour country_statistics (sans Population)
country_stats = latest_country_stats.select(
    "Country",
    col("Cumulative_cases").alias("total_cases")
)

# Pour la vaccination, récupérer la valeur maximale pour chaque pays
vaccination_latest = vaccination_data.groupBy("COUNTRY") \
    .agg(spark_max("PERSONS_VACCINATED_1PLUS_DOSE").alias("total_vaccinated"))

# Jointure sur le pays (attention à la casse et aux noms)
country_statistics = country_stats.join(
    vaccination_latest,
    country_stats.Country == vaccination_latest.COUNTRY,
    "inner"
).select(
    country_stats.Country.alias("Country"),
    "total_cases",
    "total_vaccinated"
)

##########################################
# Insertion dans PostgreSQL
##########################################

# Configuration de la connexion JDBC pour PostgreSQL
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Insertion des agrégats dans les tables PostgreSQL correspondantes
region_yearly_summary.write.jdbc(url=postgres_url, table="region_yearly_summary", mode="overwrite", properties=postgres_properties)
country_statistics.write.jdbc(url=postgres_url, table="country_statistics", mode="overwrite", properties=postgres_properties)

print("✅ Données insérées avec succès dans PostgreSQL.")

# Fermeture de la session Spark pour libérer les ressources
spark.stop()
