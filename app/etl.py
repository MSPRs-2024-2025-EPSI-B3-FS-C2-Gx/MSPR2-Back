import os

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, to_date, date_trunc, sum as spark_sum, explode, split, trim, lower, lag, when, lit, array, coalesce
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window

load_dotenv()

# Récupérer les variables d'environnement
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Variables d'environnement manquantes")

# Nettoyer les tables dans PostgreSQL dans l'ordre des dépendances
try:
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DELETE FROM daily_vaccine_statistics;")
    cur.execute("DELETE FROM weekly_statistics;")
    cur.execute("DELETE FROM vaccine;")
    cur.execute("DELETE FROM country;")
    cur.execute("DELETE FROM disease;")
    cur.execute("DELETE FROM who_region;")
    cur.close()
    conn.close()
    print("Les tables de référence ont été vidées avec succès.")
except Exception as e:
    print("Erreur lors du nettoyage des tables :", e)
    raise

# Création de la session Spark
spark = SparkSession.builder \
    .appName("ETL_COVID19_MCD") \
    .config("spark.jars", "/app/postgresql-42.7.5.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Chargement des CSV
covid_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")
vaccination_data = spark.read.option("header", "true").csv("data/data_covid/vaccination-data.csv")
vaccination_metadata = spark.read.option("header", "true").csv("data/data_covid/vaccination-metadata.csv")

# Traitement de covid_data
covid_data = covid_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))
covid_data = covid_data.fillna({"New_cases": 0, "New_deaths": 0})
# Remplacer les valeurs nulles de WHO_region par "UNKNOWN"
covid_data = covid_data.na.fill({"WHO_region": "UNKNOWN"})

# Mapping des régions WHO
who_region_mapping = {
    "EMRO": "Eastern Mediterranean Region",
    "EURO": "European Region",
    "AFRO": "African Region",
    "WPRO": "Western Pacific Region",
    "AMRO": "Region of the Americas",
    "SEARO": "South-East Asia Region",
    "UNKNOWN": "Other"
}
who_regions = covid_data.select(col("WHO_region").alias("who_region_short_code")) \
    .distinct() \
    .withColumn("who_region_name",
                when(col("who_region_short_code") == "EMRO", who_region_mapping["EMRO"])
                .when(col("who_region_short_code") == "EURO", who_region_mapping["EURO"])
                .when(col("who_region_short_code") == "AFRO", who_region_mapping["AFRO"])
                .when(col("who_region_short_code") == "WPRO", who_region_mapping["WPRO"])
                .when(col("who_region_short_code") == "AMRO", who_region_mapping["AMRO"])
                .when(col("who_region_short_code") == "SEARO", who_region_mapping["SEARO"])
                .otherwise(who_region_mapping["UNKNOWN"]))

# Construction de la table country
countries = covid_data.select(
    col("Country_code").alias("country_short_code"),
    col("Country").alias("country_name"),
    col("WHO_region").alias("who_region_short_code")
).distinct()

# Création de la table disease avec id explicite
diseases = spark.createDataFrame([(1, "COVID-19")], ["id", "name"])

# Création de la table vaccine à partir de vaccination_metadata
vaccine_window = Window.partitionBy(lit(1)).orderBy("name")
vaccines = vaccination_metadata.select(
    col("VACCINE_NAME").alias("name")
).distinct().withColumn("treated_disease", lit(1))
vaccines = vaccines.withColumn("id", F.row_number().over(vaccine_window)) \
    .select("id", "name", "treated_disease")
default_vaccine = spark.createDataFrame([(0, "unknown", 1)], ["id", "name", "treated_disease"])
vaccines_final = vaccines.unionByName(default_vaccine)

# Agrégation hebdomadaire : weekly_statistics
weekly_stats = covid_data.groupBy(
    col("Country_code").alias("country_short_code"),
    to_date(date_trunc("week", col("Date_reported"))).alias("date_of_report")
).agg(
    spark_sum("New_cases").cast(IntegerType()).alias("week_new_reported_cases"),
    spark_sum("New_deaths").cast(IntegerType()).alias("week_new_reported_deaths")
).withColumn("disease_id", lit(1))

# Traitement pour daily_vaccine_statistics
vaccination_data = vaccination_data.withColumn("TOTAL_VACCINATIONS", col("TOTAL_VACCINATIONS").cast(DoubleType()))
vaccination_data = vaccination_data.repartition("COUNTRY")
window_spec = Window.partitionBy("COUNTRY").orderBy("DATE_UPDATED")
daily_vaccine_raw = vaccination_data.withColumn(
    "prev_vaccines", lag("TOTAL_VACCINATIONS").over(window_spec)
).withColumn(
    "new_reported_shots_temp", col("TOTAL_VACCINATIONS") - col("prev_vaccines")
).withColumn(
    "new_reported_shots",
    when(col("new_reported_shots_temp").isNull(), col("TOTAL_VACCINATIONS"))
    .otherwise(col("new_reported_shots_temp"))
)
daily_vaccine = daily_vaccine_raw.withColumn(
    "vaccine_array",
    when((col("VACCINES_USED").isNull()) | (trim(col("VACCINES_USED")) == ""), array(lit("unknown")))
    .otherwise(split(trim(col("VACCINES_USED")), ","))
)
daily_vaccine_exploded = daily_vaccine.withColumn("vaccine", explode("vaccine_array"))
# Sélection et nettoyage de la colonne COUNTRY; filtrer les lignes sans date
daily_vaccine_df = daily_vaccine_exploded.select(
    lower(trim(col("COUNTRY"))).alias("country_name_clean"),
    to_date(col("DATE_UPDATED"), "yyyy-MM-dd").alias("day_of_report"),
    col("vaccine"),
    col("new_reported_shots").cast(IntegerType())
).filter(col("day_of_report").isNotNull())
# Nettoyer la table countries pour le join
countries_clean = countries.withColumn("country_name_clean", lower(trim(col("country_name")))) \
    .select("country_name_clean", "country_short_code")
daily_vaccine_with_code = daily_vaccine_df.join(
    countries_clean, on="country_name_clean", how="left"
)
daily_vaccine_final = daily_vaccine_with_code.join(
    vaccines_final, daily_vaccine_with_code.vaccine == vaccines_final.name, "left"
).select(
    "country_short_code",
    "day_of_report",
    col("id").alias("vaccine_id"),
    coalesce(col("new_reported_shots"), lit(0)).alias("new_reported_shots")
).filter(col("country_short_code").isNotNull())

# Insertion des tables dans PostgreSQL
tables = [
    (who_regions, "who_region"),
    (countries, "country"),
    (diseases, "disease"),
    (vaccines_final, "vaccine"),
    (weekly_stats, "weekly_statistics"),
    (daily_vaccine_final, "daily_vaccine_statistics")
]

for df, table_name in tables:
    print(f"Insertion de la table {table_name}")
    df.write.jdbc(url=postgres_url, table=table_name, mode="append", properties=postgres_properties)

print("✅ Données insérées avec succès dans PostgreSQL")
spark.stop()