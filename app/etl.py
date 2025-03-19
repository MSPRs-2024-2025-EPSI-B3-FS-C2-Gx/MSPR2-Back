# Importation des modules et fonctions nécessaires :
# - pyspark.sql.SparkSession : pour créer et gérer la session Spark
# - pyspark.sql.functions : pour effectuer diverses transformations sur les colonnes
# - pyspark.sql.types : pour définir explicitement les types de données lors des conversions
# - pyspark.sql.window : pour appliquer des opérations sur des fenêtres (par exemple, récupérer la dernière valeur par groupe)
# - dotenv : pour charger les variables d'environnement depuis un fichier .env
# - os : pour accéder aux variables d'environnement système
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, concat, lit, to_timestamp, max as spark_max, sum as spark_sum, row_number, when
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

# Chargement des variables d'environnement à partir du fichier .env
# Ces variables configurent les informations de connexion à la base de données PostgreSQL
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Vérification que toutes les variables nécessaires sont bien définies.
# En cas de variable manquante, une erreur est levée pour stopper l'exécution.
if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD]):
    raise ValueError("Une ou plusieurs variables d'environnement sont manquantes dans le fichier .env.")

# Initialisation de la session Spark avec l'application nommée "ETL_COVID19_Postgres"
# Le pilote PostgreSQL est inclus via le fichier JAR spécifié.
spark = SparkSession.builder \
    .appName("ETL_COVID19_Postgres") \
    .config("spark.jars", "../postgresql-42.7.5.jar") \
    .getOrCreate()

##########################################
# ETL : Agrégation annuelle par région (region_yearly_summary)
##########################################

# Extraction des données COVID-19 depuis le fichier CSV global de l'OMS
covid_data = spark.read.option("header", "true").csv("data/data_covid/WHO-COVID-19-global-data.csv")

# Transformation des données :
# 1. Conversion de la colonne "Date_reported" en type date au format "yyyy-MM-dd"
covid_data = covid_data.withColumn("Date_reported", to_date(col("Date_reported"), "yyyy-MM-dd"))

# 2. Remplacement des valeurs NULL par des valeurs par défaut pour éviter des problèmes lors des agrégations
covid_data = covid_data.fillna({
    "New_cases": 0,
    "New_deaths": 0,
    "Cumulative_cases": 0,
    "Cumulative_deaths": 0,
    "WHO_region": "UNKNOWN"
})

# 3. Conversion explicite des colonnes numériques en type Double pour garantir la précision lors des calculs
covid_data = covid_data.withColumn("New_cases", col("New_cases").cast(DoubleType())) \
                       .withColumn("New_deaths", col("New_deaths").cast(DoubleType())) \
                       .withColumn("Cumulative_cases", col("Cumulative_cases").cast(DoubleType())) \
                       .withColumn("Cumulative_deaths", col("Cumulative_deaths").cast(DoubleType()))

# 4. Création d'une colonne "Year" extraite de la date, facilitant ainsi les agrégations annuelles
covid_data = covid_data.withColumn("Year", year(col("Date_reported")))

# Récupération de la dernière valeur de "Cumulative_cases" pour chaque combinaison de pays et d'année :
# - On définit une fenêtre partitionnée par "Country" et "Year", triée par "Date_reported" en ordre décroissant.
# - La fonction row_number() permet d'identifier la dernière ligne (rn == 1) pour chaque groupe.
window_spec_country_year = Window.partitionBy("Country", "Year").orderBy(col("Date_reported").desc())
latest_per_country_year = covid_data.withColumn("rn", row_number().over(window_spec_country_year)) \
                                      .filter(col("rn") == 1) \
                                      .drop("rn")

# Agrégation par région et année :
# On calcule la somme des dernières valeurs de "Cumulative_cases" et "Cumulative_deaths" pour chaque région.
region_yearly_summary = latest_per_country_year.groupBy("WHO_region", "Year") \
    .agg(
        spark_sum("Cumulative_cases").alias("total_cases"),
        spark_sum("Cumulative_deaths").alias("total_deaths")
    )

# Ajout d'une colonne "Year_ts" qui convertit l'année en un timestamp correspondant au 1er janvier de l'année,
# ce qui peut faciliter des opérations temporelles ultérieures.
region_yearly_summary = region_yearly_summary.withColumn(
    "Year_ts",
    to_timestamp(concat(col("Year").cast("string"), lit("-01-01")), "yyyy-MM-dd")
)

##########################################
# ETL : Statistiques par pays (country_statistics)
##########################################

# Extraction des données de vaccination depuis le fichier CSV correspondant
vaccination_data = spark.read.option("header", "true").csv("data/data_covid/vaccination-data.csv")
# Suppression des doublons et remplacement des valeurs manquantes dans la colonne "PERSONS_VACCINATED_1PLUS_DOSE"
vaccination_data = vaccination_data.dropDuplicates().fillna({"PERSONS_VACCINATED_1PLUS_DOSE": 0})
# Conversion de la colonne de vaccination en type Double pour assurer la précision des calculs
vaccination_data = vaccination_data.withColumn("PERSONS_VACCINATED_1PLUS_DOSE", col("PERSONS_VACCINATED_1PLUS_DOSE").cast(DoubleType()))

# Pour obtenir les statistiques par pays, on sélectionne la dernière valeur de "Cumulative_cases" pour chaque pays.
# Une fenêtre partitionnée par "Country" est définie et triée par "Date_reported" en ordre décroissant.
window_spec_country = Window.partitionBy("Country").orderBy(col("Date_reported").desc())
latest_country_stats = covid_data.withColumn("rn", row_number().over(window_spec_country)) \
                                 .filter(col("rn") == 1) \
                                 .drop("rn")

# Sélection des colonnes essentielles pour les statistiques par pays : nom du pays et nombre total de cas
country_stats = latest_country_stats.select(
    "Country",
    col("Cumulative_cases").alias("total_cases")
)

# Pour les données de vaccination, on récupère la valeur maximale de "PERSONS_VACCINATED_1PLUS_DOSE" par pays,
# afin d'obtenir le nombre total de personnes vaccinées.
vaccination_latest = vaccination_data.groupBy("COUNTRY") \
    .agg(spark_max("PERSONS_VACCINATED_1PLUS_DOSE").alias("total_vaccinated"))

# Fusion des statistiques COVID et des données de vaccination en effectuant une jointure sur le nom du pays.
# La jointure est réalisée en tenant compte de la casse et des noms exacts.
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
# Insertion des données agrégées dans PostgreSQL
##########################################

# Construction de l'URL de connexion JDBC pour PostgreSQL en utilisant les variables d'environnement
postgres_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
# Définition des propriétés de connexion, incluant l'utilisateur, le mot de passe et le pilote JDBC
postgres_properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Insertion des DataFrames agrégés dans les tables PostgreSQL correspondantes :
# - region_yearly_summary est inséré dans la table "region_yearly_summary"
# - country_statistics est inséré dans la table "country_statistics"
# Le mode "overwrite" permet de remplacer les données existantes.
region_yearly_summary.write.jdbc(url=postgres_url, table="region_yearly_summary", mode="overwrite", properties=postgres_properties)
country_statistics.write.jdbc(url=postgres_url, table="country_statistics", mode="overwrite", properties=postgres_properties)

# Affichage d'un message de confirmation après la réussite de l'insertion des données
print("✅ Données insérées avec succès dans PostgreSQL.")

# Fermeture de la session Spark pour libérer les ressources allouées
spark.stop()