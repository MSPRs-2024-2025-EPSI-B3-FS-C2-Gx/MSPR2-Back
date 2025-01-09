from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

"""
Script ETL pour nettoyer, transformer et enrichir les données COVID-19.
Étapes :
1. Charger les fichiers de données journalières et résumées.
2. Nettoyer et transformer les données (taux de mortalité et de récupération).
3. Joindre les datasets et sauvegarder le résultat dans un fichier CSV.
"""

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("ETL_COVID19") \
    .getOrCreate()

# Chemins des fichiers
daily_data_path = "data/worldometer_coronavirus_daily_data.csv"  # Chemin vers les données journalières
summary_data_path = "data/worldometer_coronavirus_summary_data.csv"  # Chemin vers les données agrégées

# Étape 1: Charger les fichiers CSV
daily_data = spark.read.csv(daily_data_path, header=True, inferSchema=True)  # Charger les données journalières
summary_data = spark.read.csv(summary_data_path, header=True, inferSchema=True)  # Charger les données agrégées

# Étape 2: Nettoyage des doublons et gestion des valeurs manquantes
daily_data_cleaned = daily_data.dropDuplicates().na.fill(0)  # Supprimer les doublons et remplir les valeurs manquantes
summary_data_cleaned = summary_data.dropDuplicates().na.fill(0)  # Idem pour les données agrégées

# Renommer les colonnes conflictuelles pour éviter les conflits lors de la jointure
daily_data_cleaned = daily_data_cleaned.withColumnRenamed("active_cases", "daily_active_cases") \
                                       .withColumnRenamed("mortality_rate", "daily_mortality_rate") \
                                       .withColumnRenamed("recovery_rate", "daily_recovery_rate")

summary_data_cleaned = summary_data_cleaned.withColumnRenamed("active_cases", "summary_active_cases") \
                                           .withColumnRenamed("mortality_rate", "summary_mortality_rate") \
                                           .withColumnRenamed("recovery_rate", "summary_recovery_rate")

# Calcul des taux de mortalité et de récupération pour les données journalières
daily_data_cleaned = daily_data_cleaned.withColumn(
    "daily_mortality_rate",
    when(col("cumulative_total_cases") > 0, (col("cumulative_total_deaths") / col("cumulative_total_cases")) * 100).otherwise(0)
)
daily_data_cleaned = daily_data_cleaned.withColumn(
    "daily_recovery_rate",
    when(col("cumulative_total_cases") > 0, (col("daily_active_cases") / col("cumulative_total_cases")) * 100).otherwise(0)
)

# Calcul des taux similaires pour les données agrégées
summary_data_cleaned = summary_data_cleaned.withColumn(
    "summary_mortality_rate",
    when(col("total_confirmed") > 0, (col("total_deaths") / col("total_confirmed")) * 100).otherwise(0)
)
summary_data_cleaned = summary_data_cleaned.withColumn(
    "summary_recovery_rate",
    when(col("total_confirmed") > 0, (col("total_recovered") / col("total_confirmed")) * 100).otherwise(0)
)

# Étape 3: Joindre les données journalières et résumées
enriched_data = daily_data_cleaned.join(
    summary_data_cleaned, on="country", how="inner"
)

# Étape 4: Sauvegarde des données transformées
# Si vous voulez un fichier unique, décommentez la ligne suivante
# enriched_data = enriched_data.coalesce(1)

output_path = "cleaned_and_enriched_covid19_data.csv"  # Chemin du fichier de sortie
enriched_data.write.csv(output_path, header=True, mode="overwrite")  # Sauvegarde au format CSV

print(f"Données nettoyées et enrichies enregistrées dans {output_path}")

# Étape 5: Arrêter la session Spark
spark.stop()