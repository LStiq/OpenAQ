from pyspark.sql import SparkSession
import json, os
from config.settings import configure_environment, get_database_connection
from functions import get_countries, get_parameters

configure_environment()

# 1. Initialisation de Spark
spark = SparkSession.builder \
    .appName("ETL_OpenAQ") \
    .config("spark.master", "local") \
    .getOrCreate()

# 2. Extraction des données depuis l'API OpenAQ

data = get_parameters()

# Affichage des données récupérées
print("Données récupérées depuis l'API OpenAQ")
print(json.dumps(data, indent=4))

# Vérification des données
if "results" not in data:
    raise Exception("Erreur : Pas de données disponibles depuis l'API OpenAQ")

# 3. Connexion à la BDD
try:
    conn = get_database_connection()
    with conn.cursor() as cur:
        for result in data["results"]:
            parameter = result.get("parameter")
            description = result.get("description","")
            cur.execute(
                "INSERT INTO parameters (parameter, description) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (parameter, description)
            )
            conn.commit()
            print("Données insérées avec succès dans la base de données.")
finally:
    conn.close()

print("Pipeline ETL terminé avec succès !")

# 5. Étapes suivantes pour Power BI
# Connectez Power BI à la base PostgreSQL avec les mêmes identifiants.
# Créez un dashboard pour visualiser les moyennes par ville, par exemple avec un graphique ou une carte.
