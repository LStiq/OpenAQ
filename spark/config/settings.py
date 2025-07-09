import os, psycopg
from dotenv import load_dotenv
load_dotenv()

# Variables RDS
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")  # Valeur par défaut si non spécifiée
DB_NAME = os.getenv("DB_NAME")

# AQ API Configuration
AQ_KEY = os.getenv("AQ_KEY")
API_BASE_URL = "https://api.openaq.org/v3/"

# GOUV API
API_GOUV_FR = "https://geo.api.gouv.fr/communes"

# Chemins des environnements
JAVA_HOME = os.getenv("JAVA_HOME")
SPARK_HOME = os.getenv("SPARK_HOME")
PYSPARK_PYTHON = os.getenv("PYSPARK_PYTHON")

def configure_environment():
    """
    Configure les variables d'environnement nécessaires pour Spark.
    """
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["SPARK_HOME"] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
    os.environ["PATH"] += os.pathsep + os.path.join(SPARK_HOME, "bin")
    print("Environnement Spark configuré avec succès.")

def get_database_connection():
    """
    Retourne une connexion à la base de données RDS sur AWS.
    """
    try:
        connection = psycopg.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Connexion réussie à la base de données RDS.")
        return connection
    except Exception as e:
        print(f"Erreur lors de la connexion à la base de données RDS : {e}")
        raise