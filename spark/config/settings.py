import os
from dotenv import load_dotenv
dotenv_path = os.path.join(os.path.dirname(__file__),".env")
load_dotenv(dotenv_path, override=True)

# Variables PG
JDBC_URL = os.getenv("JDBC_URL")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PROPERTIES = {
        "user": DB_USER,
        "password": DB_PASSWORD,
        "driver": "org.postgresql.Driver"
}

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

print(DB_PROPERTIES, AQ_KEY)