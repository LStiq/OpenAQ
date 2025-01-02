import os
import sys

# Ajouter le chemin du projet au PYTHONPATH pour éviter les erreurs d'import
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from spark.app.spark_jobs import main as spark_main  # Import du point d'entrée de votre script Spark

if __name__ == "__main__":
    print("Exécution du pipeline ETL avec Spark")
    spark_main()
    print("Pipeline ETL terminé avec succès.")