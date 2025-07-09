from pyspark.sql import SparkSession
from spark.extraction import (
    extract_parameters_df, extract_countries_df, extract_providers_df, 
    extract_world_locations_df, extract_measurements_df, extract_cities_locations_df
)
from spark.transformation import (
    transform_parameters, transform_countries, transform_params_per_country,
    transform_providers, transform_world_locations, transform_world_sensors,
    filter_france_locations, filter_france_sensors, transform_measurements_raw, transform_measurements_agg_daily, transform_coords_france, transform_cities_points
)
from spark.scrap import get_polluants_data
from spark.load import load_parquet
from spark.config.settings import configure_environment
import pandas as pd

def main():
    # 1. Configuration et création de la session Spark
    configure_environment()
    spark = SparkSession.builder \
        .appName("ETL_OpenAQ") \
        .config("spark.master", "local") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    try:
        get_polluants_data()
        # ------------------------------------------------------------------
        # 2.1 Parameters
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("1", "Extraction des paramètres")
        spark.sparkContext.setJobDescription("Extraction des paramètres")

        parameters_df = extract_parameters_df(spark)
        cleaned_params_df = transform_parameters(parameters_df)
        spark.sparkContext.setLocalProperty("callSite.short", 
            "Ecriture du dataframe des paramètres en fichier parquet"
        )
        load_parquet(cleaned_params_df, "data_output/parameters")
        spark.sparkContext.cancelJobGroup("1")

        # ------------------------------------------------------------------
        # 2.2 Countries
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("2", "Extraction des pays et leurs paramètres")
        spark.sparkContext.setJobDescription("Extraction des pays")

        countries_df = extract_countries_df(spark)
        cleaned_countries_df = transform_countries(countries_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Ecriture du dataframe des pays en fichier parquet"
        )
        load_parquet(cleaned_countries_df, "data_output/countries")

        # 2.2.5 parameters_per_country
        spark.sparkContext.setJobGroup("2", "Extraction des paramètres par pays")
        params_per_country_df = transform_params_per_country(countries_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Filtre du dataframe des pays puis écriture du dataframe des paramètres par pays"
        )
        load_parquet(params_per_country_df, "data_output/parameters_per_country")
        spark.sparkContext.cancelJobGroup("2")

        # ------------------------------------------------------------------
        # 2.3 Providers
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("3", "Extraction des providers")

        providers_df = extract_providers_df(spark)
        cleaned_providers_df = transform_providers(providers_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Ecriture du dataframe des providers en fichier parquet"
        )
        load_parquet(cleaned_providers_df, "data_output/providers")
        spark.sparkContext.cancelJobGroup("3")

        # ------------------------------------------------------------------
        # 2.4 Locations
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("4", "Extraction de toutes les localisations dans le monde")

        world_locations_df = extract_world_locations_df(spark)
        cleaned_world_locations_df = transform_world_locations(world_locations_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Ecriture du dataframe des localisations en fichier parquet"
        )
        load_parquet(cleaned_world_locations_df, "data_output/world_locations")
        spark.sparkContext.cancelJobGroup("4")

        # ------------------------------------------------------------------
        # 2.5 Sensors
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("5", "Extraction des capteurs des localisations")
        cleaned_world_sensors_df = transform_world_sensors(world_locations_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Extraction des capteurs puis écriture du dataframe en fichier parquet"
        )
        load_parquet(cleaned_world_sensors_df, "data_output/world_sensors")
        spark.sparkContext.cancelJobGroup("5")

        # ------------------------------------------------------------------
        # 2.6 Locations & Sensors - France
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("6", 
            "Extraction des localisations et capteurs FR ayant eu des mesures à partir du 1er Janvier 2025"
        )
        france_locations_df = filter_france_locations(cleaned_world_locations_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Filtre du dataframe sur la France puis écriture des localisations"
        )
        load_parquet(france_locations_df, "data_output/france_locations")

        spark.sparkContext.setLocalProperty("callSite.short",
            "Jointure capteurs/localisations FR puis écriture du dataframe des capteurs"
        )
        france_sensors_df = filter_france_sensors(cleaned_world_sensors_df, france_locations_df)
        load_parquet(france_sensors_df, "data_output/france_sensors")
        spark.sparkContext.cancelJobGroup("6")

        # ------------------------------------------------------------------
        # 2.7 Cities of sensors - France
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("7", 
            "Extraction des villes françaises en fonction de la longitude et de la latitude"
        )
        spark.sparkContext.setLocalProperty("callSite.short",
            "Extraction de la longitude et de la latitude de la localisation"
        )
        france_locations_coord_df = transform_coords_france(france_locations_df)
        points_list = france_locations_coord_df.collect()
        
        spark.sparkContext.setLocalProperty("callSite.short",
            "Extraction des villes en fonction de la longitude et de la latitude"
        )
        cities_points_df = extract_cities_locations_df(spark,points_list)
        cleaned_cities_points_df = transform_cities_points(cities_points_df)       

        spark.sparkContext.setLocalProperty("callSite.short",
            "Ecriture du dataframe des villes des capteurs"
        )
        load_parquet(cleaned_cities_points_df, "data_output/france_cities_sensors")
        spark.sparkContext.cancelJobGroup("7")

        # ------------------------------------------------------------------
        # 2.8 Measurements sensors - France
        # ------------------------------------------------------------------
        spark.sparkContext.setJobGroup("8",
            "Extraction de toutes les mesures des capteurs FR"
        )
        #sensors_ids = [row["sensor_id"] for row in france_sensors_df.collect()]
        measurements_df = extract_measurements_df(spark,5579)
        
        measurements_df_raw = transform_measurements_raw(measurements_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Ecriture du dataframe des mesures raw en fichier parquet"
        )

        load_parquet(measurements_df_raw, "data_output/raw_measurements")

        measurements_df_agg = transform_measurements_agg_daily(measurements_df)
        spark.sparkContext.setLocalProperty("callSite.short",
            "Ecriture du dataframe des mesures agg en fichier parquet"
        )

        load_parquet(measurements_df_agg, "data_output/agg_measurements")
        spark.sparkContext.cancelJobGroup("8")

    finally:
        input("🔴 Appuyez sur 'Entrée' pour fermer Spark et l'interface UI...")
        print("Pipeline ETL terminé avec succès !")
        spark.stop()


if __name__ == "__main__":
    main()