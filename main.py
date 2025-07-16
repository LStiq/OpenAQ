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
from spark.scrap import get_normes_data
from spark.config.settings import configure_environment, DB_PROPERTIES, JDBC_URL

from contextlib import contextmanager
from pyspark.sql import SparkSession, DataFrame

class ETLManager:
    def __init__(self, spark: SparkSession, jdbc_url: str, properties: dict):
        self.spark = spark
        self.jdbc_url = jdbc_url
        self.properties = properties

    @contextmanager
    def job_group(self, group_id: str, description: str):
        self.spark.sparkContext.setJobGroup(group_id, description)
        self.spark.sparkContext.setJobDescription(description)
        try:
            yield
        finally:
            self.spark.sparkContext.cancelJobGroup(group_id)

    def table_exists(self, table_name: str) -> bool:
        try:
            tables_df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table="information_schema.tables",
                properties=self.properties
            )
            result = tables_df.filter(
                (tables_df.table_schema == "public") &
                (tables_df.table_name == table_name)
            ).limit(1).count() > 0
            return result
        except Exception as e:
            print(f"[ERREUR] Vérification table '{table_name}' a échoué : {e}")
            return False

    def read_table(self, table_name: str) -> DataFrame:
        return self.spark.read.jdbc(url=self.jdbc_url, table=table_name, properties=self.properties)

    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        df.write.jdbc(url=self.jdbc_url, table=table_name, mode=mode, properties=self.properties)

    def run_etl_step(self, 
                 group_id: str,
                 description: str,
                 table_name: str,
                 extract_fn,
                 transform_fn,
                 write_fn = None,
                 is_fact_table: bool = False):
        with self.job_group(group_id, description):
            table_exists = self.table_exists(table_name)

            if not table_exists:
                print(f"Table '{table_name}' n'existe pas. Création de la table...")
                df_extract = extract_fn(self.spark)
                df_transform = transform_fn(df_extract)
                if write_fn:
                    write_fn(df_transform, table_name)
                else:
                    self.write_table(df_transform, table_name, mode="overwrite")
                return df_transform
            
            else:
                if is_fact_table:
                    print(f"Table '{table_name}' existe déjà et est une table faits. Ajout des données (append).")
                    df_extract = extract_fn(self.spark)
                    df_transform = transform_fn(df_extract)
                    if write_fn:
                        write_fn(df_transform, table_name)
                    else:
                        self.write_table(df_transform, table_name, mode="append")
                    return df_transform
                else:
                    print(f"Table '{table_name}' existe déjà et est une table dimension. Skip de l’étape.")
                    return self.read_table(table_name)



def main():
    # 1. Configuration et création de la session Spark
    configure_environment()
    spark = SparkSession.builder \
        .appName("ETL_OpenAQ") \
        .config("spark.master", "local") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.jars", "OpenAQ/spark/jars/postgresql-42.7.3.jar") \
        .getOrCreate()


    etl = ETLManager(spark, JDBC_URL, DB_PROPERTIES)

    try:
        # ------------------------------------------------------------------
        # 2.1 Parameters
        # ------------------------------------------------------------------
        parameters_df = etl.run_etl_step(
        group_id="1",
        description="Extraction des paramètres",
        table_name="parameters",
        extract_fn=extract_parameters_df,
        transform_fn=transform_parameters
        )

        # ------------------------------------------------------------------
        # 2.2 Countries
        # ------------------------------------------------------------------
        countries_df = etl.run_etl_step(
        group_id="2",
        description="Extraction des pays",
        table_name="countries",
        extract_fn=extract_countries_df,
        transform_fn=transform_countries
        )

        # ------------------------------------------------------------------
        # 2.3 Param per Countries
        # ------------------------------------------------------------------

        param_countries_df = etl.run_etl_step(
        group_id="3",
        description="Extraction des paramètres par pays",
        table_name="parameters_per_country",
        extract_fn=lambda spark: transform_params_per_country(countries_df),
        transform_fn=lambda df: df
        )

        # ------------------------------------------------------------------
        # 2.4 Providers
        # ------------------------------------------------------------------
        providers_df = etl.run_etl_step(
            group_id="4",
            description="Extraction des providers",
            table_name="providers",
            extract_fn=extract_providers_df,
            transform_fn=transform_providers
        )

        # ------------------------------------------------------------------
        # 2.4 Locations
        # ------------------------------------------------------------------
        world_locations_df = etl.run_etl_step(
            group_id="5",
            description="Extraction de toutes les localisations dans le monde",
            table_name="world_locations",
            extract_fn=extract_world_locations_df,
            transform_fn=transform_world_locations
        )

        # ------------------------------------------------------------------
        # 2.6 Sensors
        # ------------------------------------------------------------------
        world_sensors_df = etl.run_etl_step(
            group_id="6",
            description="Extraction des capteurs des localisations",
            table_name="world_sensors",
            extract_fn=lambda spark: transform_world_sensors(world_locations_df),
            transform_fn=lambda df: df
        )

        # ------------------------------------------------------------------
        # 2.7 et 2.8 Locations & Sensors - France
        # ------------------------------------------------------------------
        france_locations_df = etl.run_etl_step(
            group_id="7",
            description="Extraction des localisations France",
            table_name="france_locations",
            extract_fn=lambda spark: filter_france_locations(world_locations_df),
            transform_fn=lambda df: df
        )

        france_sensors_df = etl.run_etl_step(
            group_id="8",
            description="Extraction des capteurs France",
            table_name="france_sensors",
            extract_fn=lambda spark: filter_france_sensors(world_sensors_df, france_locations_df),
            transform_fn=lambda df: df
        )

        # ------------------------------------------------------------------
        # 2.9 Cities of sensors - France
        # ------------------------------------------------------------------
        france_cities_sensors_df = etl.run_etl_step(
            group_id="9",
            description="Extraction des villes des capteurs France",
            table_name="france_cities_sensors",
            extract_fn=lambda spark: extract_cities_locations_df(
                spark, 
                transform_coords_france(france_locations_df).collect()
            ),
            transform_fn=transform_cities_points
        )

        # ------------------------------------------------------------------
        # 2.10 et 2.11 Measurements sensors - France
        # ------------------------------------------------------------------
        sensors_ids = [row["sensor_id"] for row in france_sensors_df.collect()]
        measurements_df = extract_measurements_df(spark, sensors_ids)
        measurements_df_raw = transform_measurements_raw(measurements_df)
        measurements_df_agg = transform_measurements_agg_daily(measurements_df)

        etl.run_etl_step(
            group_id="10",
            description="Ecriture mesures raw",
            table_name="raw_measurements",
            extract_fn=lambda spark: measurements_df_raw,
            transform_fn=lambda df: df,
            is_fact_table=True
        )

        etl.run_etl_step(
            group_id="11",
            description="Ecriture mesures agg",
            table_name="agg_measurements",
            extract_fn=lambda spark: measurements_df_agg,
            transform_fn=lambda df: df,
            is_fact_table=True
        )

    finally:
        print("Pipeline ETL terminé avec succès !")
        spark.stop()


if __name__ == "__main__":
    main()