import json, time, concurrent.futures
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, ArrayType, FloatType
)
from spark.functions import get_countries, get_measurements, get_parameters, get_providers, get_world_locations, chunk_list, get_communes


# ------------------------------------------------------------------
# 1. Définition des schémas
# ------------------------------------------------------------------

# Schéma pour un paramètre
parameter_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("units", StringType(), True),
    StructField("displayName", StringType(), True),
    StructField("description", StringType(), True)
])

# Schéma pour un pays
country_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("datetimeFirst", StringType(), True),
    StructField("datetimeLast", StringType(), True),
    StructField("name", StringType(), True),
    StructField("parameters", ArrayType(parameter_schema), True)
])

# Schéma pour un provider
provider_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("sourceName", StringType(), True),
    StructField("datetimeAdded", StringType(), True),
    StructField("datetimeFirst", StringType(), True),
    StructField("datetimeLast", StringType(), True),
    StructField("parameters", ArrayType(parameter_schema), True)
])

# Schéma pour locations
# Sous-schémas que l'on va utiliser
provider_sub_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

country_sub_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", StringType(), True)
])

sensor_parameter_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("units", StringType(), True),
    StructField("displayName", StringType(), True)
])

sensor_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("parameter", sensor_parameter_schema, True)
])

datetime_schema = StructType([
    StructField("utc", StringType(), True),
    StructField("local", StringType(), True)
])

coordinates_schema = StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True)
])

location_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("isMobile", BooleanType(), True),
    StructField("isMonitor", BooleanType(), True),
    StructField("provider", provider_sub_schema, True),
    StructField("country", country_sub_schema, True),
    StructField("coordinates", coordinates_schema, True),
    StructField("datetimeFirst", datetime_schema, True),
    StructField("datetimeLast", datetime_schema, True),
    StructField("sensors", ArrayType(sensor_schema), True)
])

# Schéma mesures
period_schema = StructType([
    StructField("label", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("datetimeFrom", datetime_schema, True),
    StructField("datetimeTo", datetime_schema, True)
])

coverage_schema = StructType([
    StructField("expectedCount", IntegerType(), True),
    StructField("observedCount", IntegerType(), True),
    StructField("percentComplete", FloatType(), True),
    StructField("percentCoverage", FloatType(), True)
])

measurement_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("value", FloatType(), True),
    StructField("parameter", parameter_schema, True),
    StructField("period", period_schema, True),
    StructField("coverage", coverage_schema, True)
])


# Schéma cities

cities_schema = StructType([
    StructField("location_id", IntegerType(), True),
    StructField("longitude", FloatType(), True),
    StructField("latitude", FloatType(), True),
    StructField("nom", StringType(), True),
    StructField("code", StringType(), True),
    StructField("codesPostaux", ArrayType(StringType()), True),
    StructField("population", IntegerType(),True)
])


def extract_parameters_df(spark: SparkSession):
    param_data = get_parameters()
    param_rdd = spark.sparkContext.parallelize([json.dumps(param) for param in param_data["results"]])
    raw_df = spark.read.schema(parameter_schema).json(param_rdd)

    return raw_df

def extract_countries_df(spark: SparkSession):
    countries_data = get_countries()
    countries_rdd = spark.sparkContext.parallelize([json.dumps(country) for country in countries_data["results"]])
    raw_df = spark.read.schema(country_schema).json(countries_rdd)

    return raw_df

def extract_providers_df(spark: SparkSession):
    providers_data = get_providers()
    providers_rdd = spark.sparkContext.parallelize([json.dumps(provider) for provider in providers_data["results"]])
    raw_df = spark.read.schema(provider_schema).json(providers_rdd)

    return raw_df

def extract_world_locations_df(spark: SparkSession):
    world_location_data = []
    page = 1
    max_workers = 10  # Nombre de requêtes en parallèle

    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page = {executor.submit(get_world_locations, p): p for p in range(page, page + max_workers)}

            empty_pages = 0  # Compteur pour détecter si on atteint la fin

            for future in concurrent.futures.as_completed(future_to_page):
                p = future_to_page[future]
                try:
                    results = future.result()
                    if results:
                        world_location_data.extend(results)
                    else:
                        empty_pages += 1  # Si une page est vide, on incrémente le compteur
                except Exception as e:
                    print(f"Erreur lors du traitement de la page {p}: {e}")

        if empty_pages == max_workers:
            break

        page += max_workers

    world_location_rdd = spark.sparkContext.parallelize([json.dumps(loc) for loc in world_location_data])
    raw_df = spark.read.schema(location_schema).json(world_location_rdd)
    return raw_df


def extract_cities_locations_df(spark: SparkSession, points_list):
    cities_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_row = {}
        for index, row in enumerate(points_list):
            lat = row["city_latitude"]
            lon = row["city_longitude"]
            future = executor.submit(get_communes,lat,lon)
            future_to_row[future] = row 
            if (index + 1) % 10 == 0: ## 10 REQUÊTES MAX PAR SECONDE
                time.sleep(1)

        for future in concurrent.futures.as_completed(future_to_row):
            properties = future.result()
            row = future_to_row[future]
            cities_list.append({
                "location_id": row["location_id"],
                "latitude": row["city_latitude"],
                "longitude": row["city_longitude"],
                **properties
            })
    cities_rdd = spark.sparkContext.parallelize([json.dumps(c) for c in cities_list])
    cities_df = spark.read.schema(cities_schema).json(cities_rdd)

    return cities_df

def extract_measurements_df(spark: SparkSession, sensors_ids):
    measurements_list = []
    MAX_REQUESTS_PER_MINUTE = 60 # L'API nous limite à 60 requêtes / min
    for chunk_index, sensors_chunk in enumerate(chunk_list(sensors_ids, MAX_REQUESTS_PER_MINUTE)):
        print(f"--- Traitement du chunk {chunk_index + 1} contenant {len(sensors_chunk)} capteurs ---")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # On lance la requête pour chaque sensor_id dans notre chunk
            future_to_sensor = {
                executor.submit(get_measurements,sensor_id): sensor_id
                for sensor_id in sensors_chunk
            }
            # Quand chaque future est terminé, on récupère les résultats
            for future in concurrent.futures.as_completed(future_to_sensor):
                sensor_id = future_to_sensor[future]
                try:
                    measurements = future.result()
                    # On stocke toutes les mesures récupérées
                    for measurement in measurements:
                        measurements_list.append({"sensor_id": sensor_id, **measurement})
                except Exception as e:
                    print(f"Erreur lors du traitement du capteur {sensor_id} : {e}")
        # Si on a encore d'autres chunks à traiter, on dort 60s pour respecter 60 requêtes/min (sauf après le dernier chunk)
        if (chunk_index + 1) * MAX_REQUESTS_PER_MINUTE < len(sensors_ids):
            print(f"--- Limite atteinte, pause 60s avant le prochain chunk ---")
            time.sleep(60)

    measurements_rdd = spark.sparkContext.parallelize([json.dumps(m) for m in measurements_list])
    raw_df = spark.read.schema(measurement_schema).json(measurements_rdd)

    return raw_df