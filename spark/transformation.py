from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, to_date, lit, date_trunc, from_utc_timestamp, round, avg, min, max, stddev, median, sum

# ------------------------------------------------------------------
# Transformation
# ------------------------------------------------------------------

def transform_parameters(df_raw: DataFrame) -> DataFrame:
    cleaned_df = df_raw.select(
        col("id").alias("param_id"),
        col("name").alias("param_name"),
        col("units").alias("param_units"),
        col("displayName").alias("param_displayName"),
        col("description").alias("param_description")
    )
    return cleaned_df

def transform_countries(df_raw: DataFrame) -> DataFrame:
    cleaned_df = df_raw.select(
        col("id").alias("country_id"),
        col("code").alias("country_code"),
        col("datetimeFirst").alias("date_first_measure"),
        col("datetimeLast").alias("date_last_measure"),
        col("name").alias("country_name")
    )
    return cleaned_df

def transform_params_per_country(df_raw: DataFrame) -> DataFrame:
    exploded_df = df_raw.select(
        col("id").alias("country_id"),
        explode(col("parameters")).alias("parameter")
    )

    cleaned_df = exploded_df.select(
        col("country_id"),
        col("parameter.id").alias("parameter_id")
    )
    return cleaned_df

def transform_providers(df_raw: DataFrame) -> DataFrame:
    cleaned_df = df_raw.select(
        col("id").alias("provider_id"),
        col("name").alias("provider_name"),
        col("sourceName").alias("provider_sourceName"),
        col("datetimeAdded").alias("provider_datetime_added"),
        col("datetimeLast").alias("provider_datetime_last"),
        col("parameters.id").alias("provider_parameters_id")
    )
    return cleaned_df

def transform_world_locations(df_raw: DataFrame) -> DataFrame:
    cleaned_df = df_raw.select(
        col("id").alias("location_id"),
        col("name").alias("location_name"),
        col("isMonitor").alias("location_monitor"),
        from_utc_timestamp(col("datetimeLast.utc"),"Europe/Paris").alias("location_datetime_last_utc"),
        col("provider.id").alias("provider_id"),
        col("provider.name").alias("provider_name"),
        col("coordinates.latitude").alias("latitude"),
        col("coordinates.longitude").alias("longitude"),
        col("country.id").alias("location_country_id"),
        col("country.name").alias("location_country_name")
    )
    return cleaned_df

def transform_world_sensors(df_raw: DataFrame) -> DataFrame:
    exploded_df = df_raw.select(
        col("id").alias("location_id"),
        col("name").alias("location_name"),
        explode(col("sensors")).alias("sensor")
    )

    cleaned_df = exploded_df.select(
        col("location_id"),
        col("sensor.id").alias("sensor_id"),
        col("sensor.name").alias("sensor_name"),
        col("sensor.parameter.id").alias("sensor_parameter_id"),
        col("sensor.parameter.displayName").alias("sensor_parameter_displayName")
    )
    return cleaned_df

def filter_france_locations(world_locations_df: DataFrame) -> DataFrame:
    filtered_df = world_locations_df.filter(
        (col("location_country_id") == "22") &
        (to_date(col("location_datetime_last_utc")) >= to_date(lit("2025-01-01")))
    )
    return filtered_df

def filter_france_sensors(all_sensors_df: DataFrame, fr_locations_df: DataFrame) -> DataFrame:    
    joined_df = all_sensors_df.join(fr_locations_df, "location_id", "inner")
    return joined_df

def transform_coords_france(france_locations_df: DataFrame) -> DataFrame:
    cleaned_df = france_locations_df.select(
        col("location_id"),
        col("longitude").alias("city_longitude"),
        col("latitude").alias("city_latitude")
    )
    return cleaned_df

def transform_cities_points(cities_df: DataFrame) -> DataFrame:
    cleaned_df = cities_df.select(
        col("location_id"),
        col("latitude"),
        col("longitude"),
        col("nom").alias("city_name"),
        col("code").alias("city_insee_code"),
        col("codesPostaux")[0].alias("city_postcode"),
        col("population").alias("city_population")
    )
    return cleaned_df

def transform_measurements_raw(df_raw: DataFrame) -> DataFrame:

    measurements_df = df_raw.select(
        col("sensor_id"),
        col("value"),
        from_utc_timestamp(col("period.datetimeFrom.utc"),"Europe/Paris").alias("period_datetime_utc"),
        col("parameter.id").alias("parameter_id"),
        col("parameter.name").alias("parameter_name"),
        col("parameter.units").alias("parameter_units"),
        col("coverage.expectedCount").alias("coverage_expectedCount"),
        col("coverage.observedCount").alias("coverage_observedCount"),
    )

    return measurements_df

def transform_measurements_agg_daily(df_raw: DataFrame) -> DataFrame:
    measurements_df_day = df_raw.withColumns({
        "day" : date_trunc("day", from_utc_timestamp(col("period.datetimeFrom.utc"), "Europe/Paris")),
    }
    )

    measurements_df = measurements_df_day.select(
        col("sensor_id"),
        col("value"),
        "day",
        col("parameter.id").alias("parameter_id"),
        col("coverage.observedCount").alias("coverage_observedCount"),
    )
    measurements_df_agg_daily = measurements_df.groupBy(["day","sensor_id","parameter_id"]).agg(
        round(avg("value"),2).alias("value"),
        round(min("value"),2).alias("min_value"),
        round(max("value"),2).alias("max_value"),
        stddev("value").alias("stddev_value"),
        round(median("value"),2).alias("median_value"),
        sum("coverage_observedCount").alias("observed_count"),
    ).withColumn("expected_count", lit(24)).orderBy("day")

    return measurements_df_agg_daily