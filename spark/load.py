# load.py

from pyspark.sql import DataFrame

def load_facts(df: DataFrame, table_name: str):
    jdbc_url = "jdbc:postgresql://130.211.68.124:5432/openaq"
    connection_properties = {
        "user": "openaq",
        "password": "YnovRNCP2025",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=jdbc_url,table=table_name, mode="overwrite", properties=connection_properties)


def load_measures(df: DataFrame, table_name: str):
    jdbc_url = "jdbc:postgresql://130.211.68.124:5432/openaq"
    connection_properties = {
        "user": "openaq",
        "password": "YnovRNCP2025",
        "driver": "org.postgresql.Driver"
    }
    df.write.jdbc(url=jdbc_url,table=table_name, mode="append", properties=connection_properties)