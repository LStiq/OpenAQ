# load.py

from pyspark.sql import DataFrame

def load_parquet(df: DataFrame, path: str, mode: str = "overwrite"):
    df.write.mode(mode).parquet(path)
