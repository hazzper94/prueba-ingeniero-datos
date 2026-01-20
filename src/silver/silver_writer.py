from pyspark.sql import DataFrame


def write_silver(df: DataFrame, output_path: str) -> None:
    (
        df
        .write
        .mode("overwrite")
        .parquet(output_path)
    )