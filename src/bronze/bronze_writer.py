from pyspark.sql import SparkSession, DataFrame


def write_bronze(
    spark: SparkSession,
    df: DataFrame,
    output_path: str
) -> None:

    (
        df
        .write
        .mode("append")
        .parquet(output_path)
    )