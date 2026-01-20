from pyspark.sql import SparkSession, DataFrame
from typing import Iterator
import time


def stream_csv(
    spark: SparkSession,
    input_path: str,
    interval_seconds: float
) -> Iterator[DataFrame]:
    """
    Simula streaming leyendo un CSV completo
    y emitiendo micro-batches fila por fila.
    """

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    rows = df.collect()

    for row in rows:
        batch_df = spark.createDataFrame([row.asDict()])
        yield batch_df
        time.sleep(interval_seconds)