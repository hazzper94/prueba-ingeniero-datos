from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date


def normalize_dates(df: DataFrame) -> DataFrame:
    """
    Normaliza columnas de fecha y timestamp.

    - event_time -> timestamp
    - ingestion_date -> date

    Registros inválidos se mantienen como null.
    """

    return (
        df
        .withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")
        )
        .withColumn(
            "ingestion_date",
            to_date(col("ingestion_date"), "yyyy-MM-dd")
        )
    )