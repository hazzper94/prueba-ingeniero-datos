from pyspark.sql import SparkSession, DataFrame


def write_bronze(
    spark: SparkSession,
    df: DataFrame,
    output_path: str
) -> None:
    """
    Escribe datos en la capa Bronze en formato Parquet.

    Parameters
    ----------
    spark : SparkSession
        Sesión activa de Spark.

    df : DataFrame
        DataFrame con los datos crudos ingestados.

    output_path : str
        Ruta donde se almacenarán los datos Bronze.
    """

    (
        df
        .write
        .mode("append")
        .parquet(output_path)
    )