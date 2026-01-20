from pyspark.sql.functions import col

def clean_silver(df):
    """
    Reglas de calidad y negocio
    """
    return (
        df
        # eliminar eventos sin loan_id
        .filter(col("loan_id").isNotNull())

        # interest_rate válido
        .filter((col("interest_rate") > 0) & (col("interest_rate") < 1))

        # days_past_due no negativos
        .withColumn(
            "days_past_due",
            col("days_past_due").cast("int")
        )

        # flag de mora
        .withColumn(
            "is_delinquent",
            col("days_past_due") > 0
        )
    )