from pyspark.sql import SparkSession
from time import sleep
import os
# Ingestion
from ingestion.csv_stream_simulator import stream_csv

# Bronze
from bronze.bronze_writer import write_bronze

# Silver
from silver.transformations import normalize_dates
from silver.silver_writer import write_silver


# -----------------------------
# CONFIGURACIÓN DE RUTAS
# -----------------------------
INPUT_PATH = "data/input/credit_events.csv"
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

# Simulación de streaming (en segundos)
INGEST_INTERVAL_SECONDS = 1  # puedes cambiar a 0.05


# -----------------------------
# SPARK SESSION
# -----------------------------
def get_spark_session() -> SparkSession:
    warehouse_dir = os.path.abspath("spark-warehouse")

    return (
        SparkSession.builder
        .appName("TechnicalTest-DataEngineering")
        .master("local[*]")
        # 🔑 EVITA HIVE
        .config("spark.sql.catalogImplementation", "in-memory")
        # 🔑 FIJA WAREHOUSE LOCAL
        .config("spark.sql.warehouse.dir", warehouse_dir)
        # 🔑 EVITA FS RAROS
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


# -----------------------------
# MAIN PIPELINE
# -----------------------------
def main():
    spark = get_spark_session()

    print("🚀 Iniciando simulación de streaming...")

    # Simulación de streaming por micro-batches
    for batch_df in stream_csv(
        spark=spark,
        input_path=INPUT_PATH,
        interval_seconds=INGEST_INTERVAL_SECONDS
    ):
        # -----------------
        # BRONZE
        # -----------------
        write_bronze(
            spark=spark,
            df=batch_df,
            output_path=BRONZE_PATH
        )

        # -----------------
        # SILVER
        # -----------------
        silver_df = normalize_dates(batch_df)

        write_silver(
            df=silver_df,
            output_path=SILVER_PATH
        )

        print("✅ Batch procesado")
        sleep(INGEST_INTERVAL_SECONDS)

    spark.stop()
    print("🛑 Pipeline finalizado")


# -----------------------------
# ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    main()
