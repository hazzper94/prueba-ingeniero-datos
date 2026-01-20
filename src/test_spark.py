from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("spark-test")
    .master("local[1]")  # MUY importante en Windows
    .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.range(5)
df.show()

spark.stop()