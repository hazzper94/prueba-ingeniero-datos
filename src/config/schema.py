from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, TimestampType, DateType
)

loan_events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), True),
    StructField("ingestion_date", DateType(), True),
    StructField("loan_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("installment_number", IntegerType(), True),
    StructField("principal_amount", DoubleType(), True),
    StructField("installment_amount", DoubleType(), True),
    StructField("outstanding_balance", DoubleType(), True),
    StructField("interest_rate", DoubleType(), True),
    StructField("term_months", IntegerType(), True),
    StructField("days_past_due", IntegerType(), True),
    StructField("loan_status", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("region", StringType(), True),
    StructField("product_type", StringType(), True),
])