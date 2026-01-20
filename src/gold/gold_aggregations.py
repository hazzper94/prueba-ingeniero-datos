from pyspark.sql.functions import sum, avg, count

def customer_metrics(df):
    return (
        df.groupBy("customer_id", "product_type")
        .agg(
            count("*").alias("total_events"),
            sum("principal_amount").alias("total_principal"),
            avg("interest_rate").alias("avg_interest_rate"),
            sum("days_past_due").alias("total_days_past_due")
        )
    )