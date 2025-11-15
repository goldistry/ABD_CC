from pyspark.sql import functions as F

def aggregate_transactions(transactions_df):
    """
    Menerima dataframe transactions_combined dan mengagregasinya 
    per msno.
    """
    
    transaction_features_df = transactions_df.groupBy("msno").agg(
        F.count("*").alias("total_transactions"),
        F.sum("payment_plan_days").alias("total_plan_days"),
        F.avg(F.col("plan_list_price") - F.col("actual_amount_paid")).alias("avg_discount"),
        F.sum(F.when(F.col("is_auto_renew") == 1, 1).otherwise(0)).alias("auto_renew_count"),
        F.sum(F.when(F.col("is_cancel") == 1, 1).otherwise(0)).alias("cancel_count"),
        F.max("transaction_date").alias("last_transaction_date"),
        F.max("membership_expire_date").alias("last_expiry_date")
    )
    return transaction_features_df