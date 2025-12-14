from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def aggregate_transactions(transactions_df):
    """
    Untuk mengagregasi data transaksi per user (msno)
    """
    # memastikan yang kolom numerik memiliki tipe data yang benar
    trans_df_casted = transactions_df \
        .withColumn("plan_list_price", col("plan_list_price").cast("float")) \
        .withColumn("actual_amount_paid", col("actual_amount_paid").cast("float"))

    transaction_features = trans_df_casted.groupBy("msno").agg(
        F.count("payment_method_id").alias("total_transactions"),
        F.sum("payment_plan_days").alias("total_plan_days"),
        F.sum("actual_amount_paid").alias("total_amount_paid"),
        F.avg("actual_amount_paid").alias("avg_amount_paid"),
        F.sum("is_auto_renew").alias("count_auto_renew"),
        F.sum("is_cancel").alias("count_cancel"),
        F.mode("payment_method_id").alias("most_frequent_payment_method")
    )
    return transaction_features