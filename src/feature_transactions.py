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
        F.count("*").alias("total_transactions"),
        F.sum("payment_plan_days").alias("total_payment_plan_days"), 
        F.avg(col("plan_list_price") - col("actual_amount_paid")).alias("avg_discount"), # rata-rata diskon per transaksi
        F.sum(F.when(col("is_auto_renew") == 1, 1).otherwise(0)).alias("count_auto_renew"), # menghitung jumlah auto-renew
        F.sum(F.when(col("is_cancel") == 1, 1).otherwise(0)).alias("count_cancel"), #jumlah pembatalan , kalau is_cancel =1
        F.max("transaction_date").alias("last_transaction_date"),
        F.max("membership_expire_date").alias("last_expiry_date")
    )
    
    return transaction_features