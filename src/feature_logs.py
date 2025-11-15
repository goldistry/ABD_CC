from pyspark.sql import functions as F
from pyspark.sql.functions import col

def aggregate_logs(user_logs_df):
    """
    Mengagregasi data user logs per msno.
    """
    print("Membuat fitur logs (ini mungkin butuh waktu)...")
    
    # Hitung total lagu untuk persentase
    logs_with_total = user_logs_df.withColumn("total_songs_day", 
        col("num_25") + col("num_50") + col("num_75") + col("num_985") + col("num_100")
    )
    
    log_features = logs_with_total.groupBy("msno").agg(
        F.avg("total_secs").alias("avg_daily_secs"),
        F.countDistinct("date").alias("total_active_days"),
        F.avg(col("num_100")).alias("avg_num_100"),
        F.avg(col("num_25")).alias("avg_num_25"),
        F.avg(F.when(col("total_songs_day") > 0, col("num_100") / col("total_songs_day"))
             .otherwise(0)).alias("percent_songs_completed")
    )
    
    return log_features