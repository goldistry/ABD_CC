from pyspark.sql import functions as F
from pyspark.sql.functions import col

def aggregate_logs(user_logs_df):
    """
    Mengagregasi data user logs per msno
    """
    # Hitung total lagu untuk persentase
    logs_with_total = user_logs_df.withColumn("total_songs_day", 
        col("num_25") + col("num_50") + col("num_75") + col("num_985") + col("num_100")
    )
    
    log_features = logs_with_total.groupBy("msno").agg(
        # Rata-rata harian (semua kategori)
        F.avg(col("num_25")).alias("avg_num_25"),
        F.avg(col("num_50")).alias("avg_num_50"),  
        F.avg(col("num_75")).alias("avg_num_75"),  
        F.avg(col("num_985")).alias("avg_num_985"),
        F.avg(col("num_100")).alias("avg_num_100"),
        
        # Rata-rata total detik harian
        F.avg("total_secs").alias("avg_daily_secs"),
        
        # Total hari aktivitas
        F.countDistinct("date").alias("total_active_days"),
        
        # Total lagu unik (Sum)
        F.sum(col("num_unq")).alias("total_unq_songs"),   # <-- DIUBAH dari avg ke sum
        
        # Persentase lagu selesai
        F.avg(F.when(col("total_songs_day") > 0, col("num_100") / col("total_songs_day"))
             .otherwise(0)).alias("percent_songs_completed")
    )
    
    print("Agregasi logs selesai.")
    return log_features