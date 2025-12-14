from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def aggregate_logs(user_logs_df):
   
    print("Memulai agregasi logs")
    
    # 1. Tentukan Tanggal Referensi pakai akhir tgl akhir data train
    ref_date = F.to_date(F.lit('2017-03-31'))
    
    # 2. Ubah 'date' (int) menjadi 'log_date' (tipe data tanggal)
    logs_with_date = user_logs_df.withColumn(
        "log_date", 
        F.to_date(col("date").cast("string"), 'yyyyMMdd')
    )
    
    # 3. Hitung hari sejak aktivitas terakhir (Recency)
    recency_features = logs_with_date.groupBy("msno").agg(
        F.max("log_date").alias("last_log_date")
    ).withColumn(
        "days_since_last_activity",
        F.datediff(ref_date, col("last_log_date"))
    )

    # 4. Filter untuk jendela waktu (30 hari & 90 hari)
    df_last_30d = logs_with_date.filter(F.datediff(ref_date, col("log_date")) <= 30)
    df_last_90d = logs_with_date.filter(F.datediff(ref_date, col("log_date")) <= 90)
    
    # 5. Agregasi Fitur "Perilaku Terbaru" (30 & 90 hari terakhir)
    
    # Agregasi 30 hari
    logs_last_30d = df_last_30d.groupBy("msno").agg(
        F.sum("total_secs").alias("total_secs_last_30d"),
        F.countDistinct("date").alias("active_days_last_30d"),
        # Hitung total lagu untuk persentase complete 30 hari
        F.sum(col("num_100")).alias("total_complete_last_30d"),
        F.sum(col("num_25") + col("num_50") + col("num_75") + col("num_985") + col("num_100")).alias("total_songs_last_30d")
    )
    
    # Agregasi 90 hari (untuk fitur tren)
    logs_last_90d = df_last_90d.groupBy("msno").agg(
        F.sum("total_secs").alias("total_secs_last_90d"),
        F.countDistinct("date").alias("active_days_last_90d")
    )

    # 6. Agregasi Fitur "Lifetime" (Fitur lama yang mungkin masih berguna)
    logs_lifetime = logs_with_date.groupBy("msno").agg(
        F.countDistinct("date").alias("lifetime_active_days"),
        F.sum(col("num_unq")).alias("lifetime_unq_songs")
    )

    # 7. Join semua fitur log menjadi satu tabel
    log_features = recency_features \
        .join(logs_last_30d, "msno", "left") \
        .join(logs_last_90d, "msno", "left") \
        .join(logs_lifetime, "msno", "left")

    # 8. Buat Fitur Persentase
    log_features = log_features.withColumn(
        "percent_complete_last_30d",
        when(
            col("total_songs_last_30d") > 0,
            col("total_complete_last_30d") / col("total_songs_last_30d")
        ).otherwise(0)
    )


    # 9. Final Cleanup
    # Pilih kolom akhir & isi NaN/NULL dengan 0 
    # Pengguna yang tidak aktif 30/90 hari --> bakal NULL saat di-join.
    
    final_cols = [
        "msno",
        "days_since_last_activity",
        "total_secs_last_30d",
        "active_days_last_30d",
        "total_secs_last_90d",
        "active_days_last_90d",
        "percent_complete_last_30d",
        "lifetime_active_days",
        "lifetime_unq_songs",
    ]

    # kolom yg bakal di export
    log_features = log_features.select(final_cols).fillna(0)
    
    print("Agregasi logs selesai.")
    return log_features