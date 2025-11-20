from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, lit, datediff, when

def aggregate_demographics(members_df, reference_date_str="2017-03-31"):
    """
    Membersihkan members_df dan membuat fitur age_group 
    serta membership_duration.
    
    Input: DataFrame Spark 'members_df' (data mentah)
    Output: DataFrame Spark 'demo_features' (siap gabung)
    """
    
    print("Memulai pre-processing demografi...")

    # 1. Hapus kolom 'gender' (sesuai rencana)
    processed_df = members_df.drop("gender")

    # 2. Membuat fitur 'age_group' dari 'bd' (umur)
    processed_df = processed_df.withColumn(
        "age_group",
        when((col("bd") > 0) & (col("bd") <= 17), "0-17 (Remaja)")
        .when((col("bd") >= 18) & (col("bd") <= 25), "18-25 (Muda)")
        .when((col("bd") >= 26) & (col("bd") <= 35), "26-35 (Dewasa)")
        .when((col("bd") >= 36) & (col("bd") <= 45), "36-45 (Paruh Baya)")
        .when((col("bd") >= 46) & (col("bd") <= 90), "46-90 (Senior)")
        .otherwise("Unknown")  # untuk umur <= 0, > 90, atau null
    )

    # 3. Membuat fitur 'membership_duration'
    # reference_date = lit(reference_date_str).cast("date")
    # reg_date = to_date(col("registration_init_time").cast("string"), "yyyyMMdd")
    
    # processed_df = processed_df.withColumn(
    #     "membership_duration_days",
    #     datediff(reference_date, reg_date)
    # )
    
    # 4. Pilih hanya kolom yang kita butuhkan untuk fitur
    demo_features = processed_df.select(
        "msno",
        "city",
        "age_group",
        "registered_via",
        # "membership_duration_days"
    )

    print("Pre-processing demografi selesai.")
    
    # Inilah jawaban atas pertanyaan "supaya bisa return"
    return demo_features