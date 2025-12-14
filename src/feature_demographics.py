from pyspark.sql import functions as F
from pyspark.sql.functions import col, when

def aggregate_demographics(members_df):
    """
    Membersihkan members_df dan membuat fitur age_group 
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
    
    # 3. Pilih hanya kolom yang dibutuhkan untuk fitur
    demo_features = processed_df.select(
        "msno",
        "city",
        "age_group",
        "registered_via",
    )

    print("Pre-processing demografi selesai.")
    
    return demo_features