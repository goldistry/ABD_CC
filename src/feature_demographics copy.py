from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, when, to_date, lit, datediff
)

spark = SparkSession.builder \
    .appName("Analisis Demografi KKBox") \
    .getOrCreate()

print("Spark Session dimulai.")

print("Membaca train_combined.csv...")
train_df = spark.read.csv(
    "dataset/train_combined.csv", 
    header=True, 
    inferSchema=True
)

print("Membaca members_v3.csv...")
members_df = spark.read.csv(
    "dataset/members_v3.csv", 
    header=True, 
    inferSchema=True
)

print("Data selesai dibaca.")

# Menggabungkan dua DataFrame menggunakan left join
print("Menggabungkan train_df dan members_df...")
demographic_df = train_df.join(
    members_df, 
    on="msno",       
    how="left"   
)

# Simpan di cache untuk mempercepat proses selanjutnya
demographic_df.cache()

print("Data berhasil digabung.")
demographic_df.printSchema()

# Pre-processing dan Feature Engineering

print("Memulai pre-processing...")

# Hapus kolom 'gender' karena terlalu banyak data kosong (66% missing)
demographic_df = demographic_df.drop("gender")

# Membuat fitur 'age_group' dari 'bd' (umur)
print("Membuat 'age_group'...")
demographic_df = demographic_df.withColumn(
    "age_group",
    when((col("bd") > 0) & (col("bd") <= 17), "0-17 (Remaja)")
    .when((col("bd") >= 18) & (col("bd") <= 25), "18-25 (Muda)")
    .when((col("bd") >= 26) & (col("bd") <= 35), "26-35 (Dewasa)")
    .when((col("bd") >= 36) & (col("bd") <= 45), "36-45 (Paruh Baya)")
    .when((col("bd") >= 46) & (col("bd") <= 90), "46-90 (Senior)")
    .otherwise("Unknown")  # untuk umur <= 0, > 90, atau null
)

# Membuat fitur 'membership_duration' dengan menghitung selisih antara tanggal referensi (Maret 2017) dan 'registration_init_time'
reference_date = lit("2017-03-31").cast("date")
# 'registration_init_time' formatnya masih dalam bentuk teks sehingga perlu diubah dulu menjadi format tanggal
reg_date = to_date(col("registration_init_time").cast("string"), "yyyyMMdd")
demographic_df = demographic_df.withColumn(
    "membership_duration_days",
    datediff(reference_date, reg_date)
)

print("Pre-processing selesai.")
demographic_df.select("msno", "age_group", "membership_duration_days").show(5)

# Agregasi dan Persiapan Visualisasi
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

print("Memulai agregasi untuk visualisasi...")

# ANALISIS 1: rata-rata is_churn berdasarkan age_group
print("Menganalisis 'age_group'...")
age_churn_rate = demographic_df.groupBy("age_group") \
    .agg(avg("is_churn").alias("churn_rate")) \
    .orderBy("age_group")

age_churn_pd = age_churn_rate.toPandas()

print(age_churn_pd)
plt.figure(figsize=(12, 6))
ax_age = sns.barplot(
    data=age_churn_pd, 
    x="age_group", 
    y="churn_rate",
    color="tab:blue"
)
ax_age.set_title("Tingkat Churn berdasarkan Kelompok Umur", fontsize=16)
ax_age.set_xlabel("Kelompok Umur")
ax_age.set_ylabel("Tingkat Churn")

for p in ax_age.patches:
    ax_age.annotate(f'{p.get_height():.1%}', 
        (p.get_x() + p.get_width() / 2., p.get_height()), 
        ha='center', va='center', xytext=(0, 9), textcoords='offset points', fontweight='bold')
plt.show()


# ANALISIS 2: rata-rata is_churn berdasarkan city
print("Menganalisis 'city'...")
city_churn_rate = demographic_df.groupBy("city") \
    .agg(avg("is_churn").alias("churn_rate")) \
    .orderBy(col("churn_rate").desc())

city_churn_pd = city_churn_rate.limit(10).toPandas()
city_churn_pd['city'] = city_churn_pd['city'].astype(str) # tipe diubah ke string dulu supaya urutan tidak terpengaruh oleh urutan dalam angka

print(city_churn_pd)
plt.figure(figsize=(12, 6))
ax_city = sns.barplot(
    data=city_churn_pd, 
    x="city", 
    y="churn_rate",
    color="tab:blue"
)
ax_city.set_title("Top 10 Kota (by ID) dengan Tingkat Churn Tertinggi", fontsize=16)
ax_city.set_xlabel("ID Kota (Anonim)")
ax_city.set_ylabel("Tingkat Churn")

for p in ax_city.patches:
    ax_city.annotate(f'{p.get_height():.2%}', 
        (p.get_x() + p.get_width() / 2., p.get_height()), 
        ha='center', va='center', xytext=(0, 9), textcoords='offset points', fontweight='bold')
plt.show()

# ANALISIS 3: rata-rata is_churn berdasarkan metode registrasi
print("Menganalisis 'registered_via'...")
reg_churn_rate = demographic_df.groupBy("registered_via") \
    .agg(avg("is_churn").alias("churn_rate")) \
    .orderBy(col("churn_rate").desc())

reg_churn_pd = reg_churn_rate.toPandas()

reg_churn_pd['registered_via'] = reg_churn_pd['registered_via'].astype(str)

print(reg_churn_pd)
plt.figure(figsize=(12, 6))
ax_reg = sns.barplot(
    data=reg_churn_pd, 
    x="registered_via", 
    y="churn_rate",
    color="tab:blue"
)
ax_reg.set_title("Tingkat Churn berdasarkan Metode Registrasi (ID Anonim)", fontsize=16)
ax_reg.set_xlabel("ID Metode Registrasi")
ax_reg.set_ylabel("Tingkat Churn")

for p in ax_reg.patches:
    ax_reg.annotate(f'{p.get_height():.1%}', 
        (p.get_x() + p.get_width() / 2., p.get_height()), 
        ha='center', va='center', xytext=(0, 9), textcoords='offset points', fontweight='bold')
plt.show()

# ANALISIS 4: jumlah churn berdasarkan lama berlangganan
print("Menganalisis 'membership_duration_days'...")
duration_sample_df = demographic_df \
    .select("is_churn", "membership_duration_days") \
    .sample(withReplacement=False, fraction=0.1, seed=42)

duration_sample_pd = duration_sample_df.toPandas()

duration_sample_pd['label_churn'] = duration_sample_pd['is_churn'].map({
    0: "0 (Tidak Churn)",
    1: "1 (Churn)"
})

plt.figure(figsize=(8, 6))
ax_box = sns.boxplot(
    data=duration_sample_pd, 
    x="label_churn",
    y="membership_duration_days"
)
ax_box.set_title("Distribusi Durasi Berlangganan", fontsize=16)
ax_box.set_xlabel("Status Pengguna")
ax_box.set_ylabel("Durasi Berlangganan (Hari)")

plt.show()

demo_features = demographic_df.select(
    "msno",
    "is_churn", 
    "city",
    "bd", 
    "registered_via",
    "age_group",
    "membership_duration_days"
)

# Simpan hasil tabel agregasi sebagai Parquet
print("Menyimpan 'demo_features.parquet'...")
demo_features.write.mode("overwrite").parquet("dataset/demo_features.parquet")