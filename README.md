Tentu, ini adalah ide yang sangat bagus. `README.md` Anda yang lama sudah tidak sesuai dengan alur kerja (workflow) dan struktur file (`src/`, `data/`, `.parquet`) yang telah kita rancang.

Berikut adalah `README.md` baru yang telah saya perbarui untuk mencerminkan struktur proyek dan alur kerja tim Anda saat ini.

-----

# Proyek Analisis Prediktif Churn KKBox

Proyek ini bertujuan untuk menganalisis dan membangun model *machine learning* untuk memprediksi *churn* pelanggan pada dataset KKBox, sebagai bagian dari mata kuliah Analisis Big Data.

-----

## 📁 Struktur Proyek

Proyek ini diorganisir dengan memisahkan logika pemrosesan data (ETL/Fitur) dari notebook analisis (EDA/Laporan) untuk memudahkan kolaborasi tim di GitHub.

  * `src/`: Berisi semua logika *feature engineering* inti dalam bentuk skrip Python (`.py`). File-file ini berisi fungsi-fungsi Spark untuk agregasi data.
  * `dataset/`: Berisi data mentah. *(File data besar seperti `*_combined.csv` atau `.zip` diabaikan oleh `.gitignore` untuk mencegah error push ke GitHub).*
  * `data/`: Menyimpan *output* fitur yang telah diproses (dalam format `.parquet`). *(Folder ini diabaikan oleh `.gitignore`).*
  * `*.ipynb`: Notebook Jupyter yang digunakan untuk *menjalankan* alur kerja (seperti *controller*) dan untuk *visualisasi* (sebagai laporan).
  * `requirements.txt`: Daftar semua library Python yang dibutuhkan untuk proyek ini.

-----

## 🚀 Alur Kerja & Cara Menjalankan

Proyek ini memiliki alur kerja yang berurutan. **Jalankan notebook sesuai urutan ini.**

### Langkah 1: Persiapan Data (Hanya Dijalankan 1x)

  * **File:** `01_Data_Preparation.ipynb` (sebelumnya `generate_file_combined.ipynb`)
  * **Tujuan:** Membaca semua file data mentah (misal: `train.csv`, `train_v2.csv`), menggabungkannya, membersihkan duplikat, dan menyimpan file bersih (`*_combined.csv`) kembali ke folder `dataset/`.
  * **Kapan dijalankan:** Hanya perlu dijalankan satu kali di awal proyek.

### Langkah 2: Feature Engineering (Controller)

  * **File:** `02_Feature_Engineering.ipynb`
  * **Tujuan:** Mengimpor fungsi-fungsi dari folder `src/`. Notebook ini menjalankan logika agregasi berat (untuk demografi, transaksi, dan log) dan menyimpan hasilnya sebagai file `.parquet` yang efisien di folder `data/`.
  * **Kapan dijalankan:** Jalankan setelah Langkah 1. Jalankan ulang jika Anda atau tim Anda mengubah logika di folder `src/`.

### Langkah 3: Analisis & Visualisasi (EDA)

  * **File:** `03_EDA_Visualizations.ipynb`
  * **Tujuan:** Membaca file `.parquet` (yang sangat cepat dan ringan) dari folder `data/` untuk membuat semua plot dan analisis EDA (sesuai rencana di proposal).
  * **Kapan dijalankan:** Ini adalah notebook "laporan" utama Anda. Anda bisa menjalankannya kapan saja setelah Langkah 2 selesai.

-----

## ⚙️ Setup dan Instalasi

Untuk menjalankan proyek ini di komputer Anda, berikut langkah-langkah yang perlu Anda lakukan:

### Prasyarat

Pastikan Anda sudah menginstal:

  * [Git](https://git-scm.com/)
  * [Python 3.10+](https://www.python.org/downloads/)
  * **Java 8 atau 11** (Sangat penting, dibutuhkan untuk menjalankan Apache Spark)

### Langkah-langkah Instalasi

1.  **Clone Repositori Ini**
    Buka terminal (Git Bash atau Command Prompt) dan jalankan perintah berikut:

    ```bash
    git clone https://github.com/goldistry/ABD_CC.git
    cd ABD_CC
    ```

    *(URL diambil dari riwayat terminal Anda)*

2.  **Siapkan Lingkungan & Kernel Python**

      * *(Opsional tapi sangat disarankan)* Buat *virtual environment* baru untuk proyek ini agar library Anda tidak tercampur:
        ```bash
        python -m venv venv
        ```
        Lalu aktifkan (di Windows):
        ```bash
        .\venv\Scripts\activate
        ```
      * Instal semua library yang dibutuhkan menggunakan `requirements.txt`:
        ```bash
        pip install -r requirements.txt
        ```

3.  **Buka Proyek di VS Code**

      * Buka Visual Studio Code.
      * Pilih `File > Open Folder...` dan buka folder `ABD_CC` yang sudah Anda *clone*.
      * Buka file notebook (misal: `03_EDA_Visualizations.ipynb`).
      * Di pojok kanan atas, VS Code akan meminta Anda memilih kernel. **Pastikan Anda memilih kernel Python dari lingkungan yang baru saja Anda instal library-nya** (misal: `venv` Anda, atau kernel `spark`).

Anda sekarang siap untuk menjalankan alur kerja proyek sesuai urutan di atas.
