# Proyek Analisis Prediktif Churn KKBox

Proyek ini bertujuan untuk menganalisis dan membangun model machine learning untuk memprediksi churn pelanggan pada dataset KKBox.

---

## Setup dan Instalasi

Untuk menjalankan proyek ini di komputer Anda, berikut steps yang perlu Anda lakukan:

### Prasyarat

Pastikan Anda sudah menginstal:
* [Git](https://git-scm.com/)
* [Anaconda](https://www.anaconda.com/products/distribution) atau [Miniconda](https://docs.conda.io/en/latest/miniconda.html)

### Langkah-langkah Instalasi

1.  **Clone Repositori Ini**
    Buka terminal atau Git Bash dan jalankan perintah berikut:
    ```bash
    git clone [https://github.com/username-anda/nama-repo-anda.git](https://github.com/username-anda/nama-repo-anda.git)
    cd nama-repo-anda
    ```

2.  **Buat Lingkungan Conda**
    Proyek ini menggunakan lingkungan Conda yang spesifik. Gunakan file `environment.yml` yang sudah disediakan untuk membuat ulang lingkungan yang sama persis. Perintah ini akan menginstal Python dan semua library yang dibutuhkan secara otomatis.
    ```bash
    conda env create -f environment.yml
    ```
    Proses ini mungkin akan memakan waktu beberapa menit.

3.  **Aktifkan Lingkungan Conda**
    Setelah pembuatan lingkungan selesai, aktifkan dengan perintah berikut:
    ```bash
    conda activate spark
    ```

4.  **Buka Proyek di VS Code**
    * Buka Visual Studio Code.
    * Pilih `File > Open Folder...` dan buka folder proyek yang sudah Anda clone.
    * Buka file notebook (`.ipynb`). Di pojok kanan atas, VS Code akan meminta Anda memilih kernel. **Pastikan Anda memilih kernel Python dari lingkungan `spark` yang baru saja Anda buat.**

Anda sekarang siap untuk menjalankan semua sel di dalam notebook.
