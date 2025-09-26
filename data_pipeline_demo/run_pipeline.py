import subprocess
import os

def run_pipeline():
    """Menjalankan seluruh pipeline data secara berurutan."""
    print("Memulai pipeline...")

    # Langkah 1: Jalankan skrip main.py untuk mengambil data baru.
    print("\n--- Langkah 1: Mengambil data baru ---")
    
    # Gunakan subprocess untuk menjalankan main.py dan tunggu hingga selesai
    # Peringatan: Pastikan Anda menghentikan skrip ini setelah selesai.
    # Jika main.py berjalan terus-menerus, maka ini tidak akan selesai
    
    # Kita asumsikan skrip main.py sudah berjalan otomatis
    # Jadi kita langsung ke langkah 2
    
    # # Jika Anda ingin menjalankannya sekali saja, gunakan ini:
    # subprocess.run(["python", "main.py"], check=True)
    # print("\n--- Pengambilan data selesai ---")

    # Langkah 2: Jalankan Streamlit dashboard
    print("\n--- Langkah 2: Menjalankan dashboard ---")
    print("Dashboard akan terbuka di browser Anda.")
    
    # Perintah untuk menjalankan Streamlit
    subprocess.run(["streamlit", "run", "dashboard.py"])
    
if __name__ == "__main__":
    run_pipeline()