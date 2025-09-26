import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Menghubungkan ke database
conn = sqlite3.connect('weather_database.db')

# Mengambil semua data dari tabel 'weather_data'
query = "SELECT * FROM weather_data;"
df = pd.read_sql_query(query, conn)

conn.close()

# Mengonversi kolom 'timestamp' menjadi format tanggal dan waktu
df['timestamp'] = pd.to_datetime(df['timestamp'])

# Filter data hanya untuk satu kota, misalnya Jakarta
jakarta_data = df[df['city'] == 'Jakarta']

# Membuat grafik
plt.figure(figsize=(10, 6)) # Mengatur ukuran grafik
plt.plot(jakarta_data['timestamp'], jakarta_data['temperature_celsius'], marker='o')

# Memberikan judul dan label
plt.title('Perubahan Suhu di Jakarta dari Waktu ke Waktu')
plt.xlabel('Waktu')
plt.ylabel('Suhu (°C)')
plt.grid(True) # Menambahkan grid
plt.xticks(rotation=45) # Memutar label waktu agar tidak bertumpuk
plt.tight_layout() # Menyesuaikan tata letak agar rapi

# Menampilkan grafik
plt.show()