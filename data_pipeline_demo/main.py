import requests
import pandas as pd
import sqlite3
from datetime import datetime
import schedule
import time
from tenacity import retry, stop_after_attempt, wait_fixed
from pymongo import MongoClient

# Ganti ini dengan string koneksi MongoDB Atlas Anda
# Perhatikan bahwa Anda juga harus mengganti <username> dan <password>
MONGO_CONNECTION_STRING = "mongodb+srv://weather_app_user:weather123@cluster0.frbc11l.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "weather_database"
COLLECTION_NAME = "weather_data"

# API Key Anda
api_key = 'cf0b096c121ca8406fd0aeba3c306146'

# Daftar kota
cities = [
    'London', 'Jakarta', 'Tokyo', 'Paris', 'New York',
    'Bandung', 'Surabaya', 'Medan', 'Makassar',
    'Beijing', 'Mumbai', 'Sydney', 'Cairo', 'Rio de Janeiro'
]

# Tambahkan decorator @retry ke fungsi pengambilan data
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_weather_data(city, api_key):
    """Fungsi yang mengambil data dari API, dengan retry otomatis jika gagal."""
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)
    response.raise_for_status() # Akan menghasilkan error jika status 4xx/5xx
    return response.json()

# Fungsi utama yang berisi pipeline ETL
def job():
    all_weather_data = []
    print(f"[{datetime.now().isoformat()}] Memulai pengambilan data...")

    for city in cities:
        try:
            data_json = fetch_weather_data(city, api_key)
            
            # Transformasi data
            temp_celsius = data_json['main']['temp'] - 273.15
            current_timestamp = datetime.now().isoformat()
            
            clean_data = {
                'city': data_json['name'],
                'temperature_celsius': temp_celsius,
                'humidity': data_json['main']['humidity'],
                'weather_description': data_json['weather'][0]['description'],
                'timestamp': current_timestamp
            }
            all_weather_data.append(clean_data)
            print(f"Data untuk {city} berhasil diambil.")

        except Exception as e:
            print(f"Gagal mengambil data untuk {city} setelah mencoba beberapa kali. Error: {e}")

    if all_weather_data:
        try:
            # Pindah ke koneksi MongoDB
            client = MongoClient(MONGO_CONNECTION_STRING)
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            
            # Memasukkan data ke MongoDB
            collection.insert_many(all_weather_data)
            
            print("Data berhasil ditambahkan ke database MongoDB Atlas.")
            client.close()
        except Exception as e:
            print(f"Gagal terhubung ke MongoDB Atlas. Error: {e}")
    else:
        print("Tidak ada data yang berhasil diambil. Proses dihentikan.")
    print("---")

# Menjadwalkan pekerjaan
schedule.every(1).minutes.do(job)

# Loop tak terbatas yang menjalankan scheduler
while True:
    schedule.run_pending()
    time.sleep(1)