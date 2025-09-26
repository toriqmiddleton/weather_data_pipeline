import streamlit as st
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt

# Ganti dengan string koneksi MongoDB Atlas Anda
MONGO_CONNECTION_STRING = "mongodb+srv://weather_app_user:weather123@cluster0.frbc11l.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "weather_database"
COLLECTION_NAME = "weather_data"

# Fungsi untuk mengambil data dari database MongoDB
@st.cache_data
def load_data():
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    # Ambil semua dokumen dari koleksi dan masukkan ke dalam DataFrame
    df = pd.DataFrame(list(collection.find()))
    client.close()
    
    # Hapus kolom _id yang otomatis dibuat oleh MongoDB
    if '_id' in df.columns:
        df.drop('_id', axis=1, inplace=True)
        
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

# Ambil data dari database
df = load_data()

if df.empty:
    st.warning("Basis data kosong. Silakan jalankan skrip utama untuk mengumpulkan data.")
else:
    # --- Widget Interaktif ---
    st.sidebar.header('Pengaturan Filter')
    min_date = df['timestamp'].min().date()
    max_date = df['timestamp'].max().date()
    
    if min_date != max_date:
        start_date, end_date = st.sidebar.slider(
            "Pilih Rentang Tanggal:",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date)
        )
        filtered_df = df[(df['timestamp'].dt.date >= start_date) & (df['timestamp'].dt.date <= end_date)]
    else:
        st.sidebar.warning("Jalankan skrip utama beberapa kali untuk mengumpulkan data historis.")
        filtered_df = df.copy()

    # --- Tampilan Metrik Utama ---
    st.header('Metrik Utama')
    if not filtered_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_temp = filtered_df['temperature_celsius'].mean()
            st.metric(label="Suhu Rata-Rata Global", value=f"{avg_temp:.2f} °C")

        with col2:
            avg_humidity = filtered_df['humidity'].mean()
            st.metric(label="Kelembaban Rata-Rata Global", value=f"{avg_humidity:.2f} %")

        with col3:
            data_count = len(filtered_df)
            st.metric(label="Total Data", value=data_count)

        # --- Visualisasi ---
        st.header('Data Mentah')
        st.dataframe(filtered_df)

        st.header('Grafik Suhu per Kota')
        city_options = sorted(filtered_df['city'].unique().tolist())
        selected_city = st.selectbox('Pilih Kota untuk Grafik Suhu:', city_options)
        
        fig_temp, ax_temp = plt.subplots(figsize=(10, 6))
        plot_df = filtered_df[filtered_df['city'] == selected_city]
        ax_temp.plot(plot_df['timestamp'], plot_df['temperature_celsius'], marker='o')
        ax_temp.set_title(f'Perubahan Suhu di {selected_city}')
        ax_temp.set_xlabel('Waktu')
        ax_temp.set_ylabel('Suhu (°C)')
        ax_temp.grid(True)
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        st.pyplot(fig_temp)

        st.header('Perbandingan Kelembaban (Rata-Rata)')
        avg_humidity_per_city = filtered_df.groupby('city')['humidity'].mean().sort_values(ascending=False)
        st.bar_chart(avg_humidity_per_city)