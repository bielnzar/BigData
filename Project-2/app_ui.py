# proyek_big_data_kafka_spark/app_ui.py
import streamlit as st
import requests
import json
# import pandas as pd # Tidak dibutuhkan lagi di sini jika API selalu tersedia

API_BASE_URL = "http://localhost:5000" 

st.set_page_config(
    page_title="Smart Home Analytics Dashboard",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

with st.sidebar:
    st.header("Tentang Aplikasi")
    st.markdown("""
    Aplikasi ini mendemonstrasikan penggunaan model Machine Learning
    untuk menganalisis data penggunaan perangkat Smart Home.
    Model dilatih menggunakan data yang di-stream melalui Kafka dan diproses dengan Spark.
    
    **Model yang Tersedia:**
    1. Prediksi Efisiensi Perangkat
    2. Prediksi Potensi Kerusakan
    3. Segmentasi Perangkat
    """)
    st.markdown("---")
    st.subheader("Status API")
    api_status_placeholder = st.empty()

def check_api_status():
    try:
        response = requests.get(f"{API_BASE_URL}/", timeout=2) 
        if response.status_code == 200:
            api_status_placeholder.success("API Terhubung")
            return True
    except requests.exceptions.ConnectionError:
        api_status_placeholder.error("API Tidak Terhubung. Pastikan API server berjalan.")
    except requests.exceptions.Timeout:
        api_status_placeholder.warning("API Timeout. Mungkin sedang sibuk atau lambat.")
    except Exception as e:
        api_status_placeholder.error(f"Error API: {e}")
    return False

api_ready = check_api_status()

def call_api(endpoint, payload):
    if not api_ready:
        st.error("Tidak dapat memanggil API karena tidak terhubung.")
        return None
    try:
        response = requests.post(f"{API_BASE_URL}/{endpoint}", json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        st.error(f"Error HTTP dari API: {http_err} - {response.text}")
    except requests.exceptions.ConnectionError:
        st.error(f"Gagal terhubung ke API di {API_BASE_URL}/{endpoint}.")
    except requests.exceptions.Timeout:
        st.error(f"API call ke {API_BASE_URL}/{endpoint} timeout.")
    except Exception as e:
        st.error(f"Terjadi kesalahan saat memanggil API: {e}")
    return None

st.title("🏠 Smart Home Analytics Dashboard")
st.markdown("Selamat datang! Silakan pilih model dan masukkan data untuk mendapatkan analisis.")

DEVICE_TYPES = ['Lights', 'Thermostat', 'Smart Speaker', 'Camera', 'Security System']

# --- Model 1: Prediksi Efisiensi Perangkat ---
st.header("1. Prediksi Efisiensi Perangkat")
st.markdown("""
Model ini memprediksi apakah sebuah perangkat smart home beroperasi secara efisien atau tidak,
berdasarkan karakteristik dan pola penggunaannya.
- **Input:** Detail perangkat seperti tipe, jam penggunaan, konsumsi energi, dll.
- **Output:** Kelas Efisiensi (Efisien/Tidak Efisien) dan probabilitasnya.
""")

with st.form(key="efficiency_form"):
    st.subheader("Masukkan Detail Perangkat:")
    col1_eff, col2_eff, col3_eff = st.columns(3)
    with col1_eff:
        device_type_eff = st.selectbox("Tipe Perangkat:", DEVICE_TYPES, key="eff_device_ui")
        usage_hours_eff = st.slider("Jam Penggunaan/Hari:", min_value=0.0, max_value=24.0, value=10.0, step=0.1, key="eff_usage_ui", format="%.1f")
    with col2_eff:
        energy_eff = st.number_input("Konsumsi Energi (kWh/hari):", min_value=0.0, value=1.0, step=0.01, key="eff_energy_ui", format="%.2f")
        pref_eff = st.selectbox("Preferensi Pengguna:", options=[0, 1], format_func=lambda x: "Tinggi" if x == 1 else "Rendah", index=1, key="eff_pref_ui")
    with col3_eff:
        malfunction_eff_feat = st.number_input("Jumlah Insiden Kerusakan (sebagai fitur):", min_value=0, value=0, step=1, key="eff_malf_feat_ui")
        age_eff = st.slider("Usia Perangkat (bulan):", min_value=0, max_value=60, value=12, step=1, key="eff_age_ui", format="%d")

    submit_button_eff = st.form_submit_button(label="✨ Prediksi Efisiensi")

if submit_button_eff:
    payload_eff = {
        "DeviceType": device_type_eff,
        "UsageHoursPerDay": usage_hours_eff,
        "EnergyConsumption": energy_eff,
        "UserPreferences": pref_eff,
        "MalfunctionIncidents": malfunction_eff_feat,
        "DeviceAgeMonths": age_eff
    }
    result_eff = call_api("predict_efficiency", payload_eff)
    
    if result_eff:
        st.subheader("📊 Hasil Prediksi Efisiensi:")
        predicted_class_num = result_eff.get("predicted_efficiency_class")
        label_eff_str = result_eff.get("label", "Tidak Diketahui") # Ambil dari API jika ada, jika tidak, default
        probability = result_eff.get("probability_of_predicted_class")

        if predicted_class_num == 1:
            st.success(f"**Perangkat Diprediksi: {label_eff_str}** 👍")
        elif predicted_class_num == 0:
            st.warning(f"**Perangkat Diprediksi: {label_eff_str}** 👎")
        else:
            st.info(f"Hasil prediksi: {label_eff_str}")


        if probability is not None:
            st.progress(float(probability)) # Pastikan probabilitas adalah float
            st.caption(f"Keyakinan model untuk kelas yang diprediksi: {float(probability)*100:.1f}%")
        
        with st.expander("Lihat Detail Payload & Respons API"):
            st.write("Payload yang Dikirim:", payload_eff)
            st.json(result_eff)

st.markdown("---")
# --- Model 2: Prediksi Potensi Kerusakan ---
st.header("2. Prediksi Potensi Kerusakan")
st.markdown("""
Model ini memprediksi perkiraan jumlah insiden kerusakan yang mungkin terjadi pada perangkat
berdasarkan karakteristik dan pola penggunaannya.
- **Input:** Detail perangkat (tanpa jumlah kerusakan saat ini).
- **Output:** Perkiraan jumlah kerusakan.
""")
with st.form(key="malfunction_form"):
    st.subheader("Masukkan Detail Perangkat:")
    col1_malf, col2_malf = st.columns(2)
    with col1_malf:
        device_type_malf = st.selectbox("Tipe Perangkat:", DEVICE_TYPES, key="malf_device_ui")
        usage_hours_malf = st.slider("Jam Penggunaan/Hari:", min_value=0.0, max_value=24.0, value=15.0, step=0.1, key="malf_usage_ui", format="%.1f")
    with col2_malf:
        energy_malf = st.number_input("Konsumsi Energi (kWh/hari):", min_value=0.0, value=2.0, step=0.01, key="malf_energy_ui", format="%.2f")
        pref_malf = st.selectbox("Preferensi Pengguna:", options=[0, 1], format_func=lambda x: "Tinggi" if x == 1 else "Rendah", key="malf_pref_ui")
        age_malf = st.slider("Usia Perangkat (bulan):", min_value=0, max_value=60, value=24, step=1, key="malf_age_ui", format="%d")
    
    submit_button_malf = st.form_submit_button(label="🛠️ Prediksi Kerusakan")

if submit_button_malf:
    payload_malf = {
        "DeviceType": device_type_malf,
        "UsageHoursPerDay": usage_hours_malf,
        "EnergyConsumption": energy_malf,
        "UserPreferences": pref_malf,
        "DeviceAgeMonths": age_malf
    }
    result_malf = call_api("predict_malfunction", payload_malf)

    if result_malf:
        st.subheader("📈 Hasil Prediksi Kerusakan:")
        predicted_malfunctions = result_malf.get("predicted_malfunctions")
        if predicted_malfunctions is not None:
            st.info(f"**Perkiraan Jumlah Insiden Kerusakan: {predicted_malfunctions:.2f}**")
            st.caption("Ini adalah estimasi. Angka yang lebih tinggi menunjukkan potensi risiko kerusakan yang lebih besar.")
        else:
            st.warning("Tidak dapat mengambil prediksi jumlah kerusakan dari API.")

        with st.expander("Lihat Detail Payload & Respons API"):
            st.write("Payload yang Dikirim:", payload_malf)
            st.json(result_malf)
st.markdown("---")

# --- Model 3: Segmentasi Perangkat ---
st.header("3. Identifikasi Segmen Perangkat")
st.markdown("""
Model ini mengelompokkan perangkat ke dalam segmen tertentu berdasarkan karakteristiknya.
Setiap segmen mewakili kelompok perangkat dengan pola serupa.
- **Input:** Detail perangkat.
- **Output:** ID Segmen dan deskripsi (jika tersedia).
""")

CLUSTER_DESCRIPTIONS = {
    0: "Segmen 'Thermostat Sentris': Cluster ini secara eksklusif berisi perangkat Thermostat. Pola penggunaan, konsumsi energi, usia, dan tingkat kerusakannya berada di sekitar nilai rata-rata keseluruhan perangkat.",
    1: "Segmen 'Fokus Keamanan': Cluster ini secara eksklusif berisi Sistem Keamanan (Security System). Karakteristik operasionalnya (penggunaan, energi, usia, kerusakan) sangat mirip dengan rata-rata perangkat pada umumnya.",
    2: "Segmen 'Visual & Penerangan': Cluster ini didominasi oleh Kamera dan Lampu (Lights). Menunjukkan pola penggunaan dan efisiensi energi yang moderat, sejalan dengan karakteristik rata-rata perangkat smart home.",
    3: "Segmen 'Asisten Suara': Cluster ini secara eksklusif berisi Smart Speaker. Perangkat ini cenderung memiliki jam penggunaan dan konsumsi energi sedikit lebih rendah dibandingkan segmen lain, dengan usia dan riwayat kerusakan yang tipikal."
}

with st.form(key="segment_form"):
    st.subheader("Masukkan Detail Perangkat:")
    col1_seg, col2_seg, col3_seg = st.columns(3)
    with col1_seg:
        device_type_seg = st.selectbox("Tipe Perangkat:", DEVICE_TYPES, key="seg_device_ui")
        usage_hours_seg = st.slider("Jam Penggunaan/Hari:", min_value=0.0, max_value=24.0, value=5.0, step=0.1, key="seg_usage_ui", format="%.1f")
    with col2_seg:
        energy_seg = st.number_input("Konsumsi Energi (kWh/hari):", min_value=0.0, value=0.8, step=0.01, key="seg_energy_ui", format="%.2f")
        pref_seg = st.selectbox("Preferensi Pengguna:", options=[0,1], format_func=lambda x: "Tinggi" if x == 1 else "Rendah", key="seg_pref_ui")
    with col3_seg:
        malfunction_seg_feat = st.number_input("Jumlah Insiden Kerusakan (sebagai fitur):", min_value=0, value=1, step=1, key="seg_malf_feat_ui")
        age_seg = st.slider("Usia Perangkat (bulan):", min_value=0, max_value=60, value=6, step=1, key="seg_age_ui", format="%d")

    submit_button_seg = st.form_submit_button(label="🧩 Identifikasi Segmen")

if submit_button_seg:
    payload_seg = {
        "DeviceType": device_type_seg,
        "UsageHoursPerDay": usage_hours_seg,
        "EnergyConsumption": energy_seg,
        "UserPreferences": pref_seg,
        "MalfunctionIncidents": malfunction_seg_feat, 
        "DeviceAgeMonths": age_seg
    }
    result_seg = call_api("get_device_segment", payload_seg)

    if result_seg:
        st.subheader("🧩 Hasil Segmentasi Perangkat:")
        segment_id = result_seg.get("device_segment_id")
        if segment_id is not None:
            segment_desc = CLUSTER_DESCRIPTIONS.get(int(segment_id), "Deskripsi untuk segmen ini belum tersedia atau ID segmen tidak dikenal.")
            st.info(f"**Perangkat Termasuk dalam Segmen: {segment_id}**")
            st.write(f"**Deskripsi Umum Segmen:** {segment_desc}")
            st.caption("Deskripsi segmen didasarkan pada analisis karakteristik rata-rata perangkat di dalamnya.")
        else:
            st.warning("Tidak dapat mengambil ID segmen dari API.")

        with st.expander("Lihat Detail Payload & Respons API"):
            st.write("Payload yang Dikirim:", payload_seg)
            st.json(result_seg)

st.markdown("---")
st.caption("Aplikasi ini dibuat untuk demonstrasi integrasi Kafka, Spark, API, dan UI. © 2025")