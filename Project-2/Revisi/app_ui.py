import streamlit as st
import requests
import json

API_BASE_URL = "http://127.0.0.1:5000" 

st.set_page_config(
    page_title="Urban Traffic Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

with st.sidebar:
    st.header("Tentang Aplikasi")
    st.markdown("""
    Dashboard interaktif untuk menganalisis dan memprediksi aspek lalu lintas perkotaan futuristik.
    Model dilatih menggunakan data dari simulasi traffic stream.
    
    **Fitur Model:**
    1. Prediksi Kepadatan Lalu Lintas
    2. Prediksi Jam Sibuk (Peak Hour)
    3. Prediksi Konsumsi Energi Kendaraan
    """)
    st.markdown("---")
    st.subheader("Status API")
    api_status_placeholder = st.empty()

def check_api_status():
    try:
        response = requests.get(f"{API_BASE_URL}/", timeout=3) 
        if response.status_code == 200:
            api_data = response.json()
            api_status_placeholder.success(f"API Terhubung ({api_data.get('message', '')})")
            st.sidebar.json(api_data.get("models_status")) # Tampilkan status model
            return True
    except requests.exceptions.RequestException:
        api_status_placeholder.error("API Tidak Terhubung. Pastikan API server (app_api.py) berjalan.")
    return False

api_ready = check_api_status()

def call_api(endpoint, payload):
    if not api_ready:
        st.error("Tidak dapat memanggil API. Periksa status koneksi di sidebar.")
        return None
    try:
        response = requests.post(f"{API_BASE_URL}/{endpoint}", json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        st.error(f"Error HTTP dari API ({response.status_code}): {response.text}")
    except Exception as e:
        st.error(f"Kesalahan saat memanggil API {endpoint}: {e}")
    return None

st.title("🚗 Urban Traffic Analytics Dashboard")

# --- Opsi Input Umum ---
CITIES = ['SolarisVille', 'AquaCity', 'Neuroburg', 'Ecoopolis', 'TechHaven', 'MetropolisX']
VEHICLE_TYPES = ['Drone', 'Flying Car', 'Autonomous Vehicle', 'Car']
WEATHER_CONDITIONS = ['Snowy', 'Solar Flare', 'Clear', 'Rainy', 'Electromagnetic Storm']
ECONOMIC_CONDITIONS = ['Stable', 'Recession', 'Booming']
DAYS_OF_WEEK = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# --- Model 1: Prediksi Kepadatan Lalu Lintas (Traffic Density) ---
st.header("1. Prediksi Kepadatan Lalu Lintas")
st.markdown("Model ini memprediksi tingkat kepadatan lalu lintas (nilai numerik).")

with st.form(key="density_form"):
    st.subheader("Masukkan Kondisi Lalu Lintas:")
    c1d, c2d, c3d = st.columns(3)
    with c1d:
        city_den = st.selectbox("Kota:", CITIES, key="den_city")
        vehicle_den = st.selectbox("Tipe Kendaraan:", VEHICLE_TYPES, key="den_vehicle")
        weather_den = st.selectbox("Kondisi Cuaca:", WEATHER_CONDITIONS, key="den_weather")
    with c2d:
        econ_den = st.selectbox("Kondisi Ekonomi:", ECONOMIC_CONDITIONS, key="den_econ")
        day_den = st.selectbox("Hari:", DAYS_OF_WEEK, key="den_day")
        hour_den = st.slider("Jam (0-23):", 0, 23, 10, key="den_hour")
    with c3d:
        speed_den = st.number_input("Kecepatan (km/h):", min_value=0.0, value=60.0, format="%.2f", key="den_speed")
        is_peak_den_feat = st.selectbox("Apakah Jam Sibuk? (fitur)", [0, 1], format_func=lambda x: "Ya" if x==1 else "Tidak", key="den_peak_feat")
        event_den = st.selectbox("Ada Kejadian Acak? (fitur)", [0, 1], format_func=lambda x: "Ya" if x==1 else "Tidak", key="den_event")
        energy_den_feat = st.number_input("Konsumsi Energi (kWh, fitur):", min_value=0.0, value=50.0, format="%.2f", key="den_energy_feat")

    submit_density = st.form_submit_button("🚦 Prediksi Kepadatan")

if submit_density:
    payload_density = {
        "City": city_den, "Vehicle Type": vehicle_den, "Weather": weather_den,
        "Economic Condition": econ_den, "Day Of Week": day_den, "Hour Of Day": hour_den,
        "Speed": speed_den, "Is Peak Hour": is_peak_den_feat, 
        "Random Event Occurred": event_den, "Energy Consumption": energy_den_feat
        # "Traffic Density" tidak diinput karena itu target
    }
    result = call_api("predict_traffic_density", payload_density)
    if result:
        st.subheader("📈 Hasil Prediksi Kepadatan:")
        st.metric(label="Prediksi Kepadatan Lalu Lintas", value=f"{result.get('predicted_traffic_density', 'N/A'):.4f}")
        st.caption("Nilai yang lebih tinggi menunjukkan kepadatan yang lebih besar.")
        with st.expander("Detail Respons API"): st.json(result)

st.markdown("---")

# --- Model 2: Prediksi Jam Sibuk (Is Peak Hour) ---
st.header("2. Prediksi Jam Sibuk (Peak Hour)")
st.markdown("Model ini memprediksi apakah kondisi saat ini termasuk jam sibuk (Ya/Tidak).")

with st.form(key="peak_form"):
    st.subheader("Masukkan Kondisi Saat Ini:")
    c1p, c2p, c3p = st.columns(3)
    with c1p:
        city_peak = st.selectbox("Kota:", CITIES, key="peak_city")
        vehicle_peak = st.selectbox("Tipe Kendaraan:", VEHICLE_TYPES, key="peak_vehicle")
        weather_peak = st.selectbox("Kondisi Cuaca:", WEATHER_CONDITIONS, key="peak_weather")
    with c2p:
        econ_peak = st.selectbox("Kondisi Ekonomi:", ECONOMIC_CONDITIONS, key="peak_econ")
        day_peak = st.selectbox("Hari:", DAYS_OF_WEEK, key="peak_day")
        hour_peak = st.slider("Jam (0-23):", 0, 23, 17, key="peak_hour") # Default jam sore
    with c3p:
        speed_peak = st.number_input("Kecepatan (km/h):", min_value=0.0, value=30.0, format="%.2f", key="peak_speed")
        event_peak = st.selectbox("Ada Kejadian Acak?", [0, 1], format_func=lambda x: "Ya" if x==1 else "Tidak", key="peak_event")
        energy_peak_feat = st.number_input("Konsumsi Energi (kWh, fitur):", min_value=0.0, value=60.0, format="%.2f", key="peak_energy_feat")
        density_peak_feat = st.number_input("Kepadatan Lalu Lintas (fitur):", min_value=0.0, value=0.7, format="%.2f", key="peak_density_feat")
    
    submit_peak = st.form_submit_button("🕒 Prediksi Jam Sibuk")

if submit_peak:
    payload_peak = {
        "City": city_peak, "Vehicle Type": vehicle_peak, "Weather": weather_peak,
        "Economic Condition": econ_peak, "Day Of Week": day_peak, "Hour Of Day": hour_peak,
        "Speed": speed_peak, "Random Event Occurred": event_peak, 
        "Energy Consumption": energy_peak_feat, "Traffic Density": density_peak_feat
        # "Is Peak Hour" tidak diinput karena itu target
    }
    result = call_api("predict_is_peak_hour", payload_peak)
    if result:
        st.subheader("🕒 Hasil Prediksi Jam Sibuk:")
        label = result.get("label", "N/A")
        prob = result.get("probability_of_predicted_class")
        if label == "Peak Hour":
            st.error(f"**Prediksi: {label}**")
        else:
            st.success(f"**Prediksi: {label}**")
        if prob is not None:
            st.progress(float(prob))
            st.caption(f"Keyakinan model: {float(prob)*100:.1f}%")
        with st.expander("Detail Respons API"): st.json(result)

st.markdown("---")

# --- Model 3: Prediksi Konsumsi Energi ---
st.header("3. Prediksi Konsumsi Energi Kendaraan")
st.markdown("Model ini memprediksi estimasi konsumsi energi kendaraan (kWh).")

with st.form(key="energy_form"):
    st.subheader("Masukkan Detail Kendaraan & Kondisi:")
    c1e, c2e, c3e = st.columns(3)
    with c1e:
        city_energy = st.selectbox("Kota:", CITIES, key="energy_city")
        vehicle_energy = st.selectbox("Tipe Kendaraan:", VEHICLE_TYPES, key="energy_vehicle")
        weather_energy = st.selectbox("Kondisi Cuaca:", WEATHER_CONDITIONS, key="energy_weather")
    with c2e:
        econ_energy = st.selectbox("Kondisi Ekonomi:", ECONOMIC_CONDITIONS, key="energy_econ")
        day_energy = st.selectbox("Hari:", DAYS_OF_WEEK, key="energy_day")
        hour_energy = st.slider("Jam (0-23):", 0, 23, 9, key="energy_hour")
    with c3e:
        speed_energy = st.number_input("Kecepatan (km/h):", min_value=0.0, value=80.0, format="%.2f", key="energy_speed")
        is_peak_energy_feat = st.selectbox("Apakah Jam Sibuk? (fitur)", [0, 1], format_func=lambda x: "Ya" if x==1 else "Tidak", key="energy_peak_feat")
        event_energy = st.selectbox("Ada Kejadian Acak? (fitur)", [0, 1], format_func=lambda x: "Ya" if x==1 else "Tidak", key="energy_event")
        density_energy_feat = st.number_input("Kepadatan Lalu Lintas (fitur):", min_value=0.0, value=0.3, format="%.2f", key="energy_density_feat")

    submit_energy = st.form_submit_button("⚡ Prediksi Konsumsi Energi")

if submit_energy:
    payload_energy = {
        "City": city_energy, "Vehicle Type": vehicle_energy, "Weather": weather_energy,
        "Economic Condition": econ_energy, "Day Of Week": day_energy, "Hour Of Day": hour_energy,
        "Speed": speed_energy, "Is Peak Hour": is_peak_energy_feat,
        "Random Event Occurred": event_energy, "Traffic Density": density_energy_feat
        # "Energy Consumption" tidak diinput karena itu target
    }
    result = call_api("predict_energy_consumption", payload_energy)
    if result:
        st.subheader("💡 Hasil Prediksi Konsumsi Energi:")
        st.metric(label="Prediksi Konsumsi Energi", value=f"{result.get('predicted_energy_consumption', 'N/A'):.2f} kWh")
        with st.expander("Detail Respons API"): st.json(result)

st.markdown("---")
st.caption("Aplikasi ini menggunakan model yang dilatih pada data simulasi lalu lintas perkotaan futuristik.")