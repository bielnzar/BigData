# eda_dataset.py
import pandas as pd

# Ganti dengan path dan nama file dataset Anda yang sebenarnya
DATASET_FULL_PATH = 'dataset/futuristic_city_traffic.csv' # Sesuaikan nama filenya

def initial_eda(file_path):
    print(f"Memulai EDA untuk dataset: {file_path}\n")
    
    try:
        # Coba baca beberapa baris dulu untuk melihat apakah ada masalah parsing
        print("Mencoba membaca sampel beberapa baris pertama...")
        df_sample = pd.read_csv(file_path, nrows=5)
        print("Sampel Data (5 baris pertama):")
        print(df_sample)
        print("\nKolom di sampel:")
        print(df_sample.columns.tolist())
        print("\nTipe data di sampel:")
        print(df_sample.dtypes)
        print("-" * 50)
    except Exception as e_sample:
        print(f"Error saat membaca sampel awal: {e_sample}")
        print("Mungkin ada masalah dengan format CSV atau path file.")
        return

    try:
        print("\nMembaca keseluruhan dataset (mungkin memakan waktu untuk file besar)...")
        df = pd.read_csv(file_path)
        print(f"Dataset berhasil dibaca. Jumlah baris: {len(df)}, Jumlah kolom: {len(df.columns)}")
        print("-" * 50)

        print("\nInformasi Dataset (df.info()):")
        df.info(verbose=True, show_counts=True) # verbose dan show_counts untuk detail
        print("-" * 50)

        print("\nStatistik Deskriptif untuk Kolom Numerik (df.describe()):")
        print(df.describe(include='number')) # Hanya numerik
        print("-" * 50)
        
        print("\nStatistik Deskriptif untuk Kolom Non-Numerik/Object (df.describe(include='object')):")
        print(df.describe(include='object')) # Hanya object/kategorikal
        print("-" * 50)

        print("\nJumlah Missing Values per Kolom:")
        missing_values = df.isnull().sum()
        print(missing_values[missing_values > 0]) # Hanya tampilkan kolom yang punya missing value
        if missing_values.sum() == 0:
            print("Tidak ada missing values di dataset ini. Bagus!")
        print("-" * 50)

        print("\nContoh Nilai Unik untuk beberapa kolom kategorikal potensial:")
        categorical_cols_to_check = ['icon', 'summary'] # Tambahkan kolom lain yang Anda curigai kategorikal
        for col_name in categorical_cols_to_check:
            if col_name in df.columns:
                unique_vals = df[col_name].unique()
                print(f"Kolom '{col_name}' (max 10 nilai unik pertama): {unique_vals[:10]}")
                print(f"  Jumlah nilai unik: {df[col_name].nunique()}")
            else:
                print(f"Kolom '{col_name}' tidak ditemukan.")
        
        # Pengecekan khusus untuk kolom 'cloudCover' dari sampel Anda
        if 'cloudCover' in df.columns:
            print("\nPengecekan khusus untuk kolom 'cloudCover':")
            print(f"  Tipe data kolom 'cloudCover': {df['cloudCover'].dtype}")
            print(f"  Nilai unik di 'cloudCover' (max 10): {df['cloudCover'].unique()[:10]}")
            # Jika tipe datanya object dan isinya selalu 'cloudCover', itu masalah.
            if df['cloudCover'].dtype == 'object' and (df['cloudCover'] == 'cloudCover').all():
                print("  PERHATIAN: Kolom 'cloudCover' tampaknya selalu berisi string 'cloudCover'. Ini perlu investigasi lebih lanjut atau mungkin dihilangkan.")
            elif df['cloudCover'].dtype != 'object': # Jika numerik, bagus
                 print(f"  Statistik 'cloudCover': Mean={df['cloudCover'].mean():.2f}, Min={df['cloudCover'].min():.2f}, Max={df['cloudCover'].max():.2f}")


        print("\nBeberapa Baris Pertama dari Dataset Lengkap:")
        print(df.head())
        print("-" * 50)
        
        print("EDA Awal Selesai.")

    except FileNotFoundError:
        print(f"Error: File tidak ditemukan di {file_path}")
    except Exception as e:
        print(f"Terjadi error saat melakukan EDA: {e}")

if __name__ == "__main__":
    initial_eda(DATASET_FULL_PATH)