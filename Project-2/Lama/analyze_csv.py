import pandas as pd
import os

DATASET_PATH = 'dataset/futuristic_city_traffic.csv'

try:
    # Construct the absolute path to the dataset
    # This assumes your script is in the Project-2 root directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_dataset_path = os.path.join(base_dir, DATASET_PATH)

    # If your script is NOT in the Project-2 root, 
    # you might need to adjust DATASET_PATH directly, e.g.:
    # DATASET_PATH = '/home/bosmuda/Kuliah/BigData/Project-2/dataset/smart_home_device_usage_data.csv'
    # Or ensure you run the script from the Project-2 root directory.

    if not os.path.exists(absolute_dataset_path):
        # Fallback if the relative path assumption is wrong, try the original path
        print(f"Trying original DATASET_PATH: {DATASET_PATH}")
        df = pd.read_csv(DATASET_PATH)
    else:
        df = pd.read_csv(absolute_dataset_path)
        
    print(f"Berhasil membaca dataset: {absolute_dataset_path if os.path.exists(absolute_dataset_path) else DATASET_PATH}\n")

    for column in df.columns:
        unique_values = df[column].unique()
        print(f"Kolom: {column}")
        # Check if the column is of object type (likely string) or has few unique values
        if df[column].dtype == 'object' or len(unique_values) <= 20:
            print(f"  Nilai Unik ({len(unique_values)}): {unique_values.tolist()}")
        else: # For numerical columns with many unique values, print a summary
            print(f"  Nilai Unik ({len(unique_values)}): Banyak nilai unik numerik.")
            print(f"    Min: {df[column].min()}")
            print(f"    Max: {df[column].max()}")
            print(f"    Contoh (5 pertama): {unique_values[:5].tolist()}")
        print("-" * 40)

except FileNotFoundError:
    print(f"Error: File dataset tidak ditemukan.")
    print(f"Pastikan path '{DATASET_PATH}' benar dan file ada di lokasi tersebut.")
    print(f"Atau, jika Anda menjalankan skrip ini dari direktori yang berbeda, sesuaikan DATASET_PATH.")
except Exception as e:
    print(f"Terjadi error: {e}") 