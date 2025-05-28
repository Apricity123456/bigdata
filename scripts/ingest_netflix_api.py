import os
from kaggle.api.kaggle_api_extended import KaggleApi

RAW_DIR = "./data/raw/netflix/"

def download_from_kaggle():
    api = KaggleApi()
    api.authenticate()

    os.makedirs(RAW_DIR, exist_ok=True)
    print("⬇ Downloading Netflix dataset via Kaggle API...")
    api.dataset_download_files('shivamb/netflix-shows', path=RAW_DIR, unzip=True)
    print("✅ Downloaded and extracted to:", RAW_DIR)

if __name__ == "__main__":
    download_from_kaggle()
