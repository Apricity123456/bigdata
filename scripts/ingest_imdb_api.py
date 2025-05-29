import requests
import gzip
import shutil
import os

# 定义目标路径
IMDB_URLS = {
    "basics": "https://datasets.imdbws.com/title.basics.tsv.gz",
    "ratings": "https://datasets.imdbws.com/title.ratings.tsv.gz"
}

RAW_DIR = "./data/raw/imdb/"

def download_and_extract(url, out_path):
    gz_path = out_path + ".gz"

    print(f"⬇ Downloading {url} ...")
    with requests.get(url, stream=True) as r:
        with open(gz_path, 'wb') as f:
            f.write(r.content)

    with gzip.open(gz_path, 'rb') as f_in:
        with open(out_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    os.remove(gz_path)
    print(f" Extracted to: {out_path}")

if __name__ == "__main__":
    os.makedirs(RAW_DIR, exist_ok=True)
    for key, url in IMDB_URLS.items():
        output_file = os.path.join(RAW_DIR, f"title.{key}.tsv")
        download_and_extract(url, output_file)
