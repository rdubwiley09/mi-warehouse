import io
import os

import py7zr
import pandas as pd
import requests
from tqdm import tqdm

CONVERT_7Z = False
RAW_DATA_DIR = "./data/raw/mi_cfr/zipped"
EXTRACTED_DATA_DIR = "./data/raw/mi_cfr"


def convert_micfr_data():
    if CONVERT_7Z:
        for file in os.listdir(RAW_DATA_DIR):
            z = py7zr.SevenZipFile(f"{RAW_DATA_DIR}/{file}")
            z.extractall(EXTRACTED_DATA_DIR)
    for file in tqdm(os.listdir(EXTRACTED_DATA_DIR)):
        if file != "zipped" and file != "parquet":
            df = pd.read_csv(f"{EXTRACTED_DATA_DIR}/{file}", encoding='latin-1', on_bad_lines='skip', low_memory=False)
            if "state_loc" in df.columns:
                df['state_loc'] = df['state_loc'].astype("str")
            df['amount'] = df['amount'].astype("str")
            df.to_parquet(f"{EXTRACTED_DATA_DIR}/parquet/{file.replace(".csv",".parquet")}")

        


if __name__ == "__main__":
    convert_micfr_data()

