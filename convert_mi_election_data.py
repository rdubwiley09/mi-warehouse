import io
import os
import zipfile

import pandas as pd
import requests
from tqdm import tqdm

RAW_DIR = "./data/raw/election_results"
def convert_election_data():
    for file in os.listdir(RAW_DIR):
        df = pd.read_csv(f"{RAW_DIR}/{file}", sep="\t", encoding='latin-1', on_bad_lines='skip', low_memory=False, header=None)
        out_file = file.replace(".txt", ".parquet")
        df.columns = [ str(col) for col in df.columns]
        df.to_parquet(f"{RAW_DIR}/{out_file}")



if __name__ == "__main__":
    convert_election_data()