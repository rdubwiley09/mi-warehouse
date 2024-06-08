import io
import zipfile

import pandas as pd
import requests
from tqdm import tqdm

DATA_DIR = "./data/raw"

NEEDED_YEARS = [
    2022,
    2023,
    2024
]

NEEDED_DATA = [
    'contributions',
    'expenditures',
    'receipts'
]


def get_urls():
    urls = []
    for year in NEEDED_YEARS:
        for data in NEEDED_DATA:
            if data == 'contributions':
                #Usually no more than four breakouts
                needed_urls = []
                for i in range(5):
                    #SoS splits these into separate files
                    attempted_url = f"https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/{year}_mi_cfr_{data}_0{i}.zip"
                    response = requests.get(attempted_url)
                    if response.status_code == 200:
                        needed_urls.append(attempted_url)
                    else:
                        if i == 0:
                            attempted_url = f"https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/{year}_mi_cfr_{data}.zip"
                            response = requests.get(attempted_url)
                            if response.status_code == 200:
                                needed_urls.append(attempted_url)
                        else:
                            break 
                if needed_urls:
                    urls.append(needed_urls)    
            else:
                urls.append(f"https://miboecfr.nictusa.com/cfr/dumpall/cfrdetail/{year}_mi_cfr_{data}.zip")
    return urls


def get_dataframe_from_url(url):
    response = requests.get(url)
    z = zipfile.ZipFile(io.BytesIO(response.content))
    filename = z.namelist()[0]
    #MI SoS uses windows encodings and sometimes has more data than neccessary
    #When we have a bad line skip it
    df = pd.read_csv(z.open(filename), sep="\t", encoding='latin-1', on_bad_lines='skip', low_memory=False)
    return filename, df


def get_multipart_dataframe_from_urls(urls):
    text_data = b""
    out_filename = None
    for url in urls:
        response = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        filename = z.namelist()[0]
        if not out_filename:
            out_filename = filename.replace("_00", "")
        with z.open(filename, 'r') as f:
            text_data += f.read()
    df = pd.read_csv(io.BytesIO(text_data), sep="\t", encoding='latin-1', on_bad_lines='skip', low_memory=False)
    return out_filename, df


def download_data(data_dir, urls):
    for url in tqdm(urls):
        try:
            #SoS is silly about these multi part files so we need a way to deal with the loop
            if type(url) == list:
                filename, df =get_multipart_dataframe_from_urls(url)
            else:
                filename, df = get_dataframe_from_url(url)
            out_file = f"{data_dir}/{filename.replace(".txt", ".parquet")}"
            #Output to parquet which is more performant
            df.to_parquet(out_file)
        except Exception as e:
            print(f"Failed for {url} with error {e}")


if __name__ == "__main__":
    urls = get_urls()
    download_data(DATA_DIR, urls)

