import io
import zipfile

import pandas as pd
import requests
from tqdm import tqdm


BASE_URL = "https://miboecfr.nictusa.com/cfr/presults/{}GEN.zip"

ELECTION_YEARS = [
    2012,
    2014,
    2016,
    2018,
    2020,
    2022
]

def get_urls():
    urls = []
    for year in ELECTION_YEARS:
        urls.append(BASE_URL.format(year))
    return urls


def get_election_data(urls):
    for url in tqdm(urls):
        year = url.split("/")[-1].split("GEN.zip")[0]
        response = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        for file in z.namelist():
            if file != 'readme.txt':
                out_file = f'./data/raw/election_results/{file}'.replace('.txt', '.parquet')
                df = pd.read_csv(z.open(file), sep="\t", encoding='latin-1', on_bad_lines='skip', low_memory=False, header=None)
                df.columns = [ str(col) for col in df.columns ]
                df.to_parquet(out_file)



if __name__ == "__main__":
    urls = get_urls()
    get_election_data(urls)