import base64
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import zipfile
from zipfile import ZipFile

import requests

legiscan_key = os.environ['LEGISCAN_API_KEY']
TEST_URL = f"https://api.legiscan.com/?key={legiscan_key}&op=getDataset&access_key=1K1zVhV8Epf8hjGNpECuUL&id=2027"

response = requests.get(TEST_URL, stream=True)
data = response.json()
zip_data = data['dataset']['zip']
base64_bytes = zip_data.encode('ascii')
message_bytes = base64.b64decode(base64_bytes)
with zipfile.ZipFile(BytesIO(message_bytes)) as zf:
    files = zf.namelist()
    for file in files:
        split = file.split("/")
        if len(split) != 4:
            continue
        legislature = split[1]
        file_type = split[2]
        filename = split[3]
        needed_loc = f'./data/raw/legiscan/{legislature}/{file_type}'
        Path(needed_loc).mkdir(parents=True, exist_ok=True)
        with zf.open(file) as f:
            out_dict = json.loads(f.read())
            with open(f'{needed_loc}/{filename}', 'w') as g:
                json.dump(out_dict, g)