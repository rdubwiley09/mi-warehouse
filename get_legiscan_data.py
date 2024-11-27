import base64
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import zipfile
from zipfile import ZipFile

import requests 
from pylegiscan.pylegiscan import (
    get_legiscan_json,
    convert_pylegiscan_json
)

GET_LEGISCAN_JSON = True

legiscan_key = os.environ['LEGISCAN_API_KEY']
access_key = "5DWcFIy2tAjgx6kK2hI3GM&id=2027"

if GET_LEGISCAN_JSON:
    get_legiscan_json(legiscan_key, access_key, './data/raw/legiscan')

convert_pylegiscan_json(
    './data/raw/legiscan/2023-2024_102nd_Legislature/people',
    './data/raw/legiscan/2023-2024_102nd_Legislature/bill',
    './data/raw/legiscan/2023-2024_102nd_Legislature/vote',
    './data/raw/legiscan/2023-2024_102nd_Legislature/parsed',
)