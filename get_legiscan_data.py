import base64
from io import BytesIO, StringIO
import json
import os
from pathlib import Path
import zipfile
from zipfile import ZipFile

import requests 
from pylegiscan.pylegiscan import (
    get_access_key,
    get_legiscan_json,
    convert_pylegiscan_json
)

GET_LEGISCAN_JSON = True
LEGISLATURE_TITLE = "2025-2026_103rd_Legislature"
START_YEAR = 2025

legiscan_key = os.environ['LEGISCAN_API_KEY']

if GET_LEGISCAN_JSON:
    access_key, session_id = get_access_key(legiscan_key, START_YEAR)
    get_legiscan_json(legiscan_key, access_key, session_id, './data/raw/legiscan')

convert_pylegiscan_json(
    f'./data/raw/legiscan/{LEGISLATURE_TITLE}/people',
    f'./data/raw/legiscan/{LEGISLATURE_TITLE}/bill',
    f'./data/raw/legiscan/{LEGISLATURE_TITLE}/vote',
    f'./data/raw/legiscan/{LEGISLATURE_TITLE}/parsed',
)