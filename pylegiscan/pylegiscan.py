import base64
from io import BytesIO
import json
import os
from pathlib import Path
import zipfile

import pandas as pd
import requests


NEEDED_BILL_DATA = [
    'bill_id',
    'session',
    'url',
    'state_link',
    'bill_number',
    'bill_type',
    'current_body',
    'title',
    'description',
    'committee'
]

def get_access_key(legiscan_key, start_year):
    url = f"https://api.legiscan.com/?key={legiscan_key}&op=getDatasetList&state=MI"
    response = requests.get(url, stream=True)
    data = response.json()
    try:
        for item in data['datasetlist']:
            if item['year_start'] == start_year:
                return item['access_key'], item['session_id']
        raise Exception("Start year not found")
    except Exception:
        raise Exception("Can't find the access key")


def get_legiscan_json(legiscan_key, access_key, session_id, out_dir):
    url = f"https://api.legiscan.com/?key={legiscan_key}&op=getDataset&access_key={access_key}&id={session_id}"
    response = requests.get(url, stream=True)
    data = response.json()
    if 'dataset' not in data:
        raise Exception("Did not get dataset, check your inputs")
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
            needed_loc = f'{out_dir}/{legislature}/{file_type}'
            Path(needed_loc).mkdir(parents=True, exist_ok=True)
            with zf.open(file) as f:
                out_dict = json.loads(f.read())
                with open(f'{needed_loc}/{filename}', 'w') as g:
                    json.dump(out_dict, g)


def parse_people(people_dir):
    out_data = []
    for file in os.listdir(people_dir):
        with open(f'{people_dir}/{file}') as f:
            out_data.append(json.load(f)['person'])
    return out_data


def parse_bills(bill_dir):
    bill_info = []
    bill_sponsors = []
    bill_history = []
    bill_amendments = []
    for file in os.listdir(bill_dir):
        with open(f'{bill_dir}/{file}') as f:
            bill_data = json.load(f)['bill']
            new_sponsors = bill_data['sponsors']
            for item in new_sponsors:
                item['bill_id'] = bill_data['bill_id']
                item['bill_number'] = bill_data['bill_number']
            bill_sponsors += new_sponsors
            new_bill_history = bill_data['history']
            for item in new_bill_history:
                item['bill_id'] = bill_data['bill_id']
                item['bill_number'] = bill_data['bill_number']
            bill_history += new_bill_history
            new_amendments = bill_data['amendments']
            for item in new_amendments:
                item['bill_id'] = bill_data['bill_id']
                item['bill_number'] = bill_data['bill_number']
            bill_amendments += new_amendments
            out_data = {}
            for key in NEEDED_BILL_DATA:
                if key in bill_data:
                    if type(bill_data[key]) != dict:
                        out_data[key] = bill_data[key]
                    else:
                        for nested_key in bill_data[key]:
                            out_data[f"{key}_{nested_key}"] = bill_data[key][nested_key]
            bill_info += [out_data]
    return bill_info, bill_sponsors, bill_history, bill_amendments


def parse_votes(vote_dir):
    vote_details = []
    vote_results = []
    for file in os.listdir(vote_dir):
        with open(f'{vote_dir}/{file}') as f:
            vote_data = json.load(f)['roll_call']
            new_details = vote_data['votes']
            for detail in new_details:
                detail['bill_id'] = vote_data['bill_id']
                detail['roll_call_id'] = vote_data['roll_call_id']
            vote_details += new_details
            vote_data.pop('votes', None)
            vote_results += [vote_data]
    return vote_results, vote_details


def convert_pylegiscan_json(
    people_dir,
    bill_dir,
    vote_dir,
    out_dir
):
    people_data = parse_people(people_dir)
    people_df = pd.DataFrame(people_data)
    people_df.to_parquet(f"{out_dir}/people.parquet")
    bill_info, bill_sponsors, bill_history, bill_amendments = parse_bills(bill_dir)
    bill_info_df = pd.DataFrame(bill_info)
    bill_info_df.to_parquet(f"{out_dir}/bill_info.parquet")
    bill_sponsors_df = pd.DataFrame(bill_sponsors)
    bill_sponsors_df.to_parquet(f"{out_dir}/bill_sponsors.parquet")
    bill_history_df = pd.DataFrame(bill_history)
    bill_history_df.to_parquet(f"{out_dir}/bill_history.parquet")
    bill_amendment_df = pd.DataFrame(bill_amendments)
    bill_amendment_df.to_parquet(f"{out_dir}/bill_amendments.parquet")
    vote_results, vote_details = parse_votes(vote_dir)
    vote_results_df = pd.DataFrame(vote_results)
    vote_results_df.to_parquet(f"{out_dir}/vote_results.parquet")
    vote_details_df = pd.DataFrame(vote_details)
    vote_details_df.to_parquet(f"{out_dir}/vote_details.parquet")