wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
rm duckdb_cli-linux-amd64.zip
sudo mv duckdb /bin

rm -rf venv
python3.12 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
python scaffold_folders.py
python get_mi_cfr_data.py

cd mi_cfr_duckdb_dbt
dbt run