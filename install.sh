wget https://github.com/duckdb/duckdb/releases/download/v1.2.0/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
rm duckdb_cli-linux-amd64.zip
sudo mv duckdb /bin

curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv

uv run --directory ./mi_warehouse_dbt dbt run