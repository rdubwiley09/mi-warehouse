## To install duckdb on windows
    winget install DuckDB.cli

## to install duckdb on mac
    brew install duckdb

## If you want the linux binary:
    wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip
    unzip duckdb_cli-linux-amd64.zip
    sudo mv duckdb /bin

## You can then verify it's installed by running
    ./duckdb


pip install wheel setuptools pip --upgrade

dbt docs generate
dbt docs serve --port 8085