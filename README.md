## Simple Install Setup
    bash install.sh

#### You may need to change the name of your python binary:
    python3.12 -m venv venv

#### for example:
    python -m venv venv

## Installation Overview
1. Install DuckDB
2. Create Python virtual environment
3. Install necessary python packages
4. Run scripts
5. Run DBT

### 1. Install DuckDB

#### To install duckdb on windows
    winget install DuckDB.cli

#### to install duckdb on mac
    brew install duckdb

#### If you want the linux binary:
    wget https://github.com/duckdb/duckdb/releases/download/v1.0.0/duckdb_cli-linux-amd64.zip
    unzip duckdb_cli-linux-amd64.zip
    sudo mv duckdb /bin

#### You can then verify it's installed by running
    ./duckdb

### 2. Create Python virtual environment

#### Need to call the venv module and a place to store the virtual environment
    python -m venv venv

#### Note that you may need to install the venv package for your version using apt
    sudo apt install python3.12-venv

#### When we have our virtual environment we then need to source it
    source venv/bin/activate

### 3. Install necessary python packages

#### This is as simple as:
    python -m pip install -r requirements.txt

### 4. Run Scripts

#### We need to scaffold some folders for our data project so we run:
    python scaffold_folders.py

#### You can alter the NEEDED_FOLDERS variable to add folders as neccesary:
    NEEDED_FOLDERS = [
        './data/mart',
        './data/ml_models',
        './data/ml',
        './data/raw',
        './data/staging',
        ...
    ]

#### We then need to get the Michigan Campaign Finance Reports data with:
    python get_mi_cfr_data.py

#### like the scaffold_folders script we can change the years we want to pull down:
    NEEDED_YEARS = [
        2022,
        2023,
        2024,
        ...
    ]

### 5. Run dbt
#### This one is also pretty simple
    cd mi_cfr_duckdb_dbt
    dbt run

## Overview of dbt

### Models

#### We want to run these in the dbt folder so:
    cd mi_cfr_duckdb_dbt

#### The models folder looks something like this
    > models
        > mart
        > ml
        > staging

#### Staging
This is the folder where we interact with the raw sources. Generally this is where we rename columns to nicer names and do the base level of data munging.

#### Mart
This is the folder for our main output of our transformations.

#### ML
This is a special folder where I give a proof of concept of running python ML models directlyl in dbt.

One can write sql files in these folders and dbt will run the sql.
Dbt also uses jinja templating to enhance sql functionality.
By default it stores these using the filename (among other configurations) as a view.
This project is running duckdb in memory so we don't create any data objects without specifying an external materialization
#### You can see that config at the top of most files such as:
    {{ config(materialized='external', location='../data/mart/state_party_contributions.parquet', format='parquet') }}

By default the materialized config is 'view'
Because we are using duckdb as the engine these files will materialize as external parquet files

#### Using models in other models:
    FROM {{ ref('stg_expenditures') }} 
This uses the templating and sql setup and you can query it just like a table or a view.

#### Some special duckdb stuff:
    read_parquet('../data/raw/*_mi_cfr_receipts.parquet', union_by_name=True)
This is very different than traditional SQL! Duckdb can work with CSV, parquet, and even JSON files. One can even setup duckdb to query remote files or other databases with some additional setup.

#### We can create custom python functions and run them:
    classify_expenditure(list_concat_expenditure, 0.75)
This requires additional setup in the plugins folder, but very useful if you have a python library that you want directly implemented in your models.

### Tests
Very similar to models, but they fail if they return a result

#### You can run them with the command:
    dbt test

### Plugins
This is a folder that does not come standard with dbt, but is additional functionality that allows us to run arbitrary python functions in our dbt models.

#### New functions are to be implemented in the functions folder and the create_function_plugin.py needs to be updated:
    class Plugin(BasePlugin):
        def configure_connection(self, conn: DuckDBPyConnection):
            conn.create_function("return_hello_array", return_hello_array)
            conn.create_function("get_substring", get_substring)
            conn.create_function("return_numpy_one", return_numpy_one)
            conn.create_function("pydantic_example", pydantic_example)
            conn.create_function("classify_expenditure", classify_expenditure)
            ....

#### You will need to import your function and write an additional:
    conn.create_function("function", function)
within the configure_connection function.

### Macros
#### Find yourself writing a lot of boilerplate or want to functionalize your SQL? Macros are that idea in dbt:
    {% macro choose_nonnull_value(first_column_name, second_column_name, out_column_name) %}
        CASE WHEN {{ first_column_name }} IS NULL THEN {{ second_column_name }} ELSE {{ first_column_name }} END AS {{ out_column_name }}
    {% endmacro %}
If you'd like to learn more I recommend the dbt guide [here](https://docs.getdbt.com/docs/build/jinja-macros)

### Docs
#### We can generate the docs to see our data model graph and can serve it with:
    dbt docs generate
    dbt docs serve --port <port number>

## Notebooks

### party_caucus_contributions
This gives a workflow for the state caucus committee funds raised using the external materializations from our dbt runs.

### zero_shot_expenditure_classification
This generates a pickle file to allow us to do the ML POC. This requires you to install the ml_requirements in your python environment and is much more computationally expensive.

### using_the_dbt_python_package
This gives an alternative way to implement python processes in the middle of your modeling by using the dbt cli to automate runs of subsets of models.

### more_on_duckdb_python_api
This gives more explanation of calling python functions within duckdb. Super useful to hack python or API calls into the middle of your dbt run.

## Further reading
#### dbt docs: [here](https://docs.getdbt.com/docs/introduction)
#### duckdb docs: [here](https://duckdb.org/docs/index)
#### dbt-duckdb github repo: [here](https://github.com/duckdb/dbt-duckdb)