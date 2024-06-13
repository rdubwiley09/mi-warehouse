{{ config(materialized='external', location='../data/mart/udf_examples.parquet', format='parquet') }}

SELECT
    return_hello_array() AS hello,
    --One indexed blah
    (return_hello_array())[1] AS hello_indexed,
    return_numpy_one() AS numpy_one,
    get_substring('hello', 3, 4) AS substring,
    pydantic_example() AS pydantic_example_extract,
    json_extract(pydantic_example(), '$.city') AS pydantic_example_extract
