from duckdb.typing import DuckDBPyType
from typing import List

def return_hello_array() -> DuckDBPyType(list[str]):
    return ["hello", "world"]