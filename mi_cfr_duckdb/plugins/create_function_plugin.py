from duckdb import DuckDBPyConnection
from duckdb.typing import DuckDBPyType

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import TargetConfig

from functions.return_hello_array import return_hello_array
from functions.numpy_dummy import return_numpy_one
from functions.pydantic_example import pydantic_example
from functions.string_index import get_substring

class Plugin(BasePlugin):
    def configure_connection(self, conn: DuckDBPyConnection):
        conn.create_function("return_hello_array", return_hello_array)
        conn.create_function("get_substring", get_substring)
        conn.create_function("return_numpy_one", return_numpy_one)
        conn.create_function("pydantic_example", pydantic_example)


    def store(self, target_config: TargetConfig):
        assert target_config.config.get("key") == "value"