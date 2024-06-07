from duckdb.typing import DuckDBPyType
from pydantic import BaseModel, Json

class Location(BaseModel):
    city: str
    state: str


#TODO: see if better way to use the model type
def pydantic_example() -> str:
    return Location(city="Detroit", state="Michigan").model_dump_json()