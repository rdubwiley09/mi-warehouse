from enum import Enum
from typing import List, Optional
from typing_extensions import Annotated

import pandas as pd
import typer

from semantic_layer.models import mi_election_results_sm


class Dimensions(str, Enum):
    election_year = 'election_year'
    office_code_description = 'office_code_description'
    district = 'district'
    district_targeting = 'district_targeting'
    winning_party = 'winning_party'
    winning_first_name = 'winning_first_name'
    winning_last_name = 'winning_last_name'


class Measures(str, Enum):
    two_way_votes = 'two_way_votes'
    democratic_votes = 'democratic_votes'
    republican_votes = 'republican_votes'
    two_way_dem_percent = 'two_way_dem_percent'


class DistrictCodeDescriptions(str, Enum):
    UsCongress = 'US Congress'
    StateRepresentative = 'State Representative'
    AG = 'AG'
    President = 'President'
    UsSenator = 'US Senator'
    StateSenator = 'State Senator'
    Governor = 'Governor'
    SOS = 'SOS'

class ElectionYear(str, Enum):
    year_2022 = '2022'
    year_2024 = '2024'


def main(
    dimensions: Optional[List[Dimensions]]=[], 
    measures: Optional[List[Measures]]=[], 
    election_year_filters: Optional[List[ElectionYear]]=None, 
    district_code_filters: Optional[List[DistrictCodeDescriptions]]=None
):
    filters = []
    if election_year_filters:
        filters += [ lambda t: t.election_year == y.value for y in election_year_filters ]
    if district_code_filters:
        filters += [{
            'operator': 'AND',
            'conditions': [
                {'field': 'office_code_description', 'operator': 'in', 'values': [ d.value for d in district_code_filters ]}
            ]
        }]
    query = mi_election_results_sm.query(
        dimensions = dimensions,
        measures = measures,
        filters = filters,
        limit=5
    ).to_ibis().to_pandas_batches()
    result = pd.concat([ item for item in query])
    typer.echo(result)
    

if __name__ == "__main__":
    typer.run(main)