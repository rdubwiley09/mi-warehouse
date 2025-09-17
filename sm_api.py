from enum import Enum
from typing import List, Optional
from typing_extensions import Annotated

import typer


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


def main(
    dimensions: Optional[List[Dimensions]]=[], 
    measures: Optional[List[Measures]]=[], 
    filters: Optional[DistrictCodeDescriptions]=None
):
    typer.echo(dimensions)
    typer.echo(measures)
    typer.echo(filters)


if __name__ == "__main__":
    typer.run(main)