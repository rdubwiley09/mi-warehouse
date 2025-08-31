import ibis
from boring_semantic_layer import SemanticModel

con = ibis.duckdb.connect(":memory:")
mi_election_results_table = con.read_parquet('data/mart/mi_elections/election_results_by_district.parquet')

# Define the semantic model
mi_election_results_sm = SemanticModel(
    name="mi_election_results",
    table=mi_election_results_table,
    dimensions={
        'election_year': lambda t: t.election_year,
        'office_code_description': lambda t: t.office_code_description,
        'district': lambda t: t.district,
        'district_targeting': lambda t: t.district_targeting,
        'winning_party': lambda t: t.winning_party,
        'winning_first_name': lambda t: t.winning_first_name,
        'winning_last_name': lambda t: t.winning_last_name
    },
    measures={
        'two_way_votes': lambda t: t.two_way_votes.sum(),
        'democratic_votes': lambda t: t.democratic_votes.sum(),
        'republican_votes': lambda t: t.republican_votes.sum(),
        'two_way_dem_percent': lambda t: t.democratic_votes.sum()/t.two_way_votes.sum(),
    }
)