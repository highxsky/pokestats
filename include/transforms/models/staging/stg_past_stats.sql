{{ config(materialized="view") }}

-- fetching only the required data from past stats, for the next layer
-- filtering out rows / pokemons with no data

with parsed as (
    SELECT
        fetch_date,
        id as poke_id,
        from_json(
            payload,
            '{
                "past_stats": [{
                    "generation": {"url": "VARCHAR"},
                    "stats": [{
                        "base_stat": "INT", 
                        "stat": {"name": "VARCHAR"}
                    }]
                }]
            }'
        ).past_stats as past_stats
    FROM {{ source('raw', 'pokemons') }}
)

select
    fetch_date,
    poke_id,
    past_stats
from parsed
where len(past_stats) <> 0