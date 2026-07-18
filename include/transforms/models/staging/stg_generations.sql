{{ config(materialized="view") }}

with parsed as (
  select
    fetch_date,
    from_json(
      payload,
      '{
        "id": "INT",
        "name": "VARCHAR",
        "names": [{"name": "VARCHAR", "language": {"name": "VARCHAR"}}],
      }'
    ) as p
  from {{ source('raw', 'generations') }}
)

select
  fetch_date,
  p.id as gen_id,
  p.name as gen_api_name,
  list_filter(p.names, lambda n: n.language.name = 'en')[-1].name as gen_name
from parsed
