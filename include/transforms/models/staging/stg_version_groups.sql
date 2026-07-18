-- One row per (version group, version) pair, linking each game version to its
-- version group and the generation it belongs to.

with raw_input as (
  select
    fetch_date,
    cast(payload ->> '$.id' as INT) as version_group_id,
    cast(split_part(payload -> '$.generation' ->> '$.url', '/', -2) as INT) as gen_id,
    payload ->> '$.name' as version_group_name,
    payload -> '$.versions' as versions
  from {{ source('raw', 'version_groups') }}
)

select
  ri.fetch_date,
  ri.version_group_id,
  ri.version_group_name,
  ri.gen_id,
  cast(split_part(v.value ->> '$.url', '/', -2) as INT) as version_id,
  v.value ->> '$.name' as version_name
-- Explode the versions JSON array: one row per version in the group
from raw_input as ri,
  json_each(ri.versions) as v
