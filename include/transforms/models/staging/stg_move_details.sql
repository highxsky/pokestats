-- One row per move, with its core battle attributes (power, accuracy, pp, priority,
-- type, damage class) and the generation that introduced it.

with parsed as (
  select
    fetch_date,
    from_json(
      payload,
      '{
            "id": "INT",
            "name": "VARCHAR",
            "power": "INT",
            "accuracy": "INT",
            "pp": "INT",
            "priority": "INT",
            "type": {"name": "VARCHAR"},
            "damage_class": {"name": "VARCHAR"},
            "generation": {"url": "VARCHAR"}
        }'
    ) as p
  from {{ source('raw', 'moves') }}
)

select
  fetch_date,
  p.id as move_id,
  p.name as move_name,
  p.power,
  p.accuracy,
  p.pp,
  p.priority,
  p.type.name as type_name,
  p.damage_class.name as damage_class,
  split_part(p.generation.url, '/', -2)::INT as gen_id
from parsed
