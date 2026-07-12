{{ config(materialized="view") }}

WITH parsed AS (
  SELECT
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
    FROM {{ source('raw', 'moves') }}
)

SELECT
    fetch_date,
    p.id as "move_id",
    p."name" as "move_name",
    p.power,
    p.accuracy,
    p.pp,
    p.priority,
    p."type".name as "type",
    p.damage_class.name as "damage_class",
    split_part(p.generation.url, '/', -2)::INT as "poke_gen"
FROM parsed