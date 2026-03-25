POKEMON_GENERATIONS = """
    SELECT 
        * 
    FROM dbt_dev_marts.mart_generations
"""

POKEMON_VERSIONS = """
    SELECT
        vg.poke_gen,
        vgv.version_name
    FROM dbt_dev_marts.mart_version_group_versions vgv
    INNER JOIN dbt_dev_marts.mart_version_groups vg
        ON vg.version_group_id = vgv.version_group_id
"""

POKEMON_LIST = """
    SELECT
        p.poke_id,
        p.poke_name,
        p.poke_gen,
        p.height,
        p.weight,
        s.hp,
        s.attack,
        s.defense,
        s.special_attack,
        s.special_defense,
        s.speed,
        s.total_stat_points,
        sp.rank,
        sp.tier
    FROM dbt_dev_marts.mart_pokemons p
    INNER JOIN dbt_dev_marts.mart_stats s
        ON p.poke_id = s.poke_id
    LEFT JOIN dbt_dev_marts.mart_strongest_pokemons sp
        ON p.poke_id = sp.poke_id
    ORDER BY p.poke_id
"""

POKEMON_TYPES = """
    SELECT
        poke_id,
        slot,
        type
    FROM dbt_dev_marts.mart_types
    ORDER BY poke_id, slot
"""

POKEMON_STARTERS = """
    SELECT
        poke_id
    FROM dbt_dev_marts.mart_strongest_starters
"""

POKEMON_LEGENDARIES = """
    SELECT
        poke_id
    FROM dbt_dev.legendary_pokemons
    WHERE is_legendary = true
"""

POKEMON_SPECIES = """
    SELECT
        poke_id,
        description,
        genus,
        is_legendary,
        is_mythical,
        is_baby,
        color,
        habitat,
        evolves_from_name,
        evolves_from_id
    FROM dbt_dev_marts.mart_pokemon_species
"""

POKEMON_MOVES = """
    SELECT
        pm.poke_id,
        m.move_name,
        m.type,
        m.power,
        m.accuracy,
        m.pp,
        m.damage_class
    FROM dbt_dev_marts.mart_pokemon_moves pm
    INNER JOIN dbt_dev_marts.mart_moves m
        ON pm.move_id = m.move_id
"""