from config import SCHEMA_BASE, SCHEMA_MARTS

POKEMON_GENERATIONS = f"""
    SELECT
        *
    FROM {SCHEMA_MARTS}.mart_generations
"""

POKEMON_LIST = f"""
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
    FROM {SCHEMA_MARTS}.mart_pokemons p
    INNER JOIN {SCHEMA_MARTS}.mart_stats s
        ON p.poke_id = s.poke_id
    LEFT JOIN {SCHEMA_MARTS}.mart_strongest_pokemons sp
        ON p.poke_id = sp.poke_id
    ORDER BY p.poke_id
"""

POKEMON_TYPES = f"""
    SELECT
        poke_id,
        slot,
        type
    FROM {SCHEMA_MARTS}.mart_types
    ORDER BY poke_id, slot
"""

POKEMON_STARTERS = f"""
    SELECT
        poke_id
    FROM {SCHEMA_MARTS}.mart_strongest_starters
"""

POKEMON_LEGENDARIES = f"""
    SELECT
        poke_id
    FROM {SCHEMA_BASE}.legendary_pokemons
    WHERE is_legendary = true
"""

POKEMON_SPECIES = f"""
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
    FROM {SCHEMA_MARTS}.mart_pokemon_species
"""

POKEMON_MOVES = f"""
    SELECT
        pm.poke_id,
        m.move_name,
        m.type,
        m.power,
        m.accuracy,
        m.pp,
        m.damage_class
    FROM {SCHEMA_MARTS}.mart_pokemon_moves pm
    INNER JOIN {SCHEMA_MARTS}.mart_moves m
        ON pm.move_id = m.move_id
"""
