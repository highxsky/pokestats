{% docs type_id %}
int: integer representation of a pokemon's type.
{% enddocs %}

{% docs slot %}
int: slot is related to a Pokémon's type: 1 for primary, 2 for secondary.
{% enddocs %}

{% docs hp %}
int: a Pokémon's health points, can range from 1 to 255, although most pokemons have stats between 30 and 120.
{% enddocs %}

{% docs attack %}
int: a Pokémon's attack stats, can range from 1 to 255, although most pokemons have stats between 30 and 120.
{% enddocs %}

{% docs defense %}
int: a Pokémon's defense stats, can range from 1 to 255, although most pokemons have stats between 30 and 120.
{% enddocs %}

{% docs special_attack %}
int: a Pokémon's special attack stats, can range from 1 to 255, although most pokemons have stats between 30 and 120.
{% enddocs %}

{% docs special_defense %}
int: a Pokémon's special defense stats, can range from 1 to 255, although most pokemons have stats between 30 and 120.
{% enddocs %}

{% docs speed %}
int: a Pokémon's speed stats, can range from 1 to 255, although most pokemons have stats between 30 and 120.
{% enddocs %}

{% docs stat_name %}
str: a Pokémon's stat name, e.g. attack or speed.
{% enddocs %}

{% docs stat_value %}
int: the value of a Pokémon's stat, e.g. 85.
{% enddocs %}

{% docs pokemon_move_id %}
Surrogate key identifying a Pokémon's move, built from (poke_id, move_id).
{% enddocs %}

{% docs valid_from_gen %}
The generation from which a type assignment became active.
{% enddocs %}

{% docs valid_to_gen %}
The last generation a type assignment was active (NULL if still current).
{% enddocs %}
