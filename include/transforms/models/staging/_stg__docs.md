{% docs fetch_date %}
timestamp: when the data was fetched from the PokéAPI.
{% enddocs %}

{% docs poke_id %}
int: a Pokémon's unique identifier, sourced from the PokéAPI 'id' field.
{% enddocs %}

{% docs poke_name %}
str: name of a Pokémon, in English.
{% enddocs %}

{% docs poke_gen %}
int: a generation is a grouping of the Pokémon games that separates them based on the Pokémon they include, e.g. gen 1 for Pokémon red/blue.
{% enddocs %}

{% docs type %}
str: each Pokémon has either one type or two types (primary AND secondary), such as normal or normal (primary) / fire (secondary).
{% enddocs %}

{% docs move_id %}
int: integer representation of a pokemon's move.
{% enddocs %}

{% docs move_name %}
str: a Pokémon's move, e.g. "fireblast"
{% enddocs %}

{% docs pp %}
int: power points, number of times a move can be used by a Pokémon, ranging from 5 to 40. Using a move consumes exactly 1 power point.
{% enddocs %}

{% docs height %}
A Pokémon's height, in meters.
{% enddocs %}

{% docs weight %}
A Pokémon's weight, in kilograms.
{% enddocs %}

{% docs color %}
The Pokémon's canonical color.
{% enddocs %}

{% docs genus %}
English classification of a Pokémon, e.g. "Mouse Pokémon".
{% enddocs %}

{% docs habitat %}
The Pokémon's habitat, e.g. forest or cave.
{% enddocs %}

{% docs poke_description %}
English Pokédex flavor text (latest version).
{% enddocs %}

{% docs is_baby %}
Whether the Pokémon is a baby form.
{% enddocs %}

{% docs is_legendary %}
Whether the Pokémon is legendary.
{% enddocs %}

{% docs is_mythical %}
Whether the Pokémon is mythical.
{% enddocs %}

{% docs evolves_from_id %}
ID of the species this Pokémon evolves from (NULL if it is a base form).
{% enddocs %}

{% docs evolves_from_name %}
Name of the species this Pokémon evolves from (NULL if it is a base form).
{% enddocs %}

{% docs gen_name %}
The generation's English display name, e.g. "Generation I".
{% enddocs %}

{% docs accuracy %}
The move's accuracy percentage (NULL for moves that can't miss).
{% enddocs %}

{% docs power %}
The move's base power (NULL for status moves).
{% enddocs %}

{% docs priority %}
Move priority bracket, ranging from -8 to +8 (0 is normal).
{% enddocs %}

{% docs damage_class %}
How a move deals damage: physical, special, or status.
{% enddocs %}

{% docs version_id %}
Identifier of a specific game version.
{% enddocs %}

{% docs version_name %}
Name of a specific game version, e.g. "red".
{% enddocs %}

{% docs version_group_id %}
Identifier of a version group, a set of games sharing the same mechanics (e.g. red-blue).
{% enddocs %}

{% docs version_group_name %}
Name of a version group, e.g. "red-blue".
{% enddocs %}
