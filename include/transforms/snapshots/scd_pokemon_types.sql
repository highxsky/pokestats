{% snapshot pokemon_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='type_id',
      strategy='check',
      check_cols=['type', 'valid_to_gen'],
    )
}}

SELECT * FROM {{ ref('int_type_history') }}

{% endsnapshot %}
