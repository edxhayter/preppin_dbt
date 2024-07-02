-- Import CTEs (all exclusion inputs)

with megas as (

    select * from {{ ref('stg_2021wk25__megas') }}

),

alolan as (

    select * from {{ ref('stg_2021wk25__alolan') }}

),

galarian as (

    select * from {{ ref('stg_2021wk25__galarian') }}

),

gigantamax as (

    select * from {{ ref('stg_2021wk25__gigantamax') }}

),

-- Union the list together and deduplicate a list of pokemon names (so use union rather than union all)

{%- set ctes = ['megas', 'alolan', 'galarian', 'gigantamax'] %}

combined as (

    {% for cte in ctes %}

    select poke_name from {{ cte }}
    
    {% if not loop.last %}
    
    union
    
    {% endif %}
    
    {% endfor %}
),

gimmick_flag as (

    select

        *,
        1 as gimmick_flag

    from combined

)

select * from gimmick_flag
