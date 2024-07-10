-- import CTEs

with pool as (

    select * from {{ ref('int_2021wk27__odds_pool') }}

),

-- logic for making the first pick

pick_1 as (

    select *
    from pool
    where pick = 'PICK_1'
    order by sort desc
    limit 1

)

{% set ctes = ["pick_2", "pick_3", "pick_4"] %}

{% for cte in ctes %}

, {{ cte }} as (

    select *
    from pool
    where pick = '{{ cte | upper }}'
    and seed not in (select seed from pick_1)
    {% if loop.index > 0 %}
    {% for prev_cte in ctes[:loop.index0] %}
        and seed not in (select seed from {{ prev_cte }})
    {% endfor %}
    {% endif %}
    order by sort desc
    limit 1

) 

{% endfor %},

combined as (

    select * from pick_1
    union
    select * from pick_2
    union
    select * from pick_3
    union 
    select * from pick_4

),

final as (

    select

        seed,
        cast(right(pick, 1) as integer) as pick

    from combined

)

select * from final

