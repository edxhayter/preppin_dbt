-- Import CTEs

with known as (

    select * from {{ ref("int_2021wk12__known_countries") }}

),

unknown as (

    select * from {{ ref("int_2021wk12__unknown_countries") }}

),

-- Union the two tables

final as (

    select * from known
    union all
    select * from unknown

)

select * from final
