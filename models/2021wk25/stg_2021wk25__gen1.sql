with source as (

    select * from {{ ref('source_2021W25__Gen_1') }}

),

-- add a recordID
add_id as (

    select

        * ,
        row_number() over (order by (select null)) as record_id
    
    from source

),

-- copy down dex number and name
with_dex_num as (
    
    select
       
        *,
        coalesce(num, lag(num) over (order by record_id asc)) as dex_num,
        coalesce(name, lag(name) over (order by record_id asc)) as poke_name
    
    from add_id

),

transformed as (

    select

        dex_num,
        poke_name,
        listagg(type, '/') within group(order by num) as type,
        max(total) as total,
        max(hp) as hp,
        max(attack) as attack,
        max(defense) as defence,
        max(sp_atk) as sp_atk,
        max(sp_def) as sp_def,
        max(speed) as speed

    from with_dex_num
    group by dex_num, poke_name

)

select * from transformed
