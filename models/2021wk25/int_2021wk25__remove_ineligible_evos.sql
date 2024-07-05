-- Import CTEs

with gen1 as (

    select * from {{ ref('stg_2021wk25__gen1') }}

),

evos as (

    select * from {{ ref('stg_2021wk25__evos') }}

),

-- join evos to gen1 to keep gen 1 records only

filter_evos as (

    select

        evos.*,

        case
            when gen1.poke_name is null then 1
            else 0
        end as flag

    from evos
    left join gen1 on evos.poke_name = gen1.poke_name

),

-- repivot to identify problem evos

repivot_evo as (

    select

        *

    from filter_evos
        pivot(max(poke_name) for evo_direction in (
            'EVOLVING_FROM',
            'EVOLVING_TO'
        )) 
),

max_flag as (

    select

        record_id,
        max(flag) as flag

    from repivot_evo
    group by record_id

),

-- table with same amount of rows but now a flag if a record needs to be filtered.
transformed as (

    select

        repivot_evo.record_id,
        max(repivot_evo."'EVOLVING_FROM'") as EVOLVES_FROM,
        max(repivot_evo."'EVOLVING_TO'") as EVOLVES_TO,

        max_flag.flag
    
    from repivot_evo
    inner join max_flag on max_flag.record_id = repivot_evo.record_id
    group by repivot_evo.record_id, max_flag.flag

)


select * from transformed
