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

        evos.*

    from evos
    join gen1 on evos.poke_name = gen1.poke_name

),

-- repivot to identify problem evos

repivot_evo as (

    select

        *

    from filter_evos
        pivot(min(poke_name) for evo_direction in (
            'EVOLVING_FROM',
            'EVOLVING_TO'
        )) 
),

-- transform and filter the bad rows
transformed as (

    select

        record_id,
        "'EVOLVING_FROM'" as EVOLVES_FROM,
        "'EVOLVING_TO'" as EVOLVES_TO
    
    from repivot_evo

)


select * from repivot_evo
