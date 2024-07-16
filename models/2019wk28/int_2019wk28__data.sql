with source as (

    select * from {{ ref('stg_2019wk28__observations') }}

),

-- skip the nested header row
transformed as (

    select

        *

    from source
    where row_num > 1

)

select * from transformed