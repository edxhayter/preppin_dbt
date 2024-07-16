with source as (

    select 

        *

    from {{ ref('source_pd_2019wk28__observations') }}

),

-- add a source row number

transformed as (

 select 

        *,
        row_number() over (order by null) as row_num

from source

)

select * from transformed
