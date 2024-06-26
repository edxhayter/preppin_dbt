with source as (

    select * from {{ ref('source_pd2024wk14__recalled_items') }}

),

transformed as (

    select *

    from source


)

select * from source