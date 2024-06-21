with source as (

    select * from {{ ref('source_pd2024wk6__input') }}

),

transformed as (

    select
        
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as id,
        staffid as staff_id,
        cast( month_1 as float ) as month_1, -- correct the datatype of month 1
        month_2,
        month_3,
        month_4,
        month_5,
        month_6,
        month_7,
        month_8,
        month_9,
        month_10,
        month_11,
        month_12
    
    from source

)

select * from transformed