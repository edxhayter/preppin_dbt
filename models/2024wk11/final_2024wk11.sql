-- Import CTE

with dates as (

    select * from {{ ref("stg_2024wk11") }}

),

-- Flag when a new month should begin

new_date_calcs as (
    select
        id,
        date_day,
        floor((row_number() over (order by id) - 1) / 28 + 1) as new_month,
        mod(id-1, 28)+1 as new_day
    from dates

),

final as (

    select

        to_char(date_day, 'dd/mm/yyyy') as date,
        lpad(to_char(new_day), 2, '0') || '/' || lpad(to_char(new_month), 2, '0') || '/' || '2024' as new_date
        
    from new_date_calcs

    where month(date_day) != new_month

)

select * from final