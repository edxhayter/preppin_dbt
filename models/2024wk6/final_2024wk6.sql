-- materialize output as table

{{ config(materialized='table') }}

-- import cte

with salary as (

    select * from {{ ref("int_2024wk6")}}

),

tax as (

    select 

    staff_id,
    annual_salary,
    case
        when annual_salary <=12570 then '0%'
        when annual_salary <=50270 then '20%'
        when annual_salary <=125140 then '40%'
        else '45%'
    end as max_band,
    case
        when annual_salary > 125140 then (annual_salary - 125140)*0.45
        else 0
    end as tax_45,
    case
        when annual_salary > 50270 and annual_salary <= 125140 then (annual_salary - 50270)*0.4
        when annual_salary > 50270 and annual_salary > 125140 then (125140-50270)*0.4
        else 0
    end as tax_40,
    case
        when annual_salary > 12570 and annual_salary <= 50270 then (annual_salary - 12570)*0.2
        when annual_salary >12570 and annual_salary > 50270 then (50270 - 12570)*0.2
        else 0
    end as tax_20,
    tax_45 + tax_40 + tax_20 as total_tax

    from salary

)

select * from tax