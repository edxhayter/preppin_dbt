-- import cte

with month_salary as (

    select * from {{ ref('stg_2024wk6') }}

),

-- unpivot the salary

unpivot_salary as (

    select

    id,
    staff_id,
    name,
    value

    from month_salary
    unpivot(
        value for name in (month_1,month_2,month_3,month_4,month_5,month_6,month_7,month_8,month_9,month_10,month_11,month_12)
    )
),

-- identify the most recent record for each employee

recent_record as (

    select 

    max(id) over (partition by staff_id) as recent_flag

    from unpivot_salary

),

-- return the final clean annual salary table

annual_salary as (

    select

    unpivot.id,
    unpivot.staff_id,
    sum(unpivot.value) as annual_salary

    from unpivot_salary as unpivot
    join recent_record on unpivot.id = recent_record.recent_flag
    group by id, staff_id
)

select * from annual_salary