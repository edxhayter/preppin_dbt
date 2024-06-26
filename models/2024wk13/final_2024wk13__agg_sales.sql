with raw_sales as (

    select * from {{ ref('int_2024wk13__easter_sales')}}

),

final as (

    select

        year(sale_date) as year,
        sale_date as sales_date,
        week_number as easter_week_number,
        day as weekday,
        day_order as weekday_order,
        product,
        sum(price) as price,
        sum(quantity) as quantity_sold,
        sum(price + quantity) as sales

    from raw_sales
    group by year, sales_date, easter_week_number, weekday, weekday_order, product

)

select * from final