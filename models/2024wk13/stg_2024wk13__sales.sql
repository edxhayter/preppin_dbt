{% set tables_to_union = [
    ref('source_pd2024wk13__2020'),
    ref('source_pd2024wk13__2021'),
    ref('source_pd2024wk13__2022'),
    ref('source_pd2024wk13__2023'),
    ref('source_pd2024wk13__2024')
] %}

with unioned_sources as (
    
    {{
    dbt_utils.union_relations(
        relations=tables_to_union,
        column_override={}
    )
    }}

),

transformed as (

    select

    to_date(sales_date, 'dd/mm/yyyy') as sale_date,
    product,
    price::numeric as price,
    quantity_sold::int as quantity

    from unioned_sources

)

select * from transformed

