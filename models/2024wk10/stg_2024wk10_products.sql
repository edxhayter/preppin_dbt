with source as (

    select * from {{ ref( "source_pd2024wk10__products") }}

),

transformed as (

    select 

        lower(product_type || '-' || replace(product_scent, ' ', '_') || '-' || case when pack_size is null then product_size else pack_size end) as join_term,
        product_type,
        product_scent,
        case when pack_size is null then product_size else pack_size end as product_size,
        unit_cost,
        selling_price     
    
    from source

)

select * from transformed