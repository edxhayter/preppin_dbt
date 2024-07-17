-- unpivot the staged model

with unpivot as (

    {{ dbt_utils.unpivot(relation=ref('stg_2019wk28__observations'), cast_to='varchar', exclude=['employee', 'row_num'], field_name = 'header', value_name = 'subheader') }}
    
),

original_headers as(

    select

        row_number() over (order by null) as col_num,
        header,      
    
    from unpivot
    
    where row_num = 1

),

headers as (

    select

        row_num as rn,
        case 
            when length(header) = 1 then null
            else header
        end as header,
        subheader
    
    from unpivot
    
    where row_num = 1

),

-- copy down header value using last_value
new_headers as (
    select

        
        row_number() over (order by null) as col_num,
        case
            when subheader is null then header 
            else coalesce(header, last_value(header) ignore nulls over (order by rn rows between unbounded preceding and current row)) || '_' || coalesce(subheader, '') 
        end as new_header,


    from headers 

),

rename_table as (

    select

        upper(regexp_replace(h.new_header, '[^A-Za-z0-9_]', '_')) as new_header,

        original_headers.header

    from new_headers h
    inner join original_headers on h.col_num = original_headers.col_num

)

select * from rename_table

