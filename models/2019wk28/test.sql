
with data as (

    select * from {{ ref('int_2019wk28__data') }}

),

header_mapping as (

    select * from {{ ref('int_2019wk28__header') }}

),

test_macro as (
        
        select

            {{ dynamic_rename('int_2019wk28__header','int_2019wk28__data') }}

        from data

)

select * from test_macro
