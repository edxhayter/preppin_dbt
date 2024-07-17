-- depends_on: {{ ref('int_2019wk28__header') }}


with source as (

    select * from {{ ref('int_2019wk28__data') }}

),

test_macro as (
        
        select

            {{ dynamic_rename('int_2019wk28__header','int_2019wk28__data') }}

        from source

)

select * from test_macro
