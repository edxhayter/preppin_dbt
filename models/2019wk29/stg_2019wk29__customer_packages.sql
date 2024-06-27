with source as (

    select * from {{ ref('source_pd2019wk29__customers') }}

),

-- split the package value into multiple rows within query using a lateral join to a flattened version of the file that splits based on the | delimeter.

transformed as (

    select

        name,
        package.value::int as package,
        frequency

    from source,
    lateral flatten(split(packages, '|')) package

)

select * from transformed