-- unpivot the rows
with source as (

{{ dbt_utils.unpivot(
  relation= ref("source_pd2021wk12__maldives_tourism"),
  cast_to= 'varchar',
  exclude=['id', 'series_measure', 'hierarchy_breakdown', 'unit_detail']
) }}

),

transformed as (

    select

        id,
        series_measure,
        hierarchy_breakdown,
        to_date(field_name, 'MON_YY') as date,
        value as tourists

    from source
    where unit_detail = 'Tourists' and tourists != 'na'

),

subset as ( --remove rows as required)

    select

        id,
        trim(
            case
                when contains(series_measure, 'United Kingdom')
                then replace(replace(replace(series_measure, 'Tourist arrivals', ''), 'from', ''), 'the', '')
                when contains(series_measure, '_')
                then replace(replace(replace(series_measure, 'Tourist arrivals', ''), 'from', ''), '_', '')
                else replace(replace(series_measure, 'Tourist arrivals', ''), 'from', '')
                end
            ) as area,
        case
            when regexp_count(hierarchy_breakdown, '/') >= 3 then 'country'
            else 'continent'
        end as stream,
        trim(split_part(hierarchy_breakdown, '/', 4)) as region,
        date,
        cast(tourists as integer) as tourists

    from transformed
    where contains(series_measure, 'Total') = 'FALSE'

)

select * from subset

