# Preppin Data 2021 Week 12: Maldives Tourism

## Inputs

The input for this challenge was a single file but the data was very wide (136 Columns with only 28 Rows). There are a range of tourist metrics and then a column for each value for a given date making the data human readable but not suitable for analysis or storage really.
<br>

## Staging

This challenge required a large unpivot to start off with. I used the opportunity to try the dbt.utils unpivot macro that is part of the dbt utils package. Using the package macro here is sensible to avoid duplicating any uneccessary code.

The exclude arguments provided are the columns that we would like to keep as columns.

```sql
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
```

The string functions that process the unpivoted data could certainly be optimized but the fundamental exercise here was to clean up the tourist origin field. THe output leaves us with a dataset for number of tourists ready to be split into two models taht can be processed seperately (Country level information and unknown countries that will be calculated from the continent level info).

## Intermediate Stage

The known countries was a simple case of renaming columns and ensuring correct datatypes. The unknown country information required additional wrangling that was fairly simple, aggregating to the same level of granularity to allow for a join that facilitates a row-level calculation to work out the unknown tourists (Continent Total - Known Country Total = Unknown Country Total).
<br>

```sql
-- import CTE

with country_info as (

    select * from {{ ref("stg_2021wk12__tourism") }}
    where stream = 'country'

),

region_info as (

    select * from {{ ref("stg_2021wk12__tourism") }}
    where stream = 'continent'

),

-- logical CTEs
-- Aggregate Country up to the area level
country_to_region as (

    select

        region,
        date,
        sum(tourists) as known_tourists

    from country_info
    group by region, date

),

-- Join the known region info with the total region info

unknown_region as (

    select

        region_info.area,
        region_info.date as date,
        region_info.tourists as total_tourists,

        country_to_region.known_tourists as known_tourists

    from region_info
    inner join country_to_region on region_info.area = country_to_region.region and region_info.date = country_to_region.date

),

-- make rows for the unknown regional

unknown_country as (

    select

        date as month,
        area as breakdown,
        'Unknown' as country,
        (total_tourists - zeroifnull(known_tourists)) as tourists

    from unknown_region

)

select * from unknown_country
```

In this sense the intermediate models make sense so that if we want to do an analysis only for the known countries we have that model in our dbt project to build off further.

## Final Reporting

The final stage was simply unioning the two interrmediate models into the table that is ready for analysis in the BI Layer of a data pipeline.
<br>

```
-- Import CTEs

with known as (

    select * from {{ ref("int_2021wk12__known_countries") }}

),

unknown as (

    select * from {{ ref("int_2021wk12__unknown_countries") }}

),

-- Union the two tables

final as (

    select * from known
    union all
    select * from unknown

)

select * from final
```

## Reflections and Learning

This challenge was failry simple, although in the past there have been challenges around large pivots/unpivots in Snowflake around the configuration particularly with varying data-types. The use of a dbt package in this instance to circumvent potential challenges and reduce the codebase required to produce such a report is a good example of the benefits of dbt packages.
