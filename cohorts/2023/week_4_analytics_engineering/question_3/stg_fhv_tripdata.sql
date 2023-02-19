{{ config(materialized='view') }}

with fhv_tripdata as 
(
  select *
  from {{ source('staging','external_fhv_tripdata') }}
)
select
    -- identifiers
    dispatching_base_num,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    cast(PUlocationID as numeric) as pickup_loc_id,
    cast(DOlocationID as numeric) as dropoff_loc_id,
    SR_Flag,
    Affiliated_base_number
from fhv_tripdata

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
