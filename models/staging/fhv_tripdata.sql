{{ config(materialized='view') }}

with tripdata as 
(
  select * from {{ source('staging','external_fhv_tripdata') }}
  where Affiliated_base_number is not null 
)
select
    -- identifiers
    cast(dispatching_base_num	as string) as dispatching_base_num,
    cast(Affiliated_base_number	as string) as affiliated_base_number,

    -- timestamps
    cast(pickup_datetime	 as timestamp) as pickup_datetime,
    cast(dropoff_datetime	 as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(PULocationID	 as integer) as  pickup_locationid,
    cast(DOLocationID	 as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag,
from tripdata


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
