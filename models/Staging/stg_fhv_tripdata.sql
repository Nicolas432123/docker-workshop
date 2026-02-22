{{ config(materialized='view') }}

with source as (
  select *
  from {{ source('raw_data', 'fhv_tripdata') }}
),

renamed as (
  select
    -- identifiers (FHV no tiene VendorID como taxi)
    cast(dispatching_base_num as string) as dispatching_base_num,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- locations
    cast(pulocationid as int64) as pickup_location_id,
    cast(dolocationid as int64) as dropoff_location_id,

    -- optional fields (dependen del año/archivo)
    cast(sr_flag as int64) as sr_flag,
    cast(affiliated_base_number as string) as affiliated_base_number

  from source
)

select *
from renamed
