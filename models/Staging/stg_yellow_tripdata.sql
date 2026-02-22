with tripdata as (
  select *
  from {{ source('raw_data', 'yellow_tripdata_1') }}
  where VendorID is not null
),

renamed as (
  select
      -- identifiers
      cast(VendorID as int64) as vendorid,
      cast(RatecodeID as int64) as ratecodeid,
      cast(PULocationID as int64) as pickup_locationid,
      cast(DOLocationID as int64) as dropoff_locationid,

      -- timestamps
      cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
      cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

      -- trip info
      store_and_fwd_flag,
      cast(passenger_count as int64) as passenger_count,
      cast(trip_distance as numeric) as trip_distance,
      1 as trip_type,  --yellow taxis can only be street-hail (trip type=1)

      -- payment info
      cast(fare_amount as numeric) as fare_amount,
      cast(extra as numeric) as extra,
      cast(mta_tax as numeric) as mta_tax,
      cast(tip_amount as numeric) as tip_amount,
      cast(tolls_amount as numeric) as tolls_amount,
      cast(improvement_surcharge as numeric) as improvement_surcharge,
      0 as ehail_fee, -- yellow taxis do not have ehail fees
      cast(total_amount as numeric) as total_amount,
      cast(payment_type as int64) as payment_type,

      -- other (típicas de yellow; si no existen, bórralas)
      cast(congestion_surcharge as numeric) as congestion_surcharge

  from tripdata
)

select * from renamed
