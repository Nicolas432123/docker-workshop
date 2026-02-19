with unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

payment_types as (
    select * from {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched as (
    select
        -- surrogate key (usa los campos reales del unioned)
        {{ dbt_utils.generate_surrogate_key([
            "cast(u.vendorid as string)",
            "cast(u.pickup_datetime as string)",
            "cast(u.pickup_locationid as string)",
            "cast(u.service_type as string)"
        ]) }} as trip_id,

        -- Renombre de columnas (lo haces aquí)
        u.vendorid            as vendor_id,
        u.service_type        as service_type,
        u.ratecodeid          as rate_code_id,

        u.pickup_locationid   as pickup_location_id,
        u.dropoff_locationid  as dropoff_location_id,

        u.pickup_datetime     as pickup_datetime,
        u.dropoff_datetime    as dropoff_datetime,

        u.store_and_fwd_flag  as store_and_fwd_flag,
        u.passenger_count     as passenger_count,
        u.trip_distance       as trip_distance,
        u.trip_type           as trip_type,

        u.fare_amount         as fare_amount,
        u.extra               as extra,
        u.mta_tax             as mta_tax,
        u.tip_amount          as tip_amount,
        u.tolls_amount        as tolls_amount,
        u.ehail_fee           as ehail_fee,
        u.improvement_surcharge as improvement_surcharge,
        u.total_amount        as total_amount,

        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description

    from unioned u
    left join payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
)

select *
from cleaned_and_enriched

qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1
