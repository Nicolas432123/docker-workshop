-- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names
with trips_unioned as (
    select * from {{ ref('int_trips_unioned') }}
),

vendors as (
    select
        cast(vendorid as int64) as vendor_id,
        {{ get_vendor_name('vendorid') }} as vendor_name
    from trips_unioned
    where vendorid is not null
    qualify row_number() over (
        partition by cast(vendorid as int64)
        order by cast(vendorid as int64)
    ) = 1
)

select *
from vendors
