1) When running:

dbt run --select int_trips_unioned


dbt builds the selected model and all of its upstream dependencies.

Since int_trips_unioned depends on:

stg_green_tripdata

stg_yellow_tripdata

dbt automatically builds those staging models first, and then builds int_trips_unioned.

This demonstrates how dbt resolves model lineage and dependency graphs automatically.

2 ) A generic accepted_values test was configured for payment_type allowing only values [1,2,3,4,5].

When a new value (6) appears in the data and the test is executed:

dbt test --select fct_trips


dbt fails the test because the new value violates the constraint.

dbt returns a non-zero exit code, which is important for CI/CD pipelines, ensuring data quality enforcement.



3) After building the project, the fct_monthly_zone_revenue model contains 14,120 records.

This count reflects the aggregation of monthly revenue grouped by pickup zone and time period.

The result confirms the model was successfully materialized and aggregated correctly.

4) Using fct_monthly_zone_revenue, filtering:

Taxi type = Green

Year = 2020

And ordering by total revenue descending, the zone with the highest revenue is:

East Harlem North

This confirms the fact model supports business-level performance analysis.

5) Using fct_monthly_zone_revenue, filtering:

Taxi type = Green

Year = 2019

Month = October

The total number of trips (total_monthly_trips) is:

421,509

This validates the aggregation logic for trip counts.

6)A staging model stg_fhv_tripdata was created with:

Filtering out rows where dispatching_base_num IS NULL

Renaming columns to follow project naming conventions

After applying the filter, the final row count is:

43,244,693

This shows correct filtering and data standardization at the staging layer.
