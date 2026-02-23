/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone where meter was engaged"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone where meter was disengaged"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare in USD"
    primary_key: true
    checks:
      - name: not_null
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Taxi type (yellow, green)"
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type from lookup"

custom_checks:
  - name: row_count_greater_than_zero
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips
    value: 1
@bruin */

-- ingestion.trips from NYC TLC parquet: yellow uses tpep_*, PULocationID, DOLocationID; green uses lpep_*.
WITH raw_trips AS (
    SELECT
        tpep_pickup_datetime::TIMESTAMP AS pickup_datetime,
        tpep_dropoff_datetime::TIMESTAMP AS dropoff_datetime,
        pu_location_id::INTEGER AS pickup_location_id,
        do_location_id::INTEGER AS dropoff_location_id,
        fare_amount,
        taxi_type,
        payment_type
    FROM ingestion.trips
    WHERE tpep_pickup_datetime::TIMESTAMP >= '{{ start_datetime }}'::TIMESTAMP
      AND tpep_pickup_datetime::TIMESTAMP < '{{ end_datetime }}'::TIMESTAMP
      AND fare_amount >= 0
)
SELECT
    t.pickup_datetime,
    t.dropoff_datetime,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.fare_amount,
    t.taxi_type,
    p.payment_type_name
FROM raw_trips t
LEFT JOIN ingestion.payment_lookup p
    ON t.payment_type = p.payment_type_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.pickup_datetime, t.dropoff_datetime,
                 t.pickup_location_id, t.dropoff_location_id, t.fare_amount
    ORDER BY t.pickup_datetime
) = 1
