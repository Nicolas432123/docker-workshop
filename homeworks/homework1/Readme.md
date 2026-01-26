1) docker run -it --rm --entrypoint bash python:3.13
    docker run -it --rm --entrypoint bash python:3.13
    python --version


2) localhost:5432 because the container to ingest data its outside thedocker network but its local

3) SELECT COUNT(*) 
FROM green_table
WHERE trip_distance <= 1
  AND lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01';

4) SELECT
    lpep_pickup_datetime::date AS day,
    MAX(trip_distance) AS max_distance
FROM green_table
WHERE trip_distance < 100
GROUP BY lpep_pickup_datetime::date
ORDER BY max_distance DESC
LIMIT 1;

5) SELECT z."Zone", x.total_sum
FROM (
  SELECT
    "PULocationID",
    SUM(total_amount) AS total_sum
  FROM green_table
  WHERE lpep_pickup_datetime >= '2025-11-18'
    AND lpep_pickup_datetime <  '2025-11-19'
  GROUP BY "PULocationID"
  ORDER BY total_sum DESC
  LIMIT 1
) x
JOIN taxi_zone_lookup z
  ON z."LocationID" = x."PULocationID";

6)SELECT z2."Zone", x.max_tip
FROM (
  SELECT
    "DOLocationID",
    MAX(tip_amount) AS max_tip
  FROM green_table
  WHERE lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime <  '2025-12-01'
    AND "PULocationID" = (
      SELECT "LocationID"
      FROM taxi_zone_lookup
      WHERE "Zone" = 'East Harlem North'
      LIMIT 1
    )
  GROUP BY "DOLocationID"
  ORDER BY max_tip DESC
  LIMIT 1
) x
JOIN taxi_zone_lookup z2
  ON z2."LocationID" = x."DOLocationID";


7) terraform init
terraform apply -auto-approve
terraform destroy

