1) SELECT COUNT(*) 
FROM zoomcamp.external_yellow_taxi;

2) I checkeh the external table an the materializde one. I had 0 bytes and 155.12 respectively


3)BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

4) SELECT COUNT(*)
FROM `wide-graph-486209-v8.zoomcamp.yellow_taxi_regular`
WHERE fare_amount = 0;

5) Partition by tpep_dropoff_datetime and Cluster on VendorID

6) SELECT DISTINCT vendor_id
FROM `wide-graph-486209-v8.zoomcamp.yellow_taxi_regular`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT vendor_id
FROM `wide-graph-486209-v8.zoomcamp.yellow_taxi_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

7)GCP Bucket

8) False , Recommended only if:

There are repetitive filters on a column.

Medium/high cardinality

Sufficiently large table (GBs)

❌ Not recommended if:

Table of female students

Columns that are rarely used

Highly variable queries.

9) BigQuery:

Does NOT read data row by row

Uses internal table metadata

Knows how many rows there are without scanning columns

👉 Result:

0 bytes processed
