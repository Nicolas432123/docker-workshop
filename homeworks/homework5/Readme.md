1) .bruin.yml and pipeline/ with pipeline.yml and assets/

  A valid Bruin project requires:

.bruin.yml → global project configuration

pipeline/

pipeline.yml → defines the pipeline

assets/ → contains SQL or Python assets

2) time_interval - incremental based on a time column

That is exactly how time_interval works:

It uses a time column (e.g., pickup_datetime)

Deletes data within the interval

Inserts refreshed data for that same interval

3) bruin run --var 'taxi_types=["yellow"]'

The variable is defined as an array:
taxi_types:
  type: array
  items:
    type: string
  default: ["yellow", "green"]

  Since it's an array, you must pass a valid JSON-style array from the CLI:

  bruin run --var 'taxi_types=["yellow"]


4) bruin run --select ingestion.trips+
  The + symbol means: Run this asset AND all downstream dependencies.

5) name: not_null

If you want to ensure a column never contains NULL values, the correct check is: name: not_null

6)The question says:

visualize the dependency graph between assets

The command that generates the DAG (Directed Acyclic Graph) is: bruin graph

7) --create: ensure tables are created from scratch
