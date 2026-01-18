#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm


def run():
    # 1) Config Postgres
    pg_user = "root"
    pg_pass = "root"
    pg_host = "localhost"
    pg_port = 5432
    pg_db = "ny_taxi"

    # 2) Qué archivo cargar
    year = 2021
    month = 1
    chunksize = 100_000
    target_table = "yellow_taxi_data"

    # 3) URL del dataset (flexible con year/month)
    prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
    url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    # 4) Tipos y columnas fecha (para que Postgres quede bien)
    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64",
    }

    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # 5) Engine (conector) a Postgres
    engine = create_engine(
        f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    )

    # 6) Test conexión
    with engine.connect() as conn:
        print("DB check:", conn.execute(text("SELECT 1")).fetchone())

    # 7) Leer CSV por chunks (iterador)
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize,
    )

    # 8) Crear tabla (solo esquema) con el primer chunk y luego ir insertando
    first = True
    for df_chunk in tqdm(df_iter, desc="Loading chunks"):
        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False,
            )
            first = False

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",     # más rápido
            chunksize=10_000,   # batches dentro de cada chunk
        )

    # 9) Validación final
    count_df = pd.read_sql(f"SELECT COUNT(*) AS rows FROM {target_table}", engine)
    print(count_df)


if __name__ == "__main__":
    run()
