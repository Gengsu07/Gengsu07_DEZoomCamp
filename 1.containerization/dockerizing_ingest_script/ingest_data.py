import argparse
import os
import time

import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres DB")


def ingest_data(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    file = params.url
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    csv_name = "output.csv"
    os.system(f"wget {file}  -O {csv_name}")
    chunksize = 100000
    df_iter = pd.read_csv(
        csv_name,
        chunksize=chunksize,
        parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"],
    )

    df = next(df_iter)
    # df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    # df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    # print(pd.io.sql.get_schema(df, name="green_taxi_data", con=engine))
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.head(0).to_sql(name=f"{table}", con=engine, if_exists="replace")
    counter = 0

    df.to_sql(name=f"{table}", con=engine, if_exists="append")
    counter += chunksize
    print(f"Insert {counter} chunks")

    try:
        while True:
            mulai = time.time()

            df = next(df_iter)

            rows = df.shape[0]

            # df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])

            # df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

            df.to_sql(name=f"{table}", con=engine, if_exists="append")
            counter += rows

            selesai = time.time()

            print(f"Insert {rows} chunks in :{selesai - mulai} total{counter}")
    except StopIteration:
        print("Done")


if __name__ == "__main__":
    parser.add_argument("--user", default="gengsu07", help="Postgres username")
    parser.add_argument("--password", default="Gengsu!sh3r3", help="Postgres password")
    parser.add_argument("--host", default="127.0.0.1", help="Postgres host")
    parser.add_argument("--db", default="ny_taxi", help="Postgres database")
    parser.add_argument("--table", default="ny_taxi", help="Table name")
    parser.add_argument("--port", default="5432", help="Postgres port")
    parser.add_argument("--url", default="nyc_taxi_green.csv", help="filepath")
    args = parser.parse_args()
    ingest_data(args)
