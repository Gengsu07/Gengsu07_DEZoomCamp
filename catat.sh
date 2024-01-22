python ingest_data.py \
    --user=gengsu07\
    --password=sgwi2341\
    --host=localhost\
    --port=5433\
    --db=ny_taxi\
    --table=ingest_taxi_data\
    --url=https://raw.githubusercontent.com/Gengsu07/Gengsu07_DEZoomCamp/main/1.containerization/dockerizing_ingest_script/nyc_taxi_green.csv

docker run -it --rm\
   --network=postgres_pgadmin_pg-network \
     ingest:latest \
     python ingest_data.py \
    --user=gengsu07 \
    --password=sgwi2341 \
    --host=postgres_pgadmin-postgres-database-1 \
    --port=5432 \
    --db=ny_taxi \
    --table=taxi_data \
    --url=https://raw.githubusercontent.com/Gengsu07/Gengsu07_DEZoomCamp/main/1.containerization/dockerizing_ingest_script/nyc_taxi_green.csv
