python ingest_data.py \
    --user=gengsu07\
    --password=Gengsu!sh3r3\
    --host=localhost\
    --port=5433\
    --db=ny_taxi\
    --table=ingest_taxi_data\
    --url=https://raw.githubusercontent.com/Gengsu07/Gengsu07_DEZoomCamp/main/1.containerization/dockerizing_ingest_script/nyc_taxi_green.csv

docker run -it \
   --network=postgres_pgadmin_pg-network \
     ingest_data:latest \ 
     python ingest_data.py \
    --user=gengsu07 \
    --password=sgwi2341 \
    --host=172.23.0.3 \
    --port=5432 \
    --db=ny_taxi \
    --table=taxi_data \
    --url=https://raw.githubusercontent.com/Gengsu07/Gengsu07_DEZoomCamp/main/1.containerization/dockerizing_ingest_script/nyc_taxi_green.csv