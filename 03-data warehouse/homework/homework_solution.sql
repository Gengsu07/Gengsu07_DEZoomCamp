
-- this is for creating external table from parquet files in GCS bucket
CREATE OR REPLACE EXTERNAL TABLE `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gengsu07-parquet-dataset/greentaxi_2022/*.parquet']
);

