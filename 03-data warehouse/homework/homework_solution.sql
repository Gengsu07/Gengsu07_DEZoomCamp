-- preparation
-- this is for creating external table from parquet files in GCS bucket
CREATE OR REPLACE EXTERNAL TABLE `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://gengsu07-parquet-dataset/greentaxi_2022/*.parquet']
);
-- this is for creating BQ table from external table
CREATE OR REPLACE TABLE `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022` AS
SELECT * FROM `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022`;


-- question 1
SELECT  
count(*)
FROM `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022`

--question 2
-- this for count the PULocationID from external table
SELECT  
count(distinct PULocationID)
FROM `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022` 

-- this for count the PULocationID from BQ table
SELECT  
count(distinct PULocationID)
FROM `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022` 


--question 3
records that have fare_amaount=0
select
count(*)
FROM `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022` 
where fare_amount=0;

--question 4
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022_partclust`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY `PULocationID` AS
SELECT * FROM `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022`;

--question 5
--read from BQ table with no partition and cluster
select
distinct PULocationID
from `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022` 
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30'

--read from BQ table with partition and cluster
select
distinct PULocationID
from `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022_partclust` 
where lpep_pickup_datetime between '2022-06-01' and '2022-06-30'