### Question 1:

What is count of records for the 2022 Green Taxi Data??

- 65,623,481
- 840,402
- 1,936,423
- 253,647

```sql
SELECT
count(*)
FROM `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022`
```

![Query](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2Fc06bd9f5-2ef8-48bf-9526-3b3a4b89bbf5%2FUntitled.png?table=block&id=17054bbc-48a6-49f8-9825-2abc014466f3&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)

### Question 2:

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

```sql
--question 2
-- this for count the PULocationID from external table
SELECT
count(distinct PULocationID)
FROM `de-tf-411908.gengsu_dezoomcamp.external_greentaxi_2022`

-- this for count the PULocationID from BQ table
SELECT
count(distinct PULocationID)
FROM `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022`
```

### Question 3 :

How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622

![question 3](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2Fc07c8454-7c79-49f9-bf37-fc6db6f6d283%2FUntitled.png?table=block&id=16bf084f-65a1-49f4-a14e-25d461e8ee4f&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)

### Question 4:

What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

Like mention in the videos:

**Query that only filter based in single column should use partition but if we need more granularity and sorted the data we can use clustering. So the lpep_pickup_datetime will bw partitioned and PUlocationID will be clustered**

```sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022_partclust`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY `PULocationID` AS
SELECT * FROM `de-tf-411908.gengsu_dezoomcamp.greentaxi_2022`;
```

## Question 5:

Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values?

Choose the answer which most closely matches.

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

```sql
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
```

## Question 6:

Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

**the files are still in GCP bucket guys…no need to worry.**

## Question 7:

It is best practice in Big Query to always cluster your data:

- True
- False

**because it is depends in the use case and needs of the data. if the data is small less than 1GB better not adding cluster to the data because it will add overhead that maybe add complexity**
