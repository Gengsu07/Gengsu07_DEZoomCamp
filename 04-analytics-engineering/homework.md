Answer

1. **What happens when we execute dbt build --vars '{'is_test_run':'true'}'** You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video.

   - It's the same as running *dbt build*
   - It applies a *limit 100* to all of our models
   - It applies a *limit 100* only to our staging models
   - Nothing

   Because it is set that if test it should only apply in 100 records, if not test like in production then apply to all records. The default value is set to true to only use 100 records.

2. **What is the code that our CI job will run? Where is this code coming from?**

   - The code that has been merged into the main branch
   - The code that is behind the creation object on the dbt*cloud_pr* schema
   - The code from any development branch that has been opened based on main
   - The code from the development branch we are requesting to merge to main

   When our improvement from branch want to main/master it would invoke Pull Request. Then because DBT already connected to github then dbt automate testing the code to perform check

   ### PREPARATION

   to import data in Big Query we can use external table from GCS files but to make sure the data types like we wanted is difficult. So i’m use another option by creating BigQuery table directly from csv files using python script. In this case the csv are have been downloaded in my local directory.

   ```python
   from google.cloud import bigquery

   # Replace with your project ID, dataset ID, table name, and folder path

   init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
   # Replace with your project ID, dataset ID, table name, and folder path
   project_id = "de-tf-411908"
   dataset_id = "trips_data_all"
   table_name = "fhv_tripsdata"
   folder_path = "../"

   # Create a BigQuery client
   client = bigquery.Client(project=project_id)

   schema = [
       bigquery.SchemaField("dispatching_base_num", "STRING"),
       bigquery.SchemaField("pickup_datetime", "DATETIME"),  # Changed to DATETIME
       bigquery.SchemaField("dropOff_datetime", "DATETIME"),  # Changed to DATETIME
       bigquery.SchemaField("PUlocationID", "INT64"),  # Changed to INT64
       bigquery.SchemaField("DOlocationID", "INT64"),  # Changed to INT64
       bigquery.SchemaField(
           "SR_Flag", "INT64"
       ),  # Changed to INT64 (assuming integer values)
       bigquery.SchemaField("Affiliated_base_number", "STRING"),
   ]

   def web_to_bq(year, service):
       for i in range(12):

           # sets the month part of the file_name string
           month = "0" + str(i + 1)
           month = month[-2:]

           # csv file_name
           file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

           job_config = bigquery.LoadJobConfig(
               schema=schema,
               skip_leading_rows=1,
               source_format=bigquery.SourceFormat.CSV,
               field_delimiter=",",
           )
           with open(file_name, "rb") as f:
               load_job = client.load_table_from_file(
                   f,
                   destination=f"{dataset_id}.{table_name}",
                   job_config=job_config,
               )
           # Wait for the job to finish
           load_job.result()  # Raises an exception if the job fails
           print(f"Data from {file_name} loaded successfully")

   if __name__ == "__main__":
       web_to_bq(2019, "fhv")

   ```

Next we create staging for fhv_tripsdata table in DBT

- edit schema.yml in staging folder..minimal without docs and testing like this:
  ```sql
  version: 2

  sources:
    - name: staging
      database: de-tf-411908
      schema: trips_data_all

      tables:
        - name: green_tripdata
        - name: yellow_tripdata
        - name: fhv_tripsdata
  ```
- Add staging_fhv_tripsdata
  ```sql
  with

  source as (

      select * from {{ source('staging', 'fhv_tripsdata') }}

  ),

  renamed as (

      select
          dispatching_base_num,
          pickup_datetime,
          dropoff_datetime,
          pulocationid,
          dolocationid,
          sr_flag,
          affiliated_base_number

      from source
      where extract(year from pickup_datetime)=2019

  )

  select * from renamed

  -- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
  {% if var('is_test_run', default=true) %}

    limit 100

  {% endif %}
  ```
  Then, create model in core folder to combine with dim_zones data
  ```sql
  {{ config(materialized="table") }}

  with
      fhv_tripsdata as (select * from {{ ref("stg_fhv_tripsdata") }}),
      dim_zones as (select * from {{ ref('dim_zones') }} where borough != 'Unknown')
  select
      fhv_tripsdata.dispatching_base_num,
      fhv_tripsdata.pickup_datetime,
      fhv_tripsdata.dropoff_datetime,
      fhv_tripsdata.pulocationid,
      fhv_tripsdata.dolocationid,
      fhv_tripsdata.sr_flag,
      fhv_tripsdata.affiliated_base_number
  from fhv_tripsdata
  inner join
      dim_zones as pickup_zone on fhv_tripsdata.pulocationid = pickup_zone.locationid
  inner join
      dim_zones as dropoff_zone on fhv_tripsdata.dolocationid = dropoff_zone.locationid
  ```

1. **What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**

   Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019. Do not add a deduplication step. Run this models without limits (is_test_run: false).

   Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones. Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. Run the dbt model without limits (is_test_run: false).

   - 12998722
   - 22998722
   - 32998722
   - 42998722

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/7079325e-a6cc-4427-b5ad-762f0ab6efec/3982ba13-bb21-4302-9d48-86ab28dc5a17/Untitled.png)

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/7079325e-a6cc-4427-b5ad-762f0ab6efec/96ec2616-0eb2-4918-ba9f-21b508f5e4c6/Untitled.png)

1. **What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

   Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

   - FHV
   - Green
   - Yellow
   - FHV and Green

   after build fact_trips and fhv_trips wihout using limit os use var is test run equal false then we make dasboard or report to see rides by service type in july 2019. Personally i am using Power BI because it is the most easy tool i have in my computer.

   so after connect to Google Bigquery and make few transformation here the result:

   ![rides by service type July 2019](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2F7b1c2a20-5b1a-45d4-9ef0-cfdfe6874bc5%2FUntitled.png?table=block&id=31d106e9-73f8-4c9d-9759-51c23940d39d&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)
