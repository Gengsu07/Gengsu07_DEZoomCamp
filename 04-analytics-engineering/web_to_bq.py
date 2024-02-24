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
