### Answer

Question

### 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

![Untitled](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2F77440374-3eac-40cb-9b45-0de43bd9cddc%2FUntitled.png?table=block&id=95057345-6291-4804-a03c-4572561b7d91&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)

266,855 rows x 20 columns

- code for data loader

  ```python
  import io
  import pandas as pd
  import requests
  if 'data_loader' not in globals():
      from mage_ai.data_preparation.decorators import data_loader
  if 'test' not in globals():
      from mage_ai.data_preparation.decorators import test

  @data_loader
  def load_data_from_api(*args, **kwargs):
      """
      Template for loading data from API
      """
      urls = ['https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
      'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
      'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz']

      parse_dates = ['lpep_pickup_datetime','lpep_dropoff_datetime']
      taxi_dtypes = {'VendorID':pd.Int64Dtype(),'passenger_count':pd.Int64Dtype(), 'RatecodeID':pd.Int64Dtype() ,
      'store_and_fwd_flag':'str',
      'RatecodeID':'str','PULocationID':pd.Int64Dtype(), 'DOLocationID':pd.Int64Dtype(),'payment_type':'str'}

      df = pd.DataFrame()
      for url in urls:
          df_temp = pd.read_csv(url, compression='gzip',parse_dates=parse_dates, dtype=taxi_dtypes)
          df = pd.concat([df, df_temp],axis=0, ignore_index=True)
      print(f'shape of the data loaded:{df.shape}')
      return df

  @test
  def test_output(output, *args) -> None:
      """
      Template code for testing the output of the block.
      """
      assert output is not None, 'The output is undefined'
  ```

- **_Code For Transformation Question 2-5_**

  ```python
  if 'transformer' not in globals():
      from mage_ai.data_preparation.decorators import transformer
  if 'test' not in globals():
      from mage_ai.data_preparation.decorators import test

  @transformer
  def transform(data, *args, **kwargs):
      """
      Template code for a transformer block.

      Add more parameters to this function if this block has multiple parent blocks.
      There should be one parameter for each output variable from each parent block.

      Args:
          data: The output from the upstream parent block
          args: The output from any additional upstream blocks (if applicable)

      Returns:
          Anything (e.g. data frame, dictionary, array, int, str, etc.)
      """
      # Specify your transformation logic here

      data = data[(data['passenger_count']>0)&(data['trip_distance']>0)]
      print(f'rows left after filtering passenger count > 0 and the trip distance > zero:{data.shape[0]}')
      data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
      print(f'unique vendor id: {data["VendorID"].unique().tolist()}')

      col_awal = data.columns
      data.columns = data.columns.str.lower().str.replace(' ','-')
      data.columns = [col.lower().replace('id','_id') if 'id' in col else col.lower().replace(' ','_') for col in data.columns ]
      colrenamed = sum(1 for col in col_awal if col not in data.columns )

      print(f'{colrenamed} columns renamed')
      return data

  @test
  def test_output(output, *args) -> None:
      """
      Template code for testing the output of the block.
      """
      assert 'vendor_id' in output.columns, 'vendor_id is in columns'
      assert output[output['passenger_count']<=0].shape[0]==0, 'passenger_count greater than 0'
      assert output[output['trip_distance']<=0].shape[0]==0, 'trip distance greater than 0'
  ```

### 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 *and* the trip distance is greater than zero, how many rows are left?

![Untitled](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2F67bc2981-b27a-4fc5-ba58-121fe6684e96%2FUntitled.png?table=block&id=b25852a5-46b6-4abc-bf94-fe091365317d&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)

139,370 rows

### 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

`data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`

### 4. Data Transformation

What are the existing values of `VendorID` in the dataset?
![](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2F67bc2981-b27a-4fc5-ba58-121fe6684e96%2FUntitled.png?table=block&id=b25852a5-46b6-4abc-bf94-fe091365317d&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)

1 or 2

### 5. Data Transformation

How many columns need to be renamed to snake case?
![](https://www.notion.so/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2F7079325e-a6cc-4427-b5ad-762f0ab6efec%2F67bc2981-b27a-4fc5-ba58-121fe6684e96%2FUntitled.png?table=block&id=b25852a5-46b6-4abc-bf94-fe091365317d&spaceId=7079325e-a6cc-4427-b5ad-762f0ab6efec&width=2000&userId=9b8717a6-3ff4-4b97-9fce-297c73702f49&cache=v2)

4 Columns

## 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

95

- code

  ```python
  from mage_ai.settings.repo import get_repo_path
  from mage_ai.io.config import ConfigFileLoader
  from mage_ai.io.google_cloud_storage import GoogleCloudStorage
  from pandas import DataFrame
  import os
  import pyarrow as pa
  import pyarrow.parquet as pq

  if 'data_exporter' not in globals():
      from mage_ai.data_preparation.decorators import data_exporter

  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'xxxxxxxx-sensored'
  bucket_name = 'gengsu07-parquet-dataset'
  project_id = 'xxxx-sensored'
  table_name = 'greentaxi'
  root_path = f'{bucket_name}/{table_name}'

  @data_exporter
  def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
      """
      Template for exporting data to a Google Cloud Storage bucket.
      Specify your configuration settings in 'io_config.yaml'.

      Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
      """
      table = pa.Table.from_pandas(df)
      gcs = pa.fs.GcsFileSystem()
      pq.write_to_dataset(
          table=table,
          root_path = root_path,
          partition_cols =['lpep_pickup_date'],
          filesystem=gcs
      )
      print(f'Jumlah partisi:{df["lpep_pickup_date"].nunique()}')
  ```
