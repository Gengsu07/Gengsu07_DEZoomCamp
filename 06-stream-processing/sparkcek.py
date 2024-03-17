from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

df = spark.read.option("header", "true").csv(
    r"D:\AI-ML\Data Engineer\DEZoomcamp\gengsu-playground\Gengsu07_DEZoomCamp\05-batch\code\fhvhv_tripdata_2021-01.csv"
)
print(df.printSchema())
