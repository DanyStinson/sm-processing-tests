import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(f'Using Spark Version: {spark.version}')

california_housing_from_delta_df = spark.read.format("delta").load("/opt/ml/processing/input/")
print('Fetched training dataset from Delta Lake')
california_housing_from_delta_df.show()