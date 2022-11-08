import pyspark, os, argparse
from delta import *

pkg_list = []
pkg_list.append("io.delta:delta-core_2.12:2.0.1")
pkg_list.append("io.delta:delta-storage:2.0.1")
packages=(",".join(pkg_list))


parser = argparse.ArgumentParser(description="app inputs and outputs")
parser.add_argument("--s3_input_bucket", type=str, help="s3 input bucket")
args = parser.parse_args()

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print(f'Using Spark Version: {spark.version}')
print(DeltaTable.isDeltaTable(spark, "packages/tests/streaming/data"))
california_housing_from_delta_df = spark.read.format("delta").load(args.s3_input_bucket)
print('Fetched training dataset from Delta Lake')
california_housing_from_delta_df.show()