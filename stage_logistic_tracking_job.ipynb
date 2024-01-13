from pyspark.sql import SparkSession
from delta.tables import *
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("DataIngestion").getOrCreate()

# Placeholder for service account JSON key path
service_account_path = "/path/to/your/service/account/key.json"

# GCS bucket details
bucket_name = "your_gcs_bucket_name"
data_directory = f"gs://{bucket_name}/input/"
archive_directory = f"gs://{bucket_name}/archive/"

# Placeholder for the staging Delta table path
staging_table_path = "dbfs:/path/to/staging_order_tracking"

# Read all CSV files from the specified GCS directory
df = spark.read.csv(data_directory, inferSchema=True, header=True)

df.show()

print("Create Stage Table")
# Check if the Delta table exists
if DeltaTable.isDeltaTable(spark, staging_table_path):
    # If table exists, overwrite it
    df.write.format("delta").mode("overwrite").save(staging_table_path)
else:
    # If not, create the table
    df.write.format("delta").mode("append").save(staging_table_path)

print("Data Inserted In Stage Table")

# Create a table in the Hive Metastore
spark.sql(f"CREATE TABLE IF NOT EXISTS staging_order_tracking USING DELTA LOCATION '{staging_table_path}'")

# List and move files individually
file_list = dbutils.fs.ls(data_directory)
for file in file_list:
    if file.name.endswith(".csv"):
        print(f"{file} Moved in archive folder")
        dbutils.fs.mv(file.path, os.path.join(archive_directory, file.name))
