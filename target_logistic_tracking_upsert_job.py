from pyspark.sql import SparkSession
from delta.tables import *

# Initialize Spark Session
spark = SparkSession.builder.appName("UpsertIntoTargetDeltaTable").getOrCreate()

# Placeholder for staging and target Delta table paths
staging_table_path = "dbfs:/path/to/staging_order_tracking"
target_table_path = "dbfs:/path/to/target_order_tracking"

# Read from the staging Delta table
staging_df = spark.read.format("delta").load(staging_table_path)
staging_df.show()
print("Data read from staging table completed")

# Check if the target Delta table exists, create it if not
if not DeltaTable.isDeltaTable(spark, target_table_path):
    staging_df.write.format("delta").save(target_table_path)

# Create DeltaTable object for the target table
target_delta_table = DeltaTable.forPath(spark, target_table_path)

# Perform upsert from staging to target table using tracking_num as key
target_delta_table.alias("target").merge(
    staging_df.alias("staging"),
    "target.tracking_num = staging.tracking_num"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

print("Data upserted in target table")

# Register the target table in the Hive Metastore (Optional)
spark.sql(f"CREATE TABLE IF NOT EXISTS target_order_tracking USING DELTA LOCATION '{target_table_path}'")
