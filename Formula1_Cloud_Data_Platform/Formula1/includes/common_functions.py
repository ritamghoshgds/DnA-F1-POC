# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def ingestiondate(inputdf):
    outputdf=inputdf.withColumn("ingestion_date",current_timestamp())
    return outputdf

# COMMAND ----------

def merge_delta_data(db_name,table_name,folder_path,merge_condition,partition_column,src_df):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    from delta.tables import DeltaTable
    if spark.catalog.tableExists(f"{db_name}.{table_name}"):
        Target = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")
        Target.alias('tgt').merge(src_df.alias('src'),merge_condition)\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        src_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")