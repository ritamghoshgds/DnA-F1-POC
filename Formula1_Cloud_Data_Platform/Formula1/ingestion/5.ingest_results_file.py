# Databricks notebook source
dbutils.widgets.text("p_data_source","")
dbutils.widgets.text("p_file_date","2021-03-21")
v_data_source = dbutils.widgets.get('p_data_source')
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_df=spark.read.option("inferSchema",True).option("header",True).json(f"{raw_path}/{v_file_date}/results.json")
display(results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit
results=results_df.drop("statusId").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("resultId","result_id").withColumnRenamed("raceId","race_id").withColumnRenamed("positionText","position_text").withColumnRenamed("positionOrder","position_order").withColumnRenamed("fastestLap","fastest_lap").withColumnRenamed("fastestLapSpeed","fastest_lap_speed").withColumnRenamed("fastestLapTime","fastest_lap_time").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
results=ingestiondate(results)
display(results)

# COMMAND ----------

results=results.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
from delta.tables import DeltaTable
if spark.catalog.tableExists("f1_processed.results"):
    Target = DeltaTable.forPath(spark, f"{processed_path}/results")
    Target.alias('tgt').merge(results.alias('src'),'tgt.result_id=src.result_id and tgt.race_id=src.race_id')\
        .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
else:
    results.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

