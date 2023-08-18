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

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pitstops_df=spark.read.schema(pit_stops_schema).option("multiLine",True).json(f"{raw_path}/{v_file_date}/pit_stops.json")
display(pitstops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,lit
pitstops=pitstops_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
pitstops=ingestiondate(pitstops)

# COMMAND ----------

merge_condition="tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"

# COMMAND ----------

merge_delta_data("f1_processed","pit_stops",processed_path,merge_condition,'race_id',pitstops)

# COMMAND ----------

