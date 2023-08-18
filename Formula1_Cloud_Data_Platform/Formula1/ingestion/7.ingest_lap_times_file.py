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

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

laptimes_df=spark.read.schema(lap_times_schema).csv(f"{raw_path}/{v_file_date}/lap_times")
display(laptimes_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
laptimes=laptimes_df.withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
laptimes=ingestiondate(laptimes)
display(laptimes)

# COMMAND ----------

merge_condition="tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data("f1_processed","lap_times",processed_path,merge_condition,'race_id',laptimes)

# COMMAND ----------

