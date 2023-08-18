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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df=spark.read.schema(qualifying_schema).option("multiLine",True).json(f"{raw_path}/{v_file_date}/qualifying")
display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
qualifying=qualifying_df.withColumnRenamed("qualifyId","qualify_id").withColumnRenamed("raceId","race_id").withColumnRenamed("driverId","driver_id").withColumnRenamed("constructorId","constructor_id").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
qualifying=ingestiondate(qualifying)

# COMMAND ----------

merge_condition="tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data("f1_processed","qualifying",processed_path,merge_condition,'race_id',qualifying)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying

# COMMAND ----------

