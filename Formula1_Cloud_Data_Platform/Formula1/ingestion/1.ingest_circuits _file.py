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

circuitsdf=spark.read.csv(f"{raw_path}/{v_file_date}/circuits.csv",header=True,inferSchema=True)
display(circuitsdf)

# COMMAND ----------

circuitsdf.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
circuit_schema=StructType([StructField("circuitId",IntegerType(),False),
                           StructField("circuitRef",StringType(),True),
                           StructField("name",StringType(),True),
                           StructField("location",StringType(),True),
                           StructField("country",StringType(),True),
                           StructField("lat",DoubleType(),True),
                           StructField("lng",DoubleType(),True),
                           StructField("alt",IntegerType(),True),
                           StructField("url",StringType(),True)])

# COMMAND ----------

circuits_df=spark.read.option("header",True).schema(circuit_schema).csv(f"{raw_path}/{v_file_date}/circuits.csv")
display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
circuits_df=circuits_df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").withColumnRenamed("lat","latitude").withColumnRenamed("lng","longitude").withColumnRenamed("alt","altitude").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date)).drop("url")
circuits_df=ingestiondate(circuits_df)
display(circuits_df)

# COMMAND ----------

circuits_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_path}/circuits/"))

# COMMAND ----------

