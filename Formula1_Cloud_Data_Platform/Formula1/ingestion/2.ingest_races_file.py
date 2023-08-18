# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
dbutils.widgets.text("p_file_date","2021-03-21")
v_data_source = dbutils.widgets.get('p_data_source')
v_file_date= dbutils.widgets.get('p_file_date')

# COMMAND ----------

races=spark.read.option("header",True).option("inferSchema",True).csv(f"{raw_path}/{v_file_date}/races.csv")
display(races)

# COMMAND ----------

from pyspark.sql.types import *
schema=StructType([StructField("raceId",IntegerType()),
                   StructField("year",IntegerType()),
                   StructField("round",IntegerType()),
                   StructField("circuitId",IntegerType()),
                   StructField("name",StringType()),
                   StructField("date",DateType()),
                   StructField("time",StringType()),
                   StructField("url",StringType())])
races_df=spark.read.option("header",True).schema(schema).csv(f"{raw_path}/{v_file_date}/races.csv")
display(races_df)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import date_format,concat,col,current_timestamp,to_timestamp,lit
races_df=races_df.drop("url").withColumn("race_timestamp",to_timestamp(concat("date",lit(" "),"time"),"yyyy-MM-dd HH:mm:ss")).withColumnRenamed("raceId","race_id").withColumnRenamed("year","race_year").withColumnRenamed("circuitId","circuit_id").drop("date").drop("time").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
races_df=ingestiondate(races_df)
display(races_df)

# COMMAND ----------

# DBTITLE 1,Write Races file with partition on Race year
races_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_path}/races/"))

# COMMAND ----------

