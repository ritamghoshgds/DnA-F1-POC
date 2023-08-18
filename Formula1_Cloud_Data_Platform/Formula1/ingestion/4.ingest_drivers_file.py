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

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema=StructType([StructField("driverId",IntegerType()),
                   StructField("driverRef",StringType()),
                   StructField("number",IntegerType()),
                   StructField("code",StringType()),
                   StructField("name",StructType([StructField("forename",StringType()),
                    StructField("surname",StringType())])),
                   StructField("dob",DateType()),
                   StructField("nationality",StringType()),
                   StructField("url",StringType()),
                   ])

# COMMAND ----------

driversdf=spark.read.schema(schema).json(f"{raw_path}/{v_file_date}/drivers.json")
display(driversdf)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat
drivers=driversdf.drop("url").withColumnRenamed("driverId","driver_id").withColumnRenamed("driverRef","driver_ref").withColumn("name",concat("name.forename",lit(" "),"name.surname")).withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
drivers=ingestiondate(drivers)
display(drivers)

# COMMAND ----------

drivers.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

