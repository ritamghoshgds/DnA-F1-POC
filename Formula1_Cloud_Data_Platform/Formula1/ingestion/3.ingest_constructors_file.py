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

schema_ddl="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"
schema=StructType([StructField("constructorId",IntegerType()),
                  StructField("constructorRef",StringType()),
                  StructField("name",StringType()),
                  StructField("nationality",StringType()),
                  StructField("url",StringType())])


# COMMAND ----------

constructorsdf=spark.read.schema(schema).json(f"{raw_path}/{v_file_date}/constructors.json")
display(constructorsdf)

# COMMAND ----------

constructorsdf=constructorsdf.drop("url").withColumnRenamed("constructorId","constructor_id").withColumnRenamed("constructorRef","constructor_ref").withColumn("data_source", lit(v_data_source)).withColumn("file_date", lit(v_file_date))
constructorsdf=ingestiondate(constructorsdf)
display(constructorsdf)

# COMMAND ----------

constructorsdf.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_path}/constructors/"))

# COMMAND ----------

