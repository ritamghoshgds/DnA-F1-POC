# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results=spark.read.format("delta").load(f"{presentation_path}/race_results").filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,count,max
race_results=race_results.withColumn("isWin",when(col("position")==1,1).otherwise(0))

# COMMAND ----------

from pyspark.sql.functions import col,when,sum,count,max,desc
race_results=race_results.groupBy("race_year","team").agg(sum("points").alias("total_points"),sum("isWin").alias("wins")).orderBy(race_results.race_year.desc())

# COMMAND ----------

from pyspark.sql.window import Window
partition=Window.partitionBy("race_year").orderBy(race_results.total_points.desc())

# COMMAND ----------

from pyspark.sql.functions import rank
constructor_standings=race_results.withColumn("rank",rank().over(partition)).orderBy(race_results.race_year.desc())

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data("f1_presentation","constructor_standings",presentation_path,merge_condition,"race_year",constructor_standings)

# COMMAND ----------

v_file_date

# COMMAND ----------

