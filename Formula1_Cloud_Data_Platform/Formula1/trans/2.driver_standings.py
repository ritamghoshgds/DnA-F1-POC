# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
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
race_results=race_results.groupBy("race_year","driver_name").agg(sum("points").alias("total_points"),sum("isWin").alias("wins"),max("driver_nationality").alias("Nationality")).orderBy(race_results.race_year.desc())

# COMMAND ----------

from pyspark.sql.window import Window
partition=Window.partitionBy("race_year").orderBy(race_results.total_points.desc())

# COMMAND ----------

from pyspark.sql.functions import rank
driver_standings=race_results.withColumn("rank",rank().over(partition)).orderBy(race_results.race_year.desc())

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data("f1_presentation","driver_standings",presentation_path,merge_condition,"race_year",driver_standings)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;

# COMMAND ----------

