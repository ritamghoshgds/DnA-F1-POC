# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races=spark.read.format("delta").load(f"{processed_path}/races").withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date")
display(races)

# COMMAND ----------

circuits=spark.read.format("delta").load(f"{processed_path}/circuits").withColumnRenamed("location", "circuit_location") 
display(circuits)

# COMMAND ----------

drivers=spark.read.format("delta").load(f"{processed_path}/drivers").withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 
display(drivers)

# COMMAND ----------

constructors=spark.read.format("delta").load(f"{processed_path}/constructors").withColumnRenamed("name", "team") 
display(constructors)

# COMMAND ----------

results=spark.read.format("delta").load(f"{processed_path}/results").filter(f"file_date = '{v_file_date}'").withColumnRenamed("time", "race_time") 
display(results)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
race_results=races.join(circuits,races.circuit_id==circuits.circuit_id,"inner").join(results,races.race_id==results.race_id,"inner").join(drivers,drivers.driver_id==results.driver_id,"inner").join(constructors,results.constructor_id==constructors.constructor_id).select(races.race_id,races.race_year,races.race_name,races.race_date,circuits.circuit_location,drivers.driver_name,drivers.driver_number,drivers.driver_nationality,results.grid,results.fastest_lap,results.race_time,results.points,results.position,constructors.team,results.file_date).withColumn("created_date",current_timestamp())

# COMMAND ----------

# display(race_results.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(race_results.points.desc()))

# COMMAND ----------

# from pyspark.sql.functions import col
# display(race_results.filter(col("race_year") ==2020).filter( col("race_name")=="Abu Dhabi Grand Prix").orderBy(race_results.points.desc()))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data('f1_presentation', 'race_results', presentation_path, merge_condition, 'race_id',race_results)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id,count(1) FROM f1_presentation.race_results group by race_id order by race_id desc;

# COMMAND ----------

