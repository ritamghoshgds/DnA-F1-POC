# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql("""CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA""")

# COMMAND ----------

spark.sql(f"""create or replace temp view race_results_src
as
select ra.race_year,
       c.name AS team_name,
       d.name AS driver_name,
       d.driver_id,
       re.race_id,
       re.position,
       re.points,
       11- re.position as calculated_points
 from f1_processed.results re inner join f1_processed.races ra on re.race_id=ra.race_id inner join f1_processed.drivers d on re.driver_id=d.driver_id inner join f1_processed.constructors c on re.constructor_id=c.constructor_id  
 where re.position<=10
 and re.file_date='{v_file_date}'
 """)

# COMMAND ----------

spark.sql("""MERGE INTO f1_presentation.calculated_race_results tgt
USING race_results_src upd
ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
WHEN MATCHED THEN
  UPDATE SET
    tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
WHEN NOT MATCHED
  THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date)
  VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
  """)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from race_results_src

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_presentation.calculated_race_results