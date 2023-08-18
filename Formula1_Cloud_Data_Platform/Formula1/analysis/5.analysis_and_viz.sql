-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

create or replace temp view alltime_dominant_driver
as
select driver_name,sum(calculated_points) as total_points,
        count(*) as total_races,
        avg(calculated_points) as Avg_points,
        rank() over(order by avg(calculated_points) desc) as driver_rank
        from f1_presentation.calculated_race_results
        group by driver_name
        having total_races>=50
        order by Avg_points desc


-- COMMAND ----------

create or replace temp view lastdecade_dominant_driver
as
select driver_name,sum(calculated_points) as total_points,
        count(*) as total_races,
        avg(calculated_points) as Avg_points,
        rank() over(order by avg(calculated_points) desc) as driver_rank
        from f1_presentation.calculated_race_results
        where race_year between 2010 and 2020
        group by driver_name
        having total_races>=50
        order by Avg_points desc

-- COMMAND ----------

create or replace temp view alltime_dominant_team
as
select team_name,sum(points) as total_points,
count(*) as total_races,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as team_rank from f1_presentation.calculated_race_results
group by team_name
having total_races>=100
order by avg_points desc


-- COMMAND ----------

create or replace temp view lastdecade_dominant_team
as
select team_name,sum(points) as total_points,
count(*) as total_races,
avg(calculated_points) as avg_points,
rank() over(order by avg(calculated_points) desc) as team_rank from f1_presentation.calculated_race_results
where race_year between 2010 and 2020
group by team_name
having total_races>=100
order by avg_points desc


-- COMMAND ----------

select * from alltime_dominant_driver

-- COMMAND ----------

select * from lastdecade_dominant_driver

-- COMMAND ----------

select * from alltime_dominant_team

-- COMMAND ----------

select * from lastdecade_dominant_team

-- COMMAND ----------

SELECT race_year, 
       driver_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE driver_name IN (select driver_name from alltime_dominant_driver where driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC

-- COMMAND ----------

SELECT race_year, 
       team_name,
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
 WHERE team_name IN (SELECT team_name FROM alltime_dominant_team WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC